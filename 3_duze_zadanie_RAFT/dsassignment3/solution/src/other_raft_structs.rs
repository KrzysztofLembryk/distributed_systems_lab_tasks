use serde::{Serialize, Deserialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;
use std::{collections::{HashSet, HashMap}};
use uuid::Uuid;

use crate::domain::{Timestamp, LogEntryContent, LogEntry, ClientRequestResponse};
use crate::types_defs::{IndexT, ServerIdT, TermT, ClientIdT, SequenceNumT};

pub struct ServerState
{
    pub persistent: PersistentState,
    pub volatile: VolatileState
}

#[derive(Serialize, Deserialize)]
pub struct PersistentState
{
    // latest term server has seen (initialized to 0 on first boot, 
    // increases monotically)
    pub current_term: TermT,
    // candidate id taht received vote in current term (None if hasn't voted, leader 
    // candidate always votes for itself)
    pub voted_for: Option<ServerIdT>,
    // log entries, first index is 1
    pub log: Vec<LogEntry>
}

impl PersistentState
{
    pub fn new(config_servers: HashSet<Uuid>) -> PersistentState
    {
        let initial_log: LogEntry = LogEntry { 
            content: LogEntryContent::Configuration { servers: config_servers }, 
            term: 0, 
            timestamp: Timestamp::ZERO 
        };

        return PersistentState { 
            current_term: 0, 
            voted_for: None, 
            log:  vec![initial_log]
        };
    }
}

pub struct VolatileState
{
    // index of highest log entry known to be committed (initialized to 0, 
    // increases monotically)
    pub commit_index: IndexT,
    // index of highest log entry applied to state machine (initialized to 0, 
    // increases monotically)
    pub last_applied: IndexT,

    // So that we can redirect clients to current leader
    pub leader_id: Option<ServerIdT>,

    // So that we can reject RequestVote received within the minimum election timeout
    // meaning request that arrived right after we got Leader's heartbeat, so we want
    // to wait Minimal election timeout from election_timeout_range before accepting
    // VoteRequests
    pub last_hearing_from_leader_timer: Option<Instant>,

    // -----------------------------------------------------------------------------
    //      LEADER volatile state, it's ALWAYS REINITIALIZED after election
    // -----------------------------------------------------------------------------
    pub leader_state: VolatileLeaderState,
}

impl VolatileState
{
    pub fn new(
        server_ids: &HashSet<ServerIdT>
    ) -> VolatileState
    {
        return VolatileState { 
            commit_index: 0, 
            last_applied: 0, 
            leader_id: None,
            last_hearing_from_leader_timer: None,
            leader_state: VolatileLeaderState::new(server_ids) 
        };
    }
}

pub enum ClientCmdState
{
    NotPresent,
    Pending,
    Executed,
}

pub struct VolatileLeaderState
{
    pub successful_heartbeat_round_happened: bool,
    pub responses_from_followers: HashSet<ServerIdT>,
    // for each server it stores index of the NEXT LOG entry to send to that server,
    // (initialized to leader last log index + 1)
    pub next_index: HashMap<ServerIdT, IndexT>,
    // for each server, index of highest log entry known to be replicated on server
    // (initialized to 0, increases monotically)
    pub match_index: HashMap<ServerIdT, IndexT>,

    pub client_session: HashMap<ClientIdT, ClientSessionData>,
    pub index_to_other_cmd: HashMap<
        IndexT, 
        (UnboundedSender<ClientRequestResponse>, OtherCommandResult)
    >,
}

impl VolatileLeaderState
{
    pub fn new(
        server_ids: &HashSet<ServerIdT>
    ) -> VolatileLeaderState
    {
        let mut next_index: HashMap<ServerIdT, IndexT> = HashMap::new();
        let mut match_index: HashMap<ServerIdT, IndexT> = HashMap::new();
        let responses_from_followers: HashSet<ServerIdT> = HashSet::new();

        for server_id in server_ids
        {
            // after election both of these hashmaps will be reinitialized, thus it 
            // doesn't matter what values we put here now, but we want all server_ids
            // to be present
            next_index.insert(*server_id, 0);
            match_index.insert(*server_id, 0);
        }

        return VolatileLeaderState { 
            successful_heartbeat_round_happened: false, 
            responses_from_followers, 
            next_index, 
            match_index,
            client_session: HashMap::new(),
            index_to_other_cmd: HashMap::new(),
        };
    }

    pub fn reinitialize(&mut self, 
        server_ids: &HashSet<ServerIdT>,
        next_index_val: IndexT
    )
    {
        self.successful_heartbeat_round_happened = false;
        self.responses_from_followers.clear(); 

        self.next_index.clear();
        self.match_index.clear();

        for server_id in server_ids
        {
            self.next_index.insert(*server_id, next_index_val);
            self.match_index.insert(*server_id, 0);
        }

        // TODO: Do we clear these?
        self.client_session.clear();
        self.index_to_other_cmd.clear();
    }

    pub fn state_of_cmd_with_sequence_num(
        &self,
        command_sequence_num: SequenceNumT,
        client_id: ClientIdT,
    ) -> ClientCmdState
    {
        if let Some(c_session) = self.client_session.get(&client_id)
        {
            let is_in_pending = 
                c_session.pending_cmds.contains(&command_sequence_num);
            let is_in_executed = 
                c_session.executed_cmds.contains_key(&command_sequence_num);

            if is_in_pending && is_in_executed
            {
                panic!(
                    "VolatileLeaderState::is_cmd_with_given_sequence_already_executed:: command with sequence number: '{}', for client: '{}' exists both in pending and in executed",command_sequence_num, client_id
                );
            }

            if is_in_pending
            {
                return ClientCmdState::Pending;
            }
            else if is_in_executed
            {
                return ClientCmdState::Executed;
            }
            else
            {
                return ClientCmdState::NotPresent;
            }
        }
        return ClientCmdState::NotPresent;
    }

    /// This function should be used only after checking the state of command with
    /// function: state_of_cmd_with_sequence_num
    pub fn get_executed_cmd(
        &self,
        command_sequence_num: SequenceNumT,
        client_id: ClientIdT,
    ) -> &StateMachineCommandResult
    {
        if let Some(c_session) = self.client_session.get(&client_id)
        {
            if let Some((_,res)) = c_session.executed_cmds.get(&command_sequence_num)
            {
                return res;
            }
            else
            {
                panic!("VolatileLeaderState::get_executed_cmd:: for client: '{}' there is no executed command with sequence number: '{}'", client_id, command_sequence_num);
            }
        }
        else
        {
            panic!("VolatileLeaderState::get_executed_cmd:: there is no session for client: '{}'", client_id);
        }
    }

    pub fn move_from_pending_to_executed(
        &mut self,
        command_sequence_num: SequenceNumT,
        client_id: ClientIdT,
        res_data: &Vec<u8>,
        lowest_sequence_num_without_response: u64,
    )
    {
        unimplemented!("Implement me");
    }

    pub fn insert_new_statemachine_pending_cmd(
        &mut self,
        client_id: ClientIdT,
        reply_to: UnboundedSender<ClientRequestResponse>,
        command_sequence_num: SequenceNumT
    )
    {
        if let Some(c_session) = self.client_session.get_mut(&client_id)
        {
            c_session.reply_to = reply_to;
            c_session.pending_cmds.insert(command_sequence_num);
        }
        else
        {
            self.client_session.insert(
                client_id, 
                ClientSessionData { 
                    reply_to, 
                    executed_cmds: HashMap::new(), 
                    pending_cmds: HashSet::from([command_sequence_num])
            });
        }
    }

    pub fn insert_new_other_cmd(
        &mut self,
        command_index: IndexT,
        cmd: OtherCommandResult,
        reply_to: UnboundedSender<ClientRequestResponse>,
    )
    {
        assert!(
            self.index_to_other_cmd.contains_key(&command_index), 
            "In VolatileLeaderState.index_to_other_cmd, index: '{}' already exists", command_index
        );

        self.index_to_other_cmd.insert(command_index, (reply_to, cmd));
    }
}

/// In client session we store only ClientRequest::Command results and their 
/// Sequence Numbers
pub struct ClientSessionData
{
    pub reply_to: UnboundedSender<ClientRequestResponse>,
    pub executed_cmds: HashMap<SequenceNumT, (IndexT, StateMachineCommandResult)>,
    pub pending_cmds: HashSet<SequenceNumT>,
}

pub struct StateMachineCommandResult
{
    pub result: Vec<u8>,
    pub client_id: Uuid,
    pub sequence_num: u64,
    pub lowest_sequence_num_without_response: u64,
}


pub enum OtherCommandResult {
    /// Create a snapshot of the current state of the state machine.
    Snapshot,
    /// Add a server to the cluster.
    AddServer { new_server: ServerIdT },
    /// Remove a server from the cluster.
    RemoveServer { old_server: ServerIdT },
    /// Open a new client session.
    RegisterClient { client_id: ClientIdT },
}

pub enum ServerType {
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader,
}