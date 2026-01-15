use serde::{Serialize, Deserialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Duration, Instant};
use std::{collections::{HashSet, HashMap}};
use uuid::Uuid;

use crate::domain::{Timestamp, LogEntryContent, LogEntry, ClientRequestResponse, ClientSession};
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
    NoClientSession,
    AlreadyDiscarded,
    NotPresent,
    Pending,
    Committed,
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
    // When we get request from client that is NOT COMMAND we also need to store 
    // channel to which we respond, and index of this command in our log
    pub index_to_other_cmd_reply: HashMap<
        IndexT, 
        UnboundedSender<ClientRequestResponse>
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
            index_to_other_cmd_reply: HashMap::new(),
        };
    }

    pub fn add_new_client_session(
        &mut self, 
        client_id: ClientIdT, 
        client_last_activity: Timestamp,
    )
    {
        let lowest_sequence_num_without_response = 0;

        self.client_session.insert(
            client_id, 
            ClientSessionData { 
                session: ClientSession::new(
                    client_last_activity,
                    lowest_sequence_num_without_response
                ),
                reply_to: HashMap::new(), 
                pending_cmds: HashSet::new()
        });
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
        // self.client_session.clear();
        // self.index_to_other_cmd_reply.clear();
    }

    pub fn state_of_cmd_with_sequence_num(
        &self,
        command_sequence_num: SequenceNumT,
        client_id: ClientIdT,
    ) -> ClientCmdState
    {
        if let Some(c_session) = self.client_session.get(&client_id)
        {
            let is_pending = 
                c_session.pending_cmds.contains(&command_sequence_num);
            let is_committed = 
                c_session.contains_committed_cmd(&command_sequence_num);
            let is_already_discarded = 
                c_session.check_if_cmd_already_discarded(&command_sequence_num);

            if is_pending && is_committed
            {
                panic!(
                    "VolatileLeaderState::is_cmd_with_given_sequence_already_executed:: command with sequence number: '{}', for client: '{}' exists both in pending and in committed",command_sequence_num, client_id
                );
            }

            if is_pending
            {
                return ClientCmdState::Pending;
            }
            else if is_committed
            {
                return ClientCmdState::Committed;
            }
            else if is_already_discarded
            {
                return ClientCmdState::AlreadyDiscarded;
            }
            else
            {
                return ClientCmdState::NotPresent;
            }
        }
        else
        {
            return ClientCmdState::NoClientSession;
        }
    }

    /// This function should be used only after checking the state of command with
    /// function: state_of_cmd_with_sequence_num
    pub fn get_committed_cmd_result(
        &self,
        command_sequence_num: SequenceNumT,
        client_id: ClientIdT,
    ) -> &Vec<u8>
    {
        if let Some(c_session) = self.client_session.get(&client_id)
        {
            if let Some(res) = c_session.session.responses.get(&command_sequence_num)
            {
                return res;
            }
            else
            {
                panic!("VolatileLeaderState::get_executed_cmd:: for client: '{}' there is no committed command with sequence number: '{}', this function shall be invoked only AFTER checking if given command is present and executed, by using: state_of_cmd_with_sequence_num", client_id, command_sequence_num);
            }
        }
        else
        {
            panic!("VolatileLeaderState::get_executed_cmd:: there is no session for client: '{}'. This function shall be invoked only AFTER checking if given command is present and executed by using: state_of_cmd_with_sequence_num", client_id);
        }
    }

    /// Function checks timestamps and session expiration, discards command results
    /// with seq_num < lowest_sequence_num_without_response
    pub fn move_from_pending_to_committed(
        &mut self,
        command_sequence_num: SequenceNumT,
        client_id: ClientIdT,
        res_data: &Vec<u8>,
        lowest_sequence_num_without_response: u64,
    )
    {
        // A session’s last activity time is the timestamp of the most recently 
        // committed log entry with this session’s client identifier. 
        // When committing a log entry, you should expire sessions before you update 
        // the last activity time.
        unimplemented!("Implement me");
    }

    pub fn append_new_reply_to_for_pending_cmd(
        &mut self,
        client_id: ClientIdT,
        reply_to: UnboundedSender<ClientRequestResponse>,
        command_sequence_num: SequenceNumT,
    )
    {
        if let Some(c_session) = self.client_session.get_mut(&client_id)
        {
            if let Some(replies_vec) = 
                c_session.reply_to.get_mut(&command_sequence_num)
            {
                replies_vec.push(reply_to);
            }     
            else
            {
                panic!("VolatileLeaderState::append_new_reply_to_for_pending_cmd:: there is no reply_to vector for client: '{}', but we want to append new reply_to for command: '{}'", client_id, command_sequence_num);
            }
        }
        else
        {
            panic!("VolatileLeaderState::append_new_reply_to_for_pending_cmd:: there is no session for client: '{}', but we want to append new reply_to for command: '{}'", client_id, command_sequence_num);
        }
    }

    pub fn insert_new_statemachine_pending_cmd(
        &mut self,
        client_id: ClientIdT,
        reply_to: UnboundedSender<ClientRequestResponse>,
        command_sequence_num: SequenceNumT,
    )
    {
        if let Some(c_session) = self.client_session.get_mut(&client_id)
        {
            c_session.reply_to.insert(command_sequence_num, vec![reply_to]);
            c_session.pending_cmds.insert(command_sequence_num);
        }
        else
        {
            panic!("VolatileLeaderState::insert_new_statemachine_pending_cmd:: there is no session for client: '{}', but we want to add pending cmd: '{}' for him", client_id, command_sequence_num);
        }
    }

    pub fn insert_new_other_cmd(
        &mut self,
        command_index: IndexT,
        reply_to: UnboundedSender<ClientRequestResponse>,
    )
    {
        assert!(
            self.index_to_other_cmd_reply.contains_key(&command_index), 
            "In VolatileLeaderState.index_to_other_cmd, index: '{}' already exists", command_index
        );

        self.index_to_other_cmd_reply.insert(command_index, reply_to);
    }

    /// Once we reply to other cmd we remove reply_to channel from our state
    pub fn reply_to_other_cmd(
        &mut self,
        command_index: IndexT,
        response: ClientRequestResponse
    )
    {
        if let Some(reply_to) = self.index_to_other_cmd_reply.remove(&command_index)
        {
            let _ = reply_to.send(response);
        }
        else
        {
            panic!("VolatileLeaderState::reply_to_other_cmd:: there is no command with idx: '{}' in index_to_other_cmd_reply", command_index);
        }
    }

}

/// In client session we store only ClientRequest::Command results and their 
/// Sequence Numbers
pub struct ClientSessionData
{
    // In session we store last activity timestamp, responses for give seqNum
    // and lowest_sequence_num_without_response.
    // We do not remove clientIds from session, once its there it stays there,
    // however if client sends msg with too old seqNum we return SESSION EXPIRED
    pub session: ClientSession,
    // For given SeqNum with client request we have a vector of channels we reply to
    // when command is committed. We need vector since while given command is pending
    // client may send many requests with the same sequence number, so we want to 
    // reply to all of them once entry is committed
    pub reply_to: HashMap<SequenceNumT, Vec<UnboundedSender<ClientRequestResponse>>>,
    // Pending, meaning not committed client requests so that we can easily check
    // for duplicate requests
    pub pending_cmds: HashSet<SequenceNumT>,
}

impl ClientSessionData
{
    pub fn contains_committed_cmd(&self, seq_num: &u64) -> bool
    {
        return self.session.responses.contains_key(&seq_num);
    }

    pub fn check_if_cmd_already_discarded(&self, seq_num: &u64) -> bool
    {
        // With each request, the client includes the lowest sequence
        // number for which it has not yet received a response, and the state 
        // machine then discards all responses for lower sequence numbers
        return *seq_num < self.session.lowest_sequence_num_without_response;
    }
}


pub enum OtherCommandData {
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