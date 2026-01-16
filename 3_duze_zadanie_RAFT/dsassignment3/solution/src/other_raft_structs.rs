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

impl ServerState
{
    pub fn clear_reply_channels(&mut self)
    {
        self.volatile.leader_state.reply_channels.clear();
    }
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
    SessionExpired,
    NoClientSession,
    AlreadyDiscarded,
    WrongLowestSeqNumVal,
    CmdResultAlreadyPresent(Vec<u8>),
    CanBeApplied,
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

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
    // TODO: client_session should be moved from LeaderState to just VolatileState
    pub client_session: HashMap<ClientIdT, ClientSession>,
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 

    // Only leader should have reply_channels, but all servers have session
    // When we get request from client we also need to store channel to which we 
    // respond, and index of this command in our log
    pub reply_channels: HashMap<
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
            reply_channels: HashMap::new(),
        };
    }

    pub fn insert_reply_channel(
        &mut self, 
        cmd_index: IndexT, 
        reply_to: UnboundedSender<ClientRequestResponse>
    )
    {
        // cmd_indexes are UNIQUE, so we safely insert new reply_to
        self.reply_channels.insert(cmd_index, reply_to);
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
            ClientSession::new(
                client_last_activity,
                lowest_sequence_num_without_response
            ),
        );
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
        // We must clear reply_channels both here and WHEN STEPPING DOWN AS LEADER
        // since if we don't do that someone who is NOT A LEADER may send msg to 
        // client, we don't want that!!!!!
        self.reply_channels.clear();
    }

    pub fn check_if_cmd_can_be_applied(
        &mut self,
        cmd_seq_num: SequenceNumT,
        cmd_lowest_seq_num_without_resp: u64,
        client_id: ClientIdT,
        cmd_entry_timestamp: Timestamp,
        session_expiration: Duration
    ) -> ClientCmdState
    {
        if let Some(c_session) = self.client_session.get_mut(&client_id)
        {
            // If there is something wrong with the command or client's sesssion
            // has expired we DO NOT PROCESS this command and do not update our 
            // session variables with values from this command.
            if session_expired(
                cmd_entry_timestamp, 
                c_session.last_activity, 
                session_expiration
            )
            {
                self.client_session.remove(&client_id);
                return ClientCmdState::SessionExpired;
            }
            if c_session.is_already_discarded(&cmd_seq_num)
            {
                return ClientCmdState::AlreadyDiscarded;
            }
            if cmd_lowest_seq_num_without_resp > cmd_seq_num
            {
                return ClientCmdState::WrongLowestSeqNumVal;
            }

            if c_session.is_cmd_result_present(&cmd_seq_num)
            {
                return ClientCmdState::CmdResultAlreadyPresent(
                    c_session.get_cmd_result(cmd_seq_num).clone()
                );
            }
            return ClientCmdState::CanBeApplied;
        }
        else
        {
            return ClientCmdState::NoClientSession;
        }
    }

    pub fn update_client_last_activity(
        &mut self,
        client_id: ClientIdT,
        cmd_entry_timestamp: Timestamp,
    )
    {
        if let Some(c_session) = self.client_session.get_mut(&client_id)
        {
            c_session.last_activity = cmd_entry_timestamp;
        }
        else
        {
            panic!("VolatileLeaderState::update_client_last_activity:: there is no session for client: {} ", client_id);
        }
    }

    pub fn update_client_lowest_seq_num_without_resp(
        &mut self,
        client_id: ClientIdT,
        cmd_lowest_seq_num_without_resp: u64,
    )
    {
        if let Some(c_session) = self.client_session.get_mut(&client_id)
        {
            c_session.lowest_sequence_num_without_response = 
                std::cmp::max(
                    cmd_lowest_seq_num_without_resp, 
                    c_session.lowest_sequence_num_without_response
            );
            self.discard_all_saved_entries_with_too_low_seq_num(client_id);
        }
        else
        {
            panic!("VolatileLeaderState::update_client_lowest_seq_num_without_resp:: there is no session for client: {} ", client_id);
        }
    }

    /// This function must be used only after using check_if_cmd_can_be_committed.
    /// <br> Updates client's session **last_activity, 
    /// lowest_seq_num_without_response** and saves **statemachine result**
    pub fn update_client_session(
        &mut self,
        client_id: ClientIdT,
        cmd_seq_num: SequenceNumT,
        cmd_lowest_seq_num_without_resp: u64,
        cmd_entry_timestamp: Timestamp,
        res_data: &Vec<u8>
    )
    {
        if let Some(c_session) = self.client_session.get_mut(&client_id)
        {
            c_session.last_activity = cmd_entry_timestamp;
            // Client may send us msg with smaller lowest_seq_num than we already 
            // have because of: 
            //      1) Client is malicious/bugged 
            //      2) Request reordering in the Internet etc.
            // Therefore we don't want to end his session or anything, we just set
            // our lowest_seq_num to the max out of both
            c_session.lowest_sequence_num_without_response = 
                std::cmp::max(
                    cmd_lowest_seq_num_without_resp, 
                    c_session.lowest_sequence_num_without_response
            );
            c_session.responses.insert(cmd_seq_num, res_data.clone());

            // We need to discard all saved responses with seq_num smaller than
            // just updated lowest_sequence_num_without_response
            self.discard_all_saved_entries_with_too_low_seq_num(client_id);
        }
        else
        {
            panic!("VolatileLeaderState::commit_cmd:: there is no session for client: {} ", client_id);
        }
    }

    fn discard_all_saved_entries_with_too_low_seq_num(
        &mut self,
        client_id: ClientIdT,
    )
    {
        if let Some(c_session) = self.client_session.get_mut(&client_id)
        {
            let lowest_seq_without_resp = 
                c_session.lowest_sequence_num_without_response;
            let mut seq_num_to_discard: Vec<u64> = Vec::new();

            for seq_num in c_session.responses.keys()
            {
                if *seq_num < lowest_seq_without_resp
                {
                    seq_num_to_discard.push(*seq_num);
                }
            }

            for seq_num in seq_num_to_discard
            {
                c_session.responses.remove(&seq_num);
            }
        }
    }

    /// Once we reply to cmd we remove reply_to channel from our state
    pub fn reply_to_client(
        &mut self,
        command_index: IndexT,
        response: ClientRequestResponse
    )
    {
        if let Some(reply_to) = self.reply_channels.remove(&command_index)
        {
            let _ = reply_to.send(response); 
        }
        // Otherwise we do nothing, if leader doesn't have reply_to channel it must 
        // mean that leader has changed, and new leader has just committed old 
        // leader's entry, and new leader doesn't know client's channels.
        // Or if we are not a leader at all we don't reply
    }

    pub fn find_and_expire_sessions(
        &mut self, 
        curr_timestamp: Timestamp, 
        session_expiration: Duration
    )
    {
        let mut sessions_to_expire: Vec<Uuid> = Vec::new();

        for (client_id, c_session) in self.client_session.iter()
        {
            if session_expired(
                curr_timestamp, 
                c_session.last_activity, 
                session_expiration
            )
            {
                sessions_to_expire.push(*client_id);
            }
        }

        for client_id in sessions_to_expire
        {
            self.client_session.remove(&client_id);
        }
    }
}

#[derive(Clone)]
pub enum ServerType {
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader,
}

fn session_expired(
    pending_entry_timestamp: Timestamp,
    last_activity: Timestamp,
    session_expiration: Duration
) -> bool
{
    // A session’s last activity time is the timestamp of the most recently 
    // committed log entry with this session’s client identifier. 
    // When committing a log entry, you should expire sessions before you update 
    // the last activity time.
    if pending_entry_timestamp.abs_diff(last_activity) > session_expiration
    {
        // Between last committed entry and currently committed entry elapsed
        // more time than allowed session_expiration time, thus session has expired 
        // and we need remove this client's session from our server
        return true;
    }
    return false;
}
