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

    pub client_session: HashMap<ClientIdT, ClientSessionData>
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
            client_session: HashMap::new()
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
    }

    pub fn insert_
}

pub struct ClientSessionData
{
    pub reply_to: UnboundedSender<ClientRequestResponse>,
    pub executed_cmds: HashMap<SequenceNumT, (IndexT, ClientRequestInfo)>,
    pub pending_cmds: HashMap<IndexT, SequenceNumT>,
}

pub enum ClientRequestInfo {
    /// Apply a command to the state machine.
    Command {
        result: Vec<u8>,
        client_id: Uuid,
        sequence_num: u64,
        lowest_sequence_num_without_response: u64,
    },
    /// Create a snapshot of the current state of the state machine.
    Snapshot,
    /// Add a server to the cluster.
    AddServer,
    /// Remove a server from the cluster.
    RemoveServer,
    /// Open a new client session.
    RegisterClient,
}
pub enum ServerType {
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader,
}