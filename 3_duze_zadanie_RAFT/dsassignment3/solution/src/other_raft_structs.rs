use serde::{Serialize, Deserialize};
use std::{collections::HashSet};
use uuid::Uuid;

use crate::{LogEntry, 
    types_defs::{IndexT, ServerIdT, TermT}
};

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
    pub fn new() -> PersistentState
    {
        return PersistentState { 
            current_term: 0, 
            voted_for: None, 
            log:  Vec::new()
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

    // -----------------------------------------------------------------------------
    // LEADER volatile state, it's ALWAYS REINITIALIZED after election
    // -----------------------------------------------------------------------------
    // for each server it stores index of the NEXT LOG netry to send to that server,
    // (initialized to leader last log index + 1)
    pub next_index: Vec<IndexT>,
    // for each server, index of highest log entry known to be replicated on server
    // (initialized to 0, increases monotically)
    pub match_index: Vec<IndexT>
}

impl VolatileState
{
    pub fn new() -> VolatileState
    {
        return VolatileState { 
            commit_index: 0, 
            last_applied: 0, 
            next_index: Vec::new(), 
            match_index:  Vec::new()
        };
    }
}

pub enum ServerType {
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader,
}