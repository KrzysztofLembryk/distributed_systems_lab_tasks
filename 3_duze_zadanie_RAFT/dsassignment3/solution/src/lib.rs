use module_system::{Handler, ModuleRef, System, TimerHandle};
use other_raft_structs::{ServerType, ServerState, PersistentState};
use rand::distr::uniform::SampleRange;
use tokio::time::Duration;
use std::collections::HashMap;

pub use domain::*;
use crate::types_defs::{IndexT, ServerIdT, TermT};

use crate::other_raft_structs::VolatileState;

mod types_defs;
mod other_raft_structs;
mod domain;

#[derive(Clone)]
struct Timeout;

struct Init;

#[non_exhaustive]
pub struct Raft 
{
    // TODO you can add fields to this struct.
    config: ServerConfig,
    role: ServerType,
    state: ServerState,
    state_machine: Box<dyn StateMachine>,
    stable_storage: Box<dyn StableStorage>,
    msg_sender: Box<dyn RaftSender>,
    timer_handle: Option<TimerHandle>,
    self_ref: ModuleRef<Self>,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> 
    {
        let mut state_machine = state_machine;
        let persistent_state: PersistentState;

        if let Some(retrieved_state) = 
            stable_storage.get(&config.self_id.to_string()).await
        {
            persistent_state = decode_from_slice(&retrieved_state).unwrap();

            recover_state_machine(&persistent_state, &mut state_machine).await;
        }
        else
        {
            persistent_state = PersistentState::new(config.servers.clone());
        }

        let _ = persistent_state.log.len().checked_sub(1).expect("
        Raft::new():: checked_sub panicked, thus persistent_state.log.len() is 0, this should never happen, stable_storage must have had corrupted data inside");
        
        let volatile_state: VolatileState = 
            VolatileState::new(&config.servers);

        let state = ServerState { 
            persistent: persistent_state, 
            volatile: volatile_state
        };

        let self_ref = system
            .register_module(|self_ref| Raft {
                config,
                role: ServerType::Follower,
                state,
                state_machine,
                stable_storage,
                msg_sender: message_sender,
                timer_handle: None,
                self_ref,
            })
            .await;

        self_ref.send(Init).await;

        return self_ref;
    }

    async fn broadcast_append_entries(&mut self) 
    {
        let header = RaftMessageHeader {
            source: self.config.self_id,
            term: self.state.persistent.current_term
        };

        let mut msgs: HashMap<ServerIdT, RaftMessage> = HashMap::new();
        let append_entries_batch_size = self.config.append_entries_batch_size;

        for server_id in &self.config.servers
        {
            let next_index = *self.state.volatile.next_index
                .get(server_id)
                .expect(&format!("Raft::broadcast_append_entries:: for server '{}' we don't have value in volatile.next_index map", server_id));
            let match_index = *self.state.volatile.match_index
                .get(server_id)
                .expect(&format!("Raft::broadcast_append_entries:: for server '{}' we don't have value in volatile.match_index map", server_id));

            let prev_log_index: IndexT = next_index
                    .checked_sub(1)
                    .expect("Raft::broadcast_append_entries:: when calculating prev_log_index, next_index.checked_sub(1) failed");
            let prev_log_term: TermT = self.state.persistent.log
                    .get(prev_log_index)
                    .expect(
                    &format!("Raft::broadcast_append_entries:: there is no log in state.persistent.log for prev_log_index: {}", prev_log_index))
                    .term;

            let mut entries: Vec<LogEntry> = Vec::new();

            // - If next_index == match_index + 1 we know that all log entries from 
            //  0 to match_index are replicated on server, so we want send to it 
            //  all new entries starting from nex_index.
            // - But if next_index == log.len() this means that there are no new 
            //  entries, so we need to send empty entries vector
            if next_index == match_index + 1
            && next_index < self.state.persistent.log.len() 
            {
                let mut n_entries_appended: usize = 0;

                // we send as many log entries as we are allowed to 
                for log_entry in &self.state.persistent.log[next_index..]
                {
                    if n_entries_appended >= append_entries_batch_size
                    {
                        break;
                    }

                    entries.push(log_entry.clone());
                    n_entries_appended += 1;
                }

            }

            let content = RaftMessageContent::AppendEntries(
                AppendEntriesArgs { 
                    prev_log_index, 
                    prev_log_term, 
                    entries, 
                    leader_commit: self.state.volatile.commit_index
                }
            );

            let append_entries_msg = RaftMessage {header: header.clone(), content};

            msgs.insert(*server_id, append_entries_msg);
        }
        self.broadcast(msgs).await;
    }

    async fn broadcast(&mut self, mut msgs: HashMap<ServerIdT, RaftMessage>)
    {
        for server_id in &self.config.servers
        {
            let msg = msgs.remove(server_id).expect("Raft::broadcast:: msgs.remove() returned None, it means that there wasn't a msg for given server, this should never happen, since we should've created an AppendEntries msg for every server");
            self.msg_sender.send(server_id, msg).await;
        }
    }


    async fn reset_timer(&mut self, interval: Duration) 
    {
        if let Some(handle) = self.timer_handle.take() 
        {
            handle.stop().await;
        }

        self.timer_handle = 
            Some(self.self_ref.request_tick(Timeout, interval).await);
    }
}

#[async_trait::async_trait]
impl Handler<Init> for Raft 
{
    async fn handle(&mut self, _msg: Init) 
    {
        let election_timeout_range = self.config.election_timeout_range.clone();
        let rand_election_timeout = 
            election_timeout_range.sample_single(&mut rand::rng()).unwrap();

        self.reset_timer(rand_election_timeout).await;
    }
}

#[async_trait::async_trait]
impl Handler<Timeout> for Raft 
{
    async fn handle(&mut self, _: Timeout) 
    {
        todo!("Implement Timeout handler");
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) 
    {
        todo!()
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft 
{
    async fn handle(&mut self, msg: ClientRequest) 
    {
        todo!()
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.

async fn recover_state_machine(
    persistent_state: &PersistentState, 
    state_machine: &mut Box<dyn StateMachine>
)
{
    // Since state machine is volatile it must be recovered after restart by
    // reapplying log entries (after applying latest snapshot)
    for cmd in &persistent_state.log
    {
        match &cmd.content
        {
            LogEntryContent::Command { data, .. } => {
                let _ = state_machine.apply(data).await;
            },
            _ => {
                // we apply only Command Log to our state machine
            }
        }
    }
}