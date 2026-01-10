use module_system::{Handler, ModuleRef, System, TimerHandle};
use other_raft_structs::{ServerType, ServerState, PersistentState};
use rand::distr::uniform::SampleRange;
use tokio::time::Duration;
use std::collections::HashMap;
use log::{debug, warn};

pub use domain::*;
use crate::types_defs::{IndexT, ServerIdT, TermT};

use crate::other_raft_structs::VolatileState;

mod types_defs;
mod other_raft_structs;
mod domain;

#[derive(Clone)]
struct ElectionTimeout;

#[derive(Clone)]
struct HeartbeatTimeout;

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
    election_timer_handle: Option<TimerHandle>,
    heartbeat_timer_handle: Option<TimerHandle>,
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
                election_timer_handle: None,
                heartbeat_timer_handle: None,
                self_ref,
            })
            .await;

        self_ref.send(Init).await;

        return self_ref;
    }

    async fn stop_heartbeat_timer(&mut self)
    {
        if let Some(handle) = self.heartbeat_timer_handle.take() 
        {
            handle.stop().await;
        }
    }

    async fn reset_heartbeat_timer(&mut self) 
    {
        if let Some(handle) = self.heartbeat_timer_handle.take() 
        {
            handle.stop().await;
        }

        self.heartbeat_timer_handle = 
            Some(self.self_ref.request_tick(HeartbeatTimeout, self.config.heartbeat_timeout).await);
    }

    async fn reset_election_timer(&mut self)
    {
        let election_timeout_range = self.config.election_timeout_range.clone();
        let rand_election_timeout = 
            election_timeout_range.sample_single(&mut rand::rng()).unwrap();

        if let Some(handle) = self.election_timer_handle.take() 
        {
            handle.stop().await;
        }

        self.election_timer_handle = 
            Some(self.self_ref.request_tick(ElectionTimeout, rand_election_timeout).await);
    }

    async fn broadcast_append_entries(&mut self) 
    {
        let header = RaftMessageHeader {
            source: self.config.self_id,
            term: self.state.persistent.current_term
        };

        let mut msgs: HashMap<ServerIdT, RaftMessage> = HashMap::new();

        for server_id in &self.config.servers
        {
            // We don't want to send msgs to ourselves
            if *server_id == self.config.self_id
            {
                continue;
            }

            let args = 
                self.create_append_entries_args_for_given_server(server_id);
            let append_entries_msg = RaftMessage {
                header: header.clone(), 
                content: RaftMessageContent::AppendEntries(args)
            };

            msgs.insert(*server_id, append_entries_msg);
        }
        self.broadcast(msgs).await;
    }

    fn create_append_entries_args_for_given_server(
        &self, 
        server_id: &ServerIdT
    ) -> AppendEntriesArgs
    {
        let next_index = *self.state.volatile.leader_state.next_index
            .get(server_id)
            .expect(&format!("Raft::broadcast_append_entries:: for server '{}' we don't have value in volatile.next_index map", server_id));
        let match_index = *self.state.volatile.leader_state.match_index
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

        let append_entries_batch_size = self.config.append_entries_batch_size;
        let mut entries: Vec<LogEntry> = Vec::new();

        // - If next_index == match_index + 1 we know that all log entries from 
        //  0 to match_index are replicated on server, so we want send to it 
        //  all new entries starting from nex_index.
        // - But if next_index == log.len() this means that there are no new 
        //  entries, so we need to send empty entries vector
        // !! This next_index < self.state.persistent.log.len() check is VITAL !!
        // When new leader is elected it appends a NoOp and sets next_index to 
        // NoOp's index.
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

        return AppendEntriesArgs { 
                prev_log_index, 
                prev_log_term, 
                entries, 
                leader_commit: self.state.volatile.commit_index
            };
    }

    async fn broadcast(&mut self, mut msgs: HashMap<ServerIdT, RaftMessage>)
    {
        for server_id in &self.config.servers
        {
            // We don't send msgs to ourselves
            if self.config.self_id != *server_id
            {
                let msg = msgs.remove(server_id).expect("Raft::broadcast:: msgs.remove() returned None, it means that there wasn't a msg for given server, this should never happen, since we should've created an AppendEntries msg for every server");
                self.msg_sender.send(server_id, msg).await;
            }
        }
    }

    async fn save_to_stable_storage(&mut self)
    {
        // TODO: maybe add better handling, or msgs instead of unwraps
        self.stable_storage.put(
            &self.config.self_id.to_string(), 
            &encode_to_vec(&self.state.persistent).unwrap()
        ).await.unwrap();
    }

    /// Set the process's term to the higher number.
    async fn update_term(&mut self, new_term: u64) 
    {
        assert!(new_term > self.state.persistent.current_term);
        self.state.persistent.current_term = new_term;
        self.state.persistent.voted_for = None;
        self.state.volatile.leader_id = None;

        // Since we change persistent state, we save it to stable storage
        self.save_to_stable_storage().await;
    }

    async fn check_for_higher_term(&mut self, msg: &RaftMessage) 
    {
        if msg.header.term > self.state.persistent.current_term 
        {
            // If we were a leader we now revert to Follower so we need to stop our
            // hearbeat timer, if we are not Leader stopping heartbeat_timer 
            // does nothing
            self.role = ServerType::Follower;
            self.stop_heartbeat_timer().await;
            self.update_term(msg.header.term).await;
        }
    }

    fn create_append_entries_response_msg(
        &self,
        success: bool,
        args: &AppendEntriesArgs, 
    ) -> RaftMessage
    {
        let response_header = RaftMessageHeader {
            source: self.config.self_id,
            term: self.state.persistent.current_term
        };

        // See domain.rs AppendEntriesResponseArgs struct
        let last_verified_log_index = args.prev_log_index + args.entries.len();

        let response_args = AppendEntriesResponseArgs {
            success,
            last_verified_log_index
        };

        return RaftMessage {
            header: response_header,
            content: RaftMessageContent::AppendEntriesResponse(response_args)
        };
    }

    /// Function applies all LogEntryContent::Command from state.persistent.log 
    /// to StateMachine, by incrementing last_applied index
    /// as long as last_applied < commit_index
    async fn apply_commited_cmds_to_state_machine(&mut self)
    {
        let commit_index = self.state.volatile.commit_index;

        while commit_index > self.state.volatile.last_applied
        {
            self.state.volatile.last_applied += 1;

            if let Some(log_entry) = 
                self.state.persistent.log.get(self.state.volatile.last_applied)
            {
                match &log_entry.content
                {
                    LogEntryContent::Command { data, .. } => {
                        self.state_machine.apply(data).await;
                    },
                    // We skip all logs apart from Command logs, since only these 
                    // we want to apply to our StateMachine - I think
                    LogEntryContent::NoOp => {

                    },
                    LogEntryContent::Configuration { .. } => {

                    },
                    LogEntryContent::RegisterClient => {

                    }
                }
            }
            else
            {
                panic!("Raft::apply_next_cmd_to_state_machine - there is no log entry for last_applied: '{}', however CommitIdx: '{}' > last_applied", self.state.volatile.last_applied, commit_index);
            }
        }
    }

    // #############################################################################
    // ########################## MSG HANDLE FUNCTIONS #############################
    // #############################################################################
    async fn handle_append_entries(
        &mut self, 
        args: &AppendEntriesArgs, 
        header: &RaftMessageHeader
    )
    {
        let leader_term = header.term;
        let leader_id = header.source;
        let response_msg;

        if leader_term >= self.state.persistent.current_term
        {
            self.state.volatile.leader_id = Some(leader_id);

            match &self.role
            {
                ServerType::Follower => {
                    self.reset_election_timer().await;
                },
                ServerType::Candidate { .. } => {
                    // If we are Candidate and got AppendEntries from Leader that has
                    // 'term >= our_term' we revert back to Follower
                    self.reset_election_timer().await;
                    self.role = ServerType::Follower;
                },
                ServerType::Leader => {
                    // Before handling any msg we do check_for_higher_term(), so if 
                    // we have old term, we revert to FOLLOWER, Leader does not send 
                    // msgs to itself and from RAFT we know that there is only one 
                    // leader for given term a time, thus this branch should be 
                    // NEVER invoked
                    panic!("Raft::handle_append_entries:: Server which is Leader handles AppendEntries msg, this should never happen, source: {}, term: {}", header.source, header.term);
                }
            }

            // Now we know that we got msg from leader, with >= term than ours, so 
            // we need to check if we have entry at prevLogIndex
            // 1) If we don't have entry at prevLogIndex we reply FALSE
            // 2) If we have entry at prevLogIndex but not with prevLogTerm we reply 
            //    FALSE
            // 3) We have entry at prevLogIndex with prevLogTerm, this means that 
            // leader has finally found the latest log entry where logs agree, so 
            // now we delete all log entries after this agree point and append all
            // new entries from args.entries that Leader has sent us

            let prev_log_index = args.prev_log_index;
            let prev_log_term = args.prev_log_term;

            if let Some(prev_entry) = self.state.persistent.log.get(prev_log_index)
            {
                if prev_entry.term == prev_log_term
                {
                    // We want to keep only logs from 0 to prev_log_index
                    // but truncate takes len, so there are prev_log_index + 1 elems
                    // from 0 to prev_log_index
                    self.state.persistent.log.truncate(prev_log_index + 1);
                    // we append all entries
                    self.state.persistent.log.extend_from_slice(&args.entries);

                    // We changed persistent state so immediately afterwards we want 
                    // to save these changes to stable storage
                    self.save_to_stable_storage().await;

                    let leader_commit_index: usize = args.leader_commit;
                    // In persistent state we always have at least one log, which is
                    // config log that we add when creating RAFT server
                    let last_new_entry_idx: usize = 
                        self.state.persistent.log
                            .len()
                            .checked_sub(1)
                            .expect("Raft::handle_append_entries:: last_new_entry_idx = state.persistent.log.len - 1 returned error, it means that len was 0, this shouldn't happen");

                    self.state.volatile.commit_index = 
                        std::cmp::min(leader_commit_index, last_new_entry_idx);

                    response_msg = 
                        self.create_append_entries_response_msg(true, args);
                }
                else
                {
                    // 2) We have entry at prevLogIdx but not with prevLogTerm
                    response_msg = 
                        self.create_append_entries_response_msg(false, args);
                }
            }
            else
            {
                // 1) We don't have entry at prevLogIndex
                response_msg =
                    self.create_append_entries_response_msg(false, args);
            }
        }
        else
        {
            // No matter what type of server we are, if AppendEntries has obsolete
            // term we reply with false, and not reset election_timeout
            response_msg = self.create_append_entries_response_msg(false, args);
        }
        self.msg_sender.send(&leader_id, response_msg).await;
    }

    async fn handle_append_entries_response(
        &mut self, 
        args: &AppendEntriesResponseArgs, 
        header: &RaftMessageHeader
    )
    {
        match self.role 
        {
            ServerType::Follower => {
                debug!("FOLLOWER got AppendEntriesResponse - it probably used to be a LEADER and stopped being one before receiving all responses, IGNORING this msg");
            },
            ServerType::Candidate { .. } => {
                debug!("CANDIDATE got AppendEntriesResponse - it probably used to be a LEADER and stopped being one before receiving all responses, IGNORING this msg");
            },
            ServerType::Leader => {
                if !self.config.servers.contains(&header.source)
                {
                    warn!("Raft::handle_append_entries_response:: Leader got response from server: '{}' which is not present in config.servers, IGNORING", header.source);
                }
                else if self.config.self_id == header.source
                {
                    panic!("Raft::handle_append_entries_response:: Leader got response from server with source_id: '{}' which is LEADER's id,
                    this should never happen, or sb is sending malicious msgs, we cannot operate further like that, PANIC", header.source);
                }
                else 
                {
                    // If we got AppendEntriesResponse and we are still a leader it
                    // means that our AppendEntries msg was received by given server
                    // and that it reset its timer, so we can add him to our set
                    // that counts how many servers responded to our heartbeat.
                    // Even if in current round of Heartbeats some server responds
                    // several times, we count it ONLY ONCE.
                    self.state.volatile
                        .leader_state
                        .responses_from_followers.insert(header.source);

                    let response_header = RaftMessageHeader {
                        source: self.config.self_id,
                        term: self.state.persistent.current_term
                    };
                    let response_args: AppendEntriesArgs;

                    if args.success
                    {
                        // We got SUCCESS - so we need to update matchIndex and also
                        // nextIndex
                        let match_index = 
                            self.state.volatile.leader_state.match_index
                                .get_mut(&header.source)
                                .unwrap();
                        // last_verified_log_index is prev_log_index + entries.len()
                        // so it's an index of highest log entry replicated on this 
                        // server, thus we set our match_index to it
                        *match_index = args.last_verified_log_index;

                        let next_index = 
                            self.state.volatile.leader_state.next_index
                                .get_mut(&header.source)
                                .unwrap();
                        // Since last_verified_log_index is highest index of entry
                        // replicated on Follower server, next entry we want to send
                        // to the server is last_verified_log_index + 1
                        *next_index = args.last_verified_log_index + 1;

                        response_args = self.    
                            create_append_entries_args_for_given_server(
                                &header.source
                        );
                        if response_args.entries.is_empty()
                        {
                            // If args.entries are empty, this means that 
                            // next_index == state.persistent.log.len(), so there are
                            // no new entries to send so we end here
                            return;
                        }
                        // otherwise we have some entries to send, so we want to send
                        // them immediately
                    }
                    else
                    {
                        // If response is not successful it means that we didn't
                        // have a match at prev_log_index, thus we need to decrement
                        // nextIdx for this server.
                        let next_index = self.state.volatile.leader_state.next_index
                                .get_mut(&header.source)
                                .unwrap();
                        *next_index = next_index
                            .checked_sub(1)
                            .expect("Raft::handle_append_entries_response:: Response was FALSE, and in next_index.checked_sub(1) we got error, next_index must've been 0.");

                        // And we need to send next AppendEntries immediately.
                        // FALSE response means that we as LEADER will have some log 
                        // entries to send to a follower, and from task we know that 
                        // if we have entries to send, we should send them 
                        // immediately 
                        response_args = self.create_append_entries_args_for_given_server(
                            &header.source
                        );
                    }

                    let response_content = 
                        RaftMessageContent::AppendEntries(response_args);

                    self.msg_sender.send(
                        &header.source, 
                        RaftMessage { 
                            header: response_header, 
                            content: response_content
                        }
                    ).await;
                }
            },
        };
    }
}

#[async_trait::async_trait]
impl Handler<Init> for Raft 
{
    async fn handle(&mut self, _msg: Init) 
    {
        self.reset_election_timer().await;
    }
}

#[async_trait::async_trait]
impl Handler<ElectionTimeout> for Raft 
{
    async fn handle(&mut self, _: ElectionTimeout) 
    {
        todo!("Implement Timeout handler");
    }
}

#[async_trait::async_trait]
impl Handler<HeartbeatTimeout> for Raft 
{
    async fn handle(&mut self, _: HeartbeatTimeout) 
    {
        match self.role
        {
            ServerType::Leader => {
                // We include ourselves when checking for majority
                let heartbeat_response_count = &mut self.state.volatile.leader_state
                    .responses_from_followers;

                heartbeat_response_count.insert(self.config.self_id);

                if heartbeat_response_count.len() > self.config.servers.len() / 2
                {
                    // When leader has ElectionTimeout, if 
                    // successful_heartbeat_round_happened is FALSE leader reverts to
                    // Follower, if it isn't we set it to FALSE to prepare for next
                    // ElectionTimeout
                    self.state.volatile.leader_state.successful_heartbeat_round_happened = true;
                }

                // We clear responses we got before next Heartbeat round
                heartbeat_response_count.clear();

                // We send heartbeat to all servers
                self.broadcast_append_entries().await;
            },
            _ => {
                // If we are not a LEADER, this means we got an old Heartbeat, 
                // HeartbeatTimeout should be already STOPPED, but we still have a 
                // HeartbeatTimeout msg in our channel queue, so we ignore it. 
                // We might go here when:
                // 1) In check_for_higher_term new Heartbeat was sent before 
                //  stopping HeartBeat timer or just after stopping it and then 
                //  after handling current msg in channel queue we still 
                //  have a HeartbeatTimeout msg - but we are a FOLLOWER
                // 2) When Leader had an election timeout without successful 
                //  Heartbeat round, so he reverted back to FOLLOWER - so he stopped
                //  HeartbeatTimer - but while handling ElectionTimeout another 
                //  HeartbeatTimeout was sent
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft 
{
    async fn handle(&mut self, msg: RaftMessage) 
    {
        // Reset the term and become a follower if we're outdated:
        // Probably we need to change that to accommodate new changes
        self.check_for_higher_term(&msg).await;

        let header = &msg.header;

        match msg.content
        {
            RaftMessageContent::AppendEntries(args) => {
                self.handle_append_entries(&args, header).await;
            },
            RaftMessageContent::AppendEntriesResponse(response_args) => {
                self.handle_append_entries_response(&response_args, header).await;
            },
            RaftMessageContent::RequestVote(args) => {

            },
            RaftMessageContent::RequestVoteResponse(response_args) => {

            },
            RaftMessageContent::InstallSnapshot(args) => {

            },
            RaftMessageContent::InstallSnapshotResponse(response_args) => {

            }
        }

        // After handling given msg and responding, if there are log entries to apply
        // and we haven't applied them yet we do it now, but only if we know they are
        // COMMITTED
        self.apply_commited_cmds_to_state_machine().await;
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