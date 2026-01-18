use module_system::{Handler, ModuleRef, System, TimerHandle};
use other_raft_structs::{ServerType, ServerState, PersistentState};
use uuid::Uuid;
use std::collections::{HashMap};
use log::{debug, warn, info};

pub use domain::*;
use crate::types_defs::{IndexT, ServerIdT, TermT};

use crate::other_raft_structs::*;

mod raft_impl;
mod types_defs;
mod other_raft_structs;
mod domain;

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
        debug!("Raft::new:: creating server: {}", config.self_id);
        let mut state_machine = state_machine;
        let volatile_state: VolatileState = 
            VolatileState::new(&config.servers);
        let persistent_state: PersistentState;

        if let Some(retrieved_state) = 
            stable_storage.get(&config.self_id.to_string()).await
        {
            persistent_state = decode_from_slice(&retrieved_state).unwrap();
            recover_state_machine(
                &persistent_state, 
                &mut state_machine,
                volatile_state.last_applied
            ).await;
        }
        else
        {
            persistent_state = PersistentState::new(config.servers.clone());
        }

        let _ = persistent_state.log.len().checked_sub(1).expect("
        Raft::new():: checked_sub panicked, thus persistent_state.log.len() is 0, this should never happen, stable_storage must have had corrupted data inside");
        

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

    async fn broadcast_request_vote(&mut self)
    {
        let header = RaftMessageHeader {
            source: self.config.self_id,
            term: self.state.persistent.current_term
        };

        let last_log_index = 
            self.state.persistent.log.len().checked_sub(1).unwrap();
        let last_log_term = 
            self.state.persistent.log.get(last_log_index).unwrap().term;

        let args = RequestVoteArgs {
            last_log_index,
            last_log_term
        };

        let mut msgs: HashMap<ServerIdT, RaftMessage> = HashMap::new();

        let request_vote_msg = RaftMessage { 
            header, 
            content: RaftMessageContent::RequestVote(args)
        };

        for server_id in &self.config.servers
        {
            // We don't want to send msgs to ourselves
            if *server_id == self.config.self_id
            {
                continue;
            }
            msgs.insert(*server_id, request_vote_msg.clone());
        }

        self.broadcast(&mut msgs).await;
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
        self.broadcast(&mut msgs).await;
    }

    fn create_append_entries_args_for_given_server(
        &self, 
        server_id: &ServerIdT
    ) -> AppendEntriesArgs
    {
        let next_index = *self.state.volatile.leader_state.next_index
            .get(server_id)
            .expect(&format!("Raft::create_append_entries_args_for_given_server:: for server '{}' we don't have value in volatile.next_index map", server_id));
        let match_index = *self.state.volatile.leader_state.match_index
            .get(server_id)
            .expect(&format!("Raft::create_append_entries_args_for_given_server:: for server '{}' we don't have value in volatile.match_index map", server_id));

        let prev_log_index: IndexT = next_index
                .checked_sub(1)
                .expect("Raft::create_append_entries_args_for_given_server:: when calculating prev_log_index, next_index.checked_sub(1) failed");
        let prev_log_term: TermT = self.state.persistent.log
                .get(prev_log_index)
                .expect(
                &format!("Raft::create_append_entries_args_for_given_server:: there is no log in state.persistent.log for prev_log_index: {}", prev_log_index))
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

    async fn broadcast(&mut self, msgs: &mut HashMap<ServerIdT, RaftMessage>)
    {
        for (server_id, msg) in msgs.drain()
        {
            // We don't send msgs to ourselves, and send msgs only to known servers
            if self.config.self_id != server_id 
            && self.config.servers.contains(&server_id)
            {
                self.msg_sender.send(&server_id, msg).await;
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

    async fn check_for_higher_term(&mut self, new_term: u64) 
    {
        if new_term > self.state.persistent.current_term 
        {
            // If we were a leader we now revert to Follower so we need to stop our
            // hearbeat timer, if we are not Leader stopping heartbeat_timer 
            // does nothing
            info!("Server: {} becomes a FOLLOWER after receiving msg with higher term", self.config.self_id);
            self.role = ServerType::Follower;
            // We must clear reply_channels when changing our role, since we don't 
            // want server that is not a leader to send msgs to clients.
            self.state.clear_reply_channels();
            self.stop_heartbeat_timer().await;
            self.update_term(new_term).await;
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

    fn update_commit_index_if_leader(&mut self)
    {
        match self.role
        {
            ServerType::Leader => {
                // We need to insert ourselves so that we can easily check if we 
                // need to update commitIndex. 
                let mut match_indexes = self.state.volatile.leader_state.match_index.clone();
                match_indexes.insert(
                    self.config.self_id, 
                    self.state.persistent.log.len() - 1
                );

                // If there is no new commit_index it returns old one
                let commit_index = find_new_commit_index(
                    self.state.volatile.commit_index, 
                    &match_indexes,
                    &self.state.persistent.log,
                    self.state.persistent.current_term
                );

                self.state.volatile.commit_index = commit_index;
            },
            _ => {return;}
        }
    }

    /// Function applies all LogEntryContent::Command from state.persistent.log 
    /// to StateMachine, by incrementing last_applied index
    /// as long as last_applied < commit_index
    async fn apply_commited_cmds_to_state_machine(&mut self)
    {
        let commit_index = self.state.volatile.commit_index;

        if commit_index > self.state.volatile.last_applied
        {
            debug!("Server: {} starts applying commands to state machine, commit_index: {}, last_applied: {}", self.config.self_id, commit_index, self.state.volatile.last_applied);
        }

        while commit_index > self.state.volatile.last_applied
        {
            self.state.volatile.last_applied += 1;
            let curr_cmd_index = self.state.volatile.last_applied;

            if let Some(log_entry) = 
                self.state.persistent.log.get(curr_cmd_index)
            {
                // Even if we get only malicious/wrong commands their TIMESTAMPS are
                // correct since created by server, so first thing we do is to 
                // find and expire all sessions if they need to be expired.
                // Otherwise we could have a client session than we would get only
                // wrong cmds and we would never expire this client's session
                self.state.volatile
                    .find_and_expire_sessions(
                        log_entry.timestamp, 
                        self.config.session_expiration
                );

                match &log_entry.content
                {
                    LogEntryContent::Command { 
                        data, 
                        client_id, 
                        sequence_num, 
                        lowest_sequence_num_without_response
                    } => {
                        match self.state.volatile.
                            check_if_cmd_can_be_applied(
                               *sequence_num, 
                               *lowest_sequence_num_without_response,
                               *client_id, 
                               log_entry.timestamp,
                               self.config.session_expiration
                        )
                        {
                            ClientCmdState::NoClientSession 
                            | ClientCmdState::SessionExpired => {
                                // Just reply with SessionExpired, we no longer have 
                                // session for this client
                                let args = CommandResponseArgs {
                                    client_id: *client_id,
                                    sequence_num: *sequence_num,
                                    content: CommandResponseContent::SessionExpired
                                };
                                let response = ClientRequestResponse
                                    ::CommandResponse(args);
                                // TODO: We can add here match that checks if we are 
                                // leader or pass our role to reply_to_client.
                                // This way we would ensure that no other server 
                                // replies to clients (we always clear reply_channels variable when leader is stepping down, but this way we would be even more sure, and less bug prone)
                                self.state.volatile.leader_state
                                    .reply_to_client(curr_cmd_index, response);
                            },
                            ClientCmdState::AlreadyDiscarded 
                            | ClientCmdState::WrongLowestSeqNumVal => {
                                // Apart from replying with SessionExpired we also
                                // need to update ClientSession last activity since
                                // even though wrong/discarded, these commands are
                                // committed.
                                self.state.volatile
                                    .update_client_last_activity(
                                        *client_id, 
                                        log_entry.timestamp
                                );
                                // We don't update LowestSeqNum val since in case:
                                // AlreadyDiscarded this means that this command's
                                // lowest_seq_num is lower than ours becaues its 
                                // seq_num is smaller than our lowestSeqNum. If
                                // its lowest_seq_num were bigger than ours we would
                                // be in WrongLowestSeqNumVal case.

                                let args = CommandResponseArgs {
                                    client_id: *client_id,
                                    sequence_num: *sequence_num,
                                    content: CommandResponseContent::SessionExpired
                                };
                                let response = ClientRequestResponse
                                    ::CommandResponse(args);
                                self.state.volatile.leader_state
                                    .reply_to_client(curr_cmd_index, response);
                            }
                            ClientCmdState::CmdResultAlreadyPresent(res_data) => {
                                // Even though result is present this command is 
                                // still committed, we just don't apply its data to
                                // our state machine, so we need to update client
                                // last_activity and lowest_seq_num
                                self.state.volatile
                                    .update_client_last_activity(
                                        *client_id, 
                                        log_entry.timestamp
                                );
                                self.state.volatile
                                    .update_client_lowest_seq_num_without_resp(
                                        *client_id, 
                                        *lowest_sequence_num_without_response
                                );

                                let content = 
                                    CommandResponseContent::CommandApplied { 
                                            output: res_data
                                };
                                let response = 
                                    ClientRequestResponse::CommandResponse(
                                        CommandResponseArgs { 
                                            client_id: *client_id, 
                                            sequence_num: *sequence_num, 
                                            content 
                                });
                                self.state.volatile.leader_state
                                    .reply_to_client(curr_cmd_index, response);
                            }
                            ClientCmdState::CanBeApplied => {
                                let result = self.state_machine.apply(data).await;

                                self.state.volatile
                                    .update_client_session(
                                        *client_id, 
                                        *sequence_num, 
                                        *lowest_sequence_num_without_response, 
                                        log_entry.timestamp, 
                                        &result
                                );
                                let content = 
                                    CommandResponseContent::CommandApplied { 
                                            output: result
                                };
                                let response = 
                                    ClientRequestResponse::CommandResponse(
                                        CommandResponseArgs { 
                                            client_id: *client_id, 
                                            sequence_num: *sequence_num, 
                                            content 
                                });
                                self.state.volatile.leader_state
                                    .reply_to_client(curr_cmd_index, response);
                            },
                        }
                    },
                    LogEntryContent::NoOp => {
                        // we do nothing
                    },
                    LogEntryContent::Configuration { .. } => {
                    },
                    LogEntryContent::RegisterClient => {
                        // Log index for given command is unique, and if command is
                        // committed all servers will have this very command for 
                        // this index, thus based on this index value we can create
                        // Uuid.
                        let client_id = uuid_from_log_index(curr_cmd_index);

                        self.state.volatile
                            .add_new_client_session(client_id, log_entry.timestamp);

                        match self.role
                        {
                            ServerType::Leader => {
                                // Only leader responds to client. 
                                self.state.volatile.leader_state.reply_to_client(
                                    curr_cmd_index, 
                                    ClientRequestResponse::RegisterClientResponse(
                                        RegisterClientResponseArgs {
                                            content: RegisterClientResponseContent::ClientRegistered { 
                                                client_id 
                                            }
                                        }
                                    )
                                );
                            },
                            _ => {
                                // anyone else just adds client session and doesn't 
                                // respond
                            }
                        }
                    }
                }
            }
            else
            {
                panic!("Raft::apply_next_cmd_to_state_machine - there is no log entry for last_applied: '{}', however CommitIdx: '{}' > last_applied", self.state.volatile.last_applied, commit_index);
            }
        }
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

// This should be moved to raft_impl/internal_msgs_handlers, but I don't want to 
// change provided lib.rs file so much
#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft 
{
    async fn handle(&mut self, msg: RaftMessage) 
    {
        let header = &msg.header;
        if !self.config.servers.contains(&header.source)
        {
            // We don't even have means to reply to such message since our 
            // msg_sender won't have channel for such source, so we ignore it.
            debug!("RaftMessage::handle:: Server: {} got msg with source: {} that is not present in config.servers, ignoring this msg", self.config.self_id, header.source);
            return;
        }

        match &msg.content
        {
            RaftMessageContent::AppendEntries(args) => {
                // Reset the term and become a follower if we're outdated:
                self.check_for_higher_term(header.term).await;
                self.handle_append_entries(&args, header).await;
            },
            RaftMessageContent::AppendEntriesResponse(response_args) => {
                self.check_for_higher_term(header.term).await;
                self.handle_append_entries_response(&response_args, header).await;
            },
            RaftMessageContent::RequestVote(args) => {
                self.handle_request_vote_with_last_hearing_timer(header, args).await;
            },
            RaftMessageContent::RequestVoteResponse(response_args) => {
                self.check_for_higher_term(header.term).await;
                self.handle_request_vote_response(header, response_args).await;
            },
            RaftMessageContent::InstallSnapshot(args) => {

            },
            RaftMessageContent::InstallSnapshotResponse(response_args) => {

            }
        }
    }
}

// This should be moved to raft_impl/client_msgs_handlers, but I don't want to change
// provided lib.rs file so much
#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft 
{
    async fn handle(&mut self, msg: ClientRequest) 
    {
        match msg.content
        {
            ClientRequestContent::Command { 
                command, 
                client_id, 
                sequence_num, 
                lowest_sequence_num_without_response 
            } => {
                self.handle_client_command(
                    &command, 
                    client_id, 
                    sequence_num, 
                    lowest_sequence_num_without_response, 
                    msg.reply_to
                ).await;
            },
            ClientRequestContent::Snapshot => {
                self.handle_client_snapshot().await;
            },
            ClientRequestContent::AddServer { new_server } => {
                self.handle_client_add_server(new_server, msg.reply_to).await;
            },
            ClientRequestContent::RemoveServer { old_server } => {
                self.handle_client_remove_server(old_server, msg.reply_to).await;
            },
            ClientRequestContent::RegisterClient => {
                self.handle_client_register(msg.reply_to).await;
            }
        }
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.

async fn recover_state_machine(
    persistent_state: &PersistentState, 
    state_machine: &mut Box<dyn StateMachine>,
    last_applied_idx: IndexT
)
{
    // When recovering we are FOLLOWER
    // Since state machine is volatile it must be recovered after restart by
    // reapplying log entries (after applying latest snapshot)
    for (idx, cmd) in persistent_state.log.iter().enumerate()
    {
        // We reapply log entries only till the last_applied_idx we got from snapshot
        if idx > last_applied_idx
        {
            break;
        }
        match &cmd.content
        {
            LogEntryContent::Command { data, .. } => {
                // TODO: here we need also to CREATE SESSIONS, add entries etc
                // we probably should just invoke here apply_commited_cmds_to_state_machine
                let _ = state_machine.apply(data).await;
            },
            _ => {
                // we apply only Command Log to our state machine
            }
        }
    }
}

// TODO: test this function
fn find_new_commit_index(
    mut commit_index: usize, 
    match_indexes: &HashMap<ServerIdT, IndexT>,
    log_entries: &Vec<LogEntry>,
    current_term: TermT
) -> usize
{
    // matchIndex is index of highest log entry known to be replicated on server.
    // We want only to preserve information about values, no server ids
    let mut match_indexes: Vec<IndexT> = match_indexes.iter().map(|(_,v)| *v).collect();
    // Sorts in ascending order
    match_indexes.sort();

    let nbr_of_all_indexes = match_indexes.len();
    let mut n_prev_biggest_indexes: usize = 0;

    // Algorithm:
    // 1) We remove currBiggestIndex that is > commitIndex 
    // 2) We check if majority of matchIndexes is >= currBiggestIndex
    // 3) If not we add it to prev_biggest_indexes
    // 4) We take another currBiggestIndex > commitIndex, 
    //  check if majority (we include indexes from prev_biggest_indexes) of 
    //  matchIndexes >= currBiggestIndex 
    // 5) If yes we set our commitIndex to this currBiggestIndex and end
    // 6) If not we repeat till there is no index > commitIndex
    while !match_indexes.is_empty()
    {
        let curr_biggest = match_indexes.remove(match_indexes.len() - 1);

        if curr_biggest <= commit_index
        {
            break;
        }

        // We also need to count curr_biggest index since it is geq than itself
        let mut n_idexes_geq: usize = 1;

        for val in match_indexes.iter().rev()
        {
            if *val >= curr_biggest
            {
                n_idexes_geq += 1;
            }
            else
            {
                // vector is sorted increasingly, so if we encounter first point
                // at which val < curr_biggest, all next vals also will be smaller
                break;
            }
        }

        if n_idexes_geq + n_prev_biggest_indexes > nbr_of_all_indexes / 2
        {
            if let Some(log) = log_entries.get(curr_biggest)
            {
                if log.term == current_term
                {
                    commit_index = curr_biggest;
                    break;
                }
                // otherwise we continue searching for commit_index, so we need to
                // add this one to prev_biggest
                n_prev_biggest_indexes += 1;
            }
            else
            {
                panic!("fine_new_commit_index:: there is no log_entry in Leader's log_entries for curr_biggest index: '{}', log_entries.len() = {}", curr_biggest, log_entries.len());
            }
        }
        else
        {
            n_prev_biggest_indexes += 1;
        }
    }
    return commit_index;
}

fn uuid_from_log_index(log_index: IndexT) -> Uuid 
{
    let mut bytes = [0u8; 16];

    bytes[..std::mem::size_of::<IndexT>()].copy_from_slice(&log_index.to_be_bytes());

    return Uuid::from_bytes(bytes);
}
