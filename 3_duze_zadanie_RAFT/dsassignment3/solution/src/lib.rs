use module_system::{Handler, ModuleRef, System, TimerHandle};
use other_raft_structs::{ServerType, ServerState, PersistentState};
use rand::distr::uniform::SampleRange;
use tokio::time::{Duration, Instant};
use uuid::Uuid;
use std::collections::{HashMap, HashSet};
use log::{debug, warn, info};

pub use domain::*;
use crate::types_defs::{IndexT, ServerIdT, TermT};

use crate::other_raft_structs::*;

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

    fn get_current_timestamp(&self) -> Duration
    {
        return Instant::now().duration_since(self.config.system_boot_time);
    }

    fn get_minimal_election_timeout(&self) -> &Duration
    {
        return self.config.election_timeout_range.start();
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
            let args_entries_are_empty = args.entries.is_empty();

            let append_entries_msg = RaftMessage {
                header: header.clone(), 
                content: RaftMessageContent::AppendEntries(args)
            };

            msgs.insert(*server_id, append_entries_msg);

            // Below is I think NOT NEEDED, we broadcast only when we either append
            // new entry - so everyone has new entry, or when hearbeat so everyone
            // has either zero entries or a few entries

            // So that we can control if we want to send append entries without logs
            // if args_entries_are_empty
            // {
            //     if send_empty_entries
            //     {
            //         msgs.insert(*server_id, append_entries_msg);
            //     }
            // }
            // else
            // {
            //     msgs.insert(*server_id, append_entries_msg);
            // }
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
            self.role = ServerType::Follower;
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
        // TODO: we need to add responding to msgs of clients here 
        let commit_index = self.state.volatile.commit_index;

        while commit_index > self.state.volatile.last_applied
        {
            self.state.volatile.last_applied += 1;

            if let Some(log_entry) = 
                self.state.persistent.log.get(self.state.volatile.last_applied)
            {
                match &log_entry.content
                {
                    LogEntryContent::Command { 
                        data, 
                        client_id, 
                        sequence_num, 
                        lowest_sequence_num_without_response
                    } => {
                        let result = self.state_machine.apply(data).await;
                        self.state.volatile.leader_state.
                            move_from_pending_to_executed(
                               *sequence_num, 
                               *client_id, 
                               &result, 
                               *lowest_sequence_num_without_response
                        );
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

            // Since we got msg from Leader, we reset our last_hearing_timer
            self.state.volatile.last_hearing_from_leader_timer = 
                Some(tokio::time::Instant::now());

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
                    // leader for given term at a time, thus this branch should be 
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

    async fn handle_request_vote(
        &mut self, 
        args: &RequestVoteArgs, 
        header: &RaftMessageHeader
    )
    {
        let candidate_term = header.term;
        let candidate_id = header.source;

        let response_header = RaftMessageHeader {
            source: self.config.self_id,
            term: self.state.persistent.current_term
        };
        let response_args: RequestVoteResponseArgs;

        match self.role 
        {
            ServerType::Follower => {
                if candidate_term < self.state.persistent.current_term
                || self.state.persistent.voted_for.is_some()
                {
                    // If old process crashed, then came back to live, and 
                    // even though it has an outdated term, it can initiate 
                    // election, so we need to reject him
                    // OR if in this term we have already voted for 
                    // someone, we reject this request
                    response_args = RequestVoteResponseArgs { 
                        vote_granted: false
                    };
                    // If we reject we do not reset_timer, since now we still wait 
                    // for new leader heartbeat
                }
                else
                {
                    // We haven't voted for anyone yet and candidate_term is big 
                    // enough, but to vote for this candidate we must also check if 
                    // his LOG is up-to-date.
                    // --> We will do that by comparing the index and term of the
                    // last entries in the logs. 
                    //      1) If the logs have last entries with different terms, 
                    //      then the log with the bigger term is more up-to-date
                    //      2) If logs end with the same term, then whichever log is
                    //      longer is more up-to-date
                    // If candidate log is more up-to-date than ours we VOTE FOR HIM
                    let my_last_log_idx = self.state.persistent.log
                        .len()
                        .checked_sub(1)
                        .unwrap(); 
                    let my_last_log_entry_term = self.state.persistent.log
                        .get(my_last_log_idx)
                        .unwrap()
                        .term;
                    let vote_granted: bool;

                    if my_last_log_entry_term == args.last_log_term
                    {
                        if my_last_log_idx <= args.last_log_index
                        {
                            vote_granted = true;
                        }
                        else
                        {
                            vote_granted = false;
                        }
                    }
                    else if my_last_log_entry_term < args.last_log_term
                    {
                        vote_granted = true;
                    }
                    else // my_last_log_entry_term > args.last_log_term
                    {
                        vote_granted = false;
                    }

                    if vote_granted
                    {
                        info!("Follower: '{}' votes for: '{}'", self.config.self_id, candidate_id);
                        self.state.persistent.voted_for = Some(candidate_id);
                        self.save_to_stable_storage().await;
                    }

                    response_args = RequestVoteResponseArgs { vote_granted };

                    // We do not reset last_hearing_timer since this msg is from 
                    // Candidate not from Leader
                    // We reset ElectionTimer to give this candidate time to gather 
                    // votes
                    self.reset_election_timer().await;
                }
            },
            ServerType::Candidate { .. } => {
                // From visualisation recommended in previous RAFT task description
                // we can see that candidate rejects other candidates requests
                response_args = RequestVoteResponseArgs { 
                    vote_granted: false
                };
            },
            ServerType::Leader => {
                // If everything works correctly we should never get here since 
                // Leader always ignores RequestVote
                panic!("handle_request_vote:: Process: '{}' that is a leader during term: '{}' got 
                RequestVoteResponse from: '{}', leader should ALWAYS IGNORE any RequestVote", self.config.self_id, self.state.persistent.current_term, header.source);
            },
        };

        self.msg_sender.send(&candidate_id, RaftMessage { 
            header: response_header, 
            content: RaftMessageContent::RequestVoteResponse(response_args)
        }).await;
    }

    async fn handle_request_vote_with_last_hearing_timer(
        &mut self, 
        header: &RaftMessageHeader,
        args: &RequestVoteArgs
    )
    {
        if let Some(last_hearing_timer) = &self.state.volatile
                                        .last_hearing_from_leader_timer
        {
            let min_election_timeout = self.get_minimal_election_timeout();

            if last_hearing_timer.elapsed() >= *min_election_timeout
            {
                // We haven't received Heartbeat from Leader during minimal
                // election timeout thus we can respond to RequestVote
                self.check_for_higher_term(header.term).await;
                self.handle_request_vote(args, header).await;
            }
            else
            {
                // Not enough time since last Leader's heartbeat has elapsed
                // for us to respond to RequestVote, 
                // so we IGNORE IT and DON'T UPDATE OUR TERM etc.
                debug!("Server: '{}' got RequestVote from: '{}', but minimal election timeout from hearing heartbeat from leader hasn't passed, thus IGNORING this msg", self.config.self_id, header.source);
            }
        }
        else
        {
            // We don't have timer, thus we are not waiting for any leader
            // heartbeat as Follower, we are Candidate or WE ARE LEADER
            match self.role
            {
                ServerType::Leader => {
                    // Leader ALWAYS ignores RequestVote
                    debug!("Leader: '{}' got RequestVote - IGNORING it", self.config.self_id);
                }
                _ => {
                    // Anyone else proceeds
                    self.check_for_higher_term(header.term).await;
                    self.handle_request_vote(args, header).await;
                }
            }
        }
    }

    async fn handle_request_vote_response(
        &mut self, 
        header: &RaftMessageHeader,
        args: &RequestVoteResponseArgs
    )
    {
        match &mut self.role 
        {
            ServerType::Follower => {
                debug!("Follower: '{}' got RequestVoteResponse from: '{}' - IGNORING it - probably this follower used to be a candidate but got msg from  Leader, and changed to its follower", 
                self.config.self_id, header.source);
            },
            ServerType::Candidate { votes_received } => {
                if args.vote_granted
                {
                    info!("Candidate: '{}' got vote from: '{}'", self.config.self_id, header.source);

                    votes_received.insert(header.source);

                    // When we get majority we immediately become leader, reset timer and start our  leadership by sending heartbeats
                    if votes_received.len() > self.config.servers.len() / 2
                    {
                        info!("Candidate: '{}' has become a LEADER", 
                            self.config.self_id);

                        self.role = ServerType::Leader;

                        // When a server becomes a leader, it must append a NoOp entry to the log (nextIndex must be initialized with the index of this entry).
                        self.state.persistent.log.push(LogEntry {
                            content: LogEntryContent::NoOp,
                            term: self.state.persistent.current_term,
                            timestamp: self.get_current_timestamp() 
                        });
                        self.save_to_stable_storage().await;

                        self.state.volatile.leader_id = Some(self.config.self_id);
                        self.state.volatile.leader_state.reinitialize(
                            &self.config.servers, 
                            self.state.persistent.log.len().checked_sub(1).unwrap() 
                        );

                        self.broadcast_append_entries().await;
                        self.reset_heartbeat_timer().await;
                        self.reset_election_timer().await;
                    }
                }
            },
            ServerType::Leader => {
                debug!("Leader: '{}' during term: '{}' got RequestVoteResponse, leader ignores this msg, probably leader got majority before receiving all votes", 
                self.config.self_id, self.state.persistent.current_term);
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
        match self.role
        {
            ServerType::Follower => {
                info!("Follower '{}' has become a Candidate", self.config.self_id);

                self.role = ServerType::Candidate { 
                    votes_received: HashSet::from(
                        [self.config.self_id]
                )};

                self.state.persistent.current_term += 1;
                self.state.persistent.voted_for = Some(self.config.self_id);

                self.state.volatile.leader_id = None;
                self.state.volatile.last_hearing_from_leader_timer = None;
                
                self.save_to_stable_storage().await;
                self.reset_election_timer().await;
                self.broadcast_request_vote().await;
            },
            ServerType::Candidate { .. } => {
                // If we are a Candidate and has election timeout, this means that
                // we didn't get enough votes to become Leader, cause if we had 
                // enough votes, in RequestVoteResponse handler we would become 
                // Leader thus we restart election

                self.role = ServerType::Candidate { 
                    votes_received: HashSet::from(
                        [self.config.self_id]
                )};

                self.state.persistent.current_term += 1;
                self.state.persistent.voted_for = Some(self.config.self_id);
                self.state.volatile.leader_id = None;
                // Setting last_hearing to None is not necessary here, but just to 
                // be sure I set it.
                self.state.volatile.last_hearing_from_leader_timer = None;
                self.save_to_stable_storage().await;

                self.reset_election_timer().await;
                self.broadcast_request_vote().await;
            },
            ServerType::Leader => {
                if self.state.volatile
                    .leader_state.successful_heartbeat_round_happened
                {
                    // We had a successful heartbeat round so we stay as Leader
                    self.state.volatile
                        .leader_state
                        .successful_heartbeat_round_happened = false;
                    self.state.volatile
                        .leader_state
                        .responses_from_followers.clear();
                }
                else
                {
                    // We didn't have successful heartbeat round during whole 
                    // election timeout thus we revert back to Follower
                    self.role = ServerType::Follower;
                    self.state.volatile.leader_id = None;
                    self.stop_heartbeat_timer().await;
                    self.reset_election_timer().await;
                }
            }
        }
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
        let header = &msg.header;

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

        // TODO: change commit_index if role is LEADER
        self.update_commit_index_if_leader();

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
        match self.role
        {
            ServerType::Leader => {
                match msg.content
                {
                    ClientRequestContent::Command { 
                        command, 
                        client_id, 
                        sequence_num, 
                        lowest_sequence_num_without_response 
                    } => {
                        match self.state.volatile.leader_state
                            .state_of_cmd_with_sequence_num(sequence_num, client_id)
                        {
                            ClientCmdState::NotPresent => {
                                // We add new entry to log, and wait for commit 
                                // before replying.
                                let client_last_activity_timestamp = 
                                    self.get_current_timestamp();

                                let log_content = LogEntryContent::Command { 
                                    data: command, 
                                    client_id, 
                                    sequence_num, 
                                    lowest_sequence_num_without_response 
                                };
                                let log_entry = LogEntry {
                                    content: log_content,
                                    term: self.state.persistent.current_term,
                                    timestamp: client_last_activity_timestamp
                                };

                                self.state.persistent.log.push(log_entry);
                                self.save_to_stable_storage().await;
                                self.state.volatile.leader_state
                                    .insert_new_statemachine_pending_cmd(
                                        client_id,
                                        msg.reply_to,
                                        sequence_num,
                                        lowest_sequence_num_without_response,
                                        client_last_activity_timestamp
                                    );
                                self.broadcast_append_entries().await;
                            },
                            ClientCmdState::Pending => {
                                // TODO: Don't really know what to do here, update 
                                // reply_to for given sequence_num??
                            },
                            ClientCmdState::Committed => {
                                // We immediately return result.
                                let res = self.state.volatile.leader_state
                                    .get_committed_cmd_result(sequence_num, client_id);
                                let response_args = CommandResponseArgs { 
                                            client_id, 
                                            sequence_num, 
                                            content: CommandResponseContent
                                                ::CommandApplied { 
                                                    output: res.clone()
                                }};

                                // TODO: if channel closed we probably shouldn't do anything, since deleteing client session will be done
                                // in different way
                                let _ = msg.reply_to.send(
                                    ClientRequestResponse::CommandResponse(
                                        response_args
                                ));
                            }
                        }

                    },
                    ClientRequestContent::Snapshot => {
                        unimplemented!("Leader - handling Client Snapshot request not implemented");
                    },
                    ClientRequestContent::AddServer { new_server } => {
                        unimplemented!("Handler<ClientRequest>:: Leader - AddServer unimplemented");
                    },
                    ClientRequestContent::RemoveServer { old_server } => {
                        unimplemented!("Handler<ClientRequest>:: Leader - RemoveServer unimplemented");
                    },
                    ClientRequestContent::RegisterClient => {
                        let client_id = Uuid::new_v4();
                        let log_content = LogEntryContent::RegisterClient; 
                        let log_entry = LogEntry {
                            content: log_content,
                            term: self.state.persistent.current_term,
                            timestamp: self.get_current_timestamp()
                        };

                        self.state.persistent.log.push(log_entry);
                        self.save_to_stable_storage().await;

                        let command_index = self.state.persistent.log.len() - 1;

                        self.state.volatile
                            .leader_state.insert_new_other_cmd(
                                command_index,
                                OtherCommandData::RegisterClient { client_id }, 
                                msg.reply_to,
                            );
                        self.broadcast_append_entries().await;
                    }
                }
            },
            _ => {
                // Anyone apart from Leader rejects Client request
                match msg.content
                {
                    ClientRequestContent::Command {client_id, sequence_num, ..} => {
                        let response_args = CommandResponseArgs {
                            client_id,
                            sequence_num,
                            content: CommandResponseContent::NotLeader { 
                                leader_hint: self.state.volatile.leader_id.clone()
                            }
                        };
                        let response = ClientRequestResponse::CommandResponse(
                            response_args
                        );

                        // We don't care if client still has channel opened or not
                        // we just send msg
                        let _ = msg.reply_to.send(response);
                    },
                    ClientRequestContent::Snapshot => {
                        unimplemented!("Follower/Candidate - handling Client Snapshot request not implemented");
                    },
                    ClientRequestContent::AddServer { new_server } => {
                        let args = AddServerResponseArgs {
                            new_server,
                            content: AddServerResponseContent::NotLeader {  
                                leader_hint: self.state.volatile.leader_id.clone()
                            }
                        };
                        let response = 
                            ClientRequestResponse::AddServerResponse(args);

                        let _ = msg.reply_to.send(response);
                    },
                    ClientRequestContent::RemoveServer { old_server } => {
                        let args = RemoveServerResponseArgs {
                            old_server,
                            content: RemoveServerResponseContent::NotLeader { 
                                leader_hint: self.state.volatile.leader_id.clone()
                            }
                        };

                        let response = 
                            ClientRequestResponse::RemoveServerResponse(args);

                        let _ = msg.reply_to.send(response);
                    },
                    ClientRequestContent::RegisterClient => {
                        let args = RegisterClientResponseArgs {
                            content: RegisterClientResponseContent::NotLeader { 
                                leader_hint: self.state.volatile.leader_id.clone()
                            }
                        };

                        let response = 
                            ClientRequestResponse::RegisterClientResponse(args);

                        let _ = msg.reply_to.send(response);
                    }
                }
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
                let _ = state_machine.apply(data).await;
            },
            _ => {
                // we apply only Command Log to our state machine
            }
        }
    }
}

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
