use crate::Raft;
use crate::domain::*;
use crate::other_raft_structs::{ServerType};

use log::{debug, warn, info};

impl Raft
{
    // #############################################################################
    // ########################## MSG HANDLE FUNCTIONS #############################
    // #############################################################################
    pub async fn handle_append_entries(
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
                    debug!("Raft::handle_append_entries:: Follower: {} got appendEntries", self.config.self_id);
                    self.reset_election_timer().await;
                },
                ServerType::Candidate { .. } => {
                    // If we are Candidate and got AppendEntries from Leader that has
                    // 'term >= our_term' we revert back to Follower
                    debug!("Raft::handle_append_entries:: Candidate: {} got appendEntries", self.config.self_id);
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
                    debug!("Raft::handle_append_entries:: Server: '{}', has equal prevTerm to prevLogTerm: {}", self.config.self_id,prev_log_term);

                    self.do_appending(args, prev_log_index).await;

                    response_msg = 
                        self.create_append_entries_response_msg(true, args);
                }
                else
                {
                    // 2) We have entry at prevLogIdx but not with prevLogTerm
                    debug!("Raft::handle_append_entries:: Server: '{}', has entry at prevLogIdx {} but not with prevLogTerm: {}", self.config.self_id , prev_log_index, prev_log_term);
                    response_msg = 
                        self.create_append_entries_response_msg(false, args);
                }
            }
            else
            {
                // 1) We don't have entry at prevLogIndex
                debug!("Raft::handle_append_entries:: Server: '{}', doesn't have entry at prevLogIndex: {}", self.config.self_id , prev_log_index);
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
        // After every AppendEntries msg we got commit index might change and we 
        // have new entries so we want to apply them now.
        self.apply_commited_cmds_to_state_machine().await;
    }

    pub async fn handle_append_entries_response(
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

    pub async fn handle_request_vote_with_last_hearing_timer(
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
                    info!("Leader: '{}' got RequestVote - IGNORING it", self.config.self_id);
                }
                _ => {
                    // Anyone else proceeds
                    self.check_for_higher_term(header.term).await;
                    self.handle_request_vote(args, header).await;
                }
            }
        }
    }

    pub async fn handle_request_vote_response(
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

    async fn do_appending(
        &mut self, 
        args: &AppendEntriesArgs, 
        prev_log_index: usize
    )
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
    }
}