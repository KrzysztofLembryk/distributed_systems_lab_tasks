use rand::distr::uniform::SampleRange;
use std::collections::{HashSet};
use tokio::time::{Duration, Instant};
use module_system::{Handler};
use log::{info};

use crate::domain::{Timestamp};
use crate::Raft;
use crate::other_raft_structs::{ServerType};

#[derive(Clone)]
struct ElectionTimeout;

#[derive(Clone)]
struct HeartbeatTimeout;


impl Raft
{
    pub fn get_current_timestamp(&self) -> Timestamp
    {
        return Instant::now().duration_since(self.config.system_boot_time);
    }

    pub fn get_minimal_election_timeout(&self) -> &Duration
    {
        return self.config.election_timeout_range.start();
    }

    pub async fn stop_heartbeat_timer(&mut self)
    {
        if let Some(handle) = self.heartbeat_timer_handle.take() 
        {
            handle.stop().await;
        }
    }

    pub async fn reset_heartbeat_timer(&mut self) 
    {
        if let Some(handle) = self.heartbeat_timer_handle.take() 
        {
            handle.stop().await;
        }

        self.heartbeat_timer_handle = 
            Some(self.self_ref.request_tick(HeartbeatTimeout, self.config.heartbeat_timeout).await);
    }

    pub async fn reset_election_timer(&mut self)
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
