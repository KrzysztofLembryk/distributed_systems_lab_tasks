use uuid::Uuid;
use tokio::sync::mpsc::UnboundedSender;

use crate::domain::*;
use crate::{Raft};
use crate::types_defs::{SequenceNumT};
use crate::other_raft_structs::{ClientCmdState, ServerType};

impl Raft
{
    pub async fn handle_client_command(
        &mut self,
        command: &Vec<u8>, 
        client_id: Uuid, 
        sequence_num: SequenceNumT, 
        lowest_sequence_num_without_response: u64, 
        reply_to: UnboundedSender<ClientRequestResponse>,
    )
    {
        match self.role
        {
            ServerType::Leader => {
                match self.state.volatile.leader_state
                    .state_of_cmd_with_sequence_num(sequence_num, client_id)
                {
                    ClientCmdState::NoClientSession => {
                        // Client doesn't have session so we reply 
                        // SessionExpired - he should send RegisterClient 
                        // request first
                        let args = CommandResponseArgs {
                            client_id,
                            sequence_num,
                            content: CommandResponseContent::SessionExpired
                        };

                        let _ = reply_to.send(
                            ClientRequestResponse::CommandResponse(args)
                        );
                    },
                    ClientCmdState::AlreadyDiscarded => {
                        // Client asks for result of CMD that is already 
                        // discarded thus we return session expired,
                        // since correctly working client won't do that
                        // TODO: should we end client's session if we get
                        // such command from him?
                        let args = CommandResponseArgs {
                            client_id,
                            sequence_num,
                            content: CommandResponseContent::SessionExpired
                        };

                        let _ = reply_to.send(
                            ClientRequestResponse::CommandResponse(args)
                        );
                    },
                    ClientCmdState::NotPresent => {
                        // We add new entry to log, and only when commit 
                        // we check timestamp, change 
                        // lowest_seq_nbr before etc.
                        let client_last_activity_timestamp = 
                            self.get_current_timestamp();

                        let log_content = LogEntryContent::Command { 
                            data: command.clone(), 
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
                                reply_to,
                                sequence_num,
                            );
                        self.broadcast_append_entries().await;
                    },
                    ClientCmdState::Pending => {
                        // We add another reply_to for cmd with this seqNum.
                        // When this cmd will be committed we will send 
                        // responses to all appended reply_to
                        self.state.volatile.leader_state.
                            append_new_reply_to_for_pending_cmd(
                                client_id, 
                                reply_to, 
                                sequence_num
                        );
                    },
                    ClientCmdState::Committed => {
                        // We immediately return result.
                        let res = self.state.volatile.leader_state
                            .get_committed_cmd_result(
                                sequence_num, 
                                client_id
                        );
                        let response_args = CommandResponseArgs { 
                                    client_id, 
                                    sequence_num, 
                                    content: CommandResponseContent
                                        ::CommandApplied { 
                                            output: res.clone()
                        }};

                        let _ = reply_to.send(
                            ClientRequestResponse::CommandResponse(
                                response_args
                        ));
                    }
                }
            },
            // Anyone apart from Leader rejects Client request
            _ => {
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
                let _ = reply_to.send(response);
            }
        }

    }

    pub async fn handle_client_register(
        &mut self,
        reply_to: UnboundedSender<ClientRequestResponse>,
    )
    {
        match self.role
        {
            ServerType::Leader => {
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
                        reply_to,
                    );
                self.broadcast_append_entries().await;
            },
            _ => {
                let args = RegisterClientResponseArgs {
                    content: RegisterClientResponseContent::NotLeader { 
                        leader_hint: self.state.volatile.leader_id.clone()
                    }
                };

                let response = 
                    ClientRequestResponse::RegisterClientResponse(args);

                let _ = reply_to.send(response);
            }
        }
    }

    pub async fn handle_client_snapshot(
        &mut self
    )
    {
        match self.role
        {
            ServerType::Leader => {
                unimplemented!("Leader - handling Client Snapshot request not implemented");
            },
            _ => {

                unimplemented!("Follower/Candidate - handling Client Snapshot request not implemented");
            }
        }
    }

    pub async fn handle_client_add_server(
        &mut self,
        new_server: Uuid,
        reply_to: UnboundedSender<ClientRequestResponse>
    )
    {
        match self.role
        {
            ServerType::Leader => {
                unimplemented!("Handler<ClientRequest>:: Leader - AddServer unimplemented");
            },
            _ => {
                let args = AddServerResponseArgs {
                    new_server,
                    content: AddServerResponseContent::NotLeader {  
                        leader_hint: self.state.volatile.leader_id.clone()
                    }
                };
                let response = 
                    ClientRequestResponse::AddServerResponse(args);

                let _ = reply_to.send(response);
            }
        }
    }

    pub async fn handle_client_remove_server(
        &mut self,
        old_server: Uuid,
        reply_to: UnboundedSender<ClientRequestResponse>
    )
    {
        match self.role
        {
            ServerType::Leader => {
                unimplemented!("Handler<ClientRequest>:: Leader - RemoveServer unimplemented");
            },
            _ => {
                let args = RemoveServerResponseArgs {
                    old_server,
                    content: RemoveServerResponseContent::NotLeader { 
                        leader_hint: self.state.volatile.leader_id.clone()
                    }
                };

                let response = 
                    ClientRequestResponse::RemoveServerResponse(args);

                let _ = reply_to.send(response);
            }
        }
    }

}