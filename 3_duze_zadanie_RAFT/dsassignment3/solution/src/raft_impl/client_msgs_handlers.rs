use uuid::Uuid;
use tokio::sync::mpsc::UnboundedSender;
use log::{info};

use crate::domain::*;
use crate::{Raft};
use crate::types_defs::{SequenceNumT};
use crate::other_raft_structs::{ServerType};

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
                // We append every command, and only when committing we either skip
                // this command or do other stuff
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

                let cmd_index = self.state.persistent.log.len() - 1;
                self.state.volatile.leader_state.insert_reply_channel(
                    cmd_index, 
                    reply_to
                );

                self.broadcast_append_entries().await;

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
                info!("Leader: {} got RegisterClient", self.config.self_id);
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
                    .leader_state.insert_reply_channel(
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
        unimplemented!("Snapshots omitted")
    }

    pub async fn handle_client_add_server(
        &mut self,
        new_server: Uuid,
        reply_to: UnboundedSender<ClientRequestResponse>
    )
    {
        unimplemented!("Cluster membership changes omitted");
    }

    pub async fn handle_client_remove_server(
        &mut self,
        old_server: Uuid,
        reply_to: UnboundedSender<ClientRequestResponse>
    )
    {
        unimplemented!("Cluster membership changes omitted");
    }

}