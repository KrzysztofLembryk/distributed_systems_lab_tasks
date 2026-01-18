use assignment_3_solution::*;
use assignment_3_test_utils::*;
use module_system::System;
use ntest::timeout;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::Instant;
use uuid::Uuid;

fn init_logger() 
{
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init();
}

#[tokio::test]
#[timeout(1000)]
async fn system_makes_progress_when_there_is_a_majority_2_alive_1_dead() {
    // given
    let mut system = System::new().await;

    let entry_data = vec![1, 2, 3, 4, 5];
    let processes = make_idents::<3>();
    let [ident_leader, ident_follower, _dead] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let [raft_leader, _raft_follower] = make_rafts(
        &mut system,
        [ident_leader, ident_follower],
        [100, 300],
        boot,
        |_| Box::new(IdentityMachine),
        |_| Box::<RamStorage>::default(),
        sender.clone(),
        async |id, mref| sender.insert(id, mref).await,
        &processes,
    )
    .await;
    sleep_ms(200).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // when
    let client_id = register_client(&raft_leader, &result_sender, &mut result_receiver).await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: entry_data.clone(),
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    assert_eq!(
        entry_data,
        *unwrap_output(&result_receiver.recv().await.unwrap())
    );

    system.shutdown().await;
}

#[tokio::test]
#[timeout(1000)]
async fn system_makes_progress_when_there_is_a_majority_1_alive() {
    // given
    init_logger();
    let mut system = System::new().await;

    let entry_data = vec![1, 2, 3, 4, 5];
    let processes = make_idents::<1>();
    let [ident_leader] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let [raft_leader] = make_rafts(
        &mut system,
        [ident_leader],
        [100],
        boot,
        |_| Box::new(IdentityMachine),
        |_| Box::<RamStorage>::default(),
        sender.clone(),
        async |id, mref| sender.insert(id, mref).await,
        &processes,
    )
    .await;
    sleep_ms(400).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // when
    let client_id = register_client(&raft_leader, &result_sender, &mut result_receiver).await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: entry_data.clone(),
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    assert_eq!(
        entry_data,
        *unwrap_output(&result_receiver.recv().await.unwrap())
    );
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(150)]
async fn system_does_not_make_progress_without_majority() {
    // given
    let mut system = System::new().await;

    let entry_data = vec![1, 2, 3, 4, 5];
    let processes = make_idents::<3>();
    let [ident_leader, ident_follower, _dead] = processes;
    let (sender, [raft_leader, _raft_follower], _) =
        make_standard_rafts::<IdentityMachine, RamStorage, _>(
            &mut system,
            [ident_leader, ident_follower],
            [100, 300],
            &processes,
        )
        .await;
    sleep_ms(200).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // when
    let client_id = register_client(&raft_leader, &result_sender, &mut result_receiver).await;

    sender.break_link(ident_follower, ident_leader).await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: entry_data,
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    sleep_ms(1000).await;

    // then
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(150)]
async fn follower_denies_vote_for_candidate_with_outdated_log() {
    // given
    let mut system = System::new().await;

    let (tx, mut rx) = unbounded_channel();
    let processes = make_idents::<3>();
    let [other_ident_1, other_ident_2, ident_follower] = processes;
    let boot = Instant::now();
    let [raft_follower] = make_rafts(
        &mut system,
        [ident_follower],
        [500],
        boot,
        |_| Box::new(DummyMachine),
        |_| Box::<RamStorage>::default(),
        RamSender { tx },
        async |_, _| {},
        &processes,
    )
    .await;
    let client_id = Uuid::from_u128(1);

    // when

    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 2,
                source: other_ident_1,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        content: LogEntryContent::RegisterClient,
                        term: 2,
                        timestamp: boot.elapsed(),
                    },
                    LogEntry {
                        content: LogEntryContent::Command {
                            data: vec![1],
                            client_id,
                            sequence_num: 0,
                            lowest_sequence_num_without_response: 0,
                        },
                        term: 2,
                        timestamp: boot.elapsed(),
                    },
                ],
                leader_commit: 0,
            }),
        })
        .await;

    // Wait longer than election timeout so that the follower does not ignore the vote request
    sleep_ms(600).await;

    // Older term of the last message.
    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 4,
                source: other_ident_2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 2,
                last_log_term: 1,
            }),
        })
        .await;

    // Shorter log in candidate.
    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 5,
                source: other_ident_2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 1,
                last_log_term: 2,
            }),
        })
        .await;

    // then
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: ident_follower,
                term: 2,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: true,
                last_verified_log_index: 2,
            })
        }
    );
    for _ in 0..2 {
        rx.recv().await.unwrap();
    }
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: ident_follower,
                term: 4,
            },
            content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                vote_granted: false,
            })
        }
    );
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: ident_follower,
                term: 5,
            },
            content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                vote_granted: false,
            })
        }
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn follower_rejects_inconsistent_append_entry() {
    // given
    let mut system = System::new().await;

    let (tx, mut rx) = unbounded_channel();
    let processes = make_idents::<2>();
    let [other_ident, ident_follower] = processes;
    let boot = Instant::now();
    let [raft_follower] = make_rafts(
        &mut system,
        [ident_follower],
        [10_000],
        boot,
        |_| Box::new(DummyMachine),
        |_| Box::<RamStorage>::default(),
        RamSender { tx },
        async |_, _| {},
        &processes,
    )
    .await;

    // when
    let client_id = Uuid::from_u128(1);

    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 1,
                source: other_ident,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        content: LogEntryContent::RegisterClient,
                        term: 1,
                        timestamp: boot.elapsed(),
                    },
                    LogEntry {
                        content: LogEntryContent::Command {
                            data: vec![1, 2, 3, 4],
                            client_id,
                            sequence_num: 0,
                            lowest_sequence_num_without_response: 0,
                        },
                        term: 1,
                        timestamp: boot.elapsed(),
                    },
                ],
                leader_commit: 0,
            }),
        })
        .await;

    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 2,
                source: other_ident,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 2,
                prev_log_term: 2,
                entries: vec![LogEntry {
                    content: LogEntryContent::Command {
                        data: vec![5, 6, 7, 8],
                        client_id,
                        sequence_num: 0,
                        lowest_sequence_num_without_response: 0,
                    },
                    term: 2,
                    timestamp: boot.elapsed(),
                }],
                leader_commit: 0,
            }),
        })
        .await;

    // then
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                term: 1,
                source: ident_follower,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: true,
                last_verified_log_index: 2,
            })
        }
    );
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                term: 2,
                source: ident_follower,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: false,
                last_verified_log_index: 3,
            })
        }
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn follower_redirects_to_leader() {
    // given
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (_sender, [leader, follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    sleep_ms(200).await;

    // when
    let (follower_result_sender, mut follower_result_receiver) = unbounded_channel();
    let (leader_result_sender, mut leader_result_receiver) = unbounded_channel();

    let client_id =
        register_client(&leader, &leader_result_sender, &mut leader_result_receiver).await;

    follower
        .send(ClientRequest {
            reply_to: follower_result_sender,
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: leader_result_sender,
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    assert_eq!(
        follower_result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::NotLeader {
                leader_hint: Some(leader_id)
            }
        })
    );
    assert_eq!(
        leader_result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![5, 6, 7, 8]
            },
        })
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn leader_steps_down_without_heartbeat_responses_from_majority() {
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (sender, [leader, _follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    let response_1 = result_receiver.recv().await.unwrap();

    sender.break_link(follower_id, leader_id).await;
    sleep_ms(200).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    let response_2 = result_receiver.recv().await.unwrap();

    assert_eq!(
        response_1,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            },
        })
    );
    assert_eq!(
        response_2,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::NotLeader { leader_hint: None }
        })
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(1000)]
async fn leader_steps_down_and_new_leader_is_elected() {
    init_logger();
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (sender, [leader, follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    let (result_sender, mut result_receiver) = unbounded_channel();

    // wait for leader election
    sleep_ms(150).await;

    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    // Partition: break link between leader and follower
    sender.break_link(follower_id, leader_id).await;
    sleep_ms(300).await; // Wait for leader to step down

    // Try to send a command to the old leader, should get NotLeader
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    let response = result_receiver.recv().await.unwrap();
    assert_eq!(
        response,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::NotLeader { leader_hint: None }
        })
    );

    // Heal the partition
    sender.fix_link(follower_id, leader_id).await;
    sleep_ms(400).await; // Wait for new election

    // Try to register a client and send a command to both servers, one should succeed
    let (result_sender2, mut result_receiver2) = unbounded_channel();
    let client_id2 = register_client(&leader, &result_sender2, &mut result_receiver2).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender2.clone(),
            content: ClientRequestContent::Command {
                command: vec![9, 8, 7, 6],
                client_id: client_id2,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    follower
        .send(ClientRequest {
            reply_to: result_sender2.clone(),
            content: ClientRequestContent::Command {
                command: vec![9, 8, 7, 6],
                client_id: client_id2,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // At least one should succeed (be applied), the other may redirect or reject
    let mut got_applied = false;
    for _ in 0..2 {
        if let ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied { output },
        }) = result_receiver2.recv().await.unwrap()
        {
            assert_eq!(client_id, client_id2);
            assert_eq!(output, vec![9, 8, 7, 6]);
            got_applied = true;
        }
    }
    assert!(got_applied, "No server applied the command after re-election");

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(100)]
async fn follower_ignores_request_vote_within_election_timeout_of_leader_heartbeat() {
    // given
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id, spy_id] = processes;
    let (sender, [leader, follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    sleep_ms(150).await;

    // when
    let (spy_sender, mut spy_receiver) = unbounded_channel();
    sender
        .insert(
            spy_id,
            Box::new(RaftSpy::new(&mut system, None, spy_sender).await),
        )
        .await;

    follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: spy_id,
                term: 2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 0,
                last_log_term: 0,
            }),
        })
        .await;
    leader
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: spy_id,
                term: 2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 0,
                last_log_term: 0,
            }),
        })
        .await;
    sleep_ms(500).await;

    // then
    while let Ok(msg) = spy_receiver.try_recv() {
        assert!(matches!(
            msg,
            RaftMessage {
                header: RaftMessageHeader {
                    source,
                    term: 1,
                },
                content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: _,
                    leader_commit: 1,
                })
            } if source == leader_id
        ));
        if let RaftMessageContent::AppendEntries(AppendEntriesArgs { entries, .. }) = msg.content {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].term, 1);
            assert_eq!(entries[0].content, LogEntryContent::NoOp);
        }
    }

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(500)]
async fn leader_steps_down_on_higher_term_message() {
    // given
    init_logger();
    let mut system = System::new().await;
    let processes = make_idents::<2>();
    let [leader_to_be_id, other_server_id] = processes;

    let (_sender, [leader_to_be, _follower], _) =
        make_standard_rafts::<IdentityMachine, RamStorage, _>(
            &mut system,
            [leader_to_be_id, other_server_id],
            [100, 300], // First server has shorter timeout, becomes leader
            &processes,
        )
        .await;

    // Wait for election to complete
    sleep_ms(150).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // Register client to confirm leadership
    let client_id = register_client(&leader_to_be, &result_sender, &mut result_receiver).await;

    // when
    // Send a message from another server with a higher term
    leader_to_be
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 2, // Current leader is in term 1
                source: other_server_id,
            },
            // whatever about msg content
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs { 
                success: false, 
                last_verified_log_index: 0 
            } 
            ),
        })
        .await;

    // Send a new command to the old leader
    leader_to_be
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    // The old leader should have stepped down and now reject the command
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::NotLeader { leader_hint: None }
        })
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(500)]
async fn leader_ignores_request_vote() {
    // given
    init_logger();
    let mut system = System::new().await;
    let processes = make_idents::<3>();
    let [leader_id, _follower_id, candidate_id] = processes;

    let (_sender, [leader, _follower, _candidate], _) =
        make_standard_rafts::<IdentityMachine, RamStorage, _>(
            &mut system,
            processes,
            [100, 300, 300], // First server has shorter timeout
            &processes,
        )
        .await;

    // Wait for election to complete
    sleep_ms(150).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // Confirm leadership by registering a client
    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    // when
    // A candidate sends a RequestVote to the established leader
    leader
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 1, // Same term as the current leader
                source: candidate_id,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 1,
                last_log_term: 1,
            }),
        })
        .await;

    // Send a new command to the leader to verify it's still the leader
    let command_data = vec![10, 20, 30];
    leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: command_data.clone(),
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    // The leader should process the command, proving it did not step down
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::CommandApplied {
                output: command_data
            },
        })
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(500)]
async fn leader_ignores_message_from_unknown_source() {
    init_logger();
    let mut system = System::new().await;
    let processes = make_idents::<2>();
    let [leader_id, follower_id] = processes;

    let (_sender, [leader, _follower], _) =
        make_standard_rafts::<IdentityMachine, RamStorage, _>(
            &mut system,
            [leader_id, follower_id],
            [100, 300],
            &processes,
        )
        .await;

    // Wait for election to complete
    sleep_ms(150).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // Confirm leadership by registering a client
    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    // when
    // Send a RaftMessage from an unknown source (not in the cluster)
    let unknown_id = Uuid::new_v4();
    assert!(!processes.contains(&unknown_id));

    leader
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 1,
                source: unknown_id,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 1,
                last_log_term: 1,
            }),
        })
        .await;

    // Send a new command to the leader to verify it's still the leader
    let command_data = vec![42, 43, 44];
    leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: command_data.clone(),
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    // The leader should process the command, proving it ignored the unknown source message
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::CommandApplied {
                output: command_data
            },
        })
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(1000)]
async fn leader_handles_multiple_duplicated_false_append_entries_responses() {
    init_logger();
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (_sender, [leader, follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    let (result_sender, mut result_receiver) = unbounded_channel();

    // Wait for leader election
    sleep_ms(150).await;

    // Register client to confirm leadership
    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    // Send two commands to the leader
    for (seq_num, cmd) in [vec![1, 2, 3], vec![4, 5, 6]].into_iter().enumerate() {
        leader
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: cmd.clone(),
                    client_id,
                    sequence_num: seq_num as u64,
                    lowest_sequence_num_without_response: 0,
                },
            })
            .await;

        // Wait for the command to be applied
        assert_eq!(
            result_receiver.recv().await.unwrap(),
            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                client_id,
                sequence_num: seq_num as u64,
                content: CommandResponseContent::CommandApplied { output: cmd }
            })
        );
    }

    // Simulate follower sending six duplicated old AppendEntriesResponse(false)
    // we check if next_index - 1 doesn't underflow
    for _ in 0..6 {
        leader
            .send(RaftMessage {
                header: RaftMessageHeader {
                    term: 1, // current term
                    source: follower_id,
                },
                content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                    success: false,
                    last_verified_log_index: 0, // old index
                }),
            })
            .await;
    }

    // After this, leader should still be able to process new commands
    let new_command = vec![7, 8, 9];
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: new_command.clone(),
                client_id,
                sequence_num: 2,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 2,
            content: CommandResponseContent::CommandApplied { output: new_command }
        })
    );
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test]
#[timeout(2000)]
async fn cluster_progresses_with_disruptive_leader_partitioned() {
    // init_logger();
    let mut system = System::new().await;

    // 4 servers
    let processes = make_idents::<4>();
    let [leader1_id, follower1_id, follower2_id, disruptive_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();

    // All servers join the cluster
    let [leader1, _follower1, _follower2, disruptive] = make_rafts(
        &mut system,
        [leader1_id, follower1_id, follower2_id, disruptive_id],
        [100, 300, 300, 150], 
        boot,
        |_| Box::new(IdentityMachine),
        |_| Box::<RamStorage>::default(),
        sender.clone(),
        async |id, mref| sender.insert(id, mref).await,
        &processes,
    )
    .await;

    // Partition: Disruptive leader can send but not receive (all links to it are broken)
    for &from in &[leader1_id, follower1_id, follower2_id] {
        sender.break_link(from, disruptive_id).await;
    }

    // Wait for elections to settle
    sleep_ms(500).await;

    // Send a client command to the healthy cluster
    for _ in 0..3
    {
        let (result_sender, mut result_receiver) = unbounded_channel();
        let client_id = register_client(&leader1, &result_sender, &mut result_receiver).await;
    
        leader1
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: vec![42, 43, 44],
                    client_id,
                    sequence_num: 0,
                    lowest_sequence_num_without_response: 0,
                },
            })
            .await;
    
        // Should be processed by the healthy cluster
        assert_eq!(
            result_receiver.recv().await.unwrap(),
            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                client_id,
                sequence_num: 0,
                content: CommandResponseContent::CommandApplied {
                    output: vec![42, 43, 44]
                }
            })
        );

        // wait and allow disruptive server to disrupt
        sleep_ms(200).await;
    }

    // Try sending a command to the disruptive leader and check it does NOT get applied
    let (disruptive_result_sender, mut disruptive_result_receiver) = unbounded_channel();

    disruptive
        .send(ClientRequest {
            reply_to: disruptive_result_sender.clone(),
            content: ClientRequestContent::RegisterClient,
        })
        .await;

    // Wait a bit and check that no response is received (since disruptive is partitioned) or NotLeader response (if election timeout happened)
    sleep_ms(400).await;
    match disruptive_result_receiver.try_recv()
    {
        Ok(val) => {
            assert_eq!(
                val,
                ClientRequestResponse::RegisterClientResponse(
                    RegisterClientResponseArgs { 
                        content: RegisterClientResponseContent::NotLeader { 
                            leader_hint: None 
                }})
            )
        }
        Err(e) => {
            match e 
            {
                // TryRecvError::Empty
                TryRecvError::Empty => {
                    //ok
                }
                _ => { 
                    panic!("Got not an empty tryRecv error")
                }
            }
        }
    };

    system.shutdown().await;
}

