use ntest::timeout;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::{Duration, Instant};

use module_system::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

fn init_logger() 
{
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn client_sessions_give_exactly_once_semantics() 
{
    let mut system = System::new().await;
    let (apply_sender, mut apply_receiver) = unbounded_channel();
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let leader = Raft::new(
        &mut system,
        make_config(
            leader_id,
            boot,
            Duration::from_millis(100),
            &processes.clone(),
        ),
        Box::new(SpyMachine { apply_sender }),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, boot, Duration::from_millis(300), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
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
    let mut responses = vec![];
    responses.push(result_receiver.recv().await.unwrap());
    responses.push(result_receiver.recv().await.unwrap());
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
    responses.push(result_receiver.recv().await.unwrap());

    for response in responses {
        assert_eq!(
            response,
            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                client_id,
                sequence_num: 0,
                content: CommandResponseContent::CommandApplied {
                    output: vec![1, 2, 3, 4]
                },
            })
        );
    }
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![1, 2, 3, 4]);
    assert!(apply_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(200)]
async fn client_sessions_are_expired() 
{
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (_sender, [leader, _follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    let client_id_1 = register_client(&leader, &result_sender, &mut result_receiver).await;
    let client_id_2 = register_client(&leader, &result_sender, &mut result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id: client_id_1,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    tokio::time::sleep(SESSION_EXPIRATION + Duration::from_millis(200)).await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id: client_id_2,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![9, 10, 11, 12],
                client_id: client_id_1,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_1,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            }
        })
    );
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_2,
            sequence_num: 0,
            content: CommandResponseContent::SessionExpired
        })
    );
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_1,
            sequence_num: 1,
            content: CommandResponseContent::SessionExpired
        })
    );
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(200)]
async fn acknowledged_outputs_are_discarded() 
{
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (_sender, [leader, _follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
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
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 1,
            },
        })
        .await;
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
    let response_2 = result_receiver.recv().await.unwrap();
    let response_3 = result_receiver.recv().await.unwrap();

    assert_eq!(
        response_1,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            }
        })
    );
    assert_eq!(
        response_2,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::CommandApplied {
                output: vec![5, 6, 7, 8]
            }
        })
    );
    assert_eq!(
        response_3,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::SessionExpired
        })
    );
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(100)]
async fn single_client_increasing_sequence_numbers_are_applied_in_order() {
    let mut system = System::new().await;
    let (apply_sender, mut apply_receiver) = unbounded_channel();
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let leader = Raft::new(
        &mut system,
        make_config(
            leader_id,
            boot,
            Duration::from_millis(100),
            &processes.clone(),
        ),
        Box::new(SpyMachine { apply_sender }),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, boot, Duration::from_millis(300), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    // Send commands with increasing sequence numbers
    for seq_num in 0..3 {
        leader
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: vec![seq_num as u8],
                    client_id,
                    sequence_num: seq_num,
                    lowest_sequence_num_without_response: 0,
                },
            })
            .await;
    }

    // Collect and check responses
    for seq_num in 0..3 {
        assert_eq!(
            result_receiver.recv().await.unwrap(),
            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                client_id,
                sequence_num: seq_num,
                content: CommandResponseContent::CommandApplied {
                    output: vec![seq_num as u8]
                },
            })
        );
        // Check that the state machine applied the command
        assert_eq!(apply_receiver.recv().await.unwrap(), vec![seq_num as u8]);
    }
    assert!(apply_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(100)]
async fn multiple_clients_interleaved_commands_are_handled_correctly() {
    let mut system = System::new().await;
    let (apply_sender, mut apply_receiver) = unbounded_channel();
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let leader = Raft::new(
        &mut system,
        make_config(
            leader_id,
            boot,
            Duration::from_millis(100),
            &processes.clone(),
        ),
        Box::new(SpyMachine { apply_sender }),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, boot, Duration::from_millis(300), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;

    let (result_sender_1, mut result_receiver_1) = unbounded_channel();
    let (result_sender_2, mut result_receiver_2) = unbounded_channel();

    sleep_ms(150).await;

    // Register two clients with separate senders/receivers
    let client_id_1 = register_client(&leader, &result_sender_1, &mut result_receiver_1).await;
    let client_id_2 = register_client(&leader, &result_sender_2, &mut result_receiver_2).await;

    // Interleave commands from both clients
    leader
        .send(ClientRequest {
            reply_to: result_sender_1.clone(),
            content: ClientRequestContent::Command {
                command: vec![10],
                client_id: client_id_1,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender_2.clone(),
            content: ClientRequestContent::Command {
                command: vec![20],
                client_id: client_id_2,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender_1.clone(),
            content: ClientRequestContent::Command {
                command: vec![11],
                client_id: client_id_1,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender_2.clone(),
            content: ClientRequestContent::Command {
                command: vec![21],
                client_id: client_id_2,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // Collect and check responses for both clients
    assert_eq!(
        result_receiver_1.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_1,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied { output: vec![10] }
        })
    );
    assert_eq!(
        result_receiver_2.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_2,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied { output: vec![20] }
        })
    );
    assert_eq!(
        result_receiver_1.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_1,
            sequence_num: 1,
            content: CommandResponseContent::CommandApplied { output: vec![11] }
        })
    );
    assert_eq!(
        result_receiver_2.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_2,
            sequence_num: 1,
            content: CommandResponseContent::CommandApplied { output: vec![21] }
        })
    );

    // Check state machine applied commands in order
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![10]);
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![20]);
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![11]);
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![21]);
    assert!(apply_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn command_on_follower_returns_not_leader() {
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let leader = Raft::new(
        &mut system,
        make_config(
            leader_id,
            boot,
            Duration::from_millis(100),
            &processes.clone(),
        ),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, boot, Duration::from_millis(300), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    // Send a command to the follower (not the leader)
    follower
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![42],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // Follower should respond with NotLeader and leader hint
    match result_receiver.recv().await.unwrap() {
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            content: CommandResponseContent::NotLeader { leader_hint, .. },
            ..
        }) => {
            assert_eq!(leader_hint, Some(leader_id));
        }
        other => panic!("Unexpected response: {:?}", other),
    }

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn register_client_on_follower_returns_not_leader() {
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let leader = Raft::new(
        &mut system,
        make_config(
            leader_id,
            boot,
            Duration::from_millis(100),
            &processes.clone(),
        ),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, boot, Duration::from_millis(300), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    // Send RegisterClient to the follower (not the leader)
    follower
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RegisterClient,
        })
        .await;

    // Follower should respond with NotLeader and leader hint
    match result_receiver.recv().await.unwrap() {
        ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
            content: RegisterClientResponseContent::NotLeader { leader_hint, .. },
            ..
        }) => {
            assert_eq!(leader_hint, Some(leader_id));
        }
        other => panic!("Unexpected response: {:?}", other),
    }

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(100)]
async fn command_result_is_cached_for_repeated_sequence_number() {
    let mut system = System::new().await;
    let (apply_sender, mut apply_receiver) = unbounded_channel();
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let leader = Raft::new(
        &mut system,
        make_config(
            leader_id,
            boot,
            Duration::from_millis(100),
            &processes.clone(),
        ),
        Box::new(SpyMachine { apply_sender }),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, boot, Duration::from_millis(300), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    // Send two commands with the same sequence number
    for _ in 0..2 {
        leader
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: vec![99, 100, 101],
                    client_id,
                    sequence_num: 42,
                    lowest_sequence_num_without_response: 0,
                },
            })
            .await;
    }

    // First response: should be applied
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 42,
            content: CommandResponseContent::CommandApplied {
                output: vec![99, 100, 101]
            }
        })
    );

    // Second response: should be from cache, identical to the first
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 42,
            content: CommandResponseContent::CommandApplied {
                output: vec![99, 100, 101]
            }
        })
    );

    // Only one application to the state machine
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![99, 100, 101]);
    assert!(apply_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
// #[timeout(900)]
async fn client_sessions_are_preserved_after_leader_change() 
{
    init_logger();
    let mut system = System::new().await;
    let (apply_sender, mut apply_receiver) = unbounded_channel();
    let processes = make_idents();
    let [leader_id, follower_0_id, follower_1_id, follower_2_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();

    // Start Raft nodes
    let leader = Raft::new(
        &mut system,
        make_config(
            leader_id,
            boot,
            Duration::from_millis(100),
            &processes.clone(),
        ),
        Box::new(SpyMachine { apply_sender }),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower_0 = Raft::new(
        &mut system,
        make_config(follower_0_id, boot, Duration::from_millis(200), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower_1 = Raft::new(
        &mut system,
        make_config(follower_1_id, boot, Duration::from_millis(300), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower_2 = Raft::new(
        &mut system,
        make_config(follower_2_id, boot, Duration::from_millis(450), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;

    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_0_id, Box::new(follower_0.clone())).await;
    sender.insert(follower_1_id, Box::new(follower_1.clone())).await;
    sender.insert(follower_2_id, Box::new(follower_2.clone())).await;


    let (result_sender, mut result_receiver) = unbounded_channel();

    // WAit for leader election
    sleep_ms(150).await;

    // Register client with the current leader
    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    // Send a command to the current leader
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // Make sure it was committed
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3]
            }
        })
    );
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![1, 2, 3]);

    // Simulate leader failure by breaking links so leader_to_be becomes leader
    sender.break_link_bidirectional(leader_id, follower_0_id).await;
    sender.break_link_bidirectional(leader_id, follower_1_id).await;
    sender.break_link_bidirectional(leader_id, follower_2_id).await;
    println!("LINKS BROKEN");
    // Wait for follower to become new leader
    sleep_ms(2969).await;
    // fix links
    // sender.fix_link_bidirectional(leader_id, follower_0_id).await;
    // sender.fix_link_bidirectional(leader_id, follower_1_id).await;
    // sender.fix_link_bidirectional(leader_id, follower_2_id).await;
    // Wait longer so that new follower is added to cluster
    // sleep_ms(900).await;

    for follower in [&follower_0, &follower_1, &follower_2] {
        follower
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: vec![4, 5, 6],
                    client_id,
                    sequence_num: 1,
                    lowest_sequence_num_without_response: 0,
                },
            })
            .await;
    }

    let mut session_expired_present = false;
    for _ in 0..3 {
        match result_receiver.recv().await.unwrap() {
            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                client_id: cid,
                sequence_num: 1,
                content: CommandResponseContent::SessionExpired
            }) if cid == client_id => { session_expired_present = true;}
            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                client_id: cid,
                sequence_num: 1,
                content: CommandResponseContent::NotLeader { .. }
            }) if cid == client_id => {}
            other => panic!("Unexpected response: {:?}", other),
        }
    }
    assert!(apply_receiver.is_empty());
    assert!(session_expired_present, "There is no SessionExpired response for client");

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn not_registered_client_gets_session_expired() 
{
    let mut system = System::new().await;
    let processes = make_idents();
    let [server_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let server = Raft::new(
        &mut system,
        make_config(
            server_id,
            boot,
            Duration::from_millis(100),
            &processes.clone(),
        ),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(server_id, Box::new(server.clone())).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // wait for election
    sleep_ms(250).await;

    // Generate a random client_id that was never registered
    let not_registered_client_id = uuid::Uuid::new_v4();

    // Send a command from the not registered client
    server
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![42],
                client_id: not_registered_client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // Should get SessionExpired
    match result_receiver.recv().await.unwrap() {
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::SessionExpired,
        }) if client_id == not_registered_client_id => {}
        other => panic!("Unexpected response: {:?}", other),
    }

    system.shutdown().await;
}

