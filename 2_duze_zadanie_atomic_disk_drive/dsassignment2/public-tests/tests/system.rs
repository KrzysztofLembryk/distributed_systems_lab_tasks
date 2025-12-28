use assignment_2_solution::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, Configuration,
    OperationReturn, PublicConfiguration, RegisterCommand, SectorVec, run_register_process,
    serialize_register_command,
    deserialize_register_command,
};
use assignment_2_test_utils::system::*;
use assignment_2_test_utils::transfer::PacketBuilder;
use hmac::Mac;
use ntest::timeout;
use serde_big_array::Array;
use std::convert::TryInto;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use log::debug;

fn init_logger() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init();
}

#[tokio::test]
#[timeout(4000)]
async fn single_process_system_completes_operation_read_without_write() 
{
    // given
    let hmac_client_key = [5; 32];
    let tcp_port = 30291;
    let storage_dir = tempdir().unwrap();
    let request_identifier = 1888;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut stream = TcpStream::connect(("127.0.0.1", tcp_port))
        .await
        .expect("Could not connect to TCP port");

    // when: send read command (no prior write)
    let read_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Read,
    });
    send_cmd(&read_cmd, &mut stream, &hmac_client_key).await;

    // then
    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(0); // Status ok
    expected.add_u64(request_identifier);
    expected.add_u32(0); // OperationReturn::Read
    expected.add_slice(&[0; 4096]); // sector data (default value)
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf_read = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf_read)
        .await
        .expect("Less data than expected");

    // asserts for read response
    let cmp_bytes = expected_len - HMAC_TAG_SIZE;
    assert_eq!(buf_read[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf_read[8..]));
}

#[tokio::test]
#[timeout(4000)]
async fn five_processes_system_completes_operation_read_without_write() {
    let hmac_client_key = [5; 32];
    let tcp_ports = [30310, 30311, 30312, 30313, 30314];
    let mut storage_dirs: Vec<_> = (0..5).map(|_| tempdir().unwrap()).collect();
    let request_identifier = 1888;

    // Spawn 5 register processes
    for i in 0..5 {
        let storage_dir = storage_dirs.remove(0);
        let config = Configuration {
            public: PublicConfiguration {
                tcp_locations: tcp_ports
                    .iter()
                    .map(|port| ("127.0.0.1".to_string(), *port))
                    .collect(),
                self_rank: (i + 1) as u8,
                n_sectors: 20,
                storage_dir: storage_dir.keep(),
            },
            hmac_system_key: [1; 64],
            hmac_client_key,
        };
        tokio::spawn(run_register_process(config));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut stream = TcpStream::connect(("127.0.0.1", tcp_ports[0]))
        .await
        .expect("Could not connect to TCP port");

    // when: send read command (no prior write)
    let read_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Read,
    });
    send_cmd(&read_cmd, &mut stream, &hmac_client_key).await;

    // then
    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(0); // Status ok
    expected.add_u64(request_identifier);
    expected.add_u32(0); // OperationReturn::Read
    expected.add_slice(&[0; 4096]); // sector data (default value)
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf_read = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf_read)
        .await
        .expect("Less data than expected");

    // asserts for read response
    let cmp_bytes = expected_len - HMAC_TAG_SIZE;
    assert_eq!(buf_read[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf_read[8..]));
}

#[tokio::test]
#[timeout(4000)]
async fn single_process_system_completes_operation_write() 
{
    // given
    let hmac_client_key = [5; 32];
    let tcp_port = 30_287;
    let storage_dir = tempdir().unwrap();
    let request_identifier = 1778;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));

    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", tcp_port))
        .await
        .expect("Could not connect to TCP port");
    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([3; 4096]))),
        },
    });

    // when
    send_cmd(&write_cmd, &mut stream, &hmac_client_key).await;

    // then
    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(0); // Status ok
    expected.add_u64(request_identifier);
    expected.add_u32(1); // OperationReturn::Write
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data then expected");

    // asserts for write response
    let cmp_bytes = expected_len - HMAC_TAG_SIZE;
    assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));
}

#[tokio::test]
#[timeout(4000)]
async fn five_processes_system_completes_operation_write() {
    let hmac_client_key = [5; 32];
    let tcp_ports = [30320, 30321, 30322, 30323, 30324];
    let mut storage_dirs: Vec<_> = (0..5).map(|_| tempdir().unwrap()).collect();
    let request_identifier = 1778;

    // Spawn 5 register processes
    for i in 0..5 {
        let storage_dir = storage_dirs.remove(0);
        let config = Configuration {
            public: PublicConfiguration {
                tcp_locations: tcp_ports
                    .iter()
                    .map(|port| ("127.0.0.1".to_string(), *port))
                    .collect(),
                self_rank: (i + 1) as u8,
                n_sectors: 20,
                storage_dir: storage_dir.keep(),
            },
            hmac_system_key: [1; 64],
            hmac_client_key,
        };
        tokio::spawn(run_register_process(config));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut stream = TcpStream::connect(("127.0.0.1", tcp_ports[0]))
        .await
        .expect("Could not connect to TCP port");

    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([3; 4096]))),
        },
    });

    // when
    send_cmd(&write_cmd, &mut stream, &hmac_client_key).await;

    // then
    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(0); // Status ok
    expected.add_u64(request_identifier);
    expected.add_u32(1); // OperationReturn::Write
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data than expected");

    // asserts for write response
    let cmp_bytes = expected_len - HMAC_TAG_SIZE;
    assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));
}

#[tokio::test]
#[timeout(4000)]
async fn single_process_system_completes_operation_write_read() 
{
    // given
    init_logger();
    let hmac_client_key = [5; 32];
    let tcp_port = 30_288;
    let storage_dir = tempdir().unwrap();
    let request_identifier = 1778;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));

    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", tcp_port))
        .await
        .expect("Could not connect to TCP port");

    // --- WRITE ---
    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([3; 4096]))),
        },
    });

    send_cmd(&write_cmd, &mut stream, &hmac_client_key).await;

    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(0); // Status ok
    expected.add_u64(request_identifier);
    expected.add_u32(1); // OperationReturn::Write
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data then expected");
    // asserts for write response
    let cmp_bytes = expected_len - HMAC_TAG_SIZE;
    assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));

    // --- READ ---
    let read_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: request_identifier + 1,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Read,
    });

    send_cmd(&read_cmd, &mut stream, &hmac_client_key).await;

    let mut expected_read = PacketBuilder::new();
    expected_read.add_u64(0); // size placeholder
    expected_read.add_u32(0); // Status ok
    expected_read.add_u64(request_identifier + 1);
    expected_read.add_u32(0); // OperationReturn::Read
    expected_read.add_slice(&[3; 4096]); // sector data
    expected_read.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected_read.update_size();
    let expected_read_len = expected_read.as_slice().len();

    let mut buf_read = vec![0u8; expected_read_len];

    stream
        .read_exact(&mut buf_read)
        .await
        .expect("Less data then expected");

    // asserts for read response
    let cmp_bytes_read = expected_read_len - HMAC_TAG_SIZE;
    assert_eq!(buf_read[..cmp_bytes_read], expected_read.as_slice()[..cmp_bytes_read]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf_read[8..]));
}

#[tokio::test]
#[timeout(4000)]
async fn five_processes_system_completes_operation_write_read() {
    // given
    // init_logger();
    let hmac_client_key = [5; 32];
    let tcp_ports = [30330, 30331, 30332, 30333, 30334];
    let mut storage_dirs: Vec<_> = (0..5).map(|_| tempdir().unwrap()).collect();
    let request_identifier = 1778;

    // Spawn 5 register processes
    for i in 0..5 {
        let storage_dir = storage_dirs.remove(0);
        let config = Configuration {
            public: PublicConfiguration {
                tcp_locations: tcp_ports
                    .iter()
                    .map(|port| ("127.0.0.1".to_string(), *port))
                    .collect(),
                self_rank: (i + 1) as u8,
                n_sectors: 20,
                storage_dir: storage_dir.keep(),
            },
            hmac_system_key: [1; 64],
            hmac_client_key,
        };
        tokio::spawn(run_register_process(config));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", tcp_ports[0]))
        .await
        .expect("Could not connect to TCP port");

    // --- WRITE ---
    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([3; 4096]))),
        },
    });

    send_cmd(&write_cmd, &mut stream, &hmac_client_key).await;

    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(0); // Status ok
    expected.add_u64(request_identifier);
    expected.add_u32(1); // OperationReturn::Write
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data than expected");
    let cmp_bytes = expected_len - HMAC_TAG_SIZE;
    assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));

    // --- READ ---
    let read_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: request_identifier + 1,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Read,
    });

    send_cmd(&read_cmd, &mut stream, &hmac_client_key).await;

    let mut expected_read = PacketBuilder::new();
    expected_read.add_u64(0); // size placeholder
    expected_read.add_u32(0); // Status ok
    expected_read.add_u64(request_identifier + 1);
    expected_read.add_u32(0); // OperationReturn::Read
    expected_read.add_slice(&[3; 4096]); // sector data
    expected_read.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected_read.update_size();
    let expected_read_len = expected_read.as_slice().len();

    let mut buf_read = vec![0u8; expected_read_len];

    stream
        .read_exact(&mut buf_read)
        .await
        .expect("Less data than expected");

    let cmp_bytes_read = expected_read_len - HMAC_TAG_SIZE;
    assert_eq!(buf_read[..cmp_bytes_read], expected_read.as_slice()[..cmp_bytes_read]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf_read[8..]));
}

#[tokio::test]
#[serial_test::serial]
#[timeout(30000)]
async fn concurrent_ops_on_the_same_sector_2_clients() {
    init_logger();
    // given
    let port_range_start = 21518;
    let n_clients = 2;
    let config = TestProcessesConfig::new(1, port_range_start);
    config.start().await;
    let mut streams = Vec::new();
    for _ in 0..n_clients {
        streams.push(config.connect(0).await);
    }
    // when
    for (i, stream) in streams.iter_mut().enumerate() {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: i.try_into().unwrap(),
                        sector_idx: 0,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(Box::new(Array([if i % 2 == 0 { 1 } else { 254 }; 4096]))),
                    },
                }),
                stream,
            )
            .await;
    }

    for stream in &mut streams {
        config.read_response(stream).await;
    }

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: n_clients,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Read,
            }),
            &mut streams[0],
        )
        .await;
    let response = config.read_response(&mut streams[0]).await;

    match response.content.op_return {
        OperationReturn::Read {
            read_data: SectorVec(sector),
        } => {
            assert!(*sector == Array([1; 4096]) || *sector == Array([254; 4096]));
        }
        _ => panic!("Expected read response"),
    }
}

#[tokio::test]
#[serial_test::serial]
#[timeout(30000)]
async fn concurrent_ops_on_the_same_sector_2_clients_5_processes() {
    // init_logger();
    // given
    let tcp_ports = [31518, 31519, 31520, 31521, 31522];
    let hmac_client_key = [5; 32];
    let mut storage_dirs: Vec<_> = (0..5).map(|_| tempdir().unwrap()).collect();
    let n_clients = 2;
    let request_identifier_base = 10000;

    // Spawn 5 register processes
    for i in 0..5 {
        let storage_dir = storage_dirs.remove(0);
        let config = Configuration {
            public: PublicConfiguration {
                tcp_locations: tcp_ports
                    .iter()
                    .map(|port| ("127.0.0.1".to_string(), *port))
                    .collect(),
                self_rank: (i + 1) as u8,
                n_sectors: 20,
                storage_dir: storage_dir.keep(),
            },
            hmac_system_key: [1; 64],
            hmac_client_key,
        };
        tokio::spawn(run_register_process(config));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect 2 clients to process 1
    let mut streams = Vec::new();
    for _ in 0..n_clients {
        streams.push(
            TcpStream::connect(("127.0.0.1", tcp_ports[0]))
                .await
                .expect("Could not connect to TCP port"),
        );
    }

    // when
    for (i, stream) in streams.iter_mut().enumerate() {
        send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: request_identifier_base + i as u64,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Write {
                    data: SectorVec(Box::new(Array([if i % 2 == 0 { 1 } else { 254 }; 4096]))),
                },
            }),
            stream,
            &hmac_client_key,
        )
        .await;
    }

    for stream in &mut streams {
        // Read and discard write responses
        let mut buf = vec![0u8; 8 + 4 + 8 + 4 + HMAC_TAG_SIZE];
        stream.read_exact(&mut buf).await.expect("Less data than expected");
    }

    // Do a read on sector 0 with client 0
    send_cmd(
        &RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: request_identifier_base + n_clients as u64,
                sector_idx: 0,
            },
            content: ClientRegisterCommandContent::Read,
        }),
        &mut streams[0],
        &hmac_client_key,
    )
    .await;

    let mut buf_read = vec![0u8; 8 + 4 + 8 + 4 + 4096 + HMAC_TAG_SIZE];
    streams[0]
        .read_exact(&mut buf_read)
        .await
        .expect("Less data than expected");

    // Check response type (OperationReturn::Read)
    let op_return = u32::from_be_bytes(buf_read[20..24].try_into().unwrap());
    assert_eq!(op_return, 0, "Expected OperationReturn::Read");

    // Check sectorvec value is either 1 or 254
    let sector_data = &buf_read[24..(24 + 4096)];
    let all_ones = sector_data.iter().all(|&b| b == 1);
    let all_254 = sector_data.iter().all(|&b| b == 254);
    assert!(
        all_ones || all_254,
        "SectorVec should be all 1 or all 254, got mixed values"
    );
}

#[tokio::test]
#[serial_test::serial]
#[timeout(30000)]
async fn concurrent_operations_on_the_same_sector() 
{
    init_logger();
    // given
    let port_range_start = 21519;
    let n_clients = 16;
    let config = TestProcessesConfig::new(1, port_range_start);
    config.start().await;
    let mut streams = Vec::new();
    for _ in 0..n_clients {
        streams.push(config.connect(0).await);
    }
    // when
    for (i, stream) in streams.iter_mut().enumerate() {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: i.try_into().unwrap(),
                        sector_idx: 0,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(Box::new(Array([if i % 2 == 0 { 1 } else { 254 }; 4096]))),
                    },
                }),
                stream,
            )
            .await;
    }

    for stream in &mut streams {
        config.read_response(stream).await;
    }

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: n_clients,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Read,
            }),
            &mut streams[0],
        )
        .await;
    let response = config.read_response(&mut streams[0]).await;

    match response.content.op_return {
        OperationReturn::Read {
            read_data: SectorVec(sector),
        } => {
            assert!(*sector == Array([1; 4096]) || *sector == Array([254; 4096]));
        }
        _ => panic!("Expected read response"),
    }
}

#[tokio::test]
#[serial_test::serial]
#[timeout(40000)]
async fn large_number_of_operations_execute_successfully() {
    // given
    let port_range_start = 21625;
    let commands_total = 32;
    let config = TestProcessesConfig::new(3, port_range_start);
    config.start().await;
    let mut stream = config.connect(2).await;

    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(Box::new(Array([cmd_idx as u8; 4096]))),
                    },
                }),
                &mut stream,
            )
            .await;
    }

    for _ in 0..commands_total {
        config.read_response(&mut stream).await;
    }

    // when
    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx + 256,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Read,
                }),
                &mut stream,
            )
            .await;
    }

    // then
    for _ in 0..commands_total {
        let response = config.read_response(&mut stream).await;
        match response.content.op_return {
            OperationReturn::Read {
                read_data: SectorVec(sector),
            } => {
                assert_eq!(
                    sector,
                    Box::new(Array(
                        [(response.content.request_identifier - 256) as u8; 4096]
                    ))
                )
            }
            _ => panic!("Expected read response"),
        }
    }
}

// #################################################################################
// ############################## INVALID MSGS #####################################
// #################################################################################
const OK_STATUS_CODE: u32 = 0;
const AUTH_FAIL_STATUS_CODE: u32 = 1;
const INVALID_SECTOR_STATUS_CODE: u32 = 2;
#[tokio::test]
#[timeout(4000)]
async fn single_process_system_rejects_invalid_sector() {
    // given
    // init_logger();
    let hmac_client_key = [5; 32];
    let tcp_port = 30289;
    let storage_dir = tempdir().unwrap();
    let request_identifier = 9999;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 10, // Only 10 sectors available
            storage_dir: storage_dir.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut stream = TcpStream::connect(("127.0.0.1", tcp_port))
        .await
        .expect("Could not connect to TCP port");

    // Try to write to an invalid sector index (out of bounds)
    let invalid_sector_idx = 100; // Out of bounds
    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: invalid_sector_idx,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([7; 4096]))),
        },
    });

    send_cmd(&write_cmd, &mut stream, &hmac_client_key).await;

    // then
    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(INVALID_SECTOR_STATUS_CODE); // Status error 
    expected.add_u64(request_identifier);
    expected.add_u32(1); // OperationReturn::Write
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data than expected");

    // Status should be non-zero (error)
    let status = u32::from_be_bytes(buf[8..12].try_into().unwrap());
    assert_eq!(status, INVALID_SECTOR_STATUS_CODE, "Expected error status for invalid sector");
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));
}

#[tokio::test]
#[timeout(4000)]
async fn three_clients_reject_invalid_sector() {
    // init_logger();
    let hmac_client_key = [5; 32];
    let tcp_port = 30300;
    let storage_dir = tempdir().unwrap();
    let request_identifier_base = 20000;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 10, // Only 10 sectors available
            storage_dir: storage_dir.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Open 3 clients
    let mut streams = Vec::new();
    for _ in 0..3 {
        let stream = TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("Could not connect to TCP port");
        streams.push(stream);
    }

    let invalid_sector_idx = 100; // Out of bounds

    // Each client sends a write to the same invalid sector
    for (i, stream) in streams.iter_mut().enumerate() {
        let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: request_identifier_base + i as u64,
                sector_idx: invalid_sector_idx,
            },
            content: ClientRegisterCommandContent::Write {
                data: SectorVec(Box::new(Array([7; 4096]))),
            },
        });
        send_cmd(&write_cmd, stream, &hmac_client_key).await;
    }

    // Each client should get the same error response
    for (i, stream) in streams.iter_mut().enumerate() {
        let mut expected = PacketBuilder::new();
        expected.add_u64(0); // size placeholder
        expected.add_u32(INVALID_SECTOR_STATUS_CODE); // Status error
        expected.add_u64(request_identifier_base + i as u64);
        expected.add_u32(1); // OperationReturn::Write
        expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
        expected.update_size();
        let expected_len = expected.as_slice().len();

        let mut buf = vec![0u8; expected_len];
        stream
            .read_exact(&mut buf)
            .await
            .expect("Less data than expected");

        let status = u32::from_be_bytes(buf[8..12].try_into().unwrap());
        assert_eq!(status, INVALID_SECTOR_STATUS_CODE, "Expected error status for invalid sector");
        assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));
    }
}

#[tokio::test]
#[timeout(4000)]
async fn single_process_system_rejects_invalid_hmac_key() 
{
    let hmac_client_key = [5; 32];
    let invalid_client_hmac_key = [9; 32]; // Different key than in config
    let tcp_port = 30400;
    let storage_dir = tempdir().unwrap();
    let request_identifier = 5555;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 10,
            storage_dir: storage_dir.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut stream = TcpStream::connect(("127.0.0.1", tcp_port))
        .await
        .expect("Could not connect to TCP port");

    // Send write command with invalid HMAC key
    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 3,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([8; 4096]))),
        },
    });

    // Use invalid_hmac_key for signing
    send_cmd(&write_cmd, &mut stream, &invalid_client_hmac_key).await;

    // then
    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(AUTH_FAIL_STATUS_CODE); // Status: auth fail
    expected.add_u64(request_identifier);
    expected.add_u32(1); // OperationReturn::Write
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data than expected");

    let status = u32::from_be_bytes(buf[8..12].try_into().unwrap());
    assert_eq!(status, AUTH_FAIL_STATUS_CODE, "Expected auth fail status for invalid HMAC");

    let cmp_bytes = expected_len - HMAC_TAG_SIZE;
    assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
    // HMAC tag in response is for the real key, so we check with the correct key
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));
}

#[tokio::test]
#[timeout(4000)]
async fn three_clients_reject_invalid_hmac_key() {
    let hmac_client_key = [5; 32];
    let invalid_keys = [[9; 32], [11; 32], [13; 32]];
    let tcp_port = 30401;
    let storage_dir = tempdir().unwrap();
    let request_identifier_base = 6000;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 10,
            storage_dir: storage_dir.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Open 3 clients, each with its own wrong hmac key
    let mut streams = Vec::new();
    for _ in 0..3 {
        let stream = TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("Could not connect to TCP port");
        streams.push(stream);
    }

    // Each client sends a write with its own wrong key
    for (i, stream) in streams.iter_mut().enumerate() {
        let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: request_identifier_base + i as u64,
                sector_idx: 4,
            },
            content: ClientRegisterCommandContent::Write {
                data: SectorVec(Box::new(Array([i as u8 + 1; 4096]))),
            },
        });
        send_cmd(&write_cmd, stream, &invalid_keys[i]).await;
    }

    // Each client should get an auth fail response
    for (i, stream) in streams.iter_mut().enumerate() {
        let mut expected = PacketBuilder::new();
        expected.add_u64(0); // size placeholder
        expected.add_u32(AUTH_FAIL_STATUS_CODE); // Status: auth fail
        expected.add_u64(request_identifier_base + i as u64);
        expected.add_u32(1); // OperationReturn::Write
        expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
        expected.update_size();
        let expected_len = expected.as_slice().len();

        let mut buf = vec![0u8; expected_len];
        stream
            .read_exact(&mut buf)
            .await
            .expect("Less data than expected");

        let status = u32::from_be_bytes(buf[8..12].try_into().unwrap());
        assert_eq!(status, AUTH_FAIL_STATUS_CODE, "Expected auth fail status for invalid HMAC");

        let cmp_bytes = expected_len - HMAC_TAG_SIZE;
        assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
        // HMAC tag in response is for the real key, so we check with the correct key
        assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));
    }
}

#[tokio::test]
#[timeout(4000)]
async fn single_process_system_rejects_invalid_packet_size() 
{
    let hmac_client_key = [5; 32];
    let tcp_port = 30402;
    let storage_dir = tempdir().unwrap();
    let request_identifier = 7000;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 10,
            storage_dir: storage_dir.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut stream = TcpStream::connect(("127.0.0.1", tcp_port))
        .await
        .expect("Could not connect to TCP port");

    // Build a valid write command
    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 3,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([8; 4096]))),
        },
    });

    // Serialize normally
    let mut data = Vec::new();
    serialize_register_command(&write_cmd, &mut data, &hmac_client_key)
        .await
        .unwrap();

    // Corrupt the size field (first 8 bytes)
    let wrong_size: u64 = (data.len() as u64) + 10; // Intentionally wrong
    data[..8].copy_from_slice(&wrong_size.to_be_bytes());

    // Send corrupted packet
    stream.write_all(&data).await.expect("Failed to send corrupted packet");

    // Try to read response, but expect connection drop (read_exact will fail)
    let mut buf = vec![0u8; 64]; // arbitrary size
    let result = stream.read_exact(&mut buf).await;

    // The server should drop the connection, so we expect an error
    assert!(result.is_err(), "Expected connection to be dropped due to invalid packet size");
}


// #################################################################################
// ############################## CONCURRENT WRITES ################################
// #################################################################################

#[tokio::test]
#[timeout(20000)]
async fn two_processes_16_clients_write_then_read_sector_0() 
{
    let hmac_client_key = [5; 32];
    let tcp_ports = [30500, 30501];
    let storage_dir1 = tempdir().unwrap();
    let storage_dir2 = tempdir().unwrap();
    let n_clients = 16;

    // Spawn two register processes
    let config1 = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![
                ("127.0.0.1".to_string(), tcp_ports[0]),
                ("127.0.0.1".to_string(), tcp_ports[1]),
            ],
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir1.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };
    let config2 = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![
                ("127.0.0.1".to_string(), tcp_ports[0]),
                ("127.0.0.1".to_string(), tcp_ports[1]),
            ],
            self_rank: 2,
            n_sectors: 20,
            storage_dir: storage_dir2.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config1));
    tokio::spawn(run_register_process(config2));
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect 16 clients to process 1
    let mut streams = Vec::new();
    for _ in 0..n_clients {
        let stream = TcpStream::connect(("127.0.0.1", tcp_ports[0]))
            .await
            .expect("Could not connect to TCP port");
        streams.push(stream);
    }

    // Each client does a Write to sector 0
    for (i, stream) in streams.iter_mut().enumerate() 
    {
        let value = if i % 2 == 0 { 1 } else { 254 };
        let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: i as u64,
                sector_idx: 0,
            },
            content: ClientRegisterCommandContent::Write {
                data: SectorVec(Box::new(Array([value; 4096]))),
            },
        });
        send_cmd(&write_cmd, stream, &hmac_client_key).await;
    }

    // Read and check all write responses using PacketBuilder
    for (i, stream) in streams.iter_mut().enumerate() {
        let mut expected = PacketBuilder::new();
        expected.add_u64(0); // size placeholder
        expected.add_u32(OK_STATUS_CODE); // Status ok
        expected.add_u64(i as u64); // request_identifier
        expected.add_u32(1); // OperationReturn::Write
        expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
        expected.update_size();
        let expected_len = expected.as_slice().len();

        let mut buf = vec![0u8; expected_len];
        stream.read_exact(&mut buf).await.expect("Less data than expected");

        let cmp_bytes = expected_len - HMAC_TAG_SIZE;
        assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
        assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));
    }

    // Do a Read on sector 0 with client 0
    let read_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: 99999,
            sector_idx: 0,
        },
        content: ClientRegisterCommandContent::Read,
    });
    send_cmd(&read_cmd, &mut streams[0], &hmac_client_key).await;

    // Read response
    let mut buf_read = vec![0u8; 8 + 4 + 8 + 4 + 4096 + HMAC_TAG_SIZE];
    streams[0]
        .read_exact(&mut buf_read)
        .await
        .expect("Less data than expected");

    // Check response type (OperationReturn::Read)
    let op_return = u32::from_be_bytes(buf_read[20..24].try_into().unwrap());
    assert_eq!(op_return, 0, "Expected OperationReturn::Read");

    // Check sectorvec value is either 1 or 254
    let sector_data = &buf_read[24..(24 + 4096)];
    let all_ones = sector_data.iter().all(|&b| b == 1);
    let all_254 = sector_data.iter().all(|&b| b == 254);
    assert!(
        all_ones || all_254,
        "SectorVec should be all 1 or all 254, got mixed values"
    );
}

#[tokio::test]
#[timeout(30000)]
async fn concurrent_writes_are_serialized() {
    use std::collections::HashMap;
    use assignment_2_solution::{SystemRegisterCommandContent, RegisterCommand};
    use tokio::net::TcpListener;

    let hmac_client_key = [5; 32];
    let hmac_system_key = [1; 64];
    let tcp_ports = [30600, 30601, 30602];
    let storage_dir1 = tempdir().unwrap();
    let storage_dir2 = tempdir().unwrap();
    let n_clients = 16;

    // Spawn two register processes
    let config1 = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![
                ("127.0.0.1".to_string(), tcp_ports[0]),
                ("127.0.0.1".to_string(), tcp_ports[1]),
                ("127.0.0.1".to_string(), tcp_ports[2]),
            ],
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir1.keep(),
        },
        hmac_system_key,
        hmac_client_key,
    };
    let config2 = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![
                ("127.0.0.1".to_string(), tcp_ports[0]),
                ("127.0.0.1".to_string(), tcp_ports[1]),
                ("127.0.0.1".to_string(), tcp_ports[2]),
            ],
            self_rank: 2,
            n_sectors: 20,
            storage_dir: storage_dir2.keep(),
        },
        hmac_system_key,
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config1));
    tokio::spawn(run_register_process(config2));

    // Start stub listener for system messages
    let listener = TcpListener::bind(("127.0.0.1", tcp_ports[2])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect 16 clients to process 1
    let mut streams = Vec::new();
    for _ in 0..n_clients {
        let stream = TcpStream::connect(("127.0.0.1", tcp_ports[0]))
            .await
            .expect("Could not connect to TCP port");
        streams.push(stream);
    }

    // Each client does a Write to sector 0
    for (i, stream) in streams.iter_mut().enumerate() {
        let value = if i % 2 == 0 { 1 } else { 254 };
        let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: i as u64,
                sector_idx: 0,
            },
            content: ClientRegisterCommandContent::Write {
                data: SectorVec(Box::new(Array([value; 4096]))),
            },
        });
        send_cmd(&write_cmd, stream, &hmac_client_key).await;
    }

    // Accept system connections from both register processes
    let mut receivers = Vec::new();
    for _ in 0..2 {
        let (receiver, _) = listener.accept().await.unwrap();
        receivers.push(receiver);
    }

    // Spawn tasks to check serialization of system writes
    let mut join_set = tokio::task::JoinSet::new();
    for mut receiver in receivers {
        let hmac_system_key = hmac_system_key.clone();
        let hmac_client_key = hmac_client_key.clone();
        join_set.spawn(async move {
            let mut data_written: HashMap<u64, SectorVec> = HashMap::new();
            loop {
                let (message, _) = deserialize_register_command(&mut receiver, &hmac_system_key, &hmac_client_key)
                    .await
                    .unwrap();
                let RegisterCommand::System(cmd) = message else { continue; };
                let SystemRegisterCommandContent::WriteProc { timestamp, write_rank: _, data_to_write } = cmd.content else { continue; };

                if let Some(val) = data_written.get(&timestamp) {
                    assert_eq!(val.0, data_to_write.0, "System write for same timestamp must be identical");
                }
                data_written.insert(timestamp, data_to_write);
                if timestamp >= n_clients as u64 {
                    break;
                }
            }
        });
    }
    // Wait for at least one system task to finish
    join_set.join_next().await;

    // Read and check all write responses using PacketBuilder
    for (i, stream) in streams.iter_mut().enumerate() {
        let mut expected = PacketBuilder::new();
        expected.add_u64(0); // size placeholder
        expected.add_u32(OK_STATUS_CODE); // Status ok
        expected.add_u64(i as u64); // request_identifier
        expected.add_u32(1); // OperationReturn::Write
        expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
        expected.update_size();
        let expected_len = expected.as_slice().len();

        let mut buf = vec![0u8; expected_len];
        stream.read_exact(&mut buf).await.expect("Less data than expected");

        let cmp_bytes = expected_len - HMAC_TAG_SIZE;
        assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
        assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));
    }

    // Do a Read on sector 0 with client 0
    let read_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: 99999,
            sector_idx: 0,
        },
        content: ClientRegisterCommandContent::Read,
    });
    send_cmd(&read_cmd, &mut streams[0], &hmac_client_key).await;

    // Read response and check with PacketBuilder
    let mut buf_read = vec![0u8; 8 + 4 + 8 + 4 + 4096 + HMAC_TAG_SIZE];
    streams[0]
        .read_exact(&mut buf_read)
        .await
        .expect("Less data than expected");

    let op_return = u32::from_be_bytes(buf_read[20..24].try_into().unwrap());
    assert_eq!(op_return, 0, "Expected OperationReturn::Read");

    let sector_data = &buf_read[24..(24 + 4096)];
    let all_ones = sector_data.iter().all(|&b| b == 1);
    let all_254 = sector_data.iter().all(|&b| b == 254);
    assert!(
        all_ones || all_254,
        "SectorVec should be all 1 or all 254, got mixed values"
    );

    let mut expected_read = PacketBuilder::new();
    expected_read.add_u64(0); // size placeholder
    expected_read.add_u32(OK_STATUS_CODE); // Status ok
    expected_read.add_u64(99999);
    expected_read.add_u32(0); // OperationReturn::Read
    if all_ones {
        expected_read.add_slice(&[1; 4096]);
    } else {
        expected_read.add_slice(&[254; 4096]);
    }
    expected_read.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected_read.update_size();
    let expected_read_len = expected_read.as_slice().len();

    let cmp_bytes_read = expected_read_len - HMAC_TAG_SIZE;
    assert_eq!(buf_read[..cmp_bytes_read], expected_read.as_slice()[..cmp_bytes_read]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf_read[8..]));
}

// #################################################################################
// ############################## CRASHSES #########################################
// #################################################################################

// To write below test you need to manually start and crash main.rs file from 
// _testing_server folder
#[tokio::test]
#[serial_test::serial]
#[timeout(60000)]
async fn two_processes_write_then_crash_and_recover_read() 
{
    init_logger();
    let hmac_client_key = [5; 32];
    let tcp_ports = [31210, 31211];
    let storage_dir1 = tempdir().unwrap();
    let request_identifier = 4242;

    // Prepare configs
    let config1 = Configuration {
        public: PublicConfiguration {
            tcp_locations: tcp_ports
                .iter()
                .map(|port| ("127.0.0.1".to_string(), *port))
                .collect(),
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir1.keep(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    let _ = tokio::spawn(run_register_process(config1));

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect client to process 1
    let mut stream = TcpStream::connect(("127.0.0.1", tcp_ports[0]))
        .await
        .expect("Could not connect to TCP port");

    // --- WRITE ---
    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 7,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([9; 4096]))),
        },
    });

    send_cmd(&write_cmd, &mut stream, &hmac_client_key).await;

    // Check write response
    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(0); // Status ok
    expected.add_u64(request_identifier);
    expected.add_u32(1); // OperationReturn::Write
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data than expected");
    let cmp_bytes = expected_len - HMAC_TAG_SIZE;
    assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));

    debug!("Write cmd passed, NOW IS THE TIME TO CRASH PROCESS");
    // --- MANUALLY crash process 2 ---
    tokio::time::sleep(Duration::from_millis(7000)).await;

    debug!("Proc 2 should be crashed, sending READ");
    // --- READ (while only one process is up) ---
    let read_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: request_identifier + 1,
            sector_idx: 7,
        },
        content: ClientRegisterCommandContent::Read,
    });

    send_cmd(&read_cmd, &mut stream, &hmac_client_key).await;

    // Restore manually other process

    // --- Read response should still be correct ---
    let mut expected_read = PacketBuilder::new();
    expected_read.add_u64(0); // size placeholder
    expected_read.add_u32(0); // Status ok
    expected_read.add_u64(request_identifier + 1);
    expected_read.add_u32(0); // OperationReturn::Read
    expected_read.add_slice(&[9; 4096]); // sector data
    expected_read.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected_read.update_size();
    let expected_read_len = expected_read.as_slice().len();

    let mut buf_read = vec![0u8; expected_read_len];
    stream
        .read_exact(&mut buf_read)
        .await
        .expect("Less data than expected");

    let cmp_bytes_read = expected_read_len - HMAC_TAG_SIZE;
    assert_eq!(buf_read[..cmp_bytes_read], expected_read.as_slice()[..cmp_bytes_read]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf_read[8..]));
}

async fn send_cmd(register_cmd: &RegisterCommand, stream: &mut TcpStream, hmac_client_key: &[u8]) {
    let mut data = Vec::new();
    serialize_register_command(register_cmd, &mut data, hmac_client_key)
        .await
        .unwrap();

    stream.write_all(&data).await.expect("TEST - send_cmd - write_all returned error");
}

fn hmac_tag_is_ok(key: &[u8], data: &[u8]) -> bool {
    let boundary = data.len() - HMAC_TAG_SIZE;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(&data[..boundary]);
    mac.verify_slice(&data[boundary..]).is_ok()
}
