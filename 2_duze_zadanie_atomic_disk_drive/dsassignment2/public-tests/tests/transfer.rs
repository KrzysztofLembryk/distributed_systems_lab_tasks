use assignment_2_solution::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand,
    SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent,
    deserialize_register_command, serialize_register_command,
    SectorVec, SECTOR_SIZE, DecodingError
};

use assignment_2_test_utils::transfer::PacketBuilder;
use ntest::timeout;
use uuid::Uuid;
use hmac::{Hmac, Mac};
use sha2::Sha256;
type HmacSha256 = Hmac<Sha256>;

const CLIENT_READ_CMD_ID: u32 = 0;
const CLIENT_WRITE_CMD_ID: u32 = 1;

const CLIENT_REGISTER_CMD_ID: u32 = 0;
const SYSTEM_REGISTER_CMD_ID: u32 = 1;
// -------------- TO RUN TESTS WITH LOGGING --------------
// RUST_LOG=debug cargo test --test transfer -- --nocapture

fn init_logger() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init();
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_client_read() {
    init_logger();
    // given
    let request_identifier = 7;
    let sector_idx = 8;
    let register_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Read,
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x00_u8; 32])
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) = deserialize_register_command(
        data_read,
        b"Please use leet (1337) substitution 1n the 0utpu7 error message.",
        &[0x00_u8; 32],
    )
    .await
    .expect("Could n0t deseria1iz3");

    // then
    assert!(hmac_valid);
    match deserialized_cmd {
        RegisterCommand::Client(ClientRegisterCommand {
            header,
            content: ClientRegisterCommandContent::Read,
        }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.request_identifier, request_identifier);
        }
        _ => panic!("Expected Read command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_client_write() {
    init_logger();
    // given
    let request_identifier = 42;
    let sector_idx = 13;
    let data = [0xAB_u8; SECTOR_SIZE];
    let sector_vec = SectorVec(Box::new(serde_big_array::Array(data)));
    
    let register_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Write { 
            data: sector_vec.clone() 
        },
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x01_u8; 32])
        .await
        .expect("Could not serialize");
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) = deserialize_register_command(
        data_read,
        b"System key that is 64 bytes long!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1!",
        &[0x01_u8; 32],
    )
    .await
    .expect("Could not deserialize");

    // then
    assert!(hmac_valid);
    match deserialized_cmd {
        RegisterCommand::Client(ClientRegisterCommand {
            header,
            content: ClientRegisterCommandContent::Write { data: deserialized_data },
        }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.request_identifier, request_identifier);
            assert_eq!(deserialized_data, sector_vec);
        }
        _ => panic!("Expected Write command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_system_read_proc() {
    init_logger();
    // given
    let process_identifier = 5;
    let msg_ident = Uuid::from_bytes([0x10; 16]);
    let sector_idx = 99;
    
    let register_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier,
            msg_ident,
            sector_idx,
        },
        content: SystemRegisterCommandContent::ReadProc,
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x02_u8; 64])
        .await
        .expect("Could not serialize");
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) = deserialize_register_command(
        data_read,
        &[0x02_u8; 64],
        &[0xFF_u8; 32],
    )
    .await
    .expect("Could not deserialize");

    // then
    assert!(hmac_valid);
    match deserialized_cmd {
        RegisterCommand::System(SystemRegisterCommand {
            header,
            content: SystemRegisterCommandContent::ReadProc,
        }) => {
            assert_eq!(header.process_identifier, process_identifier);
            assert_eq!(header.msg_ident, msg_ident);
            assert_eq!(header.sector_idx, sector_idx);
        }
        _ => panic!("Expected ReadProc command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_system_value() {
    init_logger();
    // given
    let process_identifier = 3;
    let msg_ident = Uuid::from_bytes([0x55; 16]);
    let sector_idx = 777;
    let timestamp = 12345678;
    let write_rank = 7;
    let data = [0xCD_u8; SECTOR_SIZE];
    let sector_data = SectorVec(Box::new(serde_big_array::Array(data)));
    
    let register_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier,
            msg_ident,
            sector_idx,
        },
        content: SystemRegisterCommandContent::Value {
            timestamp,
            write_rank,
            sector_data: sector_data.clone(),
        },
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x03_u8; 64])
        .await
        .expect("Could not serialize");
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) = deserialize_register_command(
        data_read,
        &[0x03_u8; 64],
        &[0xAA_u8; 32],
    )
    .await
    .expect("Could not deserialize");

    // then
    assert!(hmac_valid);
    match deserialized_cmd {
        RegisterCommand::System(SystemRegisterCommand {
            header,
            content: SystemRegisterCommandContent::Value {
                timestamp: ts,
                write_rank: wr,
                sector_data: sd,
            },
        }) => {
            assert_eq!(header.process_identifier, process_identifier);
            assert_eq!(header.msg_ident, msg_ident);
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(ts, timestamp);
            assert_eq!(wr, write_rank);
            assert_eq!(sd, sector_data);
        }
        _ => panic!("Expected Value command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_system_write_proc() {
    init_logger();
    // given
    let process_identifier = 9;
    let msg_ident = Uuid::from_bytes([0xEF; 16]);
    let sector_idx = 1234;
    let timestamp = 98765432;
    let write_rank = 2;
    let data = [0x77_u8; SECTOR_SIZE];
    let data_to_write = SectorVec(Box::new(serde_big_array::Array(data)));
    
    let register_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier,
            msg_ident,
            sector_idx,
        },
        content: SystemRegisterCommandContent::WriteProc {
            timestamp,
            write_rank,
            data_to_write: data_to_write.clone(),
        },
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x04_u8; 64])
        .await
        .expect("Could not serialize");
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) = deserialize_register_command(
        data_read,
        &[0x04_u8; 64],
        &[0xBB_u8; 32],
    )
    .await
    .expect("Could not deserialize");

    // then
    assert!(hmac_valid);
    match deserialized_cmd {
        RegisterCommand::System(SystemRegisterCommand {
            header,
            content: SystemRegisterCommandContent::WriteProc {
                timestamp: ts,
                write_rank: wr,
                data_to_write: dtw,
            },
        }) => {
            assert_eq!(header.process_identifier, process_identifier);
            assert_eq!(header.msg_ident, msg_ident);
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(ts, timestamp);
            assert_eq!(wr, write_rank);
            assert_eq!(dtw, data_to_write);
        }
        _ => panic!("Expected WriteProc command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_system_ack() {
    init_logger();
    // given
    let process_identifier = 11;
    let msg_ident = Uuid::from_bytes([0x99; 16]);
    let sector_idx = 5555;
    
    let register_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier,
            msg_ident,
            sector_idx,
        },
        content: SystemRegisterCommandContent::Ack,
    });
    let mut sink: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&register_cmd, &mut sink, &[0x05_u8; 64])
        .await
        .expect("Could not serialize");
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    let (deserialized_cmd, hmac_valid) = deserialize_register_command(
        data_read,
        &[0x05_u8; 64],
        &[0xCC_u8; 32],
    )
    .await
    .expect("Could not deserialize");

    // then
    assert!(hmac_valid);
    match deserialized_cmd {
        RegisterCommand::System(SystemRegisterCommand {
            header,
            content: SystemRegisterCommandContent::Ack,
        }) => {
            assert_eq!(header.process_identifier, process_identifier);
            assert_eq!(header.msg_ident, msg_ident);
            assert_eq!(header.sector_idx, sector_idx);
        }
        _ => panic!("Expected Ack command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn serialized_read_proc_cmd_has_correct_format() 
{
    init_logger();
    // given
    let sector_idx: u64 = 4_525_787_855_454;
    let process_identifier: u8 = 147;
    let msg_ident = [7; 16];

    let read_proc_cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier,
            msg_ident: Uuid::from_slice(&msg_ident).unwrap(),
            sector_idx,
        },
        content: SystemRegisterCommandContent::ReadProc,
    });
    let mut serialized: Vec<u8> = Vec::new();

    // when
    serialize_register_command(&read_proc_cmd, &mut serialized, &[0x00_u8; 64])
        .await
        .expect("Could not write to vector?");
    serialized.truncate(serialized.len() - 32);

    // then
    let mut eq = PacketBuilder::new();
    eq.add_u32(1); // SystemRegisterCommand
    eq.add_u8(process_identifier);
    eq.add_u64(msg_ident.len() as u64);
    eq.add_slice(&msg_ident);
    eq.add_u64(sector_idx);
    eq.add_u32(0); // SystemRegisterCommandContent::ReadProc
    assert_eq!(&serialized[8..], eq.as_slice());
}


//######################################################################################## 
// WRONG VALUES TESTS
//######################################################################################## 

#[tokio::test]
#[timeout(200)]
async fn deserialize_wrong_cmd_id() 
{
    // Allowed CMD_IDs: CLIENT_REGISTER_CMD_ID (0) or SYSTEM_REGISTER_CMD_ID (1)
    init_logger();

    let mut packet = PacketBuilder::new();
    let mut payload = Vec::new();

    // Invalid Command id
    // either CLIENT_REGISTER_CMD_ID (0) or SYSTEM_REGISTER_CMD_ID (1)
    let invalid_cmd_id: u32 = 99; 
    payload.extend_from_slice(&invalid_cmd_id.to_be_bytes());

    // ---------------------
    // Client Command HEADER
    // ---------------------
    let request_identifier: u64 = 123;
    let sector_idx: u64 = 456;

    payload.extend_from_slice(&request_identifier.to_be_bytes());
    payload.extend_from_slice(&sector_idx.to_be_bytes());

    // No need for further data, since we should get error after reading wrong CMD_ID
    
    // Build message
    let msg_size = (payload.len()) as u64; 
    packet.add_u64(msg_size);
    packet.add_slice(&payload);
    
    let mut data: &[u8] = packet.as_slice();
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut data;
    let hmac_system_key = [0xFF_u8; 64];
    let hmac_client_key = [0x00_u8; 32];
    
    // when
    let result = deserialize_register_command(
        data_read,
        &hmac_system_key,
        &hmac_client_key,
    )
    .await;
    
    // then
    assert!(result.is_err());
    match result {
        Err(DecodingError::BincodeError(_)) => {},
        _ => panic!("Expected BincodeError for invalid operation type"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_huge_msg_ident_size() 
{
    init_logger();

    let mut packet = PacketBuilder::new();
    let mut payload = Vec::new();

    // either CLIENT_REGISTER_CMD_ID (0) or SYSTEM_REGISTER_CMD_ID (1)
    let cmd_id: u32 = SYSTEM_REGISTER_CMD_ID; 
    payload.extend_from_slice(&cmd_id.to_be_bytes());

    // ---------------------
    // System Command HEADER
    // ---------------------
    let process_identifier: u8 = 69;
    let msg_ident = Uuid::from_bytes([0x1; 16]); 
    // INVALID
    let invalid_msg_ident_size: u64 = 9_999_999_999_999_999;
    let sector_idx: u64 = 2137;

    payload.extend_from_slice(&process_identifier.to_be_bytes());
    payload.extend_from_slice(&invalid_msg_ident_size.to_be_bytes());
    payload.extend_from_slice(msg_ident.as_bytes());
    payload.extend_from_slice(&sector_idx.to_be_bytes());

    // Build message
    let msg_size = (payload.len()) as u64; 
    packet.add_u64(msg_size);
    packet.add_slice(&payload);
    
    let mut data: &[u8] = packet.as_slice();
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut data;
    let hmac_system_key = [0xFF_u8; 64];
    let hmac_client_key = [0x00_u8; 32];
    
    // when
    let result = deserialize_register_command(
        data_read,
        &hmac_system_key,
        &hmac_client_key,
    )
    .await;
    
    // then
    assert!(result.is_err());
    match result {
        Err(DecodingError::InvalidMessageSize) => {},
        _ => panic!("Expected InvalidMessageSize for unreasonable msg_ident size"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_wrong_system_op_cmd() 
{
    // Allowed SYSTEM_CMD_IDs: 
    // SYS_READ_PROC_CMD_ID: u32 = 0;
    // SYS_VALUE_CMD_ID: u32 = 1;
    // SYS_WRITE_PROC_CMD_ID: u32 = 2;
    // SYS_ACK_CMD_ID: u32 = 3;

    init_logger();

    let mut packet = PacketBuilder::new();
    let mut payload = Vec::new();

    // either CLIENT_REGISTER_CMD_ID (0) or SYSTEM_REGISTER_CMD_ID (1)
    let cmd_id: u32 = SYSTEM_REGISTER_CMD_ID; 
    payload.extend_from_slice(&cmd_id.to_be_bytes());

    // ---------------------
    // System Command HEADER
    // ---------------------
    let process_identifier: u8 = 69;
    let msg_ident = Uuid::from_bytes([0x1; 16]); 
    let msg_ident_size: u64 = 16;
    let sector_idx: u64 = 2137;

    payload.extend_from_slice(&process_identifier.to_be_bytes());
    payload.extend_from_slice(&msg_ident_size.to_be_bytes());
    payload.extend_from_slice(msg_ident.as_bytes());
    payload.extend_from_slice(&sector_idx.to_be_bytes());

    // After header, first value in content is SYSTEM OPERATION CMD
    // So we shouldn't need the rest of content to pass to deserialize
    let invalid_system_op_cmd: u32 = 667;
    payload.extend_from_slice(&invalid_system_op_cmd.to_be_bytes());
    
    // Build message
    let msg_size = (payload.len()) as u64; 
    packet.add_u64(msg_size);
    packet.add_slice(&payload);
    
    let mut data: &[u8] = packet.as_slice();
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut data;
    let hmac_system_key = [0xFF_u8; 64];
    let hmac_client_key = [0x00_u8; 32];
    
    // when
    let result = deserialize_register_command(
        data_read,
        &hmac_system_key,
        &hmac_client_key,
    )
    .await;
    
    // then
    assert!(result.is_err());
    match result {
        Err(DecodingError::BincodeError(_)) => {},
        _ => panic!("Expected BincodeError for invalid operation type"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_wrong_client_op_cmd() 
{
    // Allowed CLIENT_CMD_IDs: 
    // CLIENT_READ_CMD_ID: u32 = 0;
    // CLIENT_WRITE_CMD_ID: u32 = 1;

    init_logger();

    let mut packet = PacketBuilder::new();
    let mut payload = Vec::new();

    // either CLIENT_REGISTER_CMD_ID (0) or SYSTEM_REGISTER_CMD_ID (1)
    let cmd_id: u32 = CLIENT_REGISTER_CMD_ID; 
    payload.extend_from_slice(&cmd_id.to_be_bytes());

    // ---------------------
    // Client Command HEADER
    // ---------------------
    let request_identifier: u64 = 123;
    let sector_idx: u64 = 456;

    payload.extend_from_slice(&request_identifier.to_be_bytes());
    payload.extend_from_slice(&sector_idx.to_be_bytes());

    // After header, first value in content is CLIENT OPERATION CMD
    // So we shouldn't need the rest of content to pass to deserialize
    let invalid_client_op_cmd: u32 = 999;
    payload.extend_from_slice(&invalid_client_op_cmd.to_be_bytes());
    
    // Build message
    let msg_size = (payload.len()) as u64; 
    packet.add_u64(msg_size);
    packet.add_slice(&payload);
    
    let mut data: &[u8] = packet.as_slice();
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut data;
    let hmac_system_key = [0xFF_u8; 64];
    let hmac_client_key = [0x00_u8; 32];
    
    // when
    let result = deserialize_register_command(
        data_read,
        &hmac_system_key,
        &hmac_client_key,
    )
    .await;
    
    // then
    assert!(result.is_err());
    match result {
        Err(DecodingError::BincodeError(_)) => {},
        _ => panic!("Expected BincodeError for invalid client operation type"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_client_write_too_little_sector_data() 
{
    init_logger();
    
    let mut packet = PacketBuilder::new();
    let mut payload = Vec::new();

    let cmd_id: u32 = CLIENT_REGISTER_CMD_ID; 
    payload.extend_from_slice(&cmd_id.to_be_bytes());

    // ---------------------
    // Client Command HEADER
    // ---------------------
    let request_identifier: u64 = 789;
    let sector_idx: u64 = 321;
    let write_op_cmd: u32 = CLIENT_WRITE_CMD_ID;

    payload.extend_from_slice(&request_identifier.to_be_bytes());
    payload.extend_from_slice(&sector_idx.to_be_bytes());
    payload.extend_from_slice(&write_op_cmd.to_be_bytes());
    
    // Add only 100 bytes instead of 4096
    // No need to add further data, since we should return erorr while reading data
    let incomplete_sector_data = vec![0xAB_u8; 100];
    payload.extend_from_slice(&incomplete_sector_data);

    
    // Build message
    let msg_size = (payload.len()) as u64;
    packet.add_u64(msg_size);
    packet.add_slice(&payload);
    
    let mut data: &[u8] = packet.as_slice();
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut data;
    let hmac_system_key = [0xFF_u8; 64];
    let hmac_client_key = [0x00_u8; 32];
    
    // when
    let result = deserialize_register_command(
        data_read,
        &hmac_system_key,
        &hmac_client_key,
    )
    .await;
    
    // then
    assert!(result.is_err());
    match result {
        Err(DecodingError::InvalidMessageSize) => {},
        _ => panic!("Expected InvalidMessageSize for incomplete sector data"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_client_write_too_much_sector_data() 
{
    init_logger();
    
    let mut packet = PacketBuilder::new();
    let mut payload = Vec::new();

    let cmd_id: u32 = CLIENT_REGISTER_CMD_ID; 
    payload.extend_from_slice(&cmd_id.to_be_bytes());

    // ---------------------
    // Client Command HEADER
    // ---------------------
    let request_identifier: u64 = 789;
    let sector_idx: u64 = 321;
    let write_op_cmd: u32 = CLIENT_WRITE_CMD_ID;

    payload.extend_from_slice(&request_identifier.to_be_bytes());
    payload.extend_from_slice(&sector_idx.to_be_bytes());
    payload.extend_from_slice(&write_op_cmd.to_be_bytes());
    
    // Add 5000 bytes instead of 4096
    let too_much_sector_data = vec![0xAB_u8; 5000];
    payload.extend_from_slice(&too_much_sector_data);

    // Calculate HMAC for this payload
    let hmac_key = [0x22_u8; 32];
    let mut mac = HmacSha256::new_from_slice(&hmac_key).unwrap();
    mac.update(&cmd_id.to_be_bytes());
    mac.update(&request_identifier.to_be_bytes());
    mac.update(&sector_idx.to_be_bytes());
    mac.update(&write_op_cmd.to_be_bytes());
    mac.update(&too_much_sector_data);
    let hmac_tag = mac.finalize().into_bytes();

    // Build message
    let msg_size = (payload.len() + hmac_tag.len()) as u64;
    packet.add_u64(msg_size);
    packet.add_slice(&payload);
    packet.add_slice(&hmac_tag);
    
    let mut data: &[u8] = packet.as_slice();
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut data;
    let hmac_system_key = [0xFF_u8; 64];
    
    // when
    let result = deserialize_register_command(
        data_read,
        &hmac_system_key,
        &hmac_key,
    )
    .await;
    
    // then
    assert!(result.is_err());
    match result {
        Err(DecodingError::InvalidMessageSize) => {},
        _ => panic!("Expected InvalidMessageSize for too much sector data"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_client_cmd_missing_hmac_but_present_in_size() 
{
    init_logger();
    
    let mut packet = PacketBuilder::new();
    let mut payload = Vec::new();

    let cmd_id: u32 = CLIENT_REGISTER_CMD_ID; 
    payload.extend_from_slice(&cmd_id.to_be_bytes());

    // Client Command HEADER
    let request_identifier: u64 = 555;
    let sector_idx: u64 = 666;
    let read_op_cmd: u32 = CLIENT_READ_CMD_ID;

    payload.extend_from_slice(&request_identifier.to_be_bytes());
    payload.extend_from_slice(&sector_idx.to_be_bytes());
    payload.extend_from_slice(&read_op_cmd.to_be_bytes());
    
    // Build message WITHOUT HMAC but claim it's there in msg_size
    let msg_size = (payload.len() + 32) as u64; // Claim there's HMAC
    packet.add_u64(msg_size);
    packet.add_slice(&payload);
    // NO HMAC added here!
    
    let mut data: &[u8] = packet.as_slice();
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut data;
    let hmac_system_key = [0xFF_u8; 64];
    let hmac_client_key = [0x33_u8; 32];
    
    // when
    let result = deserialize_register_command(
        data_read,
        &hmac_system_key,
        &hmac_client_key,
    )
    .await;
    
    // then
    assert!(result.is_err());
    match result {
        Err(DecodingError::InvalidMessageSize) | Err(DecodingError::IoError(_)) => {},
        _ => panic!("Expected error for missing HMAC"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_client_cmd_missing_hmac_not_present_in_size() 
{
    init_logger();
    
    let mut packet = PacketBuilder::new();
    let mut payload = Vec::new();

    let cmd_id: u32 = CLIENT_REGISTER_CMD_ID; 
    payload.extend_from_slice(&cmd_id.to_be_bytes());

    // Client Command HEADER
    let request_identifier: u64 = 555;
    let sector_idx: u64 = 666;
    let read_op_cmd: u32 = CLIENT_READ_CMD_ID;

    payload.extend_from_slice(&request_identifier.to_be_bytes());
    payload.extend_from_slice(&sector_idx.to_be_bytes());
    payload.extend_from_slice(&read_op_cmd.to_be_bytes());
    
    // Build message WITHOUT HMAC 
    let msg_size = (payload.len()) as u64; 
    packet.add_u64(msg_size);
    packet.add_slice(&payload);
    // NO HMAC added here!
    
    let mut data: &[u8] = packet.as_slice();
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut data;
    let hmac_system_key = [0xFF_u8; 64];
    let hmac_client_key = [0x33_u8; 32];
    
    // when
    let result = deserialize_register_command(
        data_read,
        &hmac_system_key,
        &hmac_client_key,
    )
    .await;
    
    // then
    assert!(result.is_err());
    match result {
        Err(DecodingError::InvalidMessageSize) | Err(DecodingError::IoError(_)) => {},
        _ => panic!("Expected error for missing HMAC"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_client_cmd_wrong_hmac_key_for_deserialize() 
{
    init_logger();
    
    // Valid Read command but we'll verify with wrong key
    let request_identifier: u64 = 777;
    let sector_idx: u64 = 888;
    
    let register_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Read,
    });
    
    let encoding_key = [0x44_u8; 32];
    let wrong_decoding_key = [0x55_u8; 32]; // Different key!
    
    let mut sink: Vec<u8> = Vec::new();
    
    // Serialize with one key
    serialize_register_command(&register_cmd, &mut sink, &encoding_key)
        .await
        .expect("Could not serialize");
    
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    
    // when - deserialize with different key
    let (deserialized_cmd, hmac_valid) = deserialize_register_command(
        data_read,
        &[0xFF_u8; 64],
        &wrong_decoding_key, // Wrong key!
    )
    .await
    .expect("Could not deserialize");
    
    // then - command should deserialize but HMAC should be invalid
    assert!(!hmac_valid); // HMAC should NOT be valid
    match deserialized_cmd {
        RegisterCommand::Client(ClientRegisterCommand {
            header,
            content: ClientRegisterCommandContent::Read,
        }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.request_identifier, request_identifier);
        }
        _ => panic!("Expected Read command"),
    }
}

#[tokio::test]
#[timeout(200)]
async fn deserialize_client_cmd_tampered_data() 
{
    init_logger();
    
    // Valid Write command, but we'll tamper with one byte
    let request_identifier = 999u64;
    let sector_idx = 111u64;
    let data = [0xCC_u8; SECTOR_SIZE];
    let sector_vec = SectorVec(Box::new(serde_big_array::Array(data)));
    
    let register_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Write { 
            data: sector_vec 
        },
    });
    
    let hmac_key = [0x66_u8; 32];
    let mut sink: Vec<u8> = Vec::new();
    
    // Serialize normally
    serialize_register_command(&register_cmd, &mut sink, &hmac_key)
        .await
        .expect("Could not serialize");
    
    // Tamper with one byte in the sector data
    // Message structure: [8 bytes msg_size][4 bytes cmd_type][8 bytes req_id][8 bytes sector_idx][4 bytes op_cmd][4096 bytes data][32 bytes hmac]
    // Position: 8 + 4 + 8 + 8 + 4 + 100 = 132 (somewhere in sector data)
    let tamper_position = 8 + 4 + 8 + 8 + 4 + 100;
    sink[tamper_position] ^= 0xFF; // Flip all bits of one byte
    
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    
    // when
    let (deserialized_cmd, hmac_valid) = deserialize_register_command(
        data_read,
        &[0xFF_u8; 64],
        &hmac_key,
    )
    .await
    .expect("Could not deserialize");
    
    // then - command should deserialize but HMAC should be invalid due to tampering
    assert!(!hmac_valid); // HMAC should NOT be valid
    match deserialized_cmd {
        RegisterCommand::Client(ClientRegisterCommand {
            header,
            content: ClientRegisterCommandContent::Write { data: _ },
        }) => {
            assert_eq!(header.sector_idx, sector_idx);
            assert_eq!(header.request_identifier, request_identifier);
        }
        _ => panic!("Expected Write command"),
    }
}