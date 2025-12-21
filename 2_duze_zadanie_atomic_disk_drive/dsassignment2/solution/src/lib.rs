mod domain;
mod storage;
mod atomic_reg;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;

pub async fn run_register_process(config: Configuration) {
    // I get config here, and from config I need to create a register process
    // Each process will communicate with other processes by TCP
    unimplemented!()
}

pub mod atomic_register_public {
    use crate::{
        ClientCommandResponse, ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager,
        SystemRegisterCommand,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Handle a client command. After the command is completed, we expect
        /// callback to be called. Note that completion of client command happens 
        /// after delivery of multiple system commands to the register, as the 
        /// algorithm specifies.
        ///
        /// This function corresponds to the handlers of Read and Write events in the
        /// (N,N)-AtomicRegister algorithm.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Handle a system command.
        ///
        /// This function corresponds to the handlers of `SystemRegisterCommand` 
        /// messages in the (N,N)-AtomicRegister algorithm.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the 
    /// system).
    /// Communication with other processes of the system is to be done by 
    /// `register_client`.
    /// And sectors must be stored in the `sectors_manager` instance.
    ///
    /// This function corresponds to the handlers of Init and Recovery events in the
    /// (N,N)-AtomicRegister algorithm.
    pub async fn build_atomic_register(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Box<dyn AtomicRegister> {
        unimplemented!()
    }
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;
    use crate::storage::stable_sector_manager::StableSectorManager;

    // 64 file_descr for one client
    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        /// Returns 4096 bytes of sector data by index.
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;

        /// Returns timestamp and write rank of the process which has saved this 
        /// data.
        /// Timestamps and ranks are relevant for atomic register algorithm, and are 
        /// described there.
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        return Arc::new(StableSectorManager::recover(path).await);
    }
}

pub mod transfer_public {
    use crate::{ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand, SECTOR_SIZE, SectorVec, SystemCommandHeader, SystemRegisterCommandContent, SystemRegisterCommand};
    use bincode::error::{DecodeError, EncodeError};
    use sha2::Sha256;
    use std::io::Error;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use hmac::{Hmac, Mac};
    use crate::domain::{CLIENT_READ_CMD_ID, CLIENT_WRITE_CMD_ID, SYS_READ_PROC_CMD_ID, SYS_VALUE_CMD_ID, SYS_WRITE_PROC_CMD_ID, SYS_ACK_CMD_ID,
    MSG_IDENT_LEN};
    use uuid::Uuid;
    use log::{debug};

    type HmacSha256 = Hmac<Sha256>;

    const HMAC_TAG_SIZE: usize = 32;
    const HMAC_CLIENT_KEY_SIZE: usize = 32;
    const HMAC_SYSTEM_KEY_SIZE: usize = 64;
    const CLIENT_REGISTER_CMD_ID: u32 = 0;
    const SYSTEM_REGISTER_CMD_ID: u32 = 1;

    #[derive(Debug)]
    pub enum EncodingError {
        IoError(Error),
        BincodeError(EncodeError),
    }

    #[derive(Debug, derive_more::Display)]
    pub enum DecodingError {
        IoError(Error),
        BincodeError(DecodeError),
        InvalidMessageSize,
    }

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), DecodingError> 
    {
        let mut reader = CmdDeserializer::new(data);
        // cmd_size is payload + Hmac
        let read_cmd_size = reader.read_cmd_size_u64().await?;
        // Either SystemRegisterCommand or ClientRegisterCommand
        let cmd_type = reader.read_u32().await?;

        match cmd_type
        {
            CLIENT_REGISTER_CMD_ID => {
                return deserialize_client_cmd(
                    read_cmd_size, 
                    cmd_type, 
                    hmac_client_key, 
                    &mut reader
                ).await;
            },
            SYSTEM_REGISTER_CMD_ID => {
                return deserialize_system_cmd(
                    read_cmd_size, 
                    cmd_type, 
                    hmac_system_key, 
                    &mut reader
                ).await;
            },
            _ => {
                return Err(DecodingError::BincodeError(
                    DecodeError::Other(
                        "While deserializing we read value for Command Type which is neither SystemRegisterCommand nor ClientRegisterCommand"
                )));
            }
        }
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), EncodingError> 
    {
        debug!("serialize_register_command - start");

        if hmac_key.len() != HMAC_CLIENT_KEY_SIZE
        && hmac_key.len() != HMAC_SYSTEM_KEY_SIZE
        {
            return Err(EncodingError::BincodeError(
                EncodeError::Other(
                    "During serialization of RegisterCommand provided hmac_key has size not equal to expected size: 32 or 64"
            )))
        }

        match cmd
        {
            RegisterCommand::Client(c_cmd) => {
                let mut payload: Vec<u8> = c_cmd.header.encode();
                payload.extend(c_cmd.content.encode());

                let encoded_cmd = do_encoding(
                    CLIENT_REGISTER_CMD_ID, 
                    &payload, 
                    hmac_key
                );

                match writer.write_all(&encoded_cmd).await
                {
                    Ok(_) => {},
                    Err(e) => {
                        return Err(EncodingError::IoError(e))
                    }
                }
            },

            RegisterCommand::System(s_cmd) => {
                let mut payload: Vec<u8> = s_cmd.header.encode();
                payload.extend(s_cmd.content.encode());

                let encoded_cmd = do_encoding(
                    SYSTEM_REGISTER_CMD_ID, 
                    &payload, 
                    hmac_key
                );

                match writer.write_all(&encoded_cmd).await
                {
                    Ok(_) => {},
                    Err(e) => {
                        return Err(EncodingError::IoError(e))
                    }
                }
            }
        }
        return Ok(());
    }

    async fn deserialize_system_cmd<'a> (
        read_cmd_size: u64,
        cmd_type: u32,
        hmac_system_key: &[u8; HMAC_SYSTEM_KEY_SIZE],
        reader: &'a mut CmdDeserializer<'a>
    ) -> Result<(RegisterCommand, bool), DecodingError> 
    {
        debug!("deserialize_system_cmd - start");
        // --------------
        // READING HEADER
        // --------------
        let process_identifier: u8 = reader.read_u8().await?;
        let msg_ident_len: u64 = reader.read_u64().await?;

        // Uuid is always 16 bytes
        if msg_ident_len != MSG_IDENT_LEN
        {
            return Err(DecodingError::InvalidMessageSize);
        }

        let msg_ident_vec = reader.read_16_bytes().await?;
        let sector_idx: u64 = reader.read_u64().await?;

        // We need to convert msg_ident_vec to Uuid
        let msg_ident_bytes: [u8; MSG_IDENT_LEN as usize] = msg_ident_vec
            .try_into()
            .map_err(|_| DecodingError::InvalidMessageSize)?;
        let msg_ident_uuid = Uuid::from_bytes(msg_ident_bytes);

        let header = SystemCommandHeader {
            process_identifier,
            msg_ident: msg_ident_uuid,
            sector_idx
        };

        // Calculate part of mac for header 
        let mut mac = HmacSha256::new_from_slice(hmac_system_key).unwrap();
        mac.update(&cmd_type.to_be_bytes());
        mac.update(&process_identifier.to_be_bytes());
        mac.update(&msg_ident_len.to_be_bytes());
        mac.update(&msg_ident_bytes);
        mac.update(&sector_idx.to_be_bytes());

        // ---------------
        // READING CONTENT
        // ---------------
        let system_op_cmd: u32 = reader.read_u32().await?;
        mac.update(&system_op_cmd.to_be_bytes());

        let content: SystemRegisterCommandContent;

        match system_op_cmd
        {
            SYS_READ_PROC_CMD_ID => {
                debug!("deserialize_system_cmd - command:  READ");
                content = SystemRegisterCommandContent::ReadProc;
            },
            SYS_VALUE_CMD_ID => {
                debug!("deserialize_system_cmd - command:  VALUE");
                let t_stamp: u64 = reader.read_u64().await?;
                let write_rank: u8 = reader.read_u8().await?;
                let sector_data: SectorVec = reader.read_sector_data().await?;
            
                mac.update(&t_stamp.to_be_bytes());
                mac.update(&write_rank.to_be_bytes());
                mac.update(&sector_data.0.0);

                content = SystemRegisterCommandContent::Value { 
                    timestamp: t_stamp, 
                    write_rank, 
                    sector_data 
                };
            },
            SYS_WRITE_PROC_CMD_ID => {
                debug!("deserialize_system_cmd - command: WRITE");
                let t_stamp: u64 = reader.read_u64().await?;
                let write_rank: u8 = reader.read_u8().await?;
                let data_to_write: SectorVec = reader.read_sector_data().await?;
            
                mac.update(&t_stamp.to_be_bytes());
                mac.update(&write_rank.to_be_bytes());
                mac.update(&data_to_write.0.0);

                content = SystemRegisterCommandContent::WriteProc  { 
                    timestamp: t_stamp, 
                    write_rank, 
                    data_to_write 
                };
            },
            SYS_ACK_CMD_ID => {
                debug!("deserialize_system_cmd - command: ACK");
                content = SystemRegisterCommandContent::Ack;
            },
            _ => {
                return Err(DecodingError::BincodeError(
                    DecodeError::Other(
                        "While deserializing System Command we read value for System Command which is neither READ, VALUE, WRITE nor ACK"
                )));
            }
        }

        let read_hmac_tag = reader.read_32_bytes().await?;
        let calc_hmac_tag: [u8; HMAC_TAG_SIZE] = mac.finalize().into_bytes().into();
        let is_hmac_valid: bool = read_hmac_tag == calc_hmac_tag;

        // This means that provided size in data is greater than the
        // size we read (there still might be some more data inside
        // reader but we don't care)
        if reader.size_read() != read_cmd_size
        {
            debug!("deserialize_system_cmd - reader.size_read ({}) != read_cmd_size ({})", reader.size_read(), read_cmd_size);
            return Err(DecodingError::InvalidMessageSize);
        }

        return Ok(
            (
                RegisterCommand::System(SystemRegisterCommand { header, content }),
                is_hmac_valid
            )
        );
    }

    async fn deserialize_client_cmd<'a> (
        read_cmd_size: u64,
        cmd_type: u32,
        hmac_client_key: &[u8; HMAC_CLIENT_KEY_SIZE],
        reader: &'a mut CmdDeserializer<'a>
    ) -> Result<(RegisterCommand, bool), DecodingError> 
    {
        debug!("deserialize_client_cmd - start");
        // --------------
        // READING HEADER
        // --------------
        let request_identifier = reader.read_u64().await?; 
        let sector_idx = reader.read_u64().await?;

        let header: ClientCommandHeader = ClientCommandHeader { 
            request_identifier, 
            sector_idx 
        };

        let mut mac = HmacSha256::new_from_slice(hmac_client_key).unwrap();
        mac.update(&cmd_type.to_be_bytes());
        mac.update(&request_identifier.to_be_bytes());
        mac.update(&sector_idx.to_be_bytes());

        // ---------------
        // READING CONTENT
        // ---------------
        let client_op_cmd = reader.read_u32().await?;
        mac.update(&client_op_cmd.to_be_bytes());

        let content: ClientRegisterCommandContent;

        match client_op_cmd
        {
            CLIENT_READ_CMD_ID => {
                debug!("deserialize_client_cmd - command: READ");
                content = ClientRegisterCommandContent::Read;
            },
            CLIENT_WRITE_CMD_ID => {
                debug!("deserialize_client_cmd - command: WRITE");
                // If there is to little data, reader returns error
                let sector_data = reader.read_sector_data().await?;
            
                // sector data is part of our payload, so we update mac
                // we need to do .0.0 to access data in tuple and then inside array
                mac.update(&sector_data.0.0);

                content = ClientRegisterCommandContent::Write { 
                    data: sector_data 
                };
            },
            _ => {
                return Err(DecodingError::BincodeError(
                    DecodeError::Other(
                        "While deserializing we read value for Client Command which is neither READ nor WRITE"
                )));
            }
        }

        let read_hmac_tag = reader.read_32_bytes().await?;
        let calc_hmac_tag: [u8; HMAC_TAG_SIZE] = mac.finalize().into_bytes().into();
        let is_hmac_valid: bool = read_hmac_tag == calc_hmac_tag;

        // This means that provided size in data is greater than the
        // size we read (there still might be some more data inside
        // reader but we don't care)
        if reader.size_read() != read_cmd_size
        {
            debug!("deserialize_client_cmd - reader.size_read ({}) != read_cmd_size ({}", reader.size_read(), read_cmd_size);
            return Err(DecodingError::InvalidMessageSize);
        }
        // TODO: should we check if reader.read_u8 returns error 
        // here? This would indicate that there is still some data
        // so the message is wrong

        return Ok(
            (
                RegisterCommand::Client(ClientRegisterCommand { header, content }),
                is_hmac_valid
            )
        );
    }

    fn do_encoding(
        command_id: u32,
        payload: &Vec<u8>,
        hmac_key: &[u8],
    ) -> Vec<u8>
    {
        let mut encoded_cmd: Vec<u8> = Vec::new();
        let cmd_id_size = std::mem::size_of::<u32>();
        let msg_size: u64 = (payload.len() + HMAC_TAG_SIZE + cmd_id_size) as u64;

        // Initialize a new MAC instance from the hmac key:
        let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
        mac.update(&command_id.to_be_bytes());
        mac.update(&payload);
        let hmac_tag = mac.finalize().into_bytes();

        encoded_cmd.extend_from_slice(&msg_size.to_be_bytes());
        encoded_cmd.extend_from_slice(&command_id.to_be_bytes());
        encoded_cmd.extend_from_slice(&payload);
        encoded_cmd.extend_from_slice(&hmac_tag);

        return encoded_cmd;
    }

    // ##############################################################################
    // Similarly to Tests' PacketBuilder I created CmdDeserializer that makes reading
    // received data easy and intuitive with error handling
    // ##############################################################################
    struct CmdDeserializer<'a> {
        // 'a - reader will live at least as long as CmdDeserializer
        reader: &'a mut (dyn AsyncRead + Send + Unpin),
        size_read: u64,
    }

    impl <'a> CmdDeserializer<'a>
    {
        fn new(data: &'a mut (dyn AsyncRead + Send + Unpin)) -> Self 
        {
            debug!("CmdDeserializer::new()");
            CmdDeserializer {reader: data, size_read: 0}
        }

        async fn read_u8(&mut self) -> Result<u8, DecodingError>
        {
            let mut buf = [0u8; 1];
            self.reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        debug!("CmdDeserializer::read_u8 - unexpectedEof");
                        DecodingError::InvalidMessageSize
                    },
                    _ => {
                        DecodingError::IoError(e)
                    }
                })?;
            self.size_read += 1;
            return Ok(buf[0]);
        }

        async fn read_u32(&mut self) -> Result<u32, DecodingError>
        {
            let mut buf = [0u8; 4];
            self.reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        debug!("CmdDeserializer::read_u32 - unexpectedEof");
                        DecodingError::InvalidMessageSize
                    },
                    _ => {
                        DecodingError::IoError(e)
                    }
                })?;
            self.size_read += 4;
            return Ok(u32::from_be_bytes(buf));
        }

        async fn read_u64(&mut self) -> Result<u64, DecodingError>
        {
            let mut buf = [0u8; 8];
            self.reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        debug!("CmdDeserializer::read_u64 - unexpectedEof");
                        DecodingError::InvalidMessageSize
                    },
                    _ => {
                        DecodingError::IoError(e)
                    }
                })?;
            self.size_read += 8;
            return Ok(u64::from_be_bytes(buf));
        }

        /// The same as read_u64 apart from that we **don't increase size_read**
        async fn read_cmd_size_u64(&mut self) -> Result<u64, DecodingError>
        {
            let mut buf = [0u8; 8];
            self.reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {

                        debug!("CmdDeserializer::read_cmd_size_u64 - unexpectedEof");
                        DecodingError::InvalidMessageSize
                    },
                    _ => {
                        DecodingError::IoError(e)
                    }
                })?;
            return Ok(u64::from_be_bytes(buf));
        }

        async fn read_32_bytes(&mut self) -> Result<[u8; 32], DecodingError>
        {
            let mut buf = [0u8; 32];
            self.reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        debug!("CmdDeserializer::read_32_bytes - unexpectedEof");
                        DecodingError::InvalidMessageSize
                    },
                    _ => {
                        DecodingError::IoError(e)
                    }
                })?;
            self.size_read += 32;
            return Ok(buf);
        }

        async fn read_16_bytes(&mut self) -> Result<[u8; 16], DecodingError>
        {
            let mut buf = [0u8; 16];
            self.reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        debug!("CmdDeserializer::read_32_bytes - unexpectedEof");
                        DecodingError::InvalidMessageSize
                    },
                    _ => {
                        DecodingError::IoError(e)
                    }
                })?;
            self.size_read += 16;
            return Ok(buf);
        }

        async fn read_sector_data(&mut self) -> Result<SectorVec, DecodingError>
        {
            // If there is to little data, reader returns error
            let sector_data = self.read_n_bytes(SECTOR_SIZE).await?;
            // We go in here only if we read exactly SECTOR_SIZE bytes
            let data_arr: [u8; SECTOR_SIZE] = sector_data
            .try_into()
            .map_err(|_| DecodingError::InvalidMessageSize)?;

            return Ok(SectorVec(Box::new(serde_big_array::Array(data_arr))));
        }

        async fn read_n_bytes(&mut self, len: usize) -> Result<Vec<u8>, DecodingError>
        {
            const MAX_ALLOWED_SIZE: usize = SECTOR_SIZE;
            if len > MAX_ALLOWED_SIZE
            {
                return Err(DecodingError::InvalidMessageSize);
            }
            
            let mut buf = vec![0u8; len];
            self.reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        debug!("CmdDeserializer::read_n_bytes ({}) - unexpectedEof", len);
                        DecodingError::InvalidMessageSize
                    },
                    _ => {
                        DecodingError::IoError(e)
                    }
                })?;
            self.size_read += len as u64;
            return Ok(buf);
        }

        fn size_read(&self) -> u64
        {
            self.size_read
        }
    }
}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in `AtomicRegister`. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    impl Broadcast
    {
        pub fn new(sys_cmd: SystemRegisterCommand) -> Broadcast
        {
            return Broadcast { cmd: Arc::new(sys_cmd) };
        }
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: u8,
    }

    impl Send
    {
        pub fn new(sys_cmd: SystemRegisterCommand, target: u8) -> Send
        {
            return Send {
                cmd: Arc::new(sys_cmd),
                target: target
            };
        }
    }
}
