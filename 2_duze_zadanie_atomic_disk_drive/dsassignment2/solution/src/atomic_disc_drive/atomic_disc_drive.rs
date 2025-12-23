use bincode::error::AllowedEnumVariants;
use tokio::net::{TcpStream};
use tokio::time::{sleep, Duration};
use std::io::{ErrorKind};
use std::{collections::HashMap, path::PathBuf};
use tokio::net::{TcpListener};
use std::sync::{Arc};
use tokio::sync::{Mutex};
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded_channel};

use crate::{DecodingError, SECTOR_SIZE, SectorVec};
use crate::transfer_public::{deserialize_register_command, serialize_client_response};
use crate::domain::{SectorIdx, Configuration, SystemRegisterCommand, ClientRegisterCommand, RegisterCommand, ClientCommandResponse, StatusCode, OperationReturn, ClientRegisterCommandContent};
use crate::sectors_manager_public::{build_sectors_manager, SectorsManager};
use crate::atomic_register_public::{AtomicRegister, build_atomic_register};
use crate::register_client::reg_client::{RegClient, NewlyCreatedSectorsTxMap};
use log::{warn, error};

type SectorSysClientTxMap = HashMap<
    SectorIdx, 
    (
        UnboundedSender<Arc<SystemRegisterCommand>>,
        UnboundedSender<Arc<ClientRegisterCommand>>
    )
>;

// Too many file descriptors opened os error
const EMFILE: i32 = 24;

struct AtomicDiscDrive
{
    storage_dir: PathBuf,
    self_rank: u8,
    n_sectors: u64,
    n_proc: u8,
    hmac_system_key: Arc<[u8; 64]>,
    hmac_client_key: Arc<[u8; 32]>,
    tcp_listener: TcpListener,
    sectors_txs: Arc<Mutex<SectorSysClientTxMap>>,
    newly_created_sectors: NewlyCreatedSectorsTxMap,
    register_client: Arc<RegClient>
}

impl AtomicDiscDrive
{
    pub async fn new(config: Configuration) -> AtomicDiscDrive
    {
        let tcp_locations = config.public.tcp_locations;
        let n_proc = tcp_locations.len() as u8;
        let self_rank = config.public.self_rank;

        if self_rank > n_proc
        {
            panic!("RegisterProcess::new - self_rank ({}) of process is greater than number of all processes ({})", self_rank, n_proc);
        }

        let tcp_listener = bind_socket_for_listening(self_rank - 1, &tcp_locations)
                    .await
                    .unwrap();
        let newly_created_sectors: NewlyCreatedSectorsTxMap = 
            Arc::new(Mutex::new(HashMap::new()));

        let (register_client, reg_client_task_handles) = RegClient::new(
            self_rank, 
            &config.hmac_system_key, 
            &tcp_locations, 
            newly_created_sectors.clone()
        );

        return AtomicDiscDrive {
            storage_dir: config.public.storage_dir,
            self_rank, 
            n_sectors: config.public.n_sectors,
            n_proc,
            hmac_system_key: Arc::new(config.hmac_system_key),
            hmac_client_key: Arc::new(config.hmac_client_key),
            tcp_listener,
            sectors_txs: Arc::new(Mutex::new(HashMap::new())),
            newly_created_sectors,
            register_client: Arc::new(register_client)
        };
    }

    pub async fn run(&mut self)
    {
        let sectors_manager: Arc<dyn SectorsManager> = 
            build_sectors_manager(self.storage_dir.clone()).await;
        
        loop {
            match self.tcp_listener.accept().await
            {
                Ok((mut conn_socket, _response_addr)) => {

                },
                Err(e) => {
                    match e.kind()
                    {
                        ErrorKind::NetworkDown => {
                            warn!("AtomicDiscDrive::tcp_listener.accept - Network is down");
                            sleep(Duration::from_millis(1000)).await;
                        },
                        ErrorKind::Other => {
                            if let Some(os_err) = e.raw_os_error()
                            {
                                if os_err == EMFILE
                                {
                                    warn!("AtomicDiscDrive::tcp_listener.accept - No free file descriptors");
                                    // No free file descriptors, thus we need to wait
                                    sleep(Duration::from_millis(400)).await;
                                }
                            }
                        },
                        _ => {
                            warn!(
                                "Got ErrorKind: {:?}, just logging, errMsg: {}",
                                    e.kind(), e
                            );
                        }
                    }
                }
            }
        }
    }
}

fn handle_connection(
    mut conn_socket: TcpStream,
    n_sectors: u64,
    hmac_system_key: Arc<[u8; 64]>,
    hmac_client_key: Arc<[u8; 32]>,
    sectors_manager: Arc<dyn SectorsManager>,
    sectors_txs: Arc<Mutex<SectorSysClientTxMap>>,
    newly_created_sectors: NewlyCreatedSectorsTxMap,
    register_client: Arc<RegClient>,
)
{
    tokio::spawn(async move {
        loop 
        {
            match deserialize_register_command(
                &mut conn_socket,
                &hmac_system_key,
                &hmac_client_key
            ).await {
                Ok((cmd, is_hmac_valid)) => {
                    if !is_hmac_valid 
                    {
                        // System messages should never have invalid HMAC since our 
                        // implementation takes care of that, however if we get wrong
                        // hmac from SystemMsg we ignore it
                        handle_invalid_client_hmac(
                            cmd, 
                            &mut conn_socket, 
                            &hmac_client_key
                        ).await;
                        // we wait for another msg from client on conn_socket,
                        // we don't drop connection with him even though he sent 
                        // msg with wrong hmac
                    }
                    else
                    {
                        match cmd {
                            RegisterCommand::Client(c_cmd) => {
                                handle_client_cmd(
                                    conn_socket,
                                    c_cmd, 
                                    sectors_manager, 
                                    sectors_txs, 
                                    newly_created_sectors, 
                                    register_client,
                                    hmac_client_key.clone(),
                                ).await;
                                // If client connects to us, we pass ownership of 
                                // his socket to AtomicRegister, which will send him
                                // value once it gets result. So we can safely end 
                                // this task here
                                return;
                            },
                            RegisterCommand::System(s_cmd) => {
                                // This means that we will be only getting messages 
                                // from other system processes, so we don't drop this
                                // connection
                                handle_sys_cmd(
                                    s_cmd, 
                                    sectors_txs.clone()
                                ).await;
                            }
                        }
                    }
                }    
                Err(e) => {
                    warn!("deserialize_register_command returned error: {:?}", e);
                    match e
                    {
                        DecodingError::IoError(e) => {
                            error!("We've got IO erorr, ending this connection, err: {:?}", e);
                            return;
                        }
                        _ => {
                            // Either InvalidMsg or BinCodeError, no need to end c
                            // connection
                        }
                    }
                }
            }
        }
    });
}

async fn handle_client_cmd(
    conn_socket: TcpStream,
    c_cmd: ClientRegisterCommand,
    sectors_manager: Arc<dyn SectorsManager>,
    sectors_txs: Arc<Mutex<SectorSysClientTxMap>>,
    newly_created_sectors: NewlyCreatedSectorsTxMap,
    register_client: Arc<RegClient>,
    hmac_client_key: Arc<[u8; 32]>,
)
{

}

async fn handle_sys_cmd(
    s_cmd: SystemRegisterCommand,
    sectors_txs: Arc<Mutex<SectorSysClientTxMap>>,
)
{

}

async fn handle_invalid_client_hmac(
    cmd: RegisterCommand,
    conn_socket: &mut TcpStream,
    hmac_client_key: &[u8; 32],
)
{
    let status = StatusCode::AuthFailure;
    match cmd {
        RegisterCommand::System(_) => {
            warn!("AtomicDiscDrive got SYstem Register command that has invalid HMAC, this should never happen");
        }
        RegisterCommand::Client(client_cmd) => {
            let request_identifier = client_cmd.header.request_identifier;

            let op_return = match client_cmd.content {
                ClientRegisterCommandContent::Read => {
                    OperationReturn::Read {
                        read_data: SectorVec
                            ::new_from_slice(&[0; SECTOR_SIZE])
                    }  
                },
                ClientRegisterCommandContent::Write { .. } => {
                    OperationReturn::Write
                }
            };

            let resp_cmd = ClientCommandResponse {
                status,
                request_identifier,
                op_return 
            };
            match serialize_client_response(
                &resp_cmd, 
                conn_socket, 
                &*hmac_client_key
            ).await
            {
                Err(e) => {
                    warn!("Sending Response command failed with error: {:?}", e);
                }
                _ => {}
            }
        }
    }
}

async fn bind_socket_for_listening(
    proc_rank: u8, 
    tcp_locations: &[(String, u16)]
) -> std::io::Result<TcpListener> 
{
    let (host, port) = &tcp_locations[proc_rank as usize];
    let addr = format!("{}:{}", host, port);
    return TcpListener::bind(&addr).await;
} 