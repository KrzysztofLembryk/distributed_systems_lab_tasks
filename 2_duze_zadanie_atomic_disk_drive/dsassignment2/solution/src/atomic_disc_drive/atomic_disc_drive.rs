use std::pin::Pin;
use tokio::net::{TcpStream};
use tokio::time::{sleep, Duration};
use std::io::{ErrorKind};
use std::{collections::HashMap, path::PathBuf};
use tokio::net::{TcpListener};
use std::sync::{Arc};
use tokio::sync::{Mutex, MutexGuard};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::atomic_register::atom_reg::spawn_atomic_register_task;
use crate::{DecodingError};
use crate::transfer_public::{deserialize_register_command, serialize_client_response, serialize_client_error_response};
use crate::domain::{SectorIdx, Configuration, SystemRegisterCommand, ClientRegisterCommand, RegisterCommand, ClientCommandResponse, StatusCode, ClientResponseOpType};
use crate::sectors_manager_public::{build_sectors_manager, SectorsManager};
use crate::register_client::reg_client::{RegClient};
use log::{warn, error, debug};

pub type SuccessCallbackFunc = Box<
            dyn FnOnce(ClientCommandResponse) 
                -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,>;
pub type SectorSysClientTxMap = HashMap<
    SectorIdx, 
    (
        UnboundedSender<Arc<SystemRegisterCommand>>,
        UnboundedSender<(Box<ClientRegisterCommand>, SuccessCallbackFunc)>
    )
>;

// Too many file descriptors opened os error
const EMFILE: i32 = 24;

pub struct AtomicDiscDrive
{
    storage_dir: PathBuf,
    self_rank: u8,
    n_sectors: u64,
    n_proc: u8,
    hmac_system_key: Arc<[u8; 64]>,
    hmac_client_key: Arc<[u8; 32]>,
    tcp_listener: TcpListener,
    all_present_sectors: Arc<Mutex<SectorSysClientTxMap>>,
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
        let all_present_sectors: Arc<Mutex<SectorSysClientTxMap>> = 
            Arc::new(Mutex::new(HashMap::new()));

        let (register_client, _reg_client_task_handles) = RegClient::new(
            self_rank, 
            &config.hmac_system_key, 
            &tcp_locations, 
            all_present_sectors.clone()
        );

        return AtomicDiscDrive {
            storage_dir: config.public.storage_dir,
            self_rank, 
            n_sectors: config.public.n_sectors,
            n_proc,
            hmac_system_key: Arc::new(config.hmac_system_key),
            hmac_client_key: Arc::new(config.hmac_client_key),
            tcp_listener,
            all_present_sectors: all_present_sectors,
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
                Ok((conn_socket, _response_addr)) => {
                    debug!("Getting connection from: {}", _response_addr);
                    spawn_task_to_handle_connection(
                        conn_socket, 
                        self.n_sectors, 
                        self.self_rank, 
                        self.n_proc, 
                        self.hmac_system_key.clone(), 
                        self.hmac_client_key.clone(), 
                        sectors_manager.clone(), 
                        self.all_present_sectors.clone(), 
                        self.register_client.clone());
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

fn spawn_task_to_handle_connection(
    mut conn_socket: TcpStream,
    n_sectors: u64,
    self_rank: u8,
    n_proc: u8,
    hmac_system_key: Arc<[u8; 64]>,
    hmac_client_key: Arc<[u8; 32]>,
    sectors_manager: Arc<dyn SectorsManager>,
    all_present_sectors: Arc<Mutex<SectorSysClientTxMap>>,
    register_client: Arc<RegClient>,
)
{
    tokio::spawn(async move {
        let mut sectors_txs: SectorSysClientTxMap = HashMap::new();
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
                        // implementation takes care of that
                        handle_invalid_hmac(
                            cmd, 
                            &mut conn_socket, 
                            &hmac_client_key
                        ).await;
                        // we encounter invalid msg we end connection
                        return;
                    }
                    else
                    {
                        match cmd {
                            RegisterCommand::Client(c_cmd) => {
                                debug!("Got client command");
                                handle_client_cmd(
                                    conn_socket,
                                    c_cmd, 
                                    all_present_sectors.clone(),
                                    sectors_manager.clone(), 
                                    register_client.clone(),
                                    hmac_client_key.clone(),
                                    self_rank,
                                    n_sectors,
                                    n_proc,
                                ).await;
                                // If client connects to us, we pass ownership of 
                                // his socket to AtomicRegister, which will send him
                                // value once it gets result. So we can safely end 
                                // this task here
                                return;
                            },
                            RegisterCommand::System(s_cmd) => {
                                // This means that we will be only getting messages 
                                // from other system processes, we might drop this
                                // connection when malicious process sends request
                                // about sector that doesn't exist (idx >= n_sectors)
                                match handle_sys_cmd(
                                    s_cmd, 
                                    &mut sectors_txs,
                                    all_present_sectors.clone(),
                                    sectors_manager.clone(), 
                                    register_client.clone(),
                                    self_rank,
                                    n_sectors,
                                    n_proc,
                                ).await
                                {
                                    Ok(_) => {},
                                    Err(e) => {
                                        warn!("When handling sys_cmd from other process we got error: '{}', ENDING CONNECTION", e);
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }    
                Err(e) => {
                    warn!("deserialize_register_command returned error: {:?}", e);
                    match e
                    {
                        DecodingError::IoError(e) => {
                            warn!("We've got IO erorr, ending this connection, err: {:?}", e);
                            return;
                        }
                        other_e => {
                            // Either InvalidMsg or BinCodeError, 
                            warn!("Recv Message is invalid, ending this connection, err: {:?}", other_e);
                            return;
                        }
                    }
                }
            }
        }
    });
}

async fn handle_client_cmd(
    mut conn_socket: TcpStream,
    c_cmd: ClientRegisterCommand,
    all_present_sectors: Arc<Mutex<SectorSysClientTxMap>>,
    sectors_manager: Arc<dyn SectorsManager>,
    register_client: Arc<RegClient>,
    hmac_client_key: Arc<[u8; 32]>,
    self_rank: u8,
    n_sectors: u64,
    n_proc: u8,
)
{
    let sector_idx = c_cmd.get_sector_idx();

    if !is_valid_sector(sector_idx, n_sectors)
    {
        let status = StatusCode::InvalidSectorIndex;
        let request_id = c_cmd.header.request_identifier;
        let op_type: ClientResponseOpType = c_cmd.get_op_type();

        match serialize_client_error_response(
            status, 
            request_id, 
            op_type, 
            &mut conn_socket, 
            &(*hmac_client_key)
        ).await
        {
            Ok(_) => {},
            Err(e) => {warn!("In handle_client_cmd we got error during serialization:{:?}", e);}
        }
        // We end connection
        return;
    }

    // if we have this sector in our sectors_txs map, this means that there is a task
    // for this sector, so we can safely send msg to it
    let mut present_sectors_lock = all_present_sectors.lock().await;

    // If we don't have txs for given sector in our map, we check if other tcp
    // receiver task has spawned it and added it to present_sectors_lock
    if let Some((_sys_cmd_tx, client_cmd_tx)) = present_sectors_lock.get(&sector_idx)
    {
        // If it is in this shared map, we send msg to it and save txs to this 
        // sector to our internal map

        debug!("Sending client cmd to sector task");
        send_client_cmd_to_sector_task(
            conn_socket, 
            c_cmd, 
            hmac_client_key.clone(), 
            &client_cmd_tx
        );
    }
    else
    {
        debug!("handle_client_cmd: no task for sector: {} yet, will create it", sector_idx);
        // If there isnt a task for this sector we spawn it, insert it to shared
        // map. Spawn sector function insert txs to shared map
        let (_sys_cmd_tx, client_cmd_tx) = spawn_sector_task(
            &mut present_sectors_lock, 
            sectors_manager, 
            register_client, 
            self_rank, 
            sector_idx, 
            n_proc
        ); 

        drop(present_sectors_lock);

        send_client_cmd_to_sector_task(
            conn_socket, 
            c_cmd, 
            hmac_client_key.clone(), 
            &client_cmd_tx
        );
    }
}

async fn handle_sys_cmd(
    s_cmd: SystemRegisterCommand,
    sectors_txs: &mut SectorSysClientTxMap,
    all_present_sectors: Arc<Mutex<SectorSysClientTxMap>>,
    sectors_manager: Arc<dyn SectorsManager>,
    register_client: Arc<RegClient>,
    self_rank: u8,
    n_sectors: u64,
    n_proc: u8,
) -> Result<(), String>
{
    let sector_idx = s_cmd.get_sector_idx();

    if !is_valid_sector(s_cmd.get_sector_idx(), n_sectors)
    {
        return Err(format!("When handling SysCmd from other process, we got request for sector: '{}', but there are only '{}' sectors in our system, and their indexes start at 0", s_cmd.get_sector_idx(), n_sectors));
    }

    if let Some((sys_cmd_tx, _)) = sectors_txs.get(&sector_idx)
    {
        send_sys_cmd_to_sector_task(
            s_cmd, 
            &sys_cmd_tx,
            sector_idx
        );
    }
    else 
    {
        let mut present_sectors_lock = all_present_sectors.lock().await;

        if let Some((sys_cmd_tx, client_cmd_tx)) = present_sectors_lock.get(&sector_idx)
        {
            send_sys_cmd_to_sector_task(
                s_cmd, 
                &sys_cmd_tx,
                sector_idx
            );

            sectors_txs.insert(sector_idx, (sys_cmd_tx.clone(), client_cmd_tx.clone()));
        }
        else
        {
            let (sys_cmd_tx, client_cmd_tx) = spawn_sector_task(
                &mut present_sectors_lock, 
                sectors_manager, 
                register_client, 
                self_rank, 
                sector_idx, 
                n_proc
            ); 

            drop(present_sectors_lock);

            send_sys_cmd_to_sector_task(
                s_cmd, 
                &sys_cmd_tx,
                sector_idx
            );

            sectors_txs.insert(sector_idx, (sys_cmd_tx, client_cmd_tx));
        }
    }
    return Ok(());
}

fn spawn_sector_task(
    present_sectors_lock: &mut MutexGuard<'_, SectorSysClientTxMap>,
    sectors_manager: Arc<dyn SectorsManager>,
    register_client: Arc<RegClient>,
    self_rank: u8,
    sector_idx: SectorIdx,
    n_proc: u8,
) -> (
    UnboundedSender<Arc<SystemRegisterCommand>>, 
    UnboundedSender<(Box<ClientRegisterCommand>, SuccessCallbackFunc)>
)
{
    let (sys_cmd_tx, sys_cmd_rx) = 
        unbounded_channel::<Arc<SystemRegisterCommand>>();
    let (client_cmd_tx, client_cmd_rx) = 
        unbounded_channel::<(Box<ClientRegisterCommand>, SuccessCallbackFunc)>();

    present_sectors_lock.insert(sector_idx, (sys_cmd_tx.clone(), client_cmd_tx.clone()));

    spawn_atomic_register_task(
        self_rank, 
        sector_idx, 
        register_client, 
        sectors_manager, 
        n_proc, 
        sys_cmd_rx, 
        client_cmd_rx
    );

    return (sys_cmd_tx, client_cmd_tx);
}

fn send_sys_cmd_to_sector_task(
    s_cmd: SystemRegisterCommand,
    sys_cmd_tx: &UnboundedSender<Arc<SystemRegisterCommand>>,
    sector_idx: SectorIdx,
)
{
    // This should never return error since our atomic register once its spawned
    // will run indefinitely, but if this somehow happens this means that atomic
    // register is closed so task has ended so we need to respawn it
    match sys_cmd_tx.send(Arc::new(s_cmd))
    {
        Ok(_) => {},
        Err(e) => {
            error!("When sending CLient command to sector: '{}', we got error from tx: '{}'. This means our sector task crashed, we need to respawn it", sector_idx, e);
        }
    }
}

fn send_client_cmd_to_sector_task(
    mut conn_socket: TcpStream,
    c_cmd: ClientRegisterCommand,
    hmac_client_key: Arc<[u8; 32]>,
    client_cmd_tx: &UnboundedSender<(Box<ClientRegisterCommand>, SuccessCallbackFunc)>,
)
{
    let sector_idx = c_cmd.get_sector_idx();

    let callback_func: SuccessCallbackFunc = Box::new(
        move |response: ClientCommandResponse| {
            return Box::pin(async move {
                debug!("SuccessCallback - serialize client response: {:?}", response);
                let fut = serialize_client_response(
                    &response, 
                    &mut conn_socket, 
                    hmac_client_key.as_ref()
                );
                let _ = fut.await;

                debug!("SuccessCallback - conn_socket should be dropped here");
            });
        }
    );

    debug!("AtomicDiscDrive::send_clinet_cmd_to_sector_task: Sending ClinetCMd to sector");
    // This should never return error since our atomic register once its spawned
    // will run indefinitely, but if this somehow happens this means that atomic
    // register is closed so task has ended so we need to respawn it
    match client_cmd_tx.send((Box::new(c_cmd), callback_func))
    {
        Ok(_) => {},
        Err(e) => {
            error!("When sending CLient command to sector: '{}', we got error from tx: '{}'. This means our sector task crashed, we need to respawn it", sector_idx, e);
        }
    }
}


fn is_valid_sector(cmd_sector: u64, n_sectors: u64) -> bool
{
    return cmd_sector < n_sectors;
}

async fn handle_invalid_hmac(
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

            let op_type: ClientResponseOpType = client_cmd.get_op_type();

            match serialize_client_error_response(
                status, 
                request_identifier,
                op_type,
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