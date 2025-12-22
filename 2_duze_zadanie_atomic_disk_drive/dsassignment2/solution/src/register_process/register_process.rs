use tokio::fs as t_fs;
use std::{collections::HashMap, path::PathBuf};
use tokio::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded_channel};

use crate::storage::storage_utils::{extract_data_from_file_name};
use crate::domain::{SectorIdx, SectorVec, Configuration, SystemRegisterCommand, ClientRegisterCommand};
use crate::sectors_manager_public::{build_sectors_manager, SectorsManager};
use crate::atomic_register_public::{AtomicRegister, build_atomic_register};

type SectorSeenCount = u8;

struct RegisterProcess
{
    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],
    storage_dir: PathBuf,
    self_rank: u8,
    n_sectors: u64,
    n_proc: u8,
    tcp_listener: TcpListener,
}

impl RegisterProcess
{
    pub async fn new(config: Configuration) -> RegisterProcess
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

        return RegisterProcess {
            hmac_system_key: config.hmac_system_key,
            hmac_client_key: config.hmac_client_key,
            storage_dir: config.public.storage_dir,
            self_rank, 
            n_sectors: config.public.n_sectors,
            n_proc,
            tcp_listener
        };
    }

    pub async fn run(&mut self)
    {
        let sectors_manager: Arc<dyn SectorsManager> = 
            build_sectors_manager(self.storage_dir.clone()).await;
        // After building sector manager all TMP files should be removed, and only
        // not corrupted files for given sectors should be present, so we again scan
        // directory to retrieve all SECTOR_IDXs so that we can spawn tasks for them,
        // since if there is a file for sector, this means that this sector was 
        // written to. But IDK if this is better than just spawning task for given
        // sector only when we get request for it for the first time
        let mut present_sectors: Vec<SectorIdx> = Vec::new();
        present_sectors = scan_dir_and_recover_sector_idxs(&self.storage_dir).await;
        
        let mut sector_txs_map: HashMap<
            SectorIdx, 
            (
                UnboundedSender<SystemRegisterCommand>,
                UnboundedSender<ClientRegisterCommand>
            )
        > = HashMap::new();

        let mut atomic_reg_handles = vec![];

        for sector_idx in present_sectors
        {
            let (tx_task_sys_cmd, mut rx_task_sys_cmd) = 
                unbounded_channel::<SystemRegisterCommand>();
            let (tx_task_client_cmd, mut rx_task_client_cmd) = 
                unbounded_channel::<ClientRegisterCommand>();
            
            sector_txs_map.insert(sector_idx, (tx_task_sys_cmd, tx_task_client_cmd));
            sector_rxs_map.insert(sector_idx, (rx_task_sys_cmd, rx_task_client_cmd));
        }

        let not_present_at_init_sectors_txs_map: Arc<Mutex<HashMap<
            SectorIdx, 
            (
                UnboundedSender<SystemRegisterCommand>,
                UnboundedSender<ClientRegisterCommand>,
                SectorSeenCount
            )
        >>> = Arc::new(Mutex::new(HashMap::new()));


        for sector_idx in present_sectors
        {
            let (rx_task_sys_cmd, rx_task_client_cmd) = 
                sector_rxs_map.remove(&sector_idx).unwrap();

            atomic_reg_handles.push( tokio::spawn(async move {
                    loop
                    {
                        let atomic_reg = build_atomic_register(
                            self.self_rank, 
                            sector_idx, 
                            register_client, 
                            sectors_manager.clone(), 
                            self.n_proc
                        ).await;
                        while let Some(client_command) = rx_task_client_cmd.recv().await 
                        {
                            
                        }
                    }
                }));
        }
        
    }

}

async fn spawn_sector_tasks(
    proc_rank: u8, 
    n_proc: u8, 
    storage_dir: &PathBuf,
)
{
    // After building sector manager all TMP files should be removed, and only
    // not corrupted files for given sectors should be present, so we again scan
    // directory to retrieve all SECTOR_IDXs so that we can spawn tasks for them,
    // since if there is a file for sector, this means that this sector was 
    // written to. But IDK if this is better than just spawning task for given
    // sector only when we get request for it for the first time
    let mut present_sectors: Vec<SectorIdx> = Vec::new();
    present_sectors = scan_dir_and_recover_sector_idxs(storage_dir).await;
    
    let mut sector_rxs_map: HashMap<
        SectorIdx, 
        (
            UnboundedReceiver<SystemRegisterCommand>,
            UnboundedReceiver<ClientRegisterCommand>
        )
    > = HashMap::new();

    let mut atomic_reg_handles = vec![];

    // First we create channels for each sector, and remember tx channel so that we
    // can send messages to these sectors
    for sector_idx in present_sectors
    {
        let (tx_task_sys_cmd, mut rx_task_sys_cmd) = 
            unbounded_channel::<SystemRegisterCommand>();
        let (tx_task_client_cmd, mut rx_task_client_cmd) = 
            unbounded_channel::<ClientRegisterCommand>();
        
        sector_txs_map.insert(sector_idx, (tx_task_sys_cmd, tx_task_client_cmd));
        sector_rxs_map.insert(sector_idx, (rx_task_sys_cmd, rx_task_client_cmd));
    }

    // having all txs we can create register_client

    for sector_idx in present_sectors
    {
        let (rx_task_sys_cmd, rx_task_client_cmd) = 
            sector_rxs_map.remove(&sector_idx).unwrap();

        atomic_reg_handles.push( tokio::spawn(async move {
                loop
                {
                    let atomic_reg = build_atomic_register(
                        proc_rank, 
                        sector_idx, 
                        register_client, 
                        sectors_manager.clone(), 
                        n_proc
                    ).await;
                    while let Some(client_command) = rx_task_client_cmd.recv().await 
                    {
                        
                    }
                }
            }));
    }
}

async fn scan_dir_and_recover_sector_idxs(
    dir_path: &PathBuf
) -> Vec<SectorIdx>
{
    let mut sector_idxs: Vec<SectorIdx> = Vec::new();
    let mut entries = t_fs::read_dir(dir_path).await.unwrap();

    while let Some(entry) = entries.next_entry().await.unwrap()
    {
        let entry_path = entry.path();
        let meta = entry.metadata().await.unwrap();

        if meta.is_file() 
        {
            // if file path ends with '.' or '..' it returns None 
            if let Some(file_name) = entry_path
                                        .file_name()
                                        .and_then(|s| s.to_str())
            {
                // At this point all files in our dir should be normal sector files, 
                // no TMP files should be present
                let (sector_idx, _, _) = extract_data_from_file_name(file_name);
                sector_idxs.push(sector_idx); 
            }
        }
    }

    return sector_idxs;
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