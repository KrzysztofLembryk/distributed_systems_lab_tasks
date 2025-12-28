use tokio::task::JoinHandle;
use uuid::Uuid;
use std::{collections::HashMap};
use tokio::time::Instant;
use tokio::net::{TcpStream};
use std::sync::{Arc};
use tokio::time::{sleep, Duration};
use tokio::sync::{Mutex};
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded_channel};

use crate::atomic_disc_drive::atomic_disc_drive::{SectorSysClientTxMap};
use crate::SectorIdx;
use crate::transfer_public::{serialize_register_command};
use crate::domain::{SystemRegisterCommand, RegisterCommand};
use crate::register_client_public::{Broadcast, RegisterClient};
use crate::register_client_public::Send as RegSend;
use log::{debug, warn};

#[derive(PartialEq, Clone, Copy)]
enum TcpSendType
{
    TcpNormalSend,
    TcpBroadcast
}

type TcpTaskTxMap = HashMap<u8, UnboundedSender<Arc<SystemRegisterCommand>>>;
type SectorTxHashMap = Mutex<HashMap<
    SectorIdx, 
    UnboundedSender<Arc<SystemRegisterCommand>>
>>;

pub struct RegClient
{
    parent_proc_rank: u8,
    tcp_task_tx_map: TcpTaskTxMap,
    sector_tx_map: SectorTxHashMap,
    all_present_sectors: Arc<Mutex<SectorSysClientTxMap>>,
}

#[async_trait::async_trait]
impl RegisterClient for RegClient
{
    async fn send(&self, msg: RegSend)
    {
        let cmd = msg.cmd;
        let sector_idx = cmd.header.sector_idx;
        let target = msg.target;

        if target == self.parent_proc_rank
        {
            self.send_msg_to_internal_sector_if_needed(
                sector_idx, 
                target, 
                cmd.clone(), 
                TcpSendType::TcpNormalSend
            ).await;
        }
        else
        {
            if let Some(tcp_tx) = &self.tcp_task_tx_map.get(&target)
            {
                tcp_tx.send(cmd.clone()).unwrap();
            }
            else
            {
                panic!("RegisterClient::send - In proc: '{}', for target proc: '{}', self.tcp_task_tx_map.get(target) returned NONE, this should never happen", self.parent_proc_rank, target);
            }
        }
    }

    async fn broadcast(&self, msg: Broadcast)
    {
        let cmd = msg.cmd;
        let sector_idx = cmd.header.sector_idx;
        let proc_rank = cmd.header.process_identifier;

        self.send_msg_to_internal_sector_if_needed(
            sector_idx, 
            proc_rank, 
            cmd.clone(), 
            TcpSendType::TcpBroadcast
        ).await;

        // there is no tcp task for our proc_id
        for (_proc_idx, tcp_task_tx) in &self.tcp_task_tx_map
        {
            tcp_task_tx.send(cmd.clone()).expect("RegClient::broadcast:: tcp_task_tx.send(cmd.clone()), returned error");
        }
    }
}

impl RegClient
{
    pub fn new(
        parent_proc_rank: u8,
        hmac_system_key: &[u8; 64],
        tcp_locations: &Vec<(String, u16)>,
        all_present_sectors: Arc<Mutex<SectorSysClientTxMap>>,
    ) -> (RegClient, Vec<JoinHandle<()>>)
    {
        let mut tcp_task_tx_map: TcpTaskTxMap = HashMap::new();

        // Do we wait for these tasks even?
        let task_handles = 
            for_every_proc_in_system_spawn_tcp_task_that_handles_communication(
                parent_proc_rank,
                &mut tcp_task_tx_map,
                hmac_system_key,
                &tcp_locations
        );

        return (
            RegClient { 
                parent_proc_rank,
                tcp_task_tx_map, 
                sector_tx_map: Mutex::new(HashMap::new()), 
                all_present_sectors 
            },
            task_handles
        );
    }

    async fn send_msg_to_internal_sector_if_needed(
        &self, 
        sector_idx: SectorIdx,
        proc_rank: u8,
        cmd: Arc<SystemRegisterCommand>,
        send_type: TcpSendType
    )
    {
        // if we have normal send, we need to check if we send for OUR process, if 
        // not we return
        if send_type == TcpSendType::TcpNormalSend
        {
            if self.parent_proc_rank != proc_rank
            {
                return;
            }
        }

        let mut sector_tx_lock = self.sector_tx_map.lock().await;
        // When doing broadcast we send msg to our sector skipping TCP part
        if let Some(sector_tx) = sector_tx_lock.get(&sector_idx)
        {
            sector_tx.send(cmd).unwrap();
            drop(sector_tx_lock);
        }
        else
        {
            // We don't have tx for this sector in map thus it must be in 
            // newly_created_sectors
            let all_present_sectors_lock = self.all_present_sectors.lock().await;

            if let Some((sys_sector_tx, _)) = all_present_sectors_lock.get(&sector_idx)
            {
                sys_sector_tx.send(cmd).unwrap();
                sector_tx_lock.insert(sector_idx, sys_sector_tx.clone());
                drop(all_present_sectors_lock);
                drop(sector_tx_lock);
            }
            else
            {
                panic!("RegisterClient::send_msg_to_internal_sector_if_needed - all_present_sectors doesn't have tx for sector {}, this should never happen since we don't have it in our sector_tx_map also", sector_idx);
            }
        }
    }
}

fn for_every_proc_in_system_spawn_tcp_task_that_handles_communication(
    parent_proc_rank: u8,
    tcp_task_tx_map: &mut TcpTaskTxMap,
    hmac_system_key: &[u8; 64],
    tcp_locations: &[(String, u16)],
) -> Vec<JoinHandle<()>>
{
    let hmac_system_key_arc: Arc<[u8; 64]> = Arc::new(*hmac_system_key);
    let mut tcp_tasks_handles = vec![];
    debug!("There are '{}' processes in system, spawning tasks for them", tcp_locations.len());

    for (proc_rank, (host, port)) in tcp_locations.iter().enumerate()
    {
        // proc ranks are from 1 to tcp_locations.len, so we need to add 1 to 
        // proc_rank since it starts from 0
        let proc_rank = proc_rank + 1;
        // We don't want to send msgs to ourselves by TCP
        if parent_proc_rank != proc_rank as u8
        {
            let (tx, rx) = 
                unbounded_channel::<Arc<SystemRegisterCommand>>();

            tcp_task_tx_map.insert((proc_rank) as u8, tx);

            tcp_tasks_handles.push(spawn_tcp_task(
                proc_rank as u8, 
                host.clone(), 
                *port, 
                rx, 
                *hmac_system_key_arc.clone()
            ));
        }
    }
    return tcp_tasks_handles;
}

type OpId = Uuid;
type SectorSysMsgHashMap = HashMap<SectorIdx, (RegisterCommand, OpId)>;

fn spawn_tcp_task(
    proc_rank: u8, 
    host: String, 
    port: u16, 
    mut rx: UnboundedReceiver<Arc<SystemRegisterCommand>>,
    hmac_system_key: [u8; 64],
) -> JoinHandle<()>
{
    let handle = tokio::spawn(async move {
        let addr = format!("{}:{}", host, port);
        let mut send_stream = reconnect(&addr, proc_rank).await;
        let mut sector_sys_msg_map: SectorSysMsgHashMap = HashMap::new();

        // We have established connection, and wait for the first msg to be sent
        if let Some(sys_reg_cmd) = rx.recv().await
        {
            let sector_idx = sys_reg_cmd.header.sector_idx;
            let op_id = sys_reg_cmd.header.msg_ident;
            let reg_cmd = RegisterCommand::System((*sys_reg_cmd)
                .clone());
            sector_sys_msg_map.insert(sector_idx, (reg_cmd, op_id));
        }

        // We send msgs by implementing STUBBORN LINK
        // 1) We send all msgs we have
        // 2) If we get new message for given sector, it means our previous message
        //    was successfully delivered, so we overwrite old one, and will send new 
        //    one.
        // 3) If no new message for given sector was received we resend old msg 
        // 4) If connection was dropped we try to reconnect till infinity
        // 5) If new msg for given sector never comes we resend old one till infinity
        // TODO: in 5) we could check if we get any ACK msg for given sector, if we
        //             get such msg we could stop sending this message
        loop
        {
            // ############ WE SEND ALL MSGS WE CAN ############
            // If we received no new messages, we resend previoues ones
            resend_all_msgs(
                &sector_sys_msg_map, 
                &mut send_stream, 
                &hmac_system_key, 
                &addr, 
                proc_rank
            ).await;

            // ############ WE WAIT TIMEOUT TO RETRANSMIT MSGS #############
            sleep(Duration::from_millis(200)).await;

            // ############ WE TRY RECEIVE ACK #############
            // We try recv ack for given sector, this means that we can stop sending
            // msgs for this sector

            // ############ WE TRY TO RECEIVE NEW MESSAGES #############
            try_recv_new_msgs(&mut sector_sys_msg_map, &mut rx);
        }
    });
    return handle;
}

async fn resend_all_msgs(
    sector_sys_msg_map: &SectorSysMsgHashMap,
    send_stream: &mut TcpStream,
    hmac_system_key: &[u8; 64],
    addr: &str,
    proc_rank: u8
)
{
    for (sector_idx, (reg_cmd, _)) in sector_sys_msg_map
    {
        // serialize sends our message since, it writes to send_stream 
        match serialize_register_command(
            &reg_cmd, 
            send_stream, 
            hmac_system_key
        ).await
        {
            Ok(_) => {},
            Err(e) => {
                warn!("While sending msg to proc: {} about sector: {}, send_stream returned error: '{:?}'\nTrying to reconnect.", proc_rank, sector_idx, e);
                *send_stream = reconnect(&addr, proc_rank).await;
                continue;
            }
        }
    }
}

fn try_recv_new_msgs(
    sector_sys_msg_map: &mut SectorSysMsgHashMap,
    rx: &mut UnboundedReceiver<Arc<SystemRegisterCommand>>
)
{
    // During our sleep we could get many new msgs, so now we read all of 
    // them, but we will spend no more than 100ms reading them so that 
    // we don't stop sending messages.
    // If we get new msg for given sector we overwrite old msg.
    let start = Instant::now();
    loop {
        if start.elapsed().as_millis() > 100 {
            break;
        }

        match rx.try_recv()
        {
            Ok(sys_reg_cmd) => {
                let sector_idx = sys_reg_cmd.header.sector_idx;
                let op_id = sys_reg_cmd.header.msg_ident;
                let reg_cmd = RegisterCommand::System((*sys_reg_cmd)
                    .clone());
                sector_sys_msg_map.insert(sector_idx, (reg_cmd, op_id));
            },
            Err(_) => {
                break;
            }
        }
    }
}

async fn reconnect(addr: &str, proc_rank: u8) -> TcpStream
{
    loop 
    {
        match TcpStream::connect(&addr).await {
            Ok(s) => {return s;},
            Err(e) => {
                warn!("Couldnt connect to process: {} with address: {}\nError: '{}'\nAfter timeout (300) will retry", proc_rank, addr, e);
                sleep(Duration::from_millis(300)).await;
            }
        };
    }
}