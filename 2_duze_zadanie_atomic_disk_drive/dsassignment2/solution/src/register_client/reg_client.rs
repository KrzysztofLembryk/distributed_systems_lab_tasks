use tokio::io::AsyncReadExt;
use uuid::Uuid;
use std::{collections::HashMap, path::PathBuf};
use tokio::time::Instant;
use tokio::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::time::{sleep, Duration};
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded_channel};

use crate::SectorIdx;
use crate::transfer_public::{serialize_register_command};
use crate::domain::{SystemRegisterCommand, RegisterCommand};
use crate::register_client_public::{Broadcast, RegisterClient};
use crate::register_client_public::Send as RegSend;
use log::debug;

enum TcpInternalMsg
{
    SendMsg(RegSend),
    BroadcastMsg(Broadcast)
}

type TcpTaskTxMap = HashMap<u8, UnboundedSender<Arc<SystemRegisterCommand>>>;

pub struct RegClient
{
    tcp_task_tx_map: TcpTaskTxMap,
    hmac_system_key: Arc<[u8; 64]>,
}

impl RegClient
{
    pub fn new(
        parent_proc_rank: u8,
        hmac_system_key: &[u8; 64],
        tcp_locations: &Vec<(String, u16)>,
    ) -> RegClient
    {
        let hmac_system_key_arc: Arc<[u8; 64]> = Arc::new(*hmac_system_key);
        let mut tcp_task_tx_map: TcpTaskTxMap = HashMap::new();

        for (proc_rank, (host, port)) in tcp_locations.iter().enumerate()
        {
            // We don't want to send msgs to ourselves by TCP
            if parent_proc_rank != proc_rank as u8
            {
    
                let (tx, rx) = 
                    unbounded_channel::<Arc<SystemRegisterCommand>>();

                tcp_task_tx_map.insert(proc_rank as u8, tx);
    
                spawn_tcp_task(
                    proc_rank as u8, 
                    host.clone(), 
                    *port, 
                    rx, 
                    *hmac_system_key_arc.clone()
                );
            }
        }
        unimplemented!()
    }
}

type OpId = Uuid;
type SectorSysMsgHashMap = HashMap<SectorIdx, (RegisterCommand, OpId)>;

fn spawn_tcp_task(
    proc_rank: u8, 
    host: String, 
    port: u16, 
    mut rx: UnboundedReceiver<Arc<SystemRegisterCommand>>,
    hmac_system_key: [u8; 64],
)
{
    tokio::spawn(async move {
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
                debug!("While sending msg to proc: {} about sector: {}, send_stream returned error: '{:?}'\nTrying to reconnect.", proc_rank, sector_idx, e);
                *send_stream = reconnect(&addr, proc_rank).await;
                continue;
            }
        }
    }
}

// TODO: change this func
async fn try_recv_acks(
    sector_sys_msg_map: &mut SectorSysMsgHashMap,
    send_stream: &mut TcpStream,
) {
    let start = Instant::now();
    let mut buf = [0u8; 16 + 8]; // 16 bytes for op_id (Uuid), 8 bytes for sector_idx (u64)
    loop {
        if start.elapsed().as_millis() > 100 {
            break;
        }
        // Try to read an ACK (non-blocking)
        match send_stream.try_read(&mut buf) {
            Ok(n) if n == 24 => {
                // Parse sector_idx and op_id from buffer
                let sector_idx = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                let op_id = Uuid::from_slice(&buf[8..24]).unwrap();
                // Remove only if both sector_idx and op_id match
                if let Some((_, stored_op_id)) = sector_sys_msg_map.get(&(sector_idx as SectorIdx)) {
                    if *stored_op_id == op_id {
                        sector_sys_msg_map.remove(&(sector_idx as SectorIdx));
                    }
                }
            }
            Ok(0) => break, // No more data
            Ok(_) => continue, // Partial read, skip
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break, // No data available
            Err(_) => break, // Other errors, break
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
                debug!("Couldnt connect to process: {} with address: {}\nError: '{}'\nAfter timeout (300) will retry", proc_rank, addr, e);
                sleep(Duration::from_millis(300)).await;
            }
        };
    }
}

async fn bind_socket_for_sending(
    proc_rank: u8,
    tcp_locations: &[(String, u16)],
) -> std::io::Result<TcpStream> {
    let (host, port) = &tcp_locations[proc_rank as usize];
    let addr = format!("{}:{}", host, port);
    return TcpStream::connect(&addr).await;
}