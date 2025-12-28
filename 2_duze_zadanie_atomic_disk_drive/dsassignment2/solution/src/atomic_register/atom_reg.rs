use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use std::pin::Pin;
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

use crate::domain::{
    ClientCommandResponse, ClientRegisterCommand, ClientRegisterCommandContent, 
    SystemRegisterCommand, SystemCommandHeader, SystemRegisterCommandContent, 
    SectorIdx, SectorVec, StatusCode, OperationReturn
};
use crate::atomic_register_public::{AtomicRegister};
use crate::sectors_manager_public::SectorsManager;
use crate::register_client_public::{RegisterClient, Broadcast};
use crate::register_client_public::Send as RegSend;
use log::debug;

type TimestampType = u64;
type WriterRankType = u8;
type ProcIdx = u8;
type SuccessCallbackFunc = Box<
            dyn FnOnce(ClientCommandResponse) 
                -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,>;
type ClientChannelMsg = (Box<ClientRegisterCommand>, SuccessCallbackFunc);
pub struct AtomReg
{
    self_ident: u8,
    sector_idx: SectorIdx,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    success_callback: Option<SuccessCallbackFunc>,
    client_request_id: Option<u64>,
    reg_state: RegisterState
}

#[async_trait::async_trait]
impl AtomicRegister for AtomReg
{
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: SuccessCallbackFunc,
    )
    {
        let header = cmd.header;
        let sector_idx: SectorIdx = header.sector_idx;

        if sector_idx != self.sector_idx
        {
            panic!("AtomicRegister::client_command - we are sector: '{}' but we got command for sector: '{}', this shouldn't happen, since it should be checked before running client_command on AtomicRegister", self.sector_idx, sector_idx);
        }

        self.client_request_id = Some(header.request_identifier);
        // We store callback function for later use, once we have response
        self.success_callback = Some(success_callback);

        let content = cmd.content;

        match content
        {
            ClientRegisterCommandContent::Read => {
                self.reg_state.prepare_for_read();
            },
            ClientRegisterCommandContent::Write { data } => {
                self.reg_state.prepare_for_write(data);
            }
        }

        let sys_cmd_header = SystemCommandHeader {
            process_identifier: self.self_ident,
            msg_ident: self.reg_state.op_id.unwrap(),
            sector_idx: self.sector_idx
        };
        let sys_cmd_content = SystemRegisterCommandContent::ReadProc;
        let sys_cmd = SystemRegisterCommand {
            header: sys_cmd_header,
            content: sys_cmd_content
        };

        self.register_client.broadcast(Broadcast::new(sys_cmd)).await;

        // After broadcast we end handling client command, and will wait for system
        // commands to get consensus on what to send
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand)
    {
        let recv_header = cmd.header;
        let sector_idx: SectorIdx = recv_header.sector_idx;

        if sector_idx != self.sector_idx
        {
            panic!("AtomicRegister::system_command - we are sector: '{}' but we got command for sector: '{}', this shouldn't happen, since it should be checked before running system_command on AtomicRegister", self.sector_idx, sector_idx);
        }

        let content = cmd.content;

        match content
        {
            SystemRegisterCommandContent::ReadProc => {
                self.handle_readproc(&recv_header).await;
            },
            SystemRegisterCommandContent::Value { 
                timestamp, 
                write_rank, 
                sector_data 
            } => {
                self.handle_value(timestamp, write_rank, sector_data, &recv_header).await;
            },
            SystemRegisterCommandContent::WriteProc { 
                timestamp, 
                write_rank, 
                data_to_write 
            } => {
                self.handle_writeproc(timestamp, write_rank, data_to_write, &recv_header).await;
            },
            SystemRegisterCommandContent::Ack => { 
                self.handle_ack(&recv_header).await;
            }
        }
    }
}

pub fn spawn_atomic_register_task(
    self_ident: u8,
    sector_idx: SectorIdx,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    mut sys_cmd_rx: UnboundedReceiver<Arc<SystemRegisterCommand>>,
    mut client_cmd_rx: UnboundedReceiver<ClientChannelMsg>,
)
{
    tokio::spawn( async move {
            let mut atomic_register = AtomReg::new(
                self_ident, 
                sector_idx, 
                register_client, 
                sectors_manager, 
                processes_count
            ).await;

            loop
            {
                tokio::select! {
                    // If we get msg from client we switch to handling ONLY system 
                    // msgs till we send response to client. Since we don't want to
                    // start next client msg computation before ending prev one
                    Some(client_msg) = client_cmd_rx.recv() => {
                        let (cmd, success_callback) = client_msg;
                        atomic_register.client_command(*cmd, success_callback).await;

                        while let Some(sys_msg) = sys_cmd_rx.recv().await {

                            atomic_register.system_command((*sys_msg).clone()).await;
                            // If we've ended handling of current sys command we can 
                            // take new one
                            if atomic_register.handling_of_client_command_ended()
                            {
                                break;
                            }
                        }
                    }
                    Some(sys_msg) = sys_cmd_rx.recv() => {
                        atomic_register.system_command((*sys_msg).clone()).await;
                    }
                }
            }
        }
    );
}


fn highest(
    readlist: &HashMap<ProcIdx, (TimestampType, WriterRankType, SectorVec)>
) -> (TimestampType, WriterRankType, SectorVec)
{
    if readlist.is_empty() {
        panic!("AtomicRegister::highest - provided readlist is empty, this should never happen, since we should have at least one process");
    }
    let mut first: bool = true;
    let mut max_ts: u64 = 0;
    let mut max_rank: u8 = 0;
    let mut ref_sector_vec: Option<&SectorVec> = None;

    for (_, (timestamp, writer_rank, sector_vec)) in readlist
    {
        if first
        {
            first = false;
            max_ts = *timestamp;
            max_rank = *writer_rank;
            ref_sector_vec = Some(sector_vec);
        }
        else
        {
            if (*timestamp, *writer_rank) > (max_ts, max_rank)
            {
                max_ts = *timestamp;
                max_rank = *writer_rank;
                ref_sector_vec = Some(sector_vec);
            }
        }
    }

    return (max_ts, max_rank, ref_sector_vec.unwrap().clone());
}

impl AtomReg
{
    pub async fn new(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> AtomReg
    {
        let (recovered_ts, recovered_wr) = 
            sectors_manager.read_metadata(sector_idx).await;
        let register_val = sectors_manager.read_data(sector_idx).await;

        return AtomReg { 
            self_ident, 
            sector_idx, 
            register_client, 
            sectors_manager, 
            processes_count,
            success_callback: None,
            client_request_id: None,
            reg_state: RegisterState::new(recovered_ts, recovered_wr, register_val)
        };
    }

    fn handling_of_client_command_ended(&self) -> bool
    {
        return self.success_callback.is_none();
    }

    async fn handle_readproc(&mut self, recv_header: &SystemCommandHeader)
    {
        let ts = self.reg_state.timestamp;
        let wr = self.reg_state.writer_rank;
        let val = self.reg_state.register_val.clone();

        let sys_cmd_header = SystemCommandHeader {
            process_identifier: self.self_ident,
            msg_ident: recv_header.msg_ident,
            sector_idx: recv_header.sector_idx
        };
        let sys_cmd_content = SystemRegisterCommandContent::Value { 
            timestamp: ts, 
            write_rank: wr, 
            sector_data: val 
        };

        self.register_client.send(RegSend::new(
            SystemRegisterCommand {
                header: sys_cmd_header,
                content: sys_cmd_content
            }, 
            recv_header.process_identifier
        )).await;
    }

    async fn handle_value(
        &mut self, 
        timestamp: TimestampType, 
        write_rank: WriterRankType, 
        sector_data: SectorVec, 
        recv_header: &SystemCommandHeader
    )
    {
        if let Some(op_id) = self.reg_state.op_id
        {
            if op_id == recv_header.msg_ident
            && !self.reg_state.write_phase
            {
                self.reg_state.readlist.insert(
                    recv_header.process_identifier,
                    (timestamp, write_rank, sector_data)
                );

                let proc_count: usize = self.processes_count as usize;

                if self.reg_state.readlist.len() > (proc_count / 2)
                && (self.reg_state.reading || self.reg_state.writing)
                {
                    debug!("HandleValue: Proc: {}, Sector: {} got majority", self.self_ident, recv_header.sector_idx);
                    self.reg_state.readlist.insert(
                        self.self_ident, 
                        (
                            self.reg_state.timestamp, 
                            self.reg_state.writer_rank,
                            self.reg_state.register_val.clone()
                        )
                    );

                    let (max_ts, max_rank, readval) = 
                        highest(&self.reg_state.readlist);

                    self.reg_state.readval = Some(readval.clone());
                    self.reg_state.readlist.clear();
                    self.reg_state.acklist.clear();
                    self.reg_state.write_phase = true;

                    let header = SystemCommandHeader {
                        process_identifier: self.self_ident,
                        msg_ident: op_id,
                        sector_idx: self.sector_idx
                    };
                    let content;

                    if self.reg_state.reading
                    {
                        content = SystemRegisterCommandContent::WriteProc { 
                            timestamp: max_ts, 
                            write_rank: max_rank, 
                            data_to_write: readval 
                        };
                    }
                    else // writing
                    {
                        self.reg_state.timestamp = max_ts + 1;
                        self.reg_state.writer_rank = self.self_ident;
                        self.reg_state.register_val = self.reg_state.writeval.clone()
                            .expect("AtomReg::handle_value::reg_state.writing - self.reg_state_.writeval is NONE, but it shouldn't be");

                        self.sectors_manager.write(
                            self.sector_idx, 
                            &(
                                self.reg_state.register_val.clone(),
                                self.reg_state.timestamp,
                                self.reg_state.writer_rank
                            )
                        ).await;

                        content = SystemRegisterCommandContent::WriteProc { 
                            timestamp: max_ts + 1, 
                            write_rank: self.self_ident, 
                            data_to_write: self.reg_state.writeval.take().unwrap()
                        };
                    }

                    self.register_client.broadcast(
                        Broadcast::new(SystemRegisterCommand {header, content})
                    ).await;
                }
            }
        }
        // If op_id is None or we are in write_phase it means
        // that we already handled this request and these commands 
        // are old and we want to ignore them.
        // if op_id != recv_header.msg_ident this means that either sb made 
        // mistake, OR we got realy old answer, and we are already processing
        // different client request, so we want to ignore it
    }

    async fn handle_writeproc(
        &mut self, 
        timestamp: TimestampType, 
        write_rank: WriterRankType, 
        data_to_write: SectorVec, 
        recv_header: &SystemCommandHeader
    )
    {
        if (self.reg_state.timestamp, self.reg_state.writer_rank) 
        < (timestamp, write_rank)
        {
            self.reg_state.timestamp = timestamp;
            self.reg_state.writer_rank = write_rank;
            self.reg_state.register_val = data_to_write;

            self.sectors_manager.write(
                self.sector_idx, 
                // Todo: we could store inside reg_state this values as
                // tuple (sector_vec, timestamp, write_rank) pass ref here
                &(
                    self.reg_state.register_val.clone(),
                    self.reg_state.timestamp,
                    self.reg_state.writer_rank
                )
            ).await;
        }

        let header = SystemCommandHeader {
            process_identifier: self.self_ident,
            msg_ident: recv_header.msg_ident,
            sector_idx: self.sector_idx
        };

        self.register_client.send(
            RegSend::new(
                SystemRegisterCommand {
                    header,
                    content: SystemRegisterCommandContent::Ack
                },
                recv_header.process_identifier)
        ).await;
    }

    async fn handle_ack(
        &mut self,
        recv_header: &SystemCommandHeader
    )
    {
        if let Some(op_id) = self.reg_state.op_id
        {
            if op_id != recv_header.msg_ident 
            || !self.reg_state.write_phase
            {
                return;
            }

            self.reg_state.acklist.insert(recv_header.process_identifier);
    
            if self.reg_state.acklist.len() > (self.processes_count / 2) as usize
            && (self.reg_state.reading || self.reg_state.writing)
            {
                debug!("HandleAck: Proc: {}, Sector: {} got majority", self.self_ident, recv_header.sector_idx);

                let client_callback: SuccessCallbackFunc = self.success_callback
                    .take()
                    .expect("AtomReg::handle_ack - self.success_callback is NONE, but it shouldn't be since we need to send result to client");
    
                self.reg_state.acklist.clear();
                self.reg_state.write_phase = false;
                let request_id = self.client_request_id.take().expect("AtomReg::handle_ack - client_request_id is NONE, but it shouldn't be");
                let response: ClientCommandResponse;
    
                if self.reg_state.reading
                {
                    self.reg_state.reading = false;
                    let readval = self.reg_state.readval
                    .take()
                    .expect("AtomReg::handle_ack - self.reg_state.readval is NONE but it shouldn't be since we are responding to READ request");
    
                    response = ClientCommandResponse {
                        status: StatusCode::Ok,
                        request_identifier: request_id,
                        op_return: OperationReturn::Read { read_data: readval  }
                    };
                }
                else
                {
                    self.reg_state.writing = false;
                    
                    response = ClientCommandResponse {
                        status: StatusCode::Ok,
                        request_identifier: request_id,
                        op_return: OperationReturn::Write 
                    };
                }
    
                client_callback(response).await;
            }
        }
    }
}

struct RegisterState
{
    timestamp: TimestampType, 
    writer_rank: WriterRankType, 
    register_val: SectorVec, 
    readlist: HashMap<ProcIdx, (TimestampType, WriterRankType, SectorVec)>,
    acklist: HashSet<ProcIdx>,
    reading: bool,
    writing: bool,
    writeval: Option<SectorVec>,
    readval: Option<SectorVec>,
    write_phase: bool,
    op_id: Option<Uuid>,
}

impl RegisterState
{
    fn new(
        timestamp: TimestampType, 
        writer_rank: WriterRankType, 
        register_val: SectorVec, 
    ) -> RegisterState
    {
        return RegisterState { 
            timestamp, 
            writer_rank, 
            register_val, 
            readlist: HashMap::new(), 
            acklist: HashSet::new(), 
            reading: false, 
            writing: false, 
            writeval: None, 
            readval: None, 
            write_phase: false,
            op_id: None,
        };
    }

    fn prepare_for_read(&mut self)
    {
        self.op_id = Some(Uuid::new_v4());
        self.readlist.clear();
        self.acklist.clear();
        self.reading = true;
    }

    fn prepare_for_write(&mut self, val_to_write: SectorVec)
    {
        self.op_id = Some(Uuid::new_v4());
        self.writeval = Some(val_to_write);
        self.acklist.clear();
        self.readlist.clear();
        self.writing = true;
    }
}