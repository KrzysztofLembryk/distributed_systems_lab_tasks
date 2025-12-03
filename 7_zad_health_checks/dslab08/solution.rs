use bincode::config::standard;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::time::Duration;
use uuid::Uuid;

use module_system::{Handler, ModuleRef, System, TimerHandle};

/// A message, which disables a process. Used for testing.
pub struct Disable;

/// A message, which enables a process. Used for testing.
pub struct Enable;

struct Init;

#[derive(Clone)]
struct Timeout;

pub struct FailureDetectorModule {
    /// This is to simulate a disabled process. Keep those checks in place.
    enabled: bool,
    timeout_handle: Option<TimerHandle>,
    delta: Duration,
    delay: Duration,
    self_ref: ModuleRef<Self>,
    // TODO add whatever fields necessary.

    // We need to know our id
    id: Uuid,
    // We are required to send msgs via UDP thus we need a socket to do that
    socket: Arc<UdpSocket>,
    // We need to know all other processes addresses to be able to send 
    // i.e. HeartBeatRequest to all of them
    all_procs: HashMap<Uuid, SocketAddr>,
    // Set of alive processes, at first all processes are alive
    alive: HashSet<Uuid>,
    // alive processes in previous tick
    prev_interval_alive: HashSet<Uuid>,
    // Set of suspected processes, at first is empty
    suspected: HashSet<Uuid>,
}

impl FailureDetectorModule {
    pub async fn new(
        system: &mut System,
        delta: Duration,
        addresses: &HashMap<Uuid, SocketAddr>,
        ident: Uuid,
    ) -> ModuleRef<Self> {
        let addr = addresses.get(&ident).unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());
        let alive: HashSet<Uuid> = addresses.keys().copied().collect();
        let prev_interval_alive = alive.clone();
        let module_ref = system
            .register_module(|self_ref| Self {
                enabled: true,
                timeout_handle: None,
                delta,
                delay: delta,
                self_ref,
                // TODO initialize the fields you added
                socket: socket.clone(),
                all_procs: addresses.clone(),
                id: ident.clone(),
                alive: alive,
                prev_interval_alive: prev_interval_alive,
                suspected: HashSet::new(),
            })
            .await;

        // spawn UDP listener -> ModuleRef bridge
        tokio::spawn(deserialize_and_forward(socket, module_ref.clone()));

        module_ref.send(Init).await;

        module_ref
    }
}

#[async_trait::async_trait]
impl Handler<Init> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Init) {
        self.timeout_handle = Some(self.self_ref.request_tick(Timeout, self.delay).await);
    }
}

/// New operation arrived at a socket.
#[async_trait::async_trait]
impl Handler<DetectorOperationUdp> for FailureDetectorModule {
    async fn handle(&mut self, msg: DetectorOperationUdp) {
        if self.enabled {
            let DetectorOperationUdp(operation, reply_addr) = msg;

            match operation
            {
                DetectorOperation::HeartbeatRequest => {
                    // We got request for Heartbeat, so we send it via our UDP
                    // socket to the reply_addr with our id
                    self.socket
                        .send_to(
                            bincode::serde::encode_to_vec(
                                &DetectorOperation::HeartbeatResponse(self.id), standard())
                                .unwrap()
                                .as_slice(),
                            &reply_addr,
                        )
                        .await
                        .expect("cannot send?");
                },
                DetectorOperation::HeartbeatResponse(proc_id) => {
                    // When we received heartbeat response, we add given proc 
                    // to alive set
                    self.alive.insert(proc_id);
                },
                DetectorOperation::AliveRequest => {
                    let prev_alive = self.prev_interval_alive.clone();
                    self.socket
                        .send_to(
                            bincode::serde::encode_to_vec(
                                &DetectorOperation::AliveInfo(prev_alive), 
                                standard()
                            ).unwrap().as_slice(),
                            &reply_addr,
                        )
                        .await
                        .expect("cannot send?");
                },
                DetectorOperation::AliveInfo(_alive_proc_set) => {
                    // Nothing in the assignment description specified that anything should be done here 
                },
            }

            // unimplemented!(
            //     "Process received UDP messages as in the algorithm.\
            //      Requests should be replied over UDP."
            // );
        }
    }
}

/// Called periodically to check send broadcast and update alive processes.
#[async_trait::async_trait]
impl Handler<Timeout> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Timeout) {
        if self.enabled {
            // unimplemented!("Implement the timeout logic.");

            // We need to always stop our timer, and create a new one with 
            // probably updated delay, we stop it first so that it won't send
            // another msg
            if let Some(t_handle) = &self.timeout_handle
            {
                t_handle.stop().await;
            }

            if !self.alive.is_disjoint(&self.suspected) 
            {
                // As in algorithm from lecture, if alive and suspected are not
                // disjoint we increase delay by delta
                self.delay += self.delta;
            }

            // We implement algorithm from slides
            for (proc_id, proc_addr) in &self.all_procs
            {
                let is_alive = self.alive.contains(proc_id);
                let is_suspected = self.suspected.contains(proc_id);

                if !is_alive && !is_suspected
                {
                    self.suspected.insert(*proc_id);
                }
                else if is_alive && is_suspected
                {
                    self.suspected.remove(proc_id);
                }

                self.socket
                    .send_to(
                        bincode::serde::encode_to_vec(
                            &DetectorOperation::HeartbeatRequest, standard())
                            .unwrap()
                            .as_slice(),
                        proc_addr,
                    )
                    .await
                    .expect("cannot send?");
            }

            self.prev_interval_alive = self.alive.clone();
            self.alive = HashSet::new();
            // We create new timer with possibly new delay
            self.timeout_handle = Some(self.self_ref.request_tick(Timeout, self.delay).await);
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Disable) {
        self.enabled = false;
    }
}

#[async_trait::async_trait]
impl Handler<Enable> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Enable) {
        self.enabled = true;
    }
}

/// Receives messages over UDP and converts them into our module system's messages
async fn deserialize_and_forward(
    socket: Arc<UdpSocket>,
    module_ref: ModuleRef<FailureDetectorModule>,
) {
    let mut buffer = vec![0];
    while let Ok((len, sender)) = socket.peek_from(&mut buffer).await {
        if len == buffer.len() {
            buffer.resize(2 * buffer.len(), 0);
        } else {
            socket.recv_from(&mut buffer).await.unwrap();
            match bincode::serde::decode_from_slice(&buffer, standard()) {
                Ok((msg, _took)) => module_ref.send(DetectorOperationUdp(msg, sender)).await,
                Err(err) => {
                    debug!("Invalid format of detector operation ({})!", err);
                }
            }
        }
    }
}

/// Received UDP message
struct DetectorOperationUdp(DetectorOperation, SocketAddr);

/// Messages that are sent over UDP
#[derive(Serialize, Deserialize)]
pub enum DetectorOperation {
    /// Request to receive a heartbeat.
    HeartbeatRequest,
    /// Response to heartbeat, contains uuid of the receiver of `HeartbeatRequest`.
    HeartbeatResponse(Uuid),
    /// Request to receive information about working processes.
    AliveRequest,
    /// Vector of processes which are alive according to `AliveRequest` receiver.
    AliveInfo(HashSet<Uuid>),
}
