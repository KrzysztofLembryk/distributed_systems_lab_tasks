use module_system::{Handler, ModuleRef, System, TimerHandle};
use other_raft_structs::{ServerType, ServerState, PersistentState};
use tokio::time::Duration;

pub use domain::*;

use crate::other_raft_structs::VolatileState;

mod types_defs;
mod other_raft_structs;
mod domain;

#[derive(Clone)]
struct Timeout;

struct Init;

#[non_exhaustive]
pub struct Raft 
{
    // TODO you can add fields to this struct.
    config: ServerConfig,
    role: ServerType,
    state: ServerState,
    state_machine: Box<dyn StateMachine>,
    stable_storage: Box<dyn StableStorage>,
    message_sender: Box<dyn RaftSender>,
    timer_handle: Option<TimerHandle>,
    self_ref: ModuleRef<Self>,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> 
    {
        let persistent_state: PersistentState;
        let volatile_state: VolatileState = VolatileState::new();

        if let Some(retrieved_state) = 
            stable_storage.get(&config.self_id.to_string()).await
        {
            persistent_state = decode_from_slice(&retrieved_state).unwrap();
        }
        else
        {
            persistent_state = PersistentState::new();
        }

        let state = ServerState { 
            persistent: persistent_state, 
            volatile: volatile_state
        };

        let self_ref = system
            .register_module(|self_ref| Raft {
                config,
                role: ServerType::Follower,
                state,
                state_machine,
                stable_storage,
                message_sender,
                timer_handle: None,
                self_ref,
            })
            .await;

        return self_ref;
    }

    async fn reset_timer(&mut self, interval: Duration) 
    {
        if let Some(handle) = self.timer_handle.take() 
        {
            handle.stop().await;
        }

        self.timer_handle = 
            Some(self.self_ref.request_tick(Timeout, interval).await);
    }
}

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, _msg: Init) {
        self.reset_timer(self.config.heartbeat_timeout).await;
    }
}

#[async_trait::async_trait]
impl Handler<Timeout> for Raft 
{
    async fn handle(&mut self, _: Timeout) 
    {
        todo!("Implement Timeout handler");
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) 
    {
        todo!()
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft 
{
    async fn handle(&mut self, msg: ClientRequest) 
    {
        todo!()
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.
