use core::panic;
use std::io::Empty;
use std::thread::JoinHandle;
use tokio::sync::mpsc::{UnboundedSender, channel, unbounded_channel};
use tokio::time::Duration;
use tokio::sync::broadcast;
use std::sync::{Arc, Weak};

/// We define trait Message; any type that will Implement Message trait must
/// also implement Send trait and have static lifetime
/// - **Send** - type can be safely transferred btwn threads
/// - **static** - type lives for the entire duration of the programme (does 
///                not contain non-static references)
pub trait Message: Send + 'static {}

/// For every type T that is both Send and static this implementes automatically
/// Message trait
impl<T: Send + 'static> Message for T {}

pub trait Module: Send + 'static {}
impl<T: Send + 'static> Module for T {}

/// A trait for modules capable of handling messages of type `M`.
/// - Here we declare trait Handler, 
/// - M must implement Message trait
/// - : Module means that any type implementing Handler<M> must also impl Module
#[async_trait::async_trait]
pub trait Handler<M: Message>: Module {
    /// Handles the message.
    async fn handle(&mut self, msg: M);
}

#[async_trait::async_trait]
trait Handlee<T: Module>: Message {
    async fn get_handled(self: Box<Self>, module: &mut T);
}

#[async_trait::async_trait]
impl<M: Message, T: Handler<M>> Handlee<T> for M {
    async fn get_handled(self: Box<Self>, module: &mut T) {
        module.handle(*self).await;
    }
}

/// A handle returned by `ModuleRef::request_tick()` can be used to stop sending further ticks.
/// non_exhaustive - means that struct may have more fields added in the future
/// - prevents code outside the current crate from relying on knowing all fields
/// - Non-exhaustive structs cannot be constructed outside of crate
#[non_exhaustive]
pub struct TimerHandle {
    // You can add fields to this struct (non_exhaustive makes it SemVer-compatible).
    msg_sender: UnboundedSender<()>,
}

impl TimerHandle {
    /// Stops the sending of ticks resulting from the corresponding call to `ModuleRef::request_tick()`.
    /// If the ticks are already stopped, does nothing.
    pub async fn stop(&self) {
        // We ignore error if other side of channel is closed
        let _ = self.msg_sender.send(());
    }
}

#[non_exhaustive]
pub struct System {
    // You can add fields to this struct (non_exhaustive makes it SemVer-compatible).
    task_handles: Vec<tokio::task::JoinHandle<()>>,
    shutdown_senders: Vec<UnboundedSender<()>>,
    tx_arc_broadcast: Arc<broadcast::Sender<()>> 
}

impl System {
    /// Registers the module in the system.
    ///
    /// Accepts a closure constructing the module (allowing it to hold a reference to itself).
    /// Returns a `ModuleRef`, which can be used then to send messages to the module.
    pub async fn register_module<T: Module>(
        &mut self,
        module_constructor: impl FnOnce(ModuleRef<T>) -> T,
    ) -> ModuleRef<T> 
    {
        // channel for sending messages to modules
        let (tx_msg, mut rx_msg)  = unbounded_channel::<Box<dyn Handlee<T>>>();
        // channel for sending shutdown command to modules
        let (tx_shutdown, mut rx_shutdown)  = unbounded_channel::<()>();
        // We create ModuleRef and give it sender channel, so that we are able
        // to communicate with tokio task using this ModuleRef, and this task
        // will receive msg and run models handler on it
        let mod_ref: ModuleRef<T> = ModuleRef{
            _marker: std::marker::PhantomData,
            msg_sender: tx_msg.clone(),
            broadcast_ref: self.tx_arc_broadcast.clone()
        };

        // Having created ModuleRef we can create module using closure
        let mut new_mod = module_constructor(mod_ref.clone());

        // We create tokio task that will receive messages for new_mod and 
        // using new_mod handle them 
        let task_handle = tokio::spawn(async move {
            loop  
            {
                tokio::select! {
                    biased; // so that we firstly check for shutdown command
                    _ = rx_shutdown.recv() => {break;},
                    Some(msg) = rx_msg.recv() => {
                        msg.get_handled(&mut new_mod).await;
                    }
                }    
            }
        });
        
        // In system we remember channels for sending shutdown messages
        // and tokio task handles
        self.shutdown_senders.push(tx_shutdown);
        self.task_handles.push(task_handle);

        mod_ref
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {
        // we will send only one message which will be shutdown
        let (tx_broadcast, _) = broadcast::channel::<()>(2);
        let arc_broadcast = Arc::new(tx_broadcast);
        System {
            task_handles: Vec::new(),
            shutdown_senders: Vec::new(),
            tx_arc_broadcast: arc_broadcast,
        }
    }

    /// Gracefully shuts the system down.
    pub async fn shutdown(&mut self) 
    {
        // we broadcast shutdown msg to Tick tasks
        let _ = self.tx_arc_broadcast.send(());

        for sender in &self.shutdown_senders        
        {
            // we send shutdown command to every tokio task
            let _ = sender.send(());
        }

        // We await tasks that we created in register_module
        // we will not await tasks created by request_tick
        for handle in self.task_handles.drain(..) {
            let _ = handle.await;
        } 
    }
}

/// A reference to a module used for sending messages.
// You can add fields to this struct (non_exhaustive makes it SemVer-compatible).
#[non_exhaustive]
pub struct ModuleRef<T: Module>
where
    Self: Send, // As T is Send, with this line we easily SemVer-promise ModuleRef is Send.
{
    // If the structure doesn't use `T` in any field, a marker field is required.
    // **It can be removed if type `T` is used in some other field.**
    //
    // A marker field is required to inform the compiler how properties
    // of this structure are related to properties of `T`
    // (i.e., the struct behaves "as it would contain ...").
    // For instance, the semantics would change if we held `&mut T` instead of `T`,
    // therefore, we need to specify variance in `T`.
    // Furthermore, the marker provides auto-trait and drop check resolution in regard to `T`.
    // Typically, `PhantomData<T>` is used, but we don't want to propagate all auto-traits of `T`.
    // We use the standard pattern to make our struct Send/Sync independent of `T`, but covariant.
    _marker: std::marker::PhantomData<fn() -> T>,

    msg_sender: UnboundedSender<Box<dyn Handlee<T>>>,
    broadcast_ref: Arc<broadcast::Sender<()>>,
}

impl<T: Module> ModuleRef<T> {
    /// Sends the message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        // if System::shutdown was invoked and then someone uses send nothing
        // happens and we ignore error (the same as in timeHandle::stop)
        let _ = self.msg_sender.send(Box::new(msg));
    }

    /// Schedules a message to be sent to the module periodically with the given interval.
    /// The first tick is sent after the interval elapses.
    /// Every call to this function results in sending new ticks and does not cancel
    /// ticks resulting from previous calls.
    pub async fn request_tick<M>(&self, message: M, delay: Duration) -> TimerHandle
    where
        M: Message + Clone,
        T: Handler<M>,
    {
        let mut interval = tokio::time::interval(delay);
        // first tick is immediate so we want to skip it
        interval.tick().await;

        // We clone module channel so that we can send msg to the module
        let tx_msg_cloned = self.msg_sender.clone();

        // We subscribe to broadcast so that System::shutdown can stop us
        let mut rx_broadcast = self.broadcast_ref.subscribe();

        // We create new channels so that we can stop ticks
        let (tx_shutdown, mut rx_shutdown) = unbounded_channel::<()>();

        // Since every call resulsts in sending new ticks without cancelling
        // previous ones, each time we will create a new tokio Task that will 
        // handle given interval
        let _ = tokio::spawn(async move {
            loop  
            {
                tokio::select! {
                    biased;
                    Some(_) = rx_shutdown.recv() => {
                        break;
                    },
                    // if we get error we ignore it
                    Ok(_) = rx_broadcast.recv() =>{break;},
                    _ = interval.tick() => {
                                    tx_msg_cloned
                                        .send(Box::new(message.clone()))
                                        .unwrap();
                    }

                }
            }
        });

        TimerHandle
        {
            msg_sender: tx_shutdown,
        }
    }
}

// You may replace this with `#[derive(Clone)]` if you want.
// It is just important to implement the Clone trait.
impl<T: Module> Clone for ModuleRef<T> {
    /// Creates a new reference to the same module.
    fn clone(&self) -> Self {
        ModuleRef { 
            _marker: std::marker::PhantomData, 
            msg_sender: self.msg_sender.clone(),
            broadcast_ref: self.broadcast_ref.clone(),
        }
    }
}
