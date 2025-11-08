use tokio::time::Duration;

pub trait Message: Send + 'static {}
impl<T: Send + 'static> Message for T {}

pub trait Module: Send + 'static {}
impl<T: Send + 'static> Module for T {}

/// A trait for modules capable of handling messages of type `M`.
#[async_trait::async_trait]
pub trait Handler<M: Message>: Module {
    /// Handles the message.
    async fn handle(&mut self, msg: M);
}

/// A handle returned by `ModuleRef::request_tick()` can be used to stop sending further ticks.
#[non_exhaustive]
pub struct TimerHandle {
    // You can add fields to this struct (non_exhaustive makes it SemVer-compatible).
}

impl TimerHandle {
    /// Stops the sending of ticks resulting from the corresponding call to `ModuleRef::request_tick()`.
    /// If the ticks are already stopped, does nothing.
    pub async fn stop(&self) {
        unimplemented!()
    }
}

#[non_exhaustive]
pub struct System {
    // You can add fields to this struct (non_exhaustive makes it SemVer-compatible).
}

impl System {
    /// Registers the module in the system.
    ///
    /// Accepts a closure constructing the module (allowing it to hold a reference to itself).
    /// Returns a `ModuleRef`, which can be used then to send messages to the module.
    pub async fn register_module<T: Module>(
        &mut self,
        module_constructor: impl FnOnce(ModuleRef<T>) -> T,
    ) -> ModuleRef<T> {
        unimplemented!()
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {
        unimplemented!()
    }

    /// Gracefully shuts the system down.
    pub async fn shutdown(&mut self) {
        unimplemented!()
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
}

impl<T: Module> ModuleRef<T> {
    /// Sends the message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        unimplemented!()
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
        unimplemented!()
    }
}

// You may replace this with `#[derive(Clone)]` if you want.
// It is just important to implement the Clone trait.
impl<T: Module> Clone for ModuleRef<T> {
    /// Creates a new reference to the same module.
    fn clone(&self) -> Self {
        unimplemented!()
    }
}
