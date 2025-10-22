// WARNING: Do not modify definitions of public types or function names in this
// file – your solution will be tested automatically! Implement all missing parts.

use crate::definitions::{
    Ident, Message, MessageHandler, Module, ModuleMessage, Num, SystemMessage,
};
use crossbeam_channel::{Receiver, Sender, unbounded};
use std::thread;
use std::thread::JoinHandle;

/// Structure representing a Divider module.
///
/// The Divider module performs the division-by-two operation.
#[derive(Debug)]
pub(crate) struct DividerModule {
    /// Identifier of the module.
    id: Ident,
    /// Identifier of the multiplier module.
    other: Option<Ident>,
    /// Queue for outgoing messages.
    queue: Sender<Message>,
}

/// Structure representing a Multiplier module.
///
/// The Multiplier module performs the multiply-by-three-add-one operation.
#[derive(Debug)]
pub(crate) struct MultiplierModule {
    /// The initial value for the computation of the Collatz problem.
    num: Num,
    /// Identifier of the module.
    id: Ident,
    /// Identifier of the other module.
    other: Option<Ident>,
    /// Queue for outgoing messages.
    queue: Sender<Message>,
}

impl DividerModule {
    /// Create the module and register it in the system.
    ///
    /// Note this function is returning an identifier rather than `Self`.
    /// That is, this module may be interacted with only through sending messages.
    pub(crate) fn create(queue: Sender<Message>) -> Ident {
        // The identifier is an opaque number for your code!
        // It represents the idea that modules refer to each other
        // by sending messages at an address.
        let id = Ident::new();
        unimplemented!("Register");
        id
    }
}

/// Method for handling messages directed at a Divider module.
impl MessageHandler for DividerModule {
    /// Get an identifier of the module.
    fn get_id(&self) -> Ident {
        self.id
    }

    /// Handle the computation-step message.
    ///
    /// Here the next number of in the sequence is calculated,
    /// or the computation is stopped.
    /// Remember to make the module send messages to itself,
    /// rather than recursively invoking methods of the module.
    fn compute_step(&mut self, idx: usize, num: Num) {
        assert!(num.is_multiple_of(2));

        unimplemented!("Process");
    }

    /// Handle the init message.
    ///
    /// The module finishes its initialization.
    fn init(&mut self, other: Ident) {
        unimplemented!("Process");
    }
}

impl MultiplierModule {
    /// Create the module and register it in the system.
    pub(crate) fn create(some_num: Num, queue: Sender<Message>) -> Ident {
        // The identifier is an opaque number for your code!
        let id = Ident::new();
        unimplemented!("Register");
        id
    }
}

impl MessageHandler for MultiplierModule {
    fn get_id(&self) -> Ident {
        self.id
    }

    /// Handle the computation-step message.
    ///
    /// Here the next number of in the sequence is calculated,
    /// or the computation is stopped.
    /// Remember to make the module send messages to itself,
    /// rather than recursively invoking methods of the module.
    fn compute_step(&mut self, idx: usize, num: Num) {
        assert!(!num.is_multiple_of(2));
        unimplemented!("Process");
    }

    /// Handle the init message.
    ///
    /// The module finishes its initialization and starts the computation
    /// by sending a message.
    fn init(&mut self, other: Ident) {
        unimplemented!("Process");
    }
}

/// Run the executor.
///
/// The executor handles `Message::System` messages locally
/// and dispatches `Message::ToModule` by calling methods of `<Module as MessageHandler>`.
/// The system should be generic to support any number of modules of any types.
/// For instance, adding a new module type to the `Module` enum in the `definitions.rs`
/// should not require changes to the implementation of this function.
///
/// The system returns the value found in the `Exit` message
/// or `None` if there are no more messages to process.
pub(crate) fn run_executor(rx: Receiver<Message>) -> JoinHandle<Option<usize>> {
    unimplemented!();
    thread::spawn(move || {
        while let Ok(msg) = rx.recv() {
            unimplemented!("Handle");
        }
        // No more messages in the system
        None
    })
}

/// Execute the Collatz problem returning the *total stopping time* of `n`.
///
/// The *total stopping time* is just the number of iterations in the computation.
///
/// PS, this function is known to stop for every possible u64 value :).
pub(crate) fn collatz(n: Num) -> usize {
    // Create the queue and two modules:
    let (tx, rx): (Sender<Message>, Receiver<Message>) = unbounded();
    let divider = DividerModule::create(tx.clone());
    let multiplier = MultiplierModule::create(n, tx.clone());

    // Initialize the modules by sending `Init` messages:
    unimplemented!();

    // Run the executor:
    run_executor(rx).join().unwrap().unwrap()
}
