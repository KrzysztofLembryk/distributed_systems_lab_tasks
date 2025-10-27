// WARNING: Do not modify definitions of public types or function names in this
// file – your solution will be tested automatically! Implement all missing parts.

use core::panic;
use std::collections::HashMap;

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
        // we clone queue so that we can use it after we move ownership
        let register_tx = queue.clone();
        let new_div_mod = DividerModule{
            id: id, // id is copied (Ident has Copy trait)
            other: None, // we don't have other yet
            queue: queue // queue is moved 
        };
        
        register_tx.send(Message::System(
                SystemMessage::RegisterModule(
                    Module::Divider(new_div_mod)))).expect("DividerModule - create - send failed");

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
    /// - `idx` is the current index in the sequence.
    fn compute_step(&mut self, idx: usize, num: Num) {
        assert!(num.is_multiple_of(2));

        let num = num / 2;
        let idx = idx + 1;

        if num.is_multiple_of(2)
        {
            self.queue.send(Message::ToModule(
                self.id,  
                ModuleMessage::ComputeStep { idx: idx, num: num }))
                .unwrap();
        } 
        else 
        {
            self.queue.send(Message::ToModule(
                self.other.expect("compute_step - DividerModule - other = None"),  
                ModuleMessage::ComputeStep { idx: idx, num: num }))
                .unwrap();
        }

        // unimplemented!("Process");
    }

    /// Handle the init message.
    ///
    /// The module finishes its initialization.
    fn init(&mut self, other: Ident) {
        self.other  = Some(other)
    }
}

impl MultiplierModule {
    /// Create the module and register it in the system.
    pub(crate) fn create(some_num: Num, queue: Sender<Message>) -> Ident {
        // The identifier is an opaque number for your code!
        let id = Ident::new();

        // we clone queue so that we can use it after we move ownership
        let register_tx = queue.clone();
        let new_mult_mod = MultiplierModule{
            id: id, // id is copied (we have Copy trait in def)
            other: None, // we don't have other yet
            queue: queue, // queue is moved 
            num: some_num // some_num is copied (Num is u64) 
        };
        
        register_tx.send(Message::System(
                SystemMessage::RegisterModule(
                    Module::Multiplier(new_mult_mod)))).unwrap();

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
    fn compute_step(&mut self, idx: usize, num: Num) 
    {
        assert!(!num.is_multiple_of(2));

        if num == 1
        {
            self.queue.send(Message::System(SystemMessage::Exit(idx))).expect("MultiplierModule - compute_step - num == 1 - send error");
        }
        else 
        {
            let num = 3 * num + 1;
            let idx = idx + 1;

            if num.is_multiple_of(2)
            {
                self.queue.send(Message::ToModule(
                    self.other.expect("compute_step - DividerModule - other = None"),  
                    ModuleMessage::ComputeStep { idx: idx, num: num }))
                    .unwrap();
            } 
            else 
            {
                self.queue.send(Message::ToModule(
                    self.id,  
                    ModuleMessage::ComputeStep { idx: idx, num: num }))
                    .unwrap();
            }
            
        }
    }

    /// Handle the init message.
    ///
    /// The module finishes its initialization and starts the computation
    /// by sending a message.
    fn init(&mut self, other: Ident) 
    {
        self.other = Some(other);
        let idx = 1;

        if self.num.is_multiple_of(2)
        {
            self.queue.send(Message::ToModule(
                self.other.expect("init - MultModule - other = None"),  
                ModuleMessage::ComputeStep { idx: idx, num: self.num }))
                .unwrap();
        } 
        else 
        {
            self.queue.send(Message::ToModule(
                self.id,
                ModuleMessage::ComputeStep { idx: idx, num: self.num }))
                .unwrap();
        }
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
    thread::spawn(move || {

        // W store hash map where:
        // --> keys = module Ident, 
        // --> values = tuple of Module and bool, where bool remembers if module was initialized
        let mut modules_map: HashMap<Ident, (Module, bool)> = HashMap::new();

        while let Ok(msg) = rx.recv() 
        {
            match msg 
            {
                Message::System(sys_msg) => {
                    match sys_msg 
                    {
                        SystemMessage::RegisterModule(module) => {
                            modules_map.insert(module.get_id(), (module, false));
                        },
                        SystemMessage::Exit(collatz_res) => {
                            return Some(collatz_res);
                        }
                    }
                },
                Message::ToModule(mod_id, mod_msg) => {
                    let (module, is_initialized) = modules_map.get_mut(&mod_id).expect("given module id is not in modules map in executor");

                    match mod_msg
                    {
                        ModuleMessage::Init{other} => {
                            if !*is_initialized
                            {
                                *is_initialized = true;
                                module.init(other);
                            }
                            else  
                            {
                                panic!("Module '{mod_id:?}' is being initialized for the second time!");     
                            }
                        },
                        ModuleMessage::ComputeStep { idx, num } => {
                            if !*is_initialized
                            {
                                panic!("Trying to compute step on module {mod_id:?} which is NOT initialized!!");
                            } 
                            module.compute_step(idx, num);
                        }
                    }
                }
            }
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
    // unimplemented!();
    send_init_to_modules(divider, multiplier, &tx);

    // Run the executor:
    run_executor(rx).join().unwrap().unwrap()
}

fn send_init_to_modules(div_id: Ident, mult_id: Ident, tx: &Sender<Message>)
{
    tx.send(Message::ToModule(div_id, ModuleMessage::Init { other: mult_id })).expect("send_init_to_modules - tx.send div_id encountered error");

    tx.send(Message::ToModule(mult_id, ModuleMessage::Init { other: div_id })).expect("send_init_to_modules - tx.send mult_id encountered error");
}
