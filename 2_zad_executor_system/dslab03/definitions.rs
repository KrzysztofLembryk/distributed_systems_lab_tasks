//! Definitions used in the task.
//!
//! This example also shows some limitations of a statically typed system,
//! as adding a new module or message types would need changes in multiple places.
//! We are going to discuss this problem soon,
//! but for now focus on this approach.
//!
//! WARNING: do not modify this file because you don't submit it
//! (and even if you do, it will be ignored).
//! Otherwise, your submission might fail to compile upon grading!

// We import the actual modules types from your solution.
use crate::solution::{DividerModule, MultiplierModule};
use std::sync::atomic::{AtomicU32, Ordering};

pub(crate) type Num = u64;

/// An enumeration representing possible modules.
///
/// This is one approach to support heterogeneous modules:
/// a new kind of module would need a new entry in this enum.
#[derive(Debug)]
pub(crate) enum Module {
    Divider(DividerModule),
    Multiplier(MultiplierModule),
}

/// The main type of messages sent in our system.
///
/// The variants specify whether the message is addressed to a module or to the executor.
/// Splitting all possible messages in a hierarchy of enumerations allows us to
/// split their handling to dedicated functions.
#[derive(Debug)]
pub(crate) enum Message {
    /// A message directed to the system executor.
    /// Note that this is a struct without named fields: a tuple struct,
    /// but one usually uses pattern-matching to extract the inner value.
    System(SystemMessage),
    /// A message directed at a concrete module.
    /// It is a tuple of `(module_identifier, actual_message)`.
    /// The system should panic if there is no registered module with such an identifier.
    ToModule(Ident, ModuleMessage),
}

/// Represents a message directed to the executor of the system.
#[derive(Debug)]
pub(crate) enum SystemMessage {
    /// Register the module in the engine.
    RegisterModule(Module),
    /// Indicate the end of calculations,
    /// making the executor to close with the provided value.
    Exit(usize),
}

/// Messages sent to/from the modules.
#[derive(Debug)]
pub(crate) enum ModuleMessage {
    /// Finalize module initialization and initiate the calculations.
    ///
    /// `other` is the identifier of the complementary module.
    ///
    /// `Init` messages should be sent only by the user of the executor system
    /// (in your solution: the `collatz()` function).
    Init { other: Ident },

    /// Initiate the next step of the calculations.
    ///
    /// `idx` is the current index in the sequence.
    /// `num` is the current number of the sequence.
    ComputeStep { idx: usize, num: Num },
}

/// Trait implemented by all modules in the system.
///
/// Each method (apart from `get_id`) corresponds to a message kind in `ModuleMessage`.
pub(crate) trait MessageHandler {
    /// Get an identifier of the module.
    fn get_id(&self) -> Ident;
    /// Handle the [`ModuleMessage::ComputeStep`] message
    // See the comments in `solution.rs` for more hints.
    fn compute_step(&mut self, idx: usize, num: Num);
    /// Handle the [`ModuleMessage::Init`] message
    fn init(&mut self, other: Ident);
}

// Helper implementation of the trait on the module enumeration.
// The implementation just dispatches the method to the corresponding module type.
// This may be done better… You may already start thinking how :).
impl MessageHandler for Module {
    fn get_id(&self) -> Ident {
        match self {
            Module::Divider(m) => m.get_id(),
            Module::Multiplier(m) => m.get_id(),
        }
    }

    fn init(&mut self, other: Ident) {
        match self {
            Module::Divider(m) => m.init(other),
            Module::Multiplier(m) => m.init(other),
        }
    }

    fn compute_step(&mut self, idx: usize, num: Num) {
        println!("Inside {:?}, idx: {idx}, value: {num}", self.get_id());
        match self {
            Module::Divider(m) => m.compute_step(idx, num),
            Module::Multiplier(m) => m.compute_step(idx, num),
        }
    }
}

/// Represents an opaque identifier of a module.
///
/// This is the "newtype" idiom: because the inner field is private,
/// code in the `solution.rs` module cannot access it.
/// It can only use the compare and hash methods automatically generated
/// by the compiler for this new type.
///
/// Because it implements these traits, you may use the value in some collections…
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) struct Ident(u32);

impl Ident {
    /// This is the only way to get a new instance of the Ident type
    pub(crate) fn new() -> Ident {
        // For the sake of simplicity, use a global atomic value
        static UNIQUE_NO: AtomicU32 = AtomicU32::new(13);
        let id = UNIQUE_NO.fetch_add(1, Ordering::AcqRel);
        Self(id)
    }
}
