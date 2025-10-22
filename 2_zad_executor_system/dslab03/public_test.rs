#[cfg(test)]
mod tests {
    use crate::definitions::{Message, MessageHandler, Module, SystemMessage};
    use crate::solution::{DividerModule, MultiplierModule, collatz, run_executor};
    use crossbeam_channel::unbounded;
    use ntest::timeout;

    #[test]
    #[timeout(200)]
    fn collatz_ends() {
        assert_eq!(collatz(1_234_567), 112);
    }

    #[test]
    fn modules_are_unit_testable() {
        let (tx, rx) = unbounded();
        MultiplierModule::create(17, tx.clone());

        assert_eq!(rx.len(), 1);
        let Message::System(SystemMessage::RegisterModule(Module::Multiplier(mut mult))) =
            rx.try_recv().unwrap()
        else {
            panic!(
                "Creating a module resulted in a different message than the correct RegisterModule!"
            )
        };

        let div_id = DividerModule::create(tx);
        let _ = mult.get_id();
        mult.init(div_id);
        mult.compute_step(109, 5);
    }

    #[test]
    #[timeout(200)]
    fn system_is_unit_testable() {
        let (tx, rx) = unbounded();
        tx.send(Message::System(SystemMessage::Exit(0))).unwrap();
        drop(tx);
        assert_eq!(run_executor(rx).join().unwrap(), Some(0));
    }
}
