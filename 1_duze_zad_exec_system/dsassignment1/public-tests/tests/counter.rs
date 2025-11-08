use assignment_1_solution::{Handler, ModuleRef, System};
use ntest::timeout;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

struct CountToFive {
    self_ref: ModuleRef<Self>,
    /// Backdoor messaging for test.
    five_sender: UnboundedSender<u8>,
}

#[async_trait::async_trait]
impl Handler<u8> for CountToFive {
    async fn handle(&mut self, msg: u8) {
        if msg == 5 {
            self.five_sender.send(msg).unwrap();
        } else {
            self.self_ref.send(msg + 1).await;
        }
    }
}

#[tokio::test]
#[timeout(300)]
async fn self_ref_works() {
    let mut system = System::new().await;
    let (five_sender, mut five_receiver) = unbounded_channel::<u8>();
    let count_to_five = system
        .register_module(|self_ref| CountToFive {
            self_ref,
            five_sender,
        })
        .await;

    count_to_five.send(1).await;

    assert_eq!(five_receiver.recv().await.unwrap(), 5);

    system.shutdown().await;
}

struct Counter {
    num: u8,
    /// Backdoor
    num_sender: UnboundedSender<u8>,
}

/// Message sent by the timer
#[derive(Clone)]
struct Tick;

#[async_trait::async_trait]
impl Handler<Tick> for Counter {
    async fn handle(&mut self, _msg: Tick) {
        self.num_sender.send(self.num).unwrap();
        self.num += 1;
    }
}

#[tokio::test]
#[timeout(500)]
async fn stopping_ticks_works() {
    let mut system = System::new().await;
    let (num_sender, mut num_receiver) = unbounded_channel();
    let counter_ref = system
        .register_module(|_| Counter { num: 0, num_sender })
        .await;

    let timer_handle = counter_ref
        .request_tick(Tick, Duration::from_millis(50))
        .await;
    tokio::time::sleep(Duration::from_millis(170)).await;
    timer_handle.stop().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut received_numbers = Vec::new();
    while let Ok(num) = num_receiver.try_recv() {
        received_numbers.push(num);
    }
    assert_eq!(received_numbers, vec![0, 1, 2]);

    system.shutdown().await;
}
