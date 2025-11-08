use assignment_1_solution::{Handler, ModuleRef, System};
use ntest::timeout;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

const ROUNDS: u32 = 5;

/// `PingPong` module
///
/// Handles two message types: [`Ball`] and [`Init`].
struct PingPong {
    /// Handle to the other module
    other: Option<ModuleRef<PingPong>>,
    /// Counter
    received_msgs: u32,
    /// Whether Init should start computation
    first: bool,
    /// Name of the current module
    name: &'static str,
    /// This is a backdoor messaging used by tests
    log_sender: UnboundedSender<String>,
}

/// A simple message type with no data.
#[derive(Clone)]
struct Ball;

/// The initialization message with a reference to the other module.
#[derive(Clone)]
struct Init {
    target: ModuleRef<PingPong>,
}

#[async_trait::async_trait]
impl Handler<Init> for PingPong {
    async fn handle(&mut self, msg: Init) {
        self.other = Some(msg.target);
        if self.first {
            self.other.as_ref().unwrap().send(Ball).await;
        }
    }
}

fn prepare_msg(name: &str, round: u32) -> String {
    format!("In {name}: received {round}\n")
}

#[async_trait::async_trait]
impl Handler<Ball> for PingPong {
    async fn handle(&mut self, _msg: Ball) {
        self.log_sender
            .send(prepare_msg(self.name, self.received_msgs))
            .unwrap();

        self.received_msgs += 1;
        if self.received_msgs < ROUNDS {
            self.other.as_ref().unwrap().send(Ball).await;
        }
    }
}

#[allow(clippy::similar_names)]
async fn initialize_system(sys: &mut System) -> UnboundedReceiver<String> {
    let (log_sender, log_receiver) = unbounded_channel();
    let ping = sys
        .register_module(|_| PingPong {
            other: None,
            name: "Ping",
            received_msgs: 0,
            first: true,
            log_sender: log_sender.clone(),
        })
        .await;
    let pong = sys
        .register_module(|_| PingPong {
            other: None,
            name: "Pong",
            received_msgs: 0,
            first: false,
            log_sender,
        })
        .await;

    pong.send(Init {
        target: ping.clone(),
    })
    .await;
    ping.send(Init { target: pong }).await;
    log_receiver
}

#[tokio::test]
#[timeout(300)]
async fn ping_pong_runs_correctly() {
    let mut sys = System::new().await;
    let mut log_receiver = initialize_system(&mut sys).await;

    for round in 0..ROUNDS {
        let names = if round < ROUNDS - 1 {
            vec!["Pong", "Ping"]
        } else {
            vec!["Pong"]
        };
        for name in names {
            assert_eq!(prepare_msg(name, round), log_receiver.recv().await.unwrap());
        }
    }

    sys.shutdown().await;
}
