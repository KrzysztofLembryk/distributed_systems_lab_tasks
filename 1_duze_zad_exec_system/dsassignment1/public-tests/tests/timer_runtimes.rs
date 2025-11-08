use assignment_1_solution::{Handler, ModuleRef, System};
use ntest::timeout;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::unbounded_channel;

/// Message sent by the timer
#[derive(Clone)]
struct Tick;

/// A module for running a callback on the second timer timeout message.
struct Timer {
    first_tick_received: bool,
    /// Backdoor for tests
    timeout_callback: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Timer {
    fn new(timeout_callback: Pin<Box<dyn Future<Output = ()> + Send>>) -> Self {
        Self {
            first_tick_received: false,
            timeout_callback: Some(timeout_callback),
        }
    }
}

#[async_trait::async_trait]
impl Handler<Tick> for Timer {
    async fn handle(&mut self, _msg: Tick) {
        if !self.first_tick_received {
            self.first_tick_received = true;
        } else if let Some(callback) = self.timeout_callback.take() {
            callback.await;
        }
    }
}

/// Register a new `Timer` module and request ticks after `duration`.
async fn set_timer(
    system: &mut System,
    timeout_callback: Pin<Box<dyn Future<Output = ()> + Send>>,
    duration: Duration,
) -> ModuleRef<Timer> {
    let timer = system
        .register_module(|_| Timer::new(timeout_callback))
        .await;
    timer.request_tick(Tick, duration).await;
    timer
}

/// Token sent by the backdoor callback.
struct Timeout;

/// Check the real-time delay
#[allow(clippy::cast_possible_wrap)]
async fn second_tick_arrives_after_correct_interval() {
    let mut sys = System::new().await;
    let (timeout_sender, mut timeout_receiver) = unbounded_channel::<Timeout>();
    let timeout_interval = Duration::from_millis(50);

    let start_instant = Instant::now();
    set_timer(
        &mut sys,
        Box::pin(async move {
            timeout_sender.send(Timeout).unwrap();
        }),
        timeout_interval,
    )
    .await;
    timeout_receiver.recv().await.unwrap();
    let elapsed = start_instant.elapsed();

    // Note: this bound may be too strict for some implementations,
    // but it likely indicates congestion.
    assert!((elapsed.as_millis() as i128 - (timeout_interval.as_millis() * 2) as i128).abs() <= 2);
    sys.shutdown().await;
}

// Run the module under various tokio runtimes.

#[tokio::test(flavor = "current_thread")]
#[timeout(300)]
async fn second_tick_arrives_after_correct_interval_this_thread() {
    second_tick_arrives_after_correct_interval().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[timeout(300)]
async fn second_tick_arrives_after_correct_interval_multi_thread() {
    second_tick_arrives_after_correct_interval().await;
}

#[cfg(tokio_unstable)]
#[tokio::test(flavor = "local")]
#[timeout(300)]
async fn second_tick_arrives_after_correct_interval_local() {
    second_tick_arrives_after_correct_interval().await;
}

#[cfg(tokio_unstable)]
#[tokio::test(flavor = "current_thread", unhandled_panic = "shutdown_runtime")]
#[timeout(300)]
async fn second_tick_arrives_after_correct_interval_current_shutdown() {
    second_tick_arrives_after_correct_interval().await;
}

// Use tokio test utils to fake time advancements in the tokio runtime.
// This is why your code should not use primitives from `std::time`!
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(300)]
async fn second_tick_arrives_after_correct_interval_virtual_time() {
    let mut sys = System::new().await;
    let (timeout_sender, mut timeout_receiver) = unbounded_channel::<Timeout>();
    let timeout_interval = Duration::from_millis(50);

    let start_instant = Instant::now();
    let tokio_start_instant = tokio::time::Instant::now();
    set_timer(
        &mut sys,
        Box::pin(async move {
            timeout_sender.send(Timeout).unwrap();
        }),
        timeout_interval,
    )
    .await;
    timeout_receiver.recv().await.unwrap();
    let elapsed = start_instant.elapsed();
    let tokio_vtime_elapsed = tokio_start_instant.elapsed();

    assert!(
        elapsed < timeout_interval,
        "The wall clock time took too long"
    );
    assert_eq!(
        tokio_vtime_elapsed,
        2 * timeout_interval,
        "The virtual time should be exact"
    );
    sys.shutdown().await;
}
