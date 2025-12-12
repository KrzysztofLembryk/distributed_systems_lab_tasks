#![allow(clippy::explicit_deref_methods)]
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::spawn;

// A type `T` is Sync when `&T` is Send:
fn immutable_data_is_sync() {
    // `Arc<T>` is Send + Sync when `T` is Send + Sync.
    // `Vec<T>` is Sync/Send when `T` is Sync/Send.
    let vec = Arc::new(vec![1, 2, 3]);
    let vec_clone = Arc::clone(&vec);

    let t1 = spawn(move || {
        println!("immutable_data_is_sync: {vec_clone:?}");
    });
    let t2 = spawn(move || {
        println!("immutable_data_is_sync: {vec:?}");
    });

    t1.join().unwrap();
    t2.join().unwrap();
}

// Immutable mutex does not prevent from mutating the value wrapped in it:
fn immutable_mutex_reference_grants_mutable_access() {
    fn modify_data_behind_mutex(data: &Mutex<Vec<u32>>) {
        // Lock the mutex:
        let mut guard = data.lock().unwrap();

        // Get mutable reference to the value:
        let mut_ref: &mut Vec<u32> = guard.deref_mut();

        // Modify the value:
        mut_ref.push(1);
    } // the `guard` is dropped, unlocking the mutex

    // An immutable mutex:
    let safe_data = Mutex::new(vec![0]);

    // Modify the protected data:
    modify_data_behind_mutex(&safe_data);

    println!(
        "immutable_mutex_reference_grants_mutable_access: {:?}",
        safe_data.lock().unwrap()
    );
}

// The combination `Arc<Mutex<T>>` is very useful for sharing some data
// between multiple threads in Rust:
fn atomic_reference() {
    let send_and_sync = Arc::new(Mutex::new(vec![1, 2, 3]));
    let cloned = Arc::clone(&send_and_sync);

    let thread = spawn(move || {
        let mut guarded_data = cloned.lock().unwrap();
        // The `guard.deref_mut()` is done automatically for method dispatch:
        guarded_data.push(4);
    });

    // Won't compile, `cloned` was moved to the closure:
    // cloned.lock().unwrap().push(5);

    // But the other reference can still be used:
    send_and_sync.lock().unwrap().push(5);

    thread.join().unwrap();
}

// Typically Condvar is paired with Mutex, and they are usually wrapped in Arc:
fn conditional_variable() {
    let shared = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
    let cloned = Arc::clone(&shared);
    let predicate = |vec: &Vec<i32>| !vec.is_empty();

    // One thread:
    let thread = spawn(move || {
        // The "reborrow" operation (`&` + `*`) is essentially equivalent to `cloned.deref()`.
        let (lock, cond) = &*cloned;
        let mut guard = lock.lock().unwrap();
        while !predicate(guard.deref()) {
            // If the predicate does not hold, call `wait()`. It atomically
            // releases the mutex and waits for a notification. The while loop
            // is required because of the possible spurious wakeups:
            guard = cond.wait(guard).unwrap();
        }
        // Note: the while loop can be replaced with `Condvar::wait_while`.
        // The predicate holds and the mutex is locked here.
        // ...
    });

    // The other thread:
    {
        let (lock, cond) = &*shared;
        let mut guard = lock.lock().unwrap();
        guard.push(0);
        // Wake up all threads waiting on the variable:
        cond.notify_all();
    }

    thread.join().unwrap();
}

fn atomic_bool() {
    let is_first = Arc::new(AtomicBool::new(true));
    let is_first_clone = Arc::clone(&is_first);

    let thread = spawn(move || {
        is_first_clone.store(false, Ordering::SeqCst);
    });

    println!("Am I first? {}", is_first.load(Ordering::SeqCst));

    thread.join().unwrap();
}

fn main() {
    immutable_data_is_sync();
    immutable_mutex_reference_grants_mutable_access();
    atomic_reference();
    conditional_variable();
    atomic_bool();
}
