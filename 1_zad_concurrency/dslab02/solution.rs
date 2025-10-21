use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::thread::spawn;

type Task = Box<dyn FnOnce() + Send>;

// You can define new types (e.g., structs) if you need.
// However, they shall not be public (i.e., do not use the `pub` keyword).

/// The thread pool.
pub struct Threadpool {
    // Add here any fields you need.
    // We suggest storing handles of the worker threads, submitted tasks,
    // and information whether the pool is running or is shutting down.
    is_shutting_down: Arc<Mutex<bool>>,

    // we need a queue for tasks --> tasks need to be ARC with mutex and vec
    // since we want multiple threads to have access and to be able to modify it
    tasks_queue: Arc<Mutex<Vec<Task>>>,

    // we want to store thread handles so we can wait for them and join them
    thread_handles: Vec<JoinHandle<()>>,

}

impl Threadpool {
    /// Create new thread pool with `workers_count` workers.
    pub fn new(workers_count: usize) -> Self {
        // unimplemented!("Initialize necessary data structures.");
        let is_shutting_down = Arc::new(Mutex::new(false));
        let tasks_queue = Arc::new(Mutex::new(Vec::new()));
        let mut handlers: Vec<JoinHandle<()>> = vec![];

        for i in 0..workers_count {
            // unimplemented!("Create the workers.");
            let is_shutting_down_clone = Arc::clone(&is_shutting_down);
            let tasks_queue_clone = Arc::clone(&tasks_queue);

            handlers.push(spawn(move || {
                Threadpool::worker_loop(
                    i, 
                    is_shutting_down_clone, 
                    tasks_queue_clone
                );
            }));
        }

        Threadpool {
            is_shutting_down: is_shutting_down,
            tasks_queue: tasks_queue,
            thread_handles: handlers,
        }

        // unimplemented!("Return the new Threadpool.");
    }

    /// Submit a new task.
    pub fn submit(&self, task: Task) {
        unimplemented!("We suggest saving the task, and notifying the worker(s)");
    }

    // We suggest extracting the implementation of the worker to an associated
    // function, like this one (however, it is not a part of the public
    // interface, so you can delete it if you implement it differently):
    fn worker_loop(
        id: usize, 
        is_running: Arc<Mutex<bool>>, 
        tasks_queue: Arc<Mutex<Vec<Task>>>) 
    {
        unimplemented!("Initialize necessary variables.");

        loop {
            unimplemented!("Wait for a task and then execute it.");
            unimplemented!(
                "If there are no tasks, and the thread pool is to be shut down, break the loop."
            );
            unimplemented!("Be careful with locking! The tasks shall be executed concurrently.");
        }
    }
}

impl Drop for Threadpool {
    /// Gracefully end the thread pool.
    ///
    /// It waits until all submitted tasks are executed,
    /// and until all threads are joined.
    fn drop(&mut self) {
        unimplemented!("Notify the workers that the thread pool is to be shut down.");
        unimplemented!("Wait for all threads to be finished.");
    }
}
