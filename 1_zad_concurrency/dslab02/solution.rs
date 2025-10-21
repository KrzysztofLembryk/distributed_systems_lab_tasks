use std::sync::{Arc, Condvar, Mutex};
use std::ops::{Deref};
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
    // we also need condVar since idea is thread will be waiting for our 
    // task vector to be not empty and if it is not empty there is a task that
    // should be done
    tasks_queue: Arc<(Mutex<Vec<Task>>, Condvar)>,

    // we want to store thread handles so we can wait for them and join them
    thread_handles: Vec<JoinHandle<()>>,

}

impl Threadpool {
    /// Create new thread pool with `workers_count` workers.
    pub fn new(workers_count: usize) -> Self {
        // unimplemented!("Initialize necessary data structures.");
        let is_shutting_down = Arc::new(Mutex::new(false));
        let tasks_queue = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
        let mut handlers: Vec<JoinHandle<()>> = vec![];

        for _ in 0..workers_count {
            // unimplemented!("Create the workers.");
            let is_shutting_down_clone = Arc::clone(&is_shutting_down);
            let tasks_queue_clone = Arc::clone(&tasks_queue);

            handlers.push(spawn(move || {
                Threadpool::worker_loop(
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
    pub fn submit(&self, task: Task) 
    {
        // unimplemented!("We suggest saving the task, and notifying the worker(s)");
        let (lock, cond) = &*self.tasks_queue;

        // We take mutex. If success, in guard we have our queue of tasks
        let mut guard = lock.lock().unwrap();

        guard.push(task);

        // As in shared_memory example we wake up all threads waiting on cond.
        cond.notify_all();
    }

    // We suggest extracting the implementation of the worker to an associated
    // function, like this one (however, it is not a part of the public
    // interface, so you can delete it if you implement it differently):
    fn worker_loop(
        is_shutting_down: Arc<Mutex<bool>>, 
        tasks_queue: Arc<(Mutex<Vec<Task>>, Condvar)>) 
    {
        // unimplemented!("Initialize necessary variables.");
        let no_task_available = |vec: &Vec<Task>| vec.is_empty();

        loop {
            // unimplemented!("Wait for a task and then execute it.");

            let (lock, cond) = &*tasks_queue;
            let mut guard = lock.lock().unwrap();

            while no_task_available(guard.deref()) {

                let shutdown = {
                    let lock = &*is_shutting_down;
                    let guard = lock.lock().unwrap();

                    *guard
                };

                if shutdown
                {
                    cond.notify_all();
                    return;
                }

                // While there are no tasks available we wait.
                // wait() atomicall releases the mutex and waits for a notification. 
                // The while loop is required because of the possible spurious wakeups
                guard = cond.wait(guard).unwrap();
            }

            let task = guard.pop().unwrap();

            // we need to release the mutex before starting task
            drop(guard);
            task();
            
            // unimplemented!(
            //     "If there are no tasks, and the thread pool is to be shut down, break the loop."
            // );
            // unimplemented!("Be careful with locking! The tasks shall be executed concurrently.");

        }
    }
}

impl Drop for Threadpool {
    /// Gracefully end the thread pool.
    ///
    /// It waits until all submitted tasks are executed,
    /// and until all threads are joined.
    fn drop(&mut self) 
    {
        // unimplemented!("Notify the workers that the thread pool is to be shut down.");
        {
            // we make a scope so that mutex is free after the end of the scope
            let lock = &*self.is_shutting_down;
            let mut shutdown = lock.lock().unwrap();
            *shutdown = true;
        } // here mutex automatically freed

        {
            let (_lock, cond) = &*self.tasks_queue;

            // we have set shutdown to true so we notify all threads that
            // we want them to shutdown
            cond.notify_all();
        } 


        // unimplemented!("Wait for all threads to be finished.");
        for worker in self.thread_handles.drain(..)
        {
            worker.join().unwrap();
        }
    }
}
