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

    // we need a queue for tasks --> tasks need to be ARC with mutex and vec
    // since we want multiple threads to have access and to be able to modify it
    // we also need condVar since idea is thread will be waiting for our 
    // task vector to be not empty and if it is not empty there is a task that
    // should be done
    // To prevent race conditions in the same mutex we store bool that says 
    // if we are shutting down
    tasks_queue: Arc<(Mutex<(Vec<Task>, bool)>, Condvar)>,

    // we want to store thread handles so we can wait for them and join them
    thread_handles: Vec<JoinHandle<()>>,

}

impl Threadpool {
    /// Create new thread pool with `workers_count` workers.
    pub fn new(workers_count: usize) -> Self {
        // unimplemented!("Initialize necessary data structures.");
        let tasks_queue = Arc::new(
            (Mutex::new((Vec::new(), false)), 
            Condvar::new()
        ));
        let mut handlers: Vec<JoinHandle<()>> = vec![];

        for _ in 0..workers_count {
            // unimplemented!("Create the workers.");
            let tasks_queue_clone = Arc::clone(&tasks_queue);

            handlers.push(spawn(move || {
                Threadpool::worker_loop(
                    tasks_queue_clone
                );
            }));
        }

        Threadpool {
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

        // We take mutex. If success, in guard we have our queue of tasks and
        // bool flag is_shutdown
        let mut guard = lock.lock().unwrap();
        
        // we save the task, guard.0 is task vec, guard.1 is is_shutting_down
        guard.0.push(task);

        // As in shared_memory example we wake up all threads waiting on cond.
        cond.notify_all();
    } // mutex freed herer

    // We suggest extracting the implementation of the worker to an associated
    // function, like this one (however, it is not a part of the public
    // interface, so you can delete it if you implement it differently):
    fn worker_loop(
        tasks_queue: Arc<(Mutex<(Vec<Task>, bool)>, Condvar)>) 
    {
        // unimplemented!("Initialize necessary variables.");
        let no_task_available = |vec: &Vec<Task>| vec.is_empty();

        loop {
            // unimplemented!("Wait for a task and then execute it.");

            let (lock, cond) = &*tasks_queue;
            let mut guard = lock.lock().unwrap();

            while no_task_available(&guard.0) {

                // since we have one mutex for bot shutdown and tasks queue 
                // there are no race conditions
                let shutdown = guard.1;

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

            let task = guard.0.pop().unwrap();

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
            let (lock, cond) = &*self.tasks_queue;

            let mut guard = lock.lock().unwrap();
            guard.1 = true;

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
