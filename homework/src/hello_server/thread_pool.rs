//! Thread pool that joins all thread when dropped.

// NOTE: Crossbeam channels are MPMC, which means that you don't need to wrap the receiver in
// Arc<Mutex<..>>. Just clone the receiver and give it to each worker thread.
use crossbeam_channel::{unbounded, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

struct Job(Box<dyn FnOnce() + Send + 'static>);

#[derive(Debug)]
struct Worker {
    _id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Worker {
    /// When dropped, the thread's `JoinHandle` must be `join`ed.  If the worker panics, then this
    /// function should panic too.  NOTE: that the thread is detached if not `join`ed explicitly.
    fn drop(&mut self) {
        let t = self.thread.take().unwrap().join().unwrap();
    }
}

/// Internal data structure for tracking the current job status. This is shared by the worker
/// closures via `Arc` so that the workers can report to the pool that it started/finished a job.
#[derive(Debug, Default)]
struct ThreadPoolInner {
    job_count: Mutex<usize>,
    empty_condvar: Condvar,
}

impl ThreadPoolInner {
    /// Increment the job count.
    fn start_job(&self) {
        let mut guard = self.job_count.lock().unwrap();
        *guard += 1;
    }

    /// Decrement the job count.
    fn finish_job(&self) {
        let mut guard = self.job_count.lock().unwrap();
        *guard -= 1;
        if *guard == 0 {
            self.empty_condvar.notify_one();
        }
    }

    /// Wait until the job count becomes 0.
    ///
    /// NOTE: We can optimize this function by adding another field to `ThreadPoolInner`, but let's
    /// not care about that in this homework.
    fn wait_empty(&self) {
        let guard = self.job_count.lock().unwrap();
        if *guard != 0 {
            let _ = self.empty_condvar.wait(guard);
        }
    }

    pub fn new() -> Self {
        Self {
            job_count: Mutex::new(0),
            empty_condvar: Condvar::new(),
        }
    }
}

/// Thread pool.
#[derive(Debug)]
pub struct ThreadPool {
    _workers: Vec<Worker>,
    job_sender: Option<Sender<Job>>,
    pool_inner: Arc<ThreadPoolInner>,
}

impl ThreadPool {
    /// Create a new ThreadPool with `size` threads. Panics if the size is 0.
    pub fn new(size: usize) -> Self {
        assert!(size > 0);
        let (sender, reciever) = unbounded::<Job>();
        let mut workers = Vec::new();
        let pool_inner = Arc::new(ThreadPoolInner::new());

        for _ in 0..size {
            let pool_inner = Arc::clone(&pool_inner);
            let reciever = reciever.clone();

            let handle = thread::spawn(move || loop {
                match reciever.recv() {
                    Ok(j) => {
                        j.0();
                        pool_inner.finish_job();
                    }
                    Err(e) => {
                        break;
                    }
                }
            });
            let worker = Worker {
                _id: workers.len(),
                thread: Some(handle),
            };
            workers.push(worker);
        }
        Self {
            _workers: workers,
            job_sender: Some(sender),
            pool_inner,
        }
    }

    /// Execute a new job in the thread pool.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(job_sender) = &self.job_sender {
            self.pool_inner.start_job();
            job_sender.send(Job(Box::new(f))).unwrap();
        }
    }

    /// Block the current thread until all jobs in the pool have been executed.  NOTE: This method
    /// has nothing to do with `JoinHandle::join`.
    pub fn join(&self) {
        self.pool_inner.wait_empty();
    }
}

impl Drop for ThreadPool {
    /// When dropped, all worker threads' `JoinHandle` must be `join`ed. If the thread panicked,
    /// then this function should panic too.
    fn drop(&mut self) {
        if let Some(job_sender) = self.job_sender.take() {
            let _ = job_sender;
        }
        while let Some(worker) = self._workers.pop() {
            let _ = worker;
        }
    }
}
