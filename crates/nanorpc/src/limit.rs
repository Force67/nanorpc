//! A counting semaphore over `std`, used to bound how many connections and
//! in-flight calls a server runs at once. `acquire` blocks for a permit;
//! `try_acquire` returns `None` instead of waiting, which lets a busy server
//! shed load rather than stall the calls it is already serving.

use std::sync::{Arc, Condvar, Mutex};

pub struct Semaphore {
    free: Mutex<usize>,
    released: Condvar,
}

impl Semaphore {
    pub fn new(permits: usize) -> Arc<Semaphore> {
        Arc::new(Semaphore {
            free: Mutex::new(permits),
            released: Condvar::new(),
        })
    }

    /// Waits for a free permit and takes it.
    pub fn acquire(self: &Arc<Self>) -> Permit {
        let mut free = self.free.lock().unwrap();
        while *free == 0 {
            free = self.released.wait(free).unwrap();
        }
        *free -= 1;
        Permit(Arc::clone(self))
    }

    /// Takes a permit if one is free, otherwise returns `None`.
    pub fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let mut free = self.free.lock().unwrap();
        if *free == 0 {
            return None;
        }
        *free -= 1;
        Some(Permit(Arc::clone(self)))
    }
}

/// Holds one permit and returns it to the semaphore on drop.
pub struct Permit(Arc<Semaphore>);

impl Drop for Permit {
    fn drop(&mut self) {
        *self.0.free.lock().unwrap() += 1;
        self.0.released.notify_one();
    }
}
