//! Thread session quota per instance;Failure preparation and successful closing are both returned through ownership leases,Do not use global TLS.
use crate::types::Error;
use std::{
    sync::{Arc, Mutex},
    thread::ThreadId,
};
pub(crate) struct ThreadSessions {
    limit: usize,
    active: Mutex<Vec<ThreadId>>,
}
impl ThreadSessions {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            active: Mutex::new(Vec::new()),
        }
    }
    pub fn enter(self: &Arc<Self>) -> Result<ThreadSession, Error> {
        let thread = std::thread::current().id();
        let mut active = self
            .active
            .lock()
            .map_err(|_| Error::InvalidState("Thread session quota lock poisoning"))?;
        if active.len() >= self.limit {
            return Err(Error::CapacityExceeded);
        }
        if active.contains(&thread) {
            return Err(Error::Busy);
        }
        active.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        active.push(thread);
        Ok(ThreadSession {
            registry: self.clone(),
            thread,
        })
    }
}
pub(crate) struct ThreadSession {
    registry: Arc<ThreadSessions>,
    thread: ThreadId,
}
impl Drop for ThreadSession {
    fn drop(&mut self) {
        // There is no user code in the quota lock;Abnormal ending still returns the identity of the original thread,not dependent on Drop the calling thread.
        let mut active = self
            .registry
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        active.retain(|thread| *thread != self.thread);
    }
}
