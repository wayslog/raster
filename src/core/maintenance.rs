//! Shared page claims for compaction and write-side budget assistance.
use super::address::Address;
use super::store::Status;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) struct Maintenance {
    pub budget: u64,
    running: AtomicBool,
    pub work: Mutex<Option<Arc<CompactionWork>>>,
}
impl Maintenance {
    pub fn new(budget: u64) -> Self {
        Self {
            budget,
            running: AtomicBool::new(false),
            work: Mutex::new(None),
        }
    }
    pub fn start(&self) -> Option<CompactionGuard<'_>> {
        self.running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| CompactionGuard(&self.running))
    }
}
/// Keeps the coordinator exclusive through assistant draining, GC, and checkpointing.
pub(crate) struct CompactionGuard<'a>(&'a AtomicBool);
impl Drop for CompactionGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
pub(crate) struct CompactionWork {
    pub begin: Address,
    pub until: Address,
    next_page: AtomicU64,
    pub assistants: AtomicUsize,
    pub failure: Mutex<Option<Status>>,
}

impl CompactionWork {
    pub fn new(begin: Address, until: Address) -> Self {
        Self {
            begin,
            until,
            next_page: AtomicU64::new(begin.page()),
            assistants: AtomicUsize::new(0),
            failure: Mutex::new(None),
        }
    }
    pub fn claim(&self) -> Option<Address> {
        let page = self.next_page.fetch_add(1, Ordering::Relaxed);
        let start = if page == self.begin.page() {
            self.begin
        } else {
            Address::new(page, 0)
        };
        (start < self.until).then_some(start)
    }
    pub fn fail(&self, status: Status) {
        self.failure.lock().unwrap().get_or_insert(status);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinator_stays_exclusive_after_assistance_closes() {
        let maintenance = Maintenance::new(0);
        let guard = maintenance.start().unwrap();
        *maintenance.work.lock().unwrap() = Some(Arc::new(CompactionWork::new(
            Address::new(0, 64),
            Address::new(1, 0),
        )));
        maintenance.work.lock().unwrap().take();
        assert!(maintenance.start().is_none());
        drop(guard);
        assert!(maintenance.start().is_some());
    }
}
