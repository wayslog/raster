//! Background log compaction (FASTER `AutoCompactHlog`, `HlogCompactionConfig`).
//!
//! A thread checks the log size every `check_interval`; once it reaches
//! `trigger_fraction` of the budget, it compacts the oldest `compact_fraction`
//! of the log (at most `max_compacted_bytes`, and never past the in-memory
//! head) and truncates it. Unlike FASTER the budget must be given and the
//! compaction point is rounded down to a page boundary.
use super::address::Address;
use super::kv::{Key, Value};
use super::store::FasterKv;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct AutoCompactionConfig {
    /// Log size, in bytes, the store aims to stay under.
    pub log_size_budget: u64,
    pub check_interval: Duration,
    pub trigger_fraction: f64,
    pub compact_fraction: f64,
    pub max_compacted_bytes: u64,
    pub threads: usize,
}

impl AutoCompactionConfig {
    /// FASTER's defaults with the given budget.
    pub fn with_budget(log_size_budget: u64) -> Self {
        Self {
            log_size_budget,
            check_interval: Duration::from_millis(250),
            trigger_fraction: 0.8,
            compact_fraction: 0.2,
            max_compacted_bytes: 512 << 20,
            threads: 4,
        }
    }

    pub(crate) fn cutoff<K: Key, V: Value>(&self, store: &FasterKv<K, V>) -> Option<Address> {
        let size = store.log_size();
        if (size as f64) < self.log_size_budget as f64 * self.trigger_fraction {
            return None;
        }
        let amount = ((size as f64 * self.compact_fraction) as u64).min(self.max_compacted_bytes);
        let begin = store.hlog.begin.load();
        let target = store.hlog.device_offset(begin) + amount;
        let page_bits = store.hlog.page_size().trailing_zeros();
        let until = Address::new(target >> page_bits, 0).min(store.hlog.safe_head.load());
        (until > begin).then_some(until)
    }
}

/// Stops the background compaction thread when dropped.
pub struct AutoCompaction {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl AutoCompaction {
    pub(crate) fn spawn(interval: Duration, mut poll: impl FnMut() + Send + 'static) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = std::thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                std::thread::park_timeout(interval);
                if stopped.load(Ordering::Acquire) {
                    break;
                }
                poll();
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for AutoCompaction {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

impl<K: Key, V: Value> FasterKv<K, V> {
    /// Log size in bytes between the begin address and the tail.
    pub fn log_size(&self) -> u64 {
        self.hlog.device_offset(self.hlog.tail()) - self.hlog.device_offset(self.hlog.begin.load())
    }

    /// Starts background compaction; it stops when the handle is dropped.
    pub fn spawn_auto_compaction(self: &Arc<Self>, config: AutoCompactionConfig) -> AutoCompaction {
        assert!(
            config.log_size_budget > 0,
            "auto compaction needs a log size budget"
        );
        let store = self.clone();
        AutoCompaction::spawn(config.check_interval, move || {
            store.auto_compact_once(&config);
        })
    }

    /// Compacts once if the log exceeds the trigger size; returns whether it did.
    pub fn auto_compact_once(&self, config: &AutoCompactionConfig) -> bool {
        let Some(until) = config.cutoff(self) else {
            return false;
        };
        let Some(mut session) = self.try_start_session() else {
            return false;
        };
        session.compact(until, true, config.threads, false).is_ok()
    }
}
