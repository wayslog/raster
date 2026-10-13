//! Hash index growth (FASTER `GrowIndex`, `GrowState`, Action::GrowIndex).
//!
//! Growth stops the world cooperatively: after every session acknowledges
//! GROW_PREPARE it performs no index operation until growth ends, and in
//! GROW_IN_PROGRESS the sessions split the old table into one twice its size
//! chunk by chunk. The old table is freed once every session has refreshed.
use super::address::Address;
use super::index::{Growth, Route};
use super::kv::{Key, Value};
use super::record::Record;
use super::state::{Action, Phase, SystemState};
use super::store::FasterKv;
use std::sync::atomic::Ordering;

/// Receives the new number of buckets.
pub type GrowCallback = Box<dyn FnOnce(u64) + Send>;

#[derive(Default)]
pub(crate) struct GrowRun {
    pub growth: Option<Growth>,
    pub callback: Option<GrowCallback>,
}

struct GrowthPtr(Growth);
// SAFETY: the old table is only freed by this epoch action, after every
// session has moved past the growth.
unsafe impl Send for GrowthPtr {}
impl GrowthPtr {
    /// # Safety
    /// No session may still reference the old table.
    unsafe fn retire(self) {
        // SAFETY: guaranteed by the caller.
        unsafe { self.0.retire_old() }
    }
}

impl<K: Key, V: Value> FasterKv<K, V> {
    /// Doubles the hash index. Returns false if another state-machine action
    /// is running. Sessions must keep refreshing for growth to finish.
    pub fn grow_index(&self, callback: Option<GrowCallback>) -> bool {
        let rest = self.state.load();
        if rest.phase != Phase::Rest || rest.action != Action::None {
            return false;
        }
        let started = SystemState {
            action: Action::GrowIndex,
            ..rest
        };
        if !self.state.compare_exchange(rest, started) {
            return false;
        }
        self.epoch.reset_phase_finished();
        *self.grow.lock().unwrap() = GrowRun {
            growth: Some(self.index.begin_growth()),
            callback,
        };
        self.grow_next_chunk.store(0, Ordering::Release);
        self.state.store(started.next());
        true
    }

    /// Global work on entering GROW_IN_PROGRESS: no session uses the index
    /// until growth ends, so the new table can become current right away.
    pub(crate) fn grow_install(&self) {
        let run = self.grow.lock().unwrap();
        self.index
            .install(run.growth.as_ref().expect("growth in progress"));
    }

    /// Splits chunks until none are left.
    pub(crate) fn grow_split_all(&self) {
        let run = self.grow.lock().unwrap();
        let growth = run.growth.as_ref().expect("growth in progress");
        let (old, new) = (growth.old() as *const _, growth.grown() as *const _);
        drop(run);
        // SAFETY: both tables live until the transition to REST, which needs
        // this session to finish GROW_IN_PROGRESS first.
        let (old, new): (&super::index::Table, &super::index::Table) = unsafe { (&*old, &*new) };
        let old_len = old.buckets();
        let route = |address: Address| self.grow_route(address, old_len);
        loop {
            let chunk = self.grow_next_chunk.fetch_add(1, Ordering::AcqRel);
            if chunk >= old.chunks() {
                return;
            }
            old.split_chunk(new, chunk, &route);
        }
    }

    /// Decides which new bucket a chain head belongs to, and where the chain
    /// continues for the other bucket (FASTER `TraceBackForOtherChainStart`).
    fn grow_route(&self, address: Address, old_len: u64) -> Route {
        let head = self.hlog.head.load();
        if address < head {
            return Route::Both;
        }
        let upper_of = |record: Record<K, V>| {
            // SAFETY: published, in-memory record under epoch protection.
            unsafe { record.key() }.hash() & old_len != 0
        };
        // SAFETY: `address` is a published in-memory record.
        let record = unsafe { Record::<K, V>::from_ptr(self.hlog.get(address)) };
        let upper = upper_of(record);
        let mut previous = record.info().previous();
        while previous >= head {
            // SAFETY: as above.
            let record = unsafe { Record::<K, V>::from_ptr(self.hlog.get(previous)) };
            if upper_of(record) != upper {
                return Route::Side {
                    upper,
                    other: Some(previous),
                };
            }
            previous = record.info().previous();
        }
        let other = (previous >= self.hlog.begin.load()).then_some(previous);
        Route::Side { upper, other }
    }

    /// Global work on returning to REST: retire the old table and report.
    pub(crate) fn grow_complete(&self) {
        let (growth, callback) = {
            let mut run = self.grow.lock().unwrap();
            (run.growth.take(), run.callback.take())
        };
        let buckets = self.index.buckets();
        if let Some(growth) = growth {
            let growth = GrowthPtr(growth);
            // SAFETY: no session can reference the old table once every
            // session has moved past this epoch.
            self.epoch
                .bump_with_action(move || unsafe { growth.retire() });
        }
        if let Some(callback) = callback {
            callback(buckets);
        }
    }
}
