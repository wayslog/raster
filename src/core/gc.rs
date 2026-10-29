//! Log truncation and index garbage collection (FASTER `ShiftBeginAddress`,
//! `GcState`, Action::GC).
//!
//! The new begin address is published at once, so reads below it return
//! NotFound. Sessions then drain I/O issued before the shift (GC_IO_PENDING),
//! the device drops whole segments below the new begin, and sessions clear
//! index entries that point below it chunk by chunk (GC_IN_PROGRESS).
use super::address::Address;
use super::kv::{Key, Value};
use super::state::{Action, Phase};
use super::store::FasterKv;
use std::sync::atomic::Ordering;

pub type TruncateCallback = Box<dyn FnOnce(u64) + Send>;
pub type GcCompleteCallback = Box<dyn FnOnce() + Send>;

#[derive(Default)]
pub(crate) struct GcRun {
    pub begin: Address,
    pub truncate_callback: Option<TruncateCallback>,
    pub complete_callback: Option<GcCompleteCallback>,
}

impl<K: Key, V: Value> FasterKv<K, V> {
    /// Truncates the log below `address`, which must lie between the current
    /// begin address and the safe read-only address. `truncated` receives the
    /// device offset storage was released up to; `complete` runs once no
    /// index entry points below `address`. Returns false if another
    /// state-machine action is running, `address` is out of range, or its
    /// page geometry or eight-byte alignment is invalid.
    /// Sessions must keep refreshing for the truncation to finish.
    pub fn shift_begin_address(
        &self,
        address: Address,
        truncated: Option<TruncateCallback>,
        complete: Option<GcCompleteCallback>,
    ) -> bool {
        let rest = self.state.load();
        if rest.phase != Phase::Rest || rest.action != Action::None {
            return false;
        }
        if !self.hlog.valid_record_boundary(address)
            || address < self.hlog.begin.load()
            || address > self.hlog.safe_read_only.load()
        {
            return false;
        }
        let started = rest.start(Action::Gc);
        if !self.state.compare_exchange(rest, started) {
            return false;
        }
        self.hlog.begin.monotonic_update(address);
        self.truncations.fetch_add(1, Ordering::AcqRel);
        self.epoch.reset_phase_finished();
        *self.gc.lock().unwrap() = GcRun {
            begin: address,
            truncate_callback: truncated,
            complete_callback: complete,
        };
        self.gc_next_chunk.store(0, Ordering::Release);
        self.state.store(started.next());
        true
    }

    /// Global work on entering GC_IN_PROGRESS: no I/O below the new begin
    /// address is in flight any more, so release that storage.
    pub(crate) fn gc_truncate(&self) {
        let (begin, callback) = {
            let mut run = self.gc.lock().unwrap();
            (run.begin, run.truncate_callback.take())
        };
        self.hlog.truncate(
            begin,
            Box::new(move |offset| {
                if let Some(callback) = callback {
                    callback(offset);
                }
            }),
        );
    }

    /// Clears index chunks until none are left (FASTER `GarbageCollect`).
    pub(crate) fn gc_collect_all(&self) {
        let begin = self.gc.lock().unwrap().begin;
        let chunks = self.index.chunks();
        loop {
            let chunk = self.gc_next_chunk.fetch_add(1, Ordering::AcqRel);
            if chunk >= chunks {
                return;
            }
            self.index
                .garbage_collect_chunk(chunk, begin, &|entry| self.skip_read_cache(entry));
        }
    }

    pub(crate) fn gc_complete(&self) {
        let callback = self.gc.lock().unwrap().complete_callback.take();
        if let Some(callback) = callback {
            callback();
        }
    }
}
