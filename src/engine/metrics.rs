//! Indicator does not execute user code,Does not change the request result;Resource counting independent of history sampling which can be turned off.
use crate::{
    api::completion::{OperationResult, Outcome},
    diagnostics::*,
    types::Effect,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Instant;
#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Read,
    Upsert,
    Rmw,
    Delete,
    Copy,
}
#[derive(Clone, Copy)]
pub(crate) enum Completed {
    Success,
    NotFound,
    Aborted,
    Failed(Effect),
}
impl Completed {
    pub fn result<T>(result: &OperationResult<T>) -> Self {
        match result {
            Ok(Outcome::Success(_)) => Self::Success,
            Ok(Outcome::NotFound) => Self::NotFound,
            Ok(Outcome::Aborted(_)) => Self::Aborted,
            Err(error) => Self::Failed(error.effect),
        }
    }
}
struct State {
    operations: [OperationStatistics; 5],
    cache: CacheStatistics,
    index: IndexStatistics,
    refresh: u64,
    maintenance: u64,
    saturated: bool,
    complete: bool,
}
pub(crate) struct Metrics {
    enabled: AtomicBool,
    active: AtomicUsize,
    pending: AtomicUsize,
    state: Mutex<State>,
}
fn add(target: &mut u64, value: u64, saturated: &mut bool) {
    if let Some(next) = target.checked_add(value) {
        *target = next;
    } else {
        *target = u64::MAX;
        *saturated = true;
    }
}
fn nanos(value: u128, saturated: &mut bool) -> u64 {
    u64::try_from(value).unwrap_or_else(|_| {
        *saturated = true;
        u64::MAX
    })
}
impl Metrics {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled: enabled.into(),
            active: 0.into(),
            pending: 0.into(),
            state: Mutex::new(State {
                operations: std::array::from_fn(|_| Default::default()),
                cache: Default::default(),
                index: Default::default(),
                refresh: 0,
                maintenance: 0,
                saturated: false,
                complete: true,
            }),
        }
    }
    pub fn enable(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }
    pub fn snapshot(&self) -> Statistics {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Statistics {
            enabled: self.enabled.load(Ordering::SeqCst),
            reads: s.operations[0].clone(),
            upserts: s.operations[1].clone(),
            rmw: s.operations[2].clone(),
            deletes: s.operations[3].clone(),
            conditional_copies: s.operations[4].clone(),
            cache: s.cache.clone(),
            index: s.index.clone(),
            refresh_nanoseconds: s.refresh,
            maintenance_nanoseconds: s.maintenance,
            saturated: s.saturated,
            measurements_complete: s.complete,
        }
    }
    pub fn activity(&self) -> Result<(usize, usize), crate::types::Error> {
        for _ in 0..8 {
            let pending = self.pending.load(Ordering::SeqCst);
            let active = self.active.load(Ordering::SeqCst);
            if pending <= active {
                return Ok((active, pending));
            }
        }
        Err(crate::types::Error::Busy)
    }
    pub fn accept(self: &Arc<Self>, kind: Kind) -> Monitor {
        if !matches!(kind, Kind::Copy) {
            self.active.fetch_add(1, Ordering::SeqCst);
        }
        let sampled = self.enabled.load(Ordering::SeqCst);
        if sampled {
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let State {
                operations,
                saturated,
                ..
            } = &mut *s;
            add(&mut operations[kind as usize].accepted, 1, saturated);
        }
        Monitor {
            metrics: self.clone(),
            kind,
            sampled,
            pending: None,
            finished: false,
            invalidations: 0,
            saturated: false,
        }
    }
    pub fn cache(&self, event: CacheEvent) {
        if !self.enabled.load(Ordering::SeqCst) {
            return;
        }
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let State {
            cache, saturated, ..
        } = &mut *s;
        let counter = match event {
            CacheEvent::Lookup => &mut cache.lookups,
            CacheEvent::Hit => &mut cache.hits,
            CacheEvent::Insert => &mut cache.insertions,
            CacheEvent::Evict => &mut cache.evictions,
            CacheEvent::Promote => &mut cache.promotions,
        };
        add(counter, 1, saturated);
    }
    pub fn index(&self, event: IndexEvent) {
        if !self.enabled.load(Ordering::SeqCst) {
            return;
        }
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let State {
            index, saturated, ..
        } = &mut *s;
        let counter = match event {
            IndexEvent::Lookup => &mut index.lookups,
            IndexEvent::Publish => &mut index.publication_attempts,
            IndexEvent::Conflict => &mut index.publication_conflicts,
        };
        add(counter, 1, saturated);
    }
    pub fn timer(&self, maintenance: bool) -> Timer<'_> {
        Timer {
            metrics: self,
            start: self.enabled.load(Ordering::SeqCst).then(Instant::now),
            maintenance,
        }
    }
}
pub(crate) enum CacheEvent {
    Lookup,
    Hit,
    Insert,
    Evict,
    Promote,
}
pub(crate) enum IndexEvent {
    Lookup,
    Publish,
    Conflict,
}
pub(crate) struct Timer<'a> {
    metrics: &'a Metrics,
    start: Option<Instant>,
    maintenance: bool,
}
impl Drop for Timer<'_> {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            let mut s = self.metrics.state.lock().unwrap_or_else(|e| e.into_inner());
            let State {
                refresh,
                maintenance,
                saturated,
                ..
            } = &mut *s;
            let nanos = nanos(start.elapsed().as_nanos(), saturated);
            add(
                if self.maintenance {
                    maintenance
                } else {
                    refresh
                },
                nanos,
                saturated,
            );
        }
    }
}
pub(crate) struct Monitor {
    metrics: Arc<Metrics>,
    kind: Kind,
    sampled: bool,
    pending: Option<Instant>,
    finished: bool,
    invalidations: u64,
    saturated: bool,
}
impl Monitor {
    pub fn sampled(&self) -> bool {
        self.sampled
    }
    pub fn pending(&mut self) {
        if self.pending.is_none() && !self.finished {
            // Resource counters do not depend on sampling; the suspended object itself
            // provides a bounded lifetime.
            self.pending = Some(Instant::now());
            if !matches!(self.kind, Kind::Copy) {
                self.metrics.pending.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    pub fn invalidate(&mut self) {
        add(&mut self.invalidations, 1, &mut self.saturated);
    }
    pub fn finish(&mut self, result: Completed, io: Option<u64>) {
        if self.finished {
            return;
        }
        self.finished = true;
        if !matches!(self.kind, Kind::Copy) {
            if self.pending.is_some() {
                self.metrics.pending.fetch_sub(1, Ordering::SeqCst);
            }
            self.metrics.active.fetch_sub(1, Ordering::SeqCst);
        }
        if !self.sampled {
            return;
        }
        let mut s = self.metrics.state.lock().unwrap_or_else(|e| e.into_inner());
        let State {
            operations,
            saturated,
            complete,
            ..
        } = &mut *s;
        *complete &= io.is_some();
        *saturated |= self.saturated;
        let stat = &mut operations[self.kind as usize];
        add(&mut stat.completed, 1, saturated);
        if let Some(start) = self.pending {
            let duration = nanos(start.elapsed().as_nanos(), saturated);
            add(&mut stat.pending_nanoseconds, duration, saturated);
        } else {
            add(&mut stat.synchronous, 1, saturated);
        }
        let counter = match result {
            Completed::Success => &mut stat.success,
            Completed::NotFound => &mut stat.not_found,
            Completed::Aborted => &mut stat.aborted,
            Completed::Failed(effect) => {
                match effect {
                    Effect::Applied => add(&mut stat.failed_after_applied, 1, saturated),
                    Effect::Unknown => add(&mut stat.failed_with_unknown_effect, 1, saturated),
                    Effect::NotApplied => (),
                };
                &mut stat.failed
            }
        };
        add(counter, 1, saturated);
        add(
            &mut stat.record_invalidations,
            self.invalidations,
            saturated,
        );
        if let Some(io) = io {
            add(&mut stat.io_completions, io, saturated);
            let bucket = if io == 0 {
                0
            } else {
                (64 - io.leading_zeros()) as usize
            };
            add(&mut stat.io_per_request[bucket], 1, saturated);
        }
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.finish(Completed::Failed(Effect::Unknown), None);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sampling_is_fixed_and_terminated_on_acceptance_idempotent_missing_measurements_cannot_be_faked_as_zero_completions()
     {
        let metrics = Arc::new(Metrics::new(false));
        let mut unsampled = metrics.accept(Kind::Upsert);
        unsampled.pending();
        metrics.enable(true);
        let mut sampled = metrics.accept(Kind::Read);
        sampled.pending();
        assert_eq!(metrics.activity().unwrap(), (2, 2));
        metrics.enable(false);
        sampled.finish(Completed::Success, Some(3));
        sampled.finish(Completed::Failed(Effect::Applied), Some(99));
        unsampled.finish(Completed::Success, Some(0));
        drop((sampled, unsampled));
        assert_eq!(metrics.activity().unwrap(), (0, 0));
        let s = metrics.snapshot();
        assert_eq!((s.reads.completed, s.reads.io_per_request[2]), (1, 1));
        assert_eq!(s.upserts.completed, 0);
        metrics.enable(true);
        drop(metrics.accept(Kind::Delete));
        let s = metrics.snapshot();
        assert_eq!(s.deletes.failed_with_unknown_effect, 1);
        assert!(!s.measurements_complete);
        assert_eq!(s.deletes.io_per_request.iter().sum::<u64>(), 0);
        assert_eq!(metrics.activity().unwrap(), (0, 0));
    }
    #[test]
    fn overflow_saturates_and_retains_failure_impact_and_conditional_replication_independent_counts()
     {
        let metrics = Arc::new(Metrics::new(true));
        let mut copy = metrics.accept(Kind::Copy);
        copy.pending();
        assert_eq!(metrics.activity().unwrap(), (0, 0));
        copy.invalidations = u64::MAX;
        copy.invalidate();
        copy.finish(Completed::Failed(Effect::Applied), Some(u64::MAX));
        let mut another = metrics.accept(Kind::Copy);
        another.finish(Completed::NotFound, Some(1));
        let s = metrics.snapshot();
        assert!(s.saturated);
        assert_eq!(s.conditional_copies.io_completions, u64::MAX);
        assert_eq!(s.conditional_copies.failed_after_applied, 1);
        assert_eq!(s.conditional_copies.not_found, 1);
        assert_eq!(s.conditional_copies.io_per_request[64], 1);
        let mut saturated = false;
        assert_eq!(nanos(u64::MAX as u128 + 1, &mut saturated), u64::MAX);
        assert!(saturated);
    }
}
