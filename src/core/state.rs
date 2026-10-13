//! System state machine (FASTER `state_transitions.h`, `phase.h`).
//!
//! `{action, phase, version}` lives in one atomic word. Only a successful CAS
//! moves the global state, so global side effects run exactly once; the
//! version increments only on the PREPARE -> IN_PROGRESS edge.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Action {
    None = 0,
    CheckpointFull = 1,
    CheckpointIndex = 2,
    CheckpointHybridLog = 3,
    Recover = 4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    Rest = 0,
    PrepIndexCheckpoint = 1,
    IndexCheckpoint = 2,
    Prepare = 3,
    InProgress = 4,
    WaitPending = 5,
    WaitFlush = 6,
    PersistenceCallback = 7,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SystemState {
    pub action: Action,
    pub phase: Phase,
    pub version: u32,
}

impl SystemState {
    pub const fn rest(version: u32) -> Self {
        Self {
            action: Action::None,
            phase: Phase::Rest,
            version,
        }
    }

    fn pack(self) -> u64 {
        self.action as u64 | (self.phase as u64) << 8 | (self.version as u64) << 32
    }

    fn unpack(word: u64) -> Self {
        let action = match word as u8 {
            0 => Action::None,
            1 => Action::CheckpointFull,
            2 => Action::CheckpointIndex,
            3 => Action::CheckpointHybridLog,
            _ => Action::Recover,
        };
        let phase = match (word >> 8) as u8 {
            0 => Phase::Rest,
            1 => Phase::PrepIndexCheckpoint,
            2 => Phase::IndexCheckpoint,
            3 => Phase::Prepare,
            4 => Phase::InProgress,
            5 => Phase::WaitPending,
            6 => Phase::WaitFlush,
            _ => Phase::PersistenceCallback,
        };
        Self {
            action,
            phase,
            version: (word >> 32) as u32,
        }
    }

    /// The state that follows `self` for its action.
    pub fn next(self) -> Self {
        use Phase::*;
        let (phase, bump) = match (self.action, self.phase) {
            (Action::CheckpointFull, Rest) => (PrepIndexCheckpoint, false),
            (Action::CheckpointFull, PrepIndexCheckpoint) => (IndexCheckpoint, false),
            (Action::CheckpointFull, IndexCheckpoint) => (Prepare, false),
            (Action::CheckpointIndex, Rest) => (PrepIndexCheckpoint, false),
            (Action::CheckpointIndex, PrepIndexCheckpoint) => (IndexCheckpoint, false),
            (Action::CheckpointIndex, IndexCheckpoint) => (Rest, false),
            (Action::CheckpointHybridLog, Rest) => (Prepare, false),
            (Action::CheckpointFull | Action::CheckpointHybridLog, Prepare) => (InProgress, true),
            (Action::CheckpointFull | Action::CheckpointHybridLog, InProgress) => {
                (WaitPending, false)
            }
            (Action::CheckpointFull | Action::CheckpointHybridLog, WaitPending) => {
                (WaitFlush, false)
            }
            (Action::CheckpointFull | Action::CheckpointHybridLog, WaitFlush) => {
                (PersistenceCallback, false)
            }
            (Action::CheckpointFull | Action::CheckpointHybridLog, PersistenceCallback) => {
                (Rest, false)
            }
            other => unreachable!("no transition from {other:?}"),
        };
        let action = if phase == Rest {
            Action::None
        } else {
            self.action
        };
        Self {
            action,
            phase,
            version: self.version + bump as u32,
        }
    }
}

#[derive(Debug)]
pub struct AtomicSystemState(AtomicU64);

impl AtomicSystemState {
    pub fn new(state: SystemState) -> Self {
        Self(AtomicU64::new(state.pack()))
    }
    #[inline]
    pub fn load(&self) -> SystemState {
        SystemState::unpack(self.0.load(Ordering::Acquire))
    }
    pub fn store(&self, state: SystemState) {
        self.0.store(state.pack(), Ordering::Release)
    }
    pub fn compare_exchange(&self, current: SystemState, next: SystemState) -> bool {
        self.0
            .compare_exchange(
                current.pack(),
                next.pack(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

/// Record headers keep 13 version bits. Only versions v and v+1 coexist in
/// the mutable log, so compare modulo 2^13 instead of wrapping silently.
pub const RECORD_VERSION_BITS: u32 = 13;

/// True if a record stamped with `record` is newer than `version`.
#[inline(always)]
pub fn record_newer(record: u32, version: u32) -> bool {
    let mask = (1u32 << RECORD_VERSION_BITS) - 1;
    let delta = record.wrapping_sub(version) & mask;
    delta != 0 && delta < (1 << (RECORD_VERSION_BITS - 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_checkpoint_sequence_bumps_version_once() {
        let mut state = SystemState {
            action: Action::CheckpointFull,
            ..SystemState::rest(7)
        };
        let mut phases = vec![];
        loop {
            state = state.next();
            phases.push((state.phase, state.version));
            if state.phase == Phase::Rest {
                break;
            }
        }
        use Phase::*;
        assert_eq!(
            phases,
            vec![
                (PrepIndexCheckpoint, 7),
                (IndexCheckpoint, 7),
                (Prepare, 7),
                (InProgress, 8),
                (WaitPending, 8),
                (WaitFlush, 8),
                (PersistenceCallback, 8),
                (Rest, 8)
            ]
        );
        assert_eq!(state.action, Action::None);
    }

    #[test]
    fn pack_round_trip_and_cas() {
        let state = SystemState {
            action: Action::CheckpointHybridLog,
            phase: Phase::WaitFlush,
            version: u32::MAX,
        };
        let atomic = AtomicSystemState::new(state);
        assert_eq!(atomic.load(), state);
        assert!(!atomic.compare_exchange(SystemState::rest(0), state.next()));
        assert!(atomic.compare_exchange(state, state.next()));
        assert_eq!(atomic.load().phase, Phase::PersistenceCallback);
    }

    #[test]
    fn record_versions_compare_across_wrap() {
        assert!(record_newer(8, 7));
        assert!(!record_newer(7, 7));
        assert!(!record_newer(6, 7));
        let v = (1 << RECORD_VERSION_BITS) - 1;
        assert!(
            record_newer((v + 1) & v, v),
            "8191 -> 0 wraps in the header"
        );
    }
}
