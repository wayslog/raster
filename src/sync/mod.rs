//! Internal concurrency primitive entry;epoch Single lock conversion exhaustive linearization interleaving via real interface.
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};
pub(crate) use std::sync::{Mutex, MutexGuard};

/// First release designed for sequential consistency;Subsequent weakening must be verified by the model.
pub(crate) const PUBLISH_ORDER: Ordering = Ordering::SeqCst;
