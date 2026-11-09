use super::*;
use crate::core::device::NullDevice;
use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Barrier, Mutex};

const LOG: LogConfig = LogConfig {
    page_bits: 12,
    memory_pages: 4,
    mutable_fraction: 0.5,
};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("raster-cold-index-{}", Guid::new()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn entry(offset: u64) -> ColdIndexEntry {
    ColdIndexEntry::Published(Address::new(0, offset))
}

fn immediate(_: ColdIndexResult) {
    panic!("unexpected deferred completion");
}

fn fill(session: &mut ColdIndexSession<'_>) {
    for key in 1..=128 {
        assert_eq!(
            session.update(
                key,
                ColdIndexUpdate::Set(Address::new(0, 64)),
                key + 1,
                immediate
            ),
            Ok(ColdIndexOutcome::Entry(entry(64)))
        );
    }
}

#[test]
fn slots_and_outer_tags_remain_distinct() {
    let dir = TempDir::new();
    let index = ColdIndex::open(&dir.0, 256, LOG).unwrap();
    let mut session = index.try_start_session().unwrap();
    let other_slot = 1 << 51;
    let other_tag = 1 << 48;
    assert_eq!(
        session.lookup(0, 1, immediate),
        Ok(ColdIndexOutcome::Entry(ColdIndexEntry::Empty))
    );
    assert_eq!(
        session.update(0, ColdIndexUpdate::Reserve, 2, immediate),
        Ok(ColdIndexOutcome::Entry(ColdIndexEntry::Reserved))
    );
    for (serial, hash) in [(3, other_slot), (4, other_tag)] {
        assert_eq!(
            session.lookup(hash, serial, immediate),
            Ok(ColdIndexOutcome::Entry(ColdIndexEntry::Empty))
        );
    }
    let address = Address::new(0, 64);
    assert_eq!(
        session.update(
            0,
            ColdIndexUpdate::CompareExchange {
                expected: ColdIndexEntry::Reserved,
                address
            },
            5,
            immediate
        ),
        Ok(ColdIndexOutcome::Entry(entry(64)))
    );
    assert_eq!(
        session.update(0, ColdIndexUpdate::Reserve, 6, immediate),
        Ok(ColdIndexOutcome::Entry(entry(64)))
    );
    assert_eq!(
        session.update(
            0,
            ColdIndexUpdate::CompareExchange {
                expected: ColdIndexEntry::Reserved,
                address
            },
            7,
            immediate
        ),
        Ok(ColdIndexOutcome::Conflict(entry(64)))
    );
}

#[test]
fn competing_publishers_preserve_the_observed_winner() {
    let dir = TempDir::new();
    let index = ColdIndex::open(&dir.0, 256, LOG).unwrap();
    {
        let mut session = index.try_start_session().unwrap();
        assert_eq!(
            session.update(0, ColdIndexUpdate::Reserve, 1, immediate),
            Ok(ColdIndexOutcome::Entry(ColdIndexEntry::Reserved))
        );
    }
    let ready = Barrier::new(2);
    let outcomes = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for offset in [64, 72] {
            let (index, ready, outcomes) = (&index, &ready, &outcomes);
            scope.spawn(move || {
                let mut session = index.try_start_session().unwrap();
                ready.wait();
                let result = session
                    .update(
                        0,
                        ColdIndexUpdate::CompareExchange {
                            expected: ColdIndexEntry::Reserved,
                            address: Address::new(0, offset),
                        },
                        1,
                        immediate,
                    )
                    .unwrap();
                outcomes.lock().unwrap().push(result);
            });
        }
    });
    let outcomes = outcomes.into_inner().unwrap();
    let mut session = index.try_start_session().unwrap();
    let Ok(ColdIndexOutcome::Entry(winner)) = session.lookup(0, 1, immediate) else {
        panic!("published address missing");
    };
    assert!(matches!(winner, ColdIndexEntry::Published(_)));
    assert!(outcomes.contains(&ColdIndexOutcome::Entry(winner)));
    assert!(outcomes.contains(&ColdIndexOutcome::Conflict(winner)));
}

fn deferred_result(
    session: &mut ColdIndexSession<'_>,
    submit: impl FnOnce(&mut ColdIndexSession<'_>, Rc<Cell<Option<ColdIndexResult>>>) -> ColdIndexResult,
) -> ColdIndexResult {
    let observed = Rc::new(Cell::new(None));
    assert_eq!(submit(session, observed.clone()), Err(Status::Pending));
    assert!(observed.get().is_none());
    assert!(session.complete_pending(true));
    observed.get().expect("operation completed")
}

fn record_completion(result: &Cell<Option<ColdIndexResult>>, outcome: ColdIndexResult) {
    assert!(
        result.replace(Some(outcome)).is_none(),
        "operation completed twice"
    );
}

#[test]
fn evicted_chunks_complete_updates_and_recover() {
    let dir = TempDir::new();
    let (token, guid) = {
        let index = ColdIndex::open(&dir.0, 256, LOG).unwrap();
        let mut session = index.try_start_session().unwrap();
        let other_slot = 1 << 51;
        for (serial, hash, offset) in [(0, 0, 64), (1, other_slot, 72)] {
            assert_eq!(
                session.update(
                    hash,
                    ColdIndexUpdate::Set(Address::new(0, offset)),
                    serial,
                    immediate
                ),
                Ok(ColdIndexOutcome::Entry(entry(offset)))
            );
        }
        fill(&mut session);
        assert!(index.store.hlog.head.load() > Address::new(0, FIRST_VALID_OFFSET));
        assert_eq!(
            deferred_result(&mut session, |session, observed| {
                session.lookup(0, 130, move |result| record_completion(&observed, result))
            }),
            Ok(ColdIndexOutcome::Entry(entry(64)))
        );
        assert_eq!(
            deferred_result(&mut session, |session, observed| {
                session.update(
                    0,
                    ColdIndexUpdate::CompareExchange {
                        expected: entry(64),
                        address: Address::new(0, 80),
                    },
                    131,
                    move |result| record_completion(&observed, result),
                )
            }),
            Ok(ColdIndexOutcome::Entry(entry(80)))
        );
        assert_eq!(
            session.lookup(other_slot, 132, immediate),
            Ok(ColdIndexOutcome::Entry(entry(72)))
        );
        let persisted = Arc::new(Mutex::new(None));
        let observed = persisted.clone();
        let token = index
            .checkpoint(
                None,
                Some(Arc::new(move |status, serial| {
                    *observed.lock().unwrap() = Some((status, serial));
                })),
            )
            .unwrap();
        assert!(session.complete_pending(true));
        assert_eq!(*persisted.lock().unwrap(), Some((Status::Ok, 132)));
        (token, session.guid())
    };

    // Recovery restores the persisted table size and its chunk routing.
    let mut index = ColdIndex::open(&dir.0, 512, LOG).unwrap();
    assert_eq!(index.recover(token).unwrap().sessions, [guid]);
    let (mut session, serial) = index.continue_session(guid).unwrap();
    assert_eq!(serial, 132);
    assert_eq!(
        session.lookup(0, 133, immediate),
        Ok(ColdIndexOutcome::Entry(entry(80)))
    );
    assert_eq!(
        deferred_result(&mut session, |session, observed| {
            session.lookup(1, 134, move |result| record_completion(&observed, result))
        }),
        Ok(ColdIndexOutcome::Entry(entry(64)))
    );
    assert_eq!(
        session.update(0, ColdIndexUpdate::Set(Address::new(0, 88)), 135, immediate),
        Ok(ColdIndexOutcome::Entry(entry(88)))
    );
}

#[test]
fn evicted_read_errors_complete_once() {
    let index = ColdIndex {
        store: FasterKv::with_device(
            StoreConfig {
                index_buckets: 256,
                log: LOG,
                ..StoreConfig::default()
            },
            Arc::new(NullDevice),
        ),
    };
    let mut session = index.try_start_session().unwrap();
    assert_eq!(
        session.update(0, ColdIndexUpdate::Set(Address::new(0, 64)), 1, immediate),
        Ok(ColdIndexOutcome::Entry(entry(64)))
    );
    fill(&mut session);
    assert_eq!(
        deferred_result(&mut session, |session, observed| {
            session.lookup(0, 130, move |result| record_completion(&observed, result))
        }),
        Err(Status::IoError)
    );
}

#[test]
fn read_only_resident_chunk_is_copied_on_update() {
    let dir = TempDir::new();
    let index = ColdIndex::open(&dir.0, 256, LOG).unwrap();
    let mut session = index.try_start_session().unwrap();
    let other_slot = 1 << 51;
    for (serial, hash, offset) in [(0, 0, 64), (1, other_slot, 72)] {
        assert_eq!(
            session.update(
                hash,
                ColdIndexUpdate::Set(Address::new(0, offset)),
                serial,
                immediate
            ),
            Ok(ColdIndexOutcome::Entry(entry(offset)))
        );
    }
    let tail = index.store.hlog.tail();
    index.store.hlog.shift_read_only_to(tail);
    session.refresh();
    assert_eq!(index.store.hlog.safe_read_only.load(), tail);
    assert!(index.store.hlog.head.load() < tail);
    assert_eq!(
        session.update(
            0,
            ColdIndexUpdate::CompareExchange {
                expected: entry(64),
                address: Address::new(0, 80),
            },
            2,
            immediate,
        ),
        Ok(ColdIndexOutcome::Entry(entry(80)))
    );
    assert!(index.store.hlog.tail() > tail);
    assert_eq!(
        session.lookup(other_slot, 3, immediate),
        Ok(ColdIndexOutcome::Entry(entry(72)))
    );
}
