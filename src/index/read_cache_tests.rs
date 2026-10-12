use super::*;
use crate::epoch::EpochManager;
use std::{
    sync::{Arc, mpsc},
    time::Duration,
};

fn put(table: &Table, hash: KeyHash, head: IndexHead) {
    assert!(matches!(
        table
            .compare_publish(table.prepare(hash).unwrap(), head)
            .unwrap(),
        PublishResult::Published
    ));
}

#[test]
fn cached_snapshots_track_publication_reservation_deletion_cache_heads_and_gc() {
    let table = Table::new(IndexConfig { buckets: 1 }).unwrap();
    let hash = KeyHash(7 << 48);
    let mut cache = SnapshotCache::default();
    let empty = table.prepare_cached(hash, &mut cache).unwrap();
    assert!(!empty.present);
    table
        .compare_publish_mode(empty, IndexHead::Empty, true)
        .unwrap();
    assert!(table.prepare_cached(hash, &mut cache).unwrap().present);
    for head in [
        IndexHead::Log(LogAddress(64)),
        IndexHead::Cache(CacheAddress(128)),
        IndexHead::Log(LogAddress(64)),
        IndexHead::Empty,
        IndexHead::Log(LogAddress(64)),
    ] {
        put(&table, hash, head);
        assert_eq!(table.prepare_cached(hash, &mut cache).unwrap().head, head);
    }
    let prior = table.prepare_cached(hash, &mut cache).unwrap();
    put(&table, hash, IndexHead::Empty);
    put(&table, hash, prior.head);
    assert!(matches!(
        table
            .compare_publish(prior, IndexHead::Log(LogAddress(192)))
            .unwrap(),
        PublishResult::Conflict(_)
    ));
    let latest = table.prepare_cached(hash, &mut cache).unwrap();
    assert_ne!(latest.revision, prior.revision);
    assert_eq!(table.clean_bucket(0, LogAddress(128)).unwrap(), 1);
    assert!(!table.prepare_cached(hash, &mut cache).unwrap().present);
    // Equal tags and cache slots must still retain the requested full hash.
    let colliding_hash = KeyHash(hash.0 | 512);
    put(&table, colliding_hash, IndexHead::Log(LogAddress(256)));
    assert_eq!(
        table
            .prepare_cached(colliding_hash, &mut cache)
            .unwrap()
            .hash,
        Some(colliding_hash)
    );
    assert_eq!(
        table.prepare_cached(hash, &mut cache).unwrap().hash,
        Some(hash)
    );
    assert_eq!(cache.entries.len(), CACHE_SLOTS);
}

#[test]
fn an_in_progress_bucket_publication_cannot_reuse_a_warmed_snapshot() {
    let table = Arc::new(Table::new(IndexConfig { buckets: 1 }).unwrap());
    let hash = KeyHash(0);
    put(&table, hash, IndexHead::Log(LogAddress(64)));
    let mut cache = SnapshotCache::default();
    table.prepare_cached(hash, &mut cache).unwrap();
    let mut held = table.buckets[0].lock().unwrap();
    let publication = BucketPublication::begin(&table.publications[0]);
    held.blocks[0][0].as_mut().unwrap().head = IndexHead::Log(LogAddress(128));
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let reader = {
        let table = table.clone();
        std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx
                .send(table.prepare_cached(hash, &mut cache))
                .unwrap();
        })
    };
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = done_rx.recv_timeout(Duration::from_millis(20));
    drop(publication);
    drop(held);
    reader.join().unwrap();
    assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
    assert_eq!(
        done_rx.recv().unwrap().unwrap().head,
        IndexHead::Log(LogAddress(128))
    );
}

#[test]
fn warmed_snapshots_reject_bucket_poison_and_interrupted_publication() {
    for during_publication in [false, true] {
        let table = Table::new(IndexConfig { buckets: 1 }).unwrap();
        let mut cache = SnapshotCache::default();
        table.prepare_cached(KeyHash(0), &mut cache).unwrap();
        assert!(
            std::panic::catch_unwind(|| {
                let _held = table.buckets[0].lock().unwrap();
                let _publication =
                    during_publication.then(|| BucketPublication::begin(&table.publications[0]));
                panic!("injected bucket poison");
            })
            .is_err()
        );
        assert!(table.prepare_cached(KeyHash(0), &mut cache).is_err());
        if during_publication {
            assert_eq!(table.publications[0].load(PUBLISH_ORDER) & 1, 1);
        }
    }
}

#[test]
fn saturated_bucket_stamps_disable_caching_without_changing_publication_semantics() {
    let table = Table::new(IndexConfig { buckets: 1 }).unwrap();
    let mut cache = SnapshotCache::default();
    table.publications[0].store(u64::MAX - 1, PUBLISH_ORDER);
    table.prepare_cached(KeyHash(0), &mut cache).unwrap();
    for at in [64, 128] {
        put(&table, KeyHash(0), IndexHead::Log(LogAddress(at)));
        assert_eq!(table.publications[0].load(PUBLISH_ORDER), u64::MAX);
        assert_eq!(
            table.prepare_cached(KeyHash(0), &mut cache).unwrap().head,
            IndexHead::Log(LogAddress(at))
        );
        assert_eq!(cache.entries[0].unwrap().stamp, u64::MAX - 1);
    }
}

#[test]
fn guarded_cache_rejects_foreign_epochs_and_tracks_growth_restore_and_route_poison() {
    let epoch = EpochManager::new().unwrap();
    let mut index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
    index.bind_epoch(&epoch).unwrap();
    let participant = epoch.register().unwrap();
    let mut cache = SnapshotCache::default();
    let hash = KeyHash(0);
    {
        let guard = epoch.enter(&participant).unwrap();
        index
            .prepare_cached_guarded(hash, &guard, &mut cache)
            .unwrap();
        let other = EpochManager::new().unwrap();
        let other_participant = other.register().unwrap();
        let foreign = other.enter(&other_participant).unwrap();
        assert!(
            index
                .prepare_cached_guarded(hash, &foreign, &mut cache)
                .is_err()
        );
        index.begin_growth().unwrap();
        assert_eq!(
            index
                .prepare_cached_guarded(hash, &guard, &mut cache)
                .unwrap()
                .table_generation,
            Generation(0)
        );
        index.grow_step(PollBudget::default()).unwrap();
        let entry = index.prepare(hash).unwrap();
        index
            .compare_publish(entry, IndexHead::Log(LogAddress(64)))
            .unwrap();
        let grown = index
            .prepare_cached_guarded(hash, &guard, &mut cache)
            .unwrap();
        assert_eq!(
            (grown.table_generation, grown.head),
            (Generation(1), IndexHead::Log(LogAddress(64)))
        );
    }
    epoch
        .defer(crate::epoch::DeferredAction::ReleaseIndex(Generation(0)))
        .unwrap();
    epoch.advance().unwrap();
    assert_eq!(
        epoch.collect().unwrap(),
        vec![crate::epoch::DeferredAction::ReleaseIndex(Generation(0))]
    );
    // SAFETY: The bound epoch delivered retirement after the only reader exited.
    unsafe { index.release_retired(Generation(0)) }.unwrap();
    index
        .restore(crate::format::IndexSnapshot {
            buckets: 2,
            generation: Generation(1),
            entries: vec![crate::format::IndexEntry {
                bucket: 0,
                tag: hash.tag(),
                address: LogAddress(256),
            }],
        })
        .unwrap();
    let guard = epoch.enter(&participant).unwrap();
    assert_eq!(
        index
            .prepare_cached_guarded(hash, &guard, &mut cache)
            .unwrap()
            .head,
        IndexHead::Log(LogAddress(256))
    );
    assert!(
        std::panic::catch_unwind(|| {
            let _route = index.state.write().unwrap();
            panic!("injected route poison");
        })
        .is_err()
    );
    assert!(
        index
            .prepare_cached_guarded(hash, &guard, &mut cache)
            .is_err()
    );
}

#[test]
fn streaming_reads_bypass_probes_then_resume_with_fresh_snapshots() {
    let epoch = EpochManager::new().unwrap();
    let mut index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
    index.bind_epoch(&epoch).unwrap();
    let participant = epoch.register().unwrap();
    let guard = epoch.enter(&participant).unwrap();
    let mut cache = SnapshotCache::default();
    for hash in 0..CACHE_SLOTS {
        index
            .prepare_cached_guarded(KeyHash(hash as u64), &guard, &mut cache)
            .unwrap();
    }
    assert_eq!(cache.bypass, CACHE_SLOTS * 8);
    let original = cache.entries[0].unwrap().entry;
    index
        .compare_publish(
            index.prepare(KeyHash(0)).unwrap(),
            IndexHead::Log(LogAddress(64)),
        )
        .unwrap();
    for _ in 0..CACHE_SLOTS * 8 {
        assert_eq!(
            index
                .prepare_cached_guarded(KeyHash(0), &guard, &mut cache)
                .unwrap()
                .head,
            IndexHead::Log(LogAddress(64))
        );
    }
    assert_eq!(cache.bypass, 0);
    assert_eq!(cache.entries[0].unwrap().entry, original);
    let current = index
        .prepare_cached_guarded(KeyHash(0), &guard, &mut cache)
        .unwrap();
    assert_eq!(current.head, IndexHead::Log(LogAddress(64)));
    assert_eq!(cache.entries[0].unwrap().entry, current);
    index
        .prepare_cached_guarded(KeyHash(0), &guard, &mut cache)
        .unwrap();
    assert_eq!(cache.misses, 0);
    // Bypass must retain the canonical guard and poison checks.
    cache.bypass = 2;
    let foreign_epoch = EpochManager::new().unwrap();
    let foreign_participant = foreign_epoch.register().unwrap();
    let foreign = foreign_epoch.enter(&foreign_participant).unwrap();
    assert!(
        index
            .prepare_cached_guarded(KeyHash(0), &foreign, &mut cache)
            .is_err()
    );
    assert!(
        std::panic::catch_unwind(|| {
            let active = index.state.read().unwrap().active.clone();
            let _bucket = active.buckets[0].lock().unwrap();
            panic!("injected bypass bucket poison");
        })
        .is_err()
    );
    assert!(
        index
            .prepare_cached_guarded(KeyHash(0), &guard, &mut cache)
            .is_err()
    );
}
