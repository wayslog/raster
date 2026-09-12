use super::*;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
};
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Request;
impl Keyed<Schema> for Request {
    fn key(&self) -> &u64 {
        &1
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((7, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
impl ReadOperation<Schema> for Request {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(5))
}

#[test]
fn resident_session_read_does_not_wait_for_allocator_metadata() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session
            .upsert(Serial(0), Request)
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(Outcome::Success(())))
    ));
    session.close(deadline()).unwrap();

    let result = std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (start_tx, start_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let owner = &store;
        let reader = scope.spawn(move || {
            let mut session = owner.start_session(Default::default()).unwrap();
            ready_tx.send(()).unwrap();
            start_rx.recv().unwrap();
            let value = match session.read(Serial(1), Request, ReadOptions::default()) {
                Ok(Submission::Ready(Ok(Outcome::Success(value)))) => Ok(value),
                _ => Err("resident read did not complete synchronously"),
            };
            result_tx.send(value).unwrap();
            session.close(deadline()).unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let allocator = store.inner.log.pool.state.lock().unwrap();
        start_tx.send(()).unwrap();
        let result = result_rx.recv_timeout(Duration::from_secs(2));
        // Always release the deliberately held metadata lock before joining,
        // including when the old implementation times out here.
        drop(allocator);
        reader.join().unwrap();
        result
    });
    store.shutdown(deadline()).unwrap();
    assert_eq!(
        result,
        Ok(Ok(7)),
        "published reads must not wait for allocator bookkeeping"
    );
}

#[test]
fn allocation_tail_tracks_padding_alignment_reuse_and_rejection() {
    let pool = PagePool::new_at(64, 2, PageId(10)).unwrap();
    assert_eq!(pool.tail().unwrap(), LogAddress(640));
    let first = pool.reserve(7, 1).unwrap();
    assert_eq!(pool.tail().unwrap(), LogAddress(647));
    let second = pool.reserve(8, 16).unwrap();
    assert_eq!(second.address().unwrap(), LogAddress(656));
    assert_eq!(pool.tail().unwrap(), LogAddress(664));
    assert!(pool.reserve(65, 1).is_err());
    assert_eq!(pool.tail().unwrap(), LogAddress(664));
    assert_eq!(pool.pad_tail().unwrap(), LogAddress(704));
    assert_eq!(pool.tail().unwrap(), LogAddress(704));
    let third = pool.reserve(64, 8).unwrap();
    assert_eq!(pool.tail().unwrap(), LogAddress(768));
    assert!(pool.reserve(1, 1).is_err());
    assert_eq!(pool.tail().unwrap(), LogAddress(768));
    drop(first);
    drop(second);
    pool.release(PageId(10), Generation(0)).unwrap();
    assert_eq!(pool.tail().unwrap(), LogAddress(768));
    let next = pool.reserve(8, 8).unwrap();
    assert_eq!(next.address().unwrap(), LogAddress(768));
    assert_eq!(next.generation(), Generation(1));
    assert_eq!(pool.tail().unwrap(), LogAddress(776));
    drop(third);
}

#[test]
fn concurrent_tail_observation_never_precedes_a_returned_reservation() {
    let pool = PagePool::new(64, 256).unwrap();
    let start = std::sync::Barrier::new(5);
    let mut addresses = std::thread::scope(|scope| {
        let workers = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    start.wait();
                    let mut addresses = Vec::new();
                    let mut observed = LogAddress(0);
                    for _ in 0..256 {
                        let range = pool.reserve(16, 8).unwrap();
                        let address = range.address().unwrap();
                        let tail = pool.tail().unwrap();
                        assert!(tail >= address.checked_add(16).unwrap());
                        assert!(tail >= observed);
                        observed = tail;
                        addresses.push(address.0);
                    }
                    addresses
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    addresses.sort_unstable();
    assert_eq!(addresses, (0..1024).map(|n| n * 16).collect::<Vec<_>>());
    assert_eq!(pool.tail().unwrap(), LogAddress(16384));
}

#[test]
fn cached_tail_preserves_exhaustion_and_allocator_poisoning() {
    let pool = PagePool::new_at(64, 1, PageId(u64::MAX / 64)).unwrap();
    let before = pool.tail().unwrap();
    assert!(pool.reserve(8, 8).is_err());
    assert_eq!(pool.tail().unwrap(), before);
    let pool = PagePool::new(64, 1).unwrap();
    let _range = pool.reserve(8, 8).unwrap();
    let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = pool.state.lock().unwrap();
        panic!("allocator poisoning test");
    }));
    assert!(poison.is_err());
    assert!(matches!(
        pool.tail(),
        Err(Error::InvalidState("Page pool lock poisoning"))
    ));
}
