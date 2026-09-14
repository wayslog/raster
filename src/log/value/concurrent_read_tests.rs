use super::*;
use crate::schema::builtin::AtomicU64Value;
use std::sync::atomic::AtomicU64;

/// An existing expert layout that supports updates but has not opted into reads.
struct UpdateOnly;
// SAFETY: All permits and views are delegated unchanged to AtomicU64Value.
// Omitting concurrent_reads deliberately retains the trait's default exclusion.
unsafe impl ValueLayout for UpdateOnly {
    type Owned = u64;
    type Read<'a> = u64;
    type Update<'a> = &'a AtomicU64;
    fn concurrent_updates(&self) -> bool {
        true
    }
    fn format_id(&self) -> FormatId {
        AtomicU64Value.format_id()
    }
    fn plan(&self, value: &u64) -> Result<ValuePlan, Error> {
        AtomicU64Value.plan(value)
    }
    fn plan_decode(&self, bytes: &[u8]) -> Result<ValuePlan, Error> {
        AtomicU64Value.plan_decode(bytes)
    }
    fn decode_owned(&self, bytes: &[u8]) -> Result<u64, Error> {
        AtomicU64Value.decode_owned(bytes)
    }
    fn initialize(&self, permit: InitPermit<'_>, value: u64) -> Result<(), Error> {
        AtomicU64Value.initialize(permit, value)
    }
    fn read<'a>(&'a self, permit: ReadPermit<'a>) -> Result<u64, Error> {
        AtomicU64Value.read(permit)
    }
    fn update<'a>(&'a self, permit: UpdatePermit<'a>) -> Result<&'a AtomicU64, Error> {
        AtomicU64Value.update(permit)
    }
    fn stable_encoded_len(&self, permit: StablePermit<'_>) -> Result<usize, Error> {
        AtomicU64Value.stable_encoded_len(permit)
    }
    fn encode_stable(&self, permit: StablePermit<'_>, bytes: &mut [u8]) -> Result<(), Error> {
        AtomicU64Value.encode_stable(permit, bytes)
    }
    fn decode_initialize(&self, bytes: &[u8], permit: InitPermit<'_>) -> Result<(), Error> {
        AtomicU64Value.decode_initialize(bytes, permit)
    }
    fn drop_value(&self, permit: DropPermit<'_>) -> Result<(), Error> {
        AtomicU64Value.drop_value(permit)
    }
}

#[test]
fn atomic_reads_share_access_with_reads_and_updates_but_exclude_delete_and_snapshot() {
    let pool = PagePool::new(4096, 2).unwrap();
    let value =
        PageValue::initialize_record(&pool, Arc::new(AtomicU64Value), b"key", None, 42).unwrap();
    assert!(matches!(
        value.try_read_live(|first| {
            assert_eq!(first, 42);
            assert!(matches!(
                value.try_read_live(|next| next),
                Ok(ValueAccess::Ready(Some(42)))
            ));
            assert!(matches!(
                value.update_at_version(CheckpointVersion(0), |v| {
                    v.store(43, Ordering::SeqCst);
                    Ok(())
                }),
                Ok(ValueAccess::Ready(Some(())))
            ));
            assert!(matches!(
                value.try_read_live(|next| next),
                Ok(ValueAccess::Ready(Some(43)))
            ));
            assert!(matches!(value.snapshot_record(), Err(Error::Busy)));
            assert!(matches!(
                value.tombstone_at_version(CheckpointVersion(0), || {
                    panic!("delete must not publish while a read permit is held")
                }),
                Ok(ValueAccess::Contended)
            ));
        }),
        Ok(ValueAccess::Ready(Some(())))
    ));
    assert!(value.snapshot_record().is_ok());
    assert!(matches!(
        value.tombstone_at_version(CheckpointVersion(0), || Ok(true)),
        Ok(ValueAccess::Ready(Some(true)))
    ));
    assert!(matches!(
        value.try_read_live(|_| panic!("tombstone must not construct a value view")),
        Ok(ValueAccess::Ready(None))
    ));
}

#[test]
fn concurrent_update_support_does_not_implicitly_enable_concurrent_reads() {
    let pool = PagePool::new(4096, 2).unwrap();
    let value = PageValue::initialize(&pool, Arc::new(UpdateOnly), 42).unwrap();
    assert!(value.layout.concurrent_updates());
    assert!(!value.layout.concurrent_reads());
    value
        .try_read_live(|_| {
            assert!(matches!(
                value.try_read_live(|_| ()),
                Ok(ValueAccess::Contended)
            ));
            assert!(matches!(
                value.update_at_version(CheckpointVersion(0), |_| Ok(())),
                Ok(ValueAccess::Contended)
            ));
        })
        .unwrap();
    assert!(matches!(
        value.try_read_live(|n| n),
        Ok(ValueAccess::Ready(Some(42)))
    ));
}

#[test]
fn copy_update_source_read_still_excludes_atomic_updates() {
    let pool = PagePool::new(4096, 2).unwrap();
    let value = PageValue::initialize(&pool, Arc::new(AtomicU64Value), 42).unwrap();
    value
        .try_read(|_| {
            assert!(matches!(
                value.try_read_live(|_| ()),
                Ok(ValueAccess::Contended)
            ));
            assert!(matches!(
                value.update_at_version(CheckpointVersion(0), |_| Ok(())),
                Ok(ValueAccess::Contended)
            ));
        })
        .unwrap();
}
