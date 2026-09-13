//! In-page value ownership:The object is returned only if the initialization is successful,All views are used within the quorum permission scope.
use super::{
    ValueAccess,
    gate::MutationGate,
    page::{PagePool, PageRange},
};
use crate::{schema::value::*, types::*};
use std::{
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub(super) fn record_bytes(key_len: usize, plan: ValuePlan) -> Result<(usize, usize), Error> {
    let plan = plan.validate()?;
    let prefix = 48usize
        .checked_add(key_len)
        .ok_or(Error::CapacityExceeded)?;
    let offset = prefix
        .checked_add(plan.alignment - 1)
        .ok_or(Error::CapacityExceeded)?
        & !(plan.alignment - 1);
    let total = offset
        .checked_add(plan.capacity.max(1))
        .and_then(|n| n.checked_add(4))
        .ok_or(Error::CapacityExceeded)?;
    u32::try_from(total).map_err(|_| Error::CapacityExceeded)?;
    Ok((offset, total))
}

/// Independent read-only active object of disk value,Separation of life cycle and log page pool.
pub(crate) struct TemporaryValue<V: ValueLayout> {
    value: PageValue<V>,
    local: PhantomData<std::rc::Rc<()>>,
}
impl<V: ValueLayout> TemporaryValue<V> {
    pub fn decode(layout: Arc<V>, encoded: &[u8], limit: usize) -> Result<Self, Error> {
        if encoded.len() > limit {
            return Err(Error::CapacityExceeded);
        }
        let plan = layout.plan_decode(encoded)?.validate()?;
        let bytes = plan
            .capacity
            .max(plan.alignment)
            .max(1)
            .checked_next_power_of_two()
            .ok_or(Error::CapacityExceeded)?;
        if bytes > limit {
            return Err(Error::CapacityExceeded);
        }
        let pool = PagePool::new(bytes, 1)?;
        let value = PageValue::decode(&pool, layout, encoded, plan)?;
        value.seal()?;
        Ok(Self {
            value,
            local: PhantomData,
        })
    }
    pub fn read<R>(&self, read: impl for<'a> FnOnce(V::Read<'a>) -> R) -> Result<R, Error> {
        self.value.read(read)
    }
}

pub(crate) struct PageValue<V: ValueLayout> {
    layout: Arc<V>,
    range: Option<PageRange>,
    initialized: bool,
    tombstone: AtomicBool,
    value_offset: usize,
    capacity: usize,
    key_len: usize,
    previous: Option<LogAddress>,
    version: CheckpointVersion,
    gate: MutationGate,
    failed: AtomicBool,
    sealed: AtomicBool,
}
macro_rules! permit {
    ($owner:expr,$name:ident) => {{
        let range = $owner.range.as_ref().expect("value_range_exists");
        $name {
            pointer: $owner.value_pointer(),
            length: $owner.capacity,
            generation: range.generation(),
            guard: PhantomData,
            local: PhantomData,
        }
    }};
}
impl<V: ValueLayout> PageValue<V> {
    #[cfg(test)]
    pub fn initialize(pool: &PagePool, layout: Arc<V>, value: V::Owned) -> Result<Self, Error> {
        let plan = layout.plan(&value)?.validate()?;
        let range = pool.reserve(plan.capacity.max(1), plan.alignment)?;
        let mut owner = Self {
            layout,
            range: Some(range),
            initialized: false,
            tombstone: AtomicBool::new(false),
            value_offset: 0,
            capacity: plan.capacity.max(1),
            key_len: 0,
            previous: None,
            version: CheckpointVersion(0),
            gate: MutationGate::default(),
            failed: AtomicBool::new(false),
            sealed: AtomicBool::new(false),
        };
        owner.layout.initialize(permit!(owner, InitPermit), value)?;
        owner.initialized = true;
        Ok(owner)
    }
    #[cfg(test)]
    pub fn initialize_record(
        pool: &PagePool,
        layout: Arc<V>,
        key: &[u8],
        previous: Option<LogAddress>,
        value: V::Owned,
    ) -> Result<Self, Error> {
        let plan = layout.plan(&value)?.validate()?;
        Self::allocate_record(pool, layout, key, previous, plan)?.initialize_owned(value)
    }
    pub fn allocate_record(
        pool: &PagePool,
        layout: Arc<V>,
        key: &[u8],
        previous: Option<LogAddress>,
        plan: ValuePlan,
    ) -> Result<Self, Error> {
        if let Some(address) = previous {
            address.validate()?;
        }
        let plan = plan.validate()?;
        let (value_offset, total) = record_bytes(key.len(), plan)?;
        let mut range = pool.reserve(total, plan.alignment)?;
        if previous.is_some_and(|address| range.address().is_ok_and(|current| address >= current)) {
            return Err(Error::InvalidFormat(
                "The predecessor must be earlier than the current record",
            ));
        }
        range.bytes_mut()[48..48 + key.len()].copy_from_slice(key);
        Ok(Self {
            layout,
            range: Some(range),
            initialized: false,
            tombstone: AtomicBool::new(false),
            value_offset,
            capacity: plan.capacity.max(1),
            key_len: key.len(),
            previous,
            version: CheckpointVersion(0),
            gate: MutationGate::default(),
            failed: AtomicBool::new(false),
            sealed: AtomicBool::new(false),
        })
    }
    pub fn initialize_owned(mut self, value: V::Owned) -> Result<Self, Error> {
        if self.initialized || self.is_tombstone() {
            return Err(Error::InvalidState(
                "Value cannot be initialized repeatedly",
            ));
        }
        self.layout.initialize(permit!(self, InitPermit), value)?;
        self.initialized = true;
        Ok(self)
    }
    fn value_pointer(&self) -> std::ptr::NonNull<u8> {
        let pointer = self.range.as_ref().expect("value_range_exists").pointer();
        // SAFETY: Check value offset and capacity within allocation during construction,Ranges do not move or overlap after initialization.
        unsafe { std::ptr::NonNull::new_unchecked(pointer.as_ptr().add(self.value_offset)) }
    }
    pub fn key(&self) -> &[u8] {
        let offset = if self.value_offset == 0 { 0 } else { 48 };
        // SAFETY: Keys are copied to independent prefixes before initialization,Immutable after publishing,and does not overlap with all value permission ranges.
        unsafe {
            std::slice::from_raw_parts(
                self.range
                    .as_ref()
                    .expect("value_range_exists")
                    .pointer()
                    .as_ptr()
                    .add(offset),
                self.key_len,
            )
        }
    }
    #[cfg(test)]
    pub fn version(&self) -> CheckpointVersion {
        self.version
    }
    /// Only set if exclusive ownership is held before the record is published,Version is immutable after release.
    pub fn with_version(mut self, version: CheckpointVersion) -> Self {
        self.version = version;
        self
    }
    pub fn previous(&self) -> Option<LogAddress> {
        self.previous
    }
    pub fn tombstone(
        pool: &PagePool,
        layout: Arc<V>,
        key: &[u8],
        previous: Option<LogAddress>,
    ) -> Result<Self, Error> {
        if let Some(address) = previous {
            address.validate()?;
        }
        let prefix = 48usize
            .checked_add(key.len())
            .ok_or(Error::CapacityExceeded)?;
        let total = prefix.checked_add(4).ok_or(Error::CapacityExceeded)?;
        let mut range = pool.reserve(total, 8)?;
        if previous.is_some_and(|address| range.address().is_ok_and(|current| address >= current)) {
            return Err(Error::InvalidFormat("Precursor must precede Tombstone"));
        }
        range.bytes_mut()[48..prefix].copy_from_slice(key);
        Ok(Self {
            layout,
            range: Some(range),
            initialized: false,
            tombstone: AtomicBool::new(true),
            value_offset: prefix,
            capacity: 0,
            key_len: key.len(),
            previous,
            version: CheckpointVersion(0),
            gate: MutationGate::default(),
            failed: AtomicBool::new(false),
            sealed: AtomicBool::new(false),
        })
    }
    pub fn is_tombstone(&self) -> bool {
        self.tombstone.load(Ordering::SeqCst)
    }
    pub fn decode(
        pool: &PagePool,
        layout: Arc<V>,
        encoded: &[u8],
        plan: ValuePlan,
    ) -> Result<Self, Error> {
        let plan = plan.validate()?;
        if encoded.len() != plan.encoded_bytes {
            return Err(Error::Codec("The encoding length does not match the plan"));
        }
        let range = pool.reserve(plan.capacity.max(1), plan.alignment)?;
        let mut owner = Self {
            layout,
            range: Some(range),
            initialized: false,
            tombstone: AtomicBool::new(false),
            value_offset: 0,
            capacity: plan.capacity.max(1),
            key_len: 0,
            previous: None,
            version: CheckpointVersion(0),
            gate: MutationGate::default(),
            failed: AtomicBool::new(false),
            sealed: AtomicBool::new(false),
        };
        owner
            .layout
            .decode_initialize(encoded, permit!(owner, InitPermit))?;
        owner.initialized = true;
        Ok(owner)
    }
    #[cfg(test)]
    pub fn generation(&self) -> Generation {
        self.range
            .as_ref()
            .expect("value_range_exists")
            .generation()
    }
    pub fn address(&self) -> Result<LogAddress, Error> {
        self.range.as_ref().expect("value_range_exists").address()
    }
    fn ready(&self) -> Result<(), Error> {
        if self.is_tombstone() {
            return Err(Error::InvalidState(
                "Tombstones do not contain active values",
            ));
        }
        if self.failed.load(Ordering::SeqCst) {
            Err(Error::InvalidState("Value access failed to close"))
        } else {
            Ok(())
        }
    }
    pub fn read<R>(&self, f: impl for<'a> FnOnce(V::Read<'a>) -> R) -> Result<R, Error> {
        match self.try_read(f)? {
            ValueAccess::Ready(value) => Ok(value),
            ValueAccess::Contended => Err(Error::Busy),
        }
    }
    pub fn try_read<R>(
        &self,
        f: impl for<'a> FnOnce(V::Read<'a>) -> R,
    ) -> Result<ValueAccess<R>, Error> {
        match self.try_read_live(f)? {
            ValueAccess::Ready(Some(value)) => Ok(ValueAccess::Ready(value)),
            ValueAccess::Ready(None) => Err(Error::InvalidState(
                "Tombstones do not contain active values",
            )),
            ValueAccess::Contended => Ok(ValueAccess::Contended),
        }
    }
    /// Tombstone Checking and User Value View Shared Exclusive License,Avoid concurrent deletions from being falsely reported as layout errors.
    pub fn try_read_live<R>(
        &self,
        f: impl for<'a> FnOnce(V::Read<'a>) -> R,
    ) -> Result<ValueAccess<Option<R>>, Error> {
        let _gate = match self.gate.try_replace() {
            Ok(gate) => gate,
            Err(Error::Busy) => return Ok(ValueAccess::Contended),
            Err(error) => return Err(error),
        };
        if self.is_tombstone() {
            return Ok(ValueAccess::Ready(None));
        }
        self.ready()?;
        Ok(ValueAccess::Ready(Some(f(self
            .layout
            .read(permit!(self, ReadPermit))?))))
    }
    /// Published under the same source license as Tombstone;Contention,Records will not be modified in both freezing and cross-version.
    pub fn tombstone_at_version(
        &self,
        version: CheckpointVersion,
        publish: impl FnOnce() -> Result<bool, Error>,
    ) -> Result<ValueAccess<Option<bool>>, Error> {
        if self.version != version {
            return Ok(ValueAccess::Ready(None));
        }
        let _gate = match self.gate.try_replace() {
            Ok(gate) => gate,
            Err(Error::Busy) => return Ok(ValueAccess::Contended),
            Err(error) => return Err(error),
        };
        if self.failed.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("Value access failed to close"));
        }
        if self.sealed.load(Ordering::SeqCst) {
            return Ok(ValueAccess::Ready(None));
        }
        let published = publish()?;
        if published {
            self.tombstone.store(true, Ordering::SeqCst);
        }
        Ok(ValueAccess::Ready(Some(published)))
    }
    #[cfg(test)]
    pub fn update<R>(
        &self,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        self.update_if_mutable(f)?
            .ok_or(Error::InvalidState("Records have stopped updating"))
    }
    /// None Indicates that it has been frozen before the user callback is executed.;Check with seal Use the same arbitration gate.
    #[cfg(test)]
    pub fn update_if_mutable<R>(
        &self,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<Option<R>, Error> {
        match self.try_update_if_mutable(f)? {
            ValueAccess::Ready(value) => Ok(value),
            ValueAccess::Contended => Err(Error::Busy),
        }
    }
    fn try_update_if_mutable<R>(
        &self,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<ValueAccess<Option<R>>, Error> {
        self.ready()?;
        if self.sealed.load(Ordering::SeqCst) {
            return Ok(ValueAccess::Ready(None));
        }
        let _shared;
        let _exclusive;
        if self.layout.concurrent_updates() {
            _shared = Some(match self.gate.try_update() {
                Ok(gate) => gate,
                Err(Error::Busy) => return Ok(ValueAccess::Contended),
                Err(error) => return Err(error),
            });
            _exclusive = None;
        } else {
            _exclusive = Some(match self.gate.try_replace() {
                Ok(gate) => gate,
                Err(Error::Busy) => return Ok(ValueAccess::Contended),
                Err(error) => return Err(error),
            });
            _shared = None;
        }
        self.ready()?;
        if self.sealed.load(Ordering::SeqCst) {
            return Ok(ValueAccess::Ready(None));
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            f(self.layout.update(permit!(self, UpdatePermit))?)
        }));
        match result {
            Ok(value) => value.map(|value| ValueAccess::Ready(Some(value))),
            Err(panic) => {
                self.failed.store(true, Ordering::SeqCst);
                resume_unwind(panic)
            }
        }
    }
    /// Only operations with the same version can apply for in-place updates.;Different versions are returned before calling user code None.
    pub fn update_at_version<R>(
        &self,
        version: CheckpointVersion,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<ValueAccess<Option<R>>, Error> {
        if self.version != version {
            return Ok(ValueAccess::Ready(None));
        }
        self.try_update_if_mutable(f)
    }
    pub fn seal(&self) -> Result<(), Error> {
        let _gate = self.gate.try_replace()?;
        self.sealed.store(true, Ordering::SeqCst);
        Ok(())
    }
    /// Owned disk records after freezing.Values and memory layout are encoded separately,Keep the logical address slot unchanged.
    pub fn encode_record(&self, maximum_version: CheckpointVersion) -> Result<Vec<u8>, Error> {
        self.copy_record(Some(maximum_version))
    }
    /// Complete record occupies slot length,For scanning to check whether the boundary falls within the slot.
    pub fn record_bytes(&self) -> usize {
        self.range
            .as_ref()
            .expect("Active Record Holding Allocation")
            .len()
    }
    /// Scan briefly to exclude all updates and then copy the code,Do not modify sealed or log boundary.
    pub fn snapshot_record(&self) -> Result<Vec<u8>, Error> {
        self.copy_record(None)
    }
    fn copy_record(&self, maximum_version: Option<CheckpointVersion>) -> Result<Vec<u8>, Error> {
        let _gate = self.gate.try_replace()?;
        self.copy_record_locked(maximum_version)
    }
    /// Conditional replication takes the current bytes within the source's exclusive license and performs a synchronous release;Closures cannot wait I/O.
    pub fn with_record_snapshot<R>(
        &self,
        publish: impl FnOnce(&[u8]) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let _gate = self.gate.try_replace()?;
        let bytes = self.copy_record_locked(None)?;
        publish(&bytes)
    }
    fn copy_record_locked(
        &self,
        maximum_version: Option<CheckpointVersion>,
    ) -> Result<Vec<u8>, Error> {
        use crate::format::{HEADER_BYTES, Record, RecordHeader};
        if maximum_version.is_some_and(|version| self.version > version) {
            return Err(Error::InvalidState(
                "The recorded version exceeds the disk brushing range",
            ));
        }
        if self.value_offset == 0
            || maximum_version.is_some() && !self.sealed.load(Ordering::SeqCst)
        {
            return Err(Error::InvalidState(
                "Flashing requires complete frozen records",
            ));
        }
        let len = self.range.as_ref().expect("value_range_exists").len();
        let capacity = len
            .checked_sub(HEADER_BYTES + self.key_len + 4)
            .ok_or(Error::CapacityExceeded)?;
        let value_len = if self.is_tombstone() {
            0
        } else {
            self.ready()?;
            self.layout
                .stable_encoded_len(permit!(self, StablePermit))?
        };
        if value_len > capacity {
            return Err(Error::CapacityExceeded);
        }
        let mut value = Vec::new();
        value
            .try_reserve_exact(value_len)
            .map_err(|_| Error::OutOfMemory)?;
        value.resize(value_len, 0);
        if !self.is_tombstone() {
            self.layout
                .encode_stable(permit!(self, StablePermit), &mut value)?;
        }
        let record = Record {
            header: RecordHeader {
                previous: self.previous,
                version: self.version,
                key_bytes: u32::try_from(self.key_len).map_err(|_| Error::CapacityExceeded)?,
                value_bytes: u32::try_from(value_len).map_err(|_| Error::CapacityExceeded)?,
                capacity_bytes: u32::try_from(capacity).map_err(|_| Error::CapacityExceeded)?,
                tombstone: self.is_tombstone(),
                invalid: false,
                final_record: false,
            },
            key: self.key(),
            value: &value,
        };
        let mut output = Vec::new();
        output
            .try_reserve_exact(len)
            .map_err(|_| Error::OutOfMemory)?;
        output.resize(len, 0);
        record.encode(&mut output)?;
        Ok(output)
    }
    #[cfg(test)]
    pub fn encode(&self, output: &mut [u8]) -> Result<(), Error> {
        self.ready()?;
        let _gate = self.gate.try_replace()?;
        self.ready()?;
        self.layout
            .encode_stable(permit!(self, StablePermit), output)
    }
}
impl<V: ValueLayout> Drop for PageValue<V> {
    fn drop(&mut self) {
        if self.initialized {
            let result = catch_unwind(AssertUnwindSafe(|| {
                self.layout.drop_value(permit!(self, DropPermit))
            }));
            if !matches!(result, Ok(Ok(()))) {
                // Unsure whether allocation is retained when destruction is complete,Avoid potential external resources still referencing freed memory.
                if let Some(range) = self.range.take() {
                    std::mem::forget(range);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builtin::{AtomicU64Value, ByteValueCodec, SerializedValue};
    #[test]
    fn in_situ_tombstones_must_pass_the_source_license_version_freeze_and_release_determination() {
        let pool = PagePool::new(4096, 2).unwrap();
        let value = PageValue::initialize_record(&pool, Arc::new(AtomicU64Value), b"key", None, 7)
            .unwrap()
            .with_version(CheckpointVersion(3));
        let entered = std::sync::Barrier::new(2);
        let release = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                value
                    .read(|v| {
                        entered.wait();
                        release.wait();
                        assert_eq!(v, 7);
                    })
                    .unwrap()
            });
            entered.wait();
            assert!(matches!(
                value.tombstone_at_version(CheckpointVersion(3), || panic!(
                    "Read permission has not been released yet"
                )),
                Ok(ValueAccess::Contended)
            ));
            release.wait();
        });
        assert!(matches!(
            value.tombstone_at_version(CheckpointVersion(4), || panic!(
                "The version does not match and cannot be published."
            )),
            Ok(ValueAccess::Ready(None))
        ));
        assert!(matches!(
            value.tombstone_at_version(CheckpointVersion(3), || Ok(false)),
            Ok(ValueAccess::Ready(Some(false)))
        ));
        assert_eq!(value.read(|v| v).unwrap(), 7);
        value
            .with_record_snapshot(|bytes| {
                assert!(!crate::format::Record::decode(bytes)?.header.tombstone);
                assert!(matches!(
                    value.tombstone_at_version(CheckpointVersion(3), || panic!(
                        "Reproduction permission has not been released"
                    )),
                    Ok(ValueAccess::Contended)
                ));
                Ok(())
            })
            .unwrap();
        assert!(matches!(
            value.tombstone_at_version(CheckpointVersion(3), || Ok(true)),
            Ok(ValueAccess::Ready(Some(true)))
        ));
        assert!(matches!(
            value.try_read_live(|_| panic!("Tombstones must not call value callbacks")),
            Ok(ValueAccess::Ready(None))
        ));
        value.seal().unwrap();
        assert!(matches!(
            value.tombstone_at_version(CheckpointVersion(3), || panic!(
                "Frozen records may not be released in situ"
            )),
            Ok(ValueAccess::Ready(None))
        ));
        let encoded = value.encode_record(CheckpointVersion(3)).unwrap();
        let record = crate::format::Record::decode(&encoded).unwrap();
        assert!(record.header.tombstone);
        assert!(record.value.is_empty());
        assert_eq!(record.header.version, CheckpointVersion(3));
    }
    #[test]
    fn layout_and_operations_returning_busy_errors_cannot_be_disguised_as_permission_contention() {
        struct BusyCodec;
        impl ValueCodec for BusyCodec {
            type Value = Vec<u8>;
            fn format_id(&self) -> FormatId {
                FormatId([99; 16])
            }
            fn encode(&self, value: &Vec<u8>) -> Result<Vec<u8>, Error> {
                Ok(value.clone())
            }
            fn decode(&self, _: &[u8]) -> Result<Vec<u8>, Error> {
                Err(Error::Busy)
            }
        }
        let pool = PagePool::new(4096, 2).unwrap();
        let value =
            PageValue::initialize(&pool, Arc::new(SerializedValue::new(BusyCodec)), vec![1, 2])
                .unwrap();
        let called = std::cell::Cell::new(0);
        {
            let _gate = value.gate.try_replace().unwrap();
            assert!(matches!(
                value.try_read(|_| called.set(1)),
                Ok(ValueAccess::Contended)
            ));
        }
        assert!(matches!(
            value.try_read(|_| called.set(1)),
            Err(Error::Busy)
        ));
        assert_eq!(called.get(), 0);
        let atomic = PageValue::initialize(&pool, Arc::new(AtomicU64Value), 1).unwrap();
        assert!(matches!(
            atomic.try_read(|_| {
                called.set(called.get() + 1);
                Err::<(), Error>(Error::Busy)
            }),
            Ok(ValueAccess::Ready(Err(Error::Busy)))
        ));
        assert!(matches!(
            atomic.update_at_version(CheckpointVersion(0), |_| {
                called.set(called.get() + 1);
                Err::<(), Error>(Error::Busy)
            }),
            Err(Error::Busy)
        ));
        assert_eq!(called.get(), 2);
    }
    #[test]
    fn variable_length_temporary_values_are_planned_as_coded_and_over_budget_inputs_are_rejected() {
        let layout = Arc::new(SerializedValue::new(ByteValueCodec));
        for bytes in [vec![], vec![0, 255, 128], vec![42; 17]] {
            let temporary = TemporaryValue::decode(layout.clone(), &bytes, 64).unwrap();
            assert_eq!(temporary.read(|v| v).unwrap(), bytes);
        }
        assert!(TemporaryValue::decode(layout, &[1; 65], 64).is_err());
        assert!(TemporaryValue::decode(Arc::new(AtomicU64Value), &[1; 7], 64).is_err());
    }
    #[test]
    fn the_variable_length_value_is_shortened_and_encoded_according_to_the_current_length_and_remains_in_the_slot()
     {
        use crate::format::Record;
        let pool = PagePool::new(4096, 2).unwrap();
        let value = PageValue::initialize_record(
            &pool,
            Arc::new(SerializedValue::new(ByteValueCodec)),
            b"key",
            None,
            vec![7; 19],
        )
        .unwrap();
        let value = value.with_version(CheckpointVersion(3));
        assert!(value.encode_record(CheckpointVersion(3)).is_err());
        value.update(|mut v| v.replace(&vec![0, 255, 128])).unwrap();
        value.seal().unwrap();
        let encoded = value.encode_record(CheckpointVersion(3)).unwrap();
        assert_eq!(encoded.len(), value.range.as_ref().unwrap().len());
        let decoded = Record::decode(&encoded).unwrap();
        assert_eq!(decoded.key, b"key");
        assert_eq!(decoded.value, [0, 255, 128]);
        assert_eq!(decoded.header.value_bytes, 3);
        assert_eq!(decoded.header.version, CheckpointVersion(3));
        assert!(decoded.header.capacity_bytes >= 19);
        let again = value.encode_record(CheckpointVersion(3)).unwrap();
        assert_eq!(encoded, again);
    }
    #[test]
    fn atomic_values_and_tombstone_stable_records_can_be_decoded_by_the_format_layer() {
        use crate::format::Record;
        let pool = PagePool::new(4096, 2).unwrap();
        let value =
            PageValue::initialize_record(&pool, Arc::new(AtomicU64Value), b"a", None, u64::MAX)
                .unwrap();
        value.seal().unwrap();
        let bytes = value.encode_record(CheckpointVersion(0)).unwrap();
        let record = Record::decode(&bytes).unwrap();
        assert_eq!(record.value, u64::MAX.to_le_bytes());
        let tombstone = PageValue::tombstone(
            &pool,
            Arc::new(AtomicU64Value),
            b"a",
            Some(value.address().unwrap()),
        )
        .unwrap();
        tombstone.seal().unwrap();
        let bytes = tombstone.encode_record(CheckpointVersion(1)).unwrap();
        let record = Record::decode(&bytes).unwrap();
        assert!(record.header.tombstone);
        assert!(record.value.is_empty());
        assert_eq!(record.header.previous, Some(value.address().unwrap()));
        assert_eq!(record.key, b"a");
    }
    #[test]
    fn normal_value_initialization_growth_rejection_and_stable_encoding() {
        let pool = PagePool::new(256, 1).unwrap();
        let value = PageValue::initialize(
            &pool,
            Arc::new(SerializedValue::new(ByteValueCodec)),
            vec![1, 2, 3],
        )
        .unwrap();
        assert_eq!(value.read(|v| v).unwrap(), [1, 2, 3]);
        value.update(|mut view| view.replace(&vec![9])).unwrap();
        assert!(value.update(|mut view| view.replace(&vec![0; 40])).is_err());
        assert_eq!(value.read(|v| v).unwrap(), [9]);
        let mut out = [0; 1];
        value.encode(&mut out).unwrap();
        assert_eq!(out, [9]);
    }
    #[test]
    fn atomic_layout_cross_thread_update_and_logical_placement() {
        let pool = PagePool::new(256, 1).unwrap();
        let value = PageValue::initialize(&pool, Arc::new(AtomicU64Value), 0).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..100 {
                        loop {
                            match value.update(|v| {
                                v.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            }) {
                                Ok(()) => break,
                                Err(Error::Busy) => std::thread::yield_now(),
                                Err(e) => panic!("{e}"),
                            }
                        }
                    }
                });
            }
        });
        assert_eq!(value.read(|v| v).unwrap(), 400);
        let mut out = [0; 8];
        value.encode(&mut out).unwrap();
        assert_eq!(out, 400u64.to_le_bytes());
        let plan = AtomicU64Value.prepare(400).unwrap().plan();
        let restored = PageValue::decode(&pool, Arc::new(AtomicU64Value), &out, plan).unwrap();
        assert_eq!(restored.read(|v| v).unwrap(), 400);
    }
    #[test]
    fn the_user_panicked_and_refused_to_continue_making_changes() {
        let pool = PagePool::new(256, 1).unwrap();
        let value = PageValue::initialize(&pool, Arc::new(AtomicU64Value), 0).unwrap();
        assert!(
            catch_unwind(AssertUnwindSafe(|| value.update::<()>(|v| {
                v.store(1, Ordering::SeqCst);
                panic!("Panic after modification")
            })))
            .is_err()
        );
        assert!(value.read(|v| v).is_err());
        assert!(value.update(|_| Ok(())).is_err());
    }
    #[test]
    fn corrupt_decoding_does_not_return_visible_values_and_the_range_is_releasable() {
        let pool = PagePool::new(256, 1).unwrap();
        let plan = ValuePlan {
            live_bytes: 8,
            encoded_bytes: 7,
            capacity: 8,
            alignment: 8,
        };
        assert!(PageValue::decode(&pool, Arc::new(AtomicU64Value), &[0; 7], plan).is_err());
        pool.release(PageId(0), Generation(0)).unwrap();
    }
}

#[cfg(test)]
#[path = "value/encoding_tests.rs"]
mod encoding_tests;
