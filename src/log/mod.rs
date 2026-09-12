//! Mixed log page status,Access permission and allocation interface;Do not directly invoke user operations.
use crate::{config::LogConfig, schema::value::ValueLayout, types::*};
use std::{collections::BTreeMap, marker::PhantomData, rc::Rc, sync::Arc};

/// Contended It only means that the value access permission has not been obtained yet;Layout read/update Or the operation error cannot be transferred to this state..
pub(crate) enum ValueAccess<T> {
    Ready(T),
    Contended,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Frontiers {
    pub begin: LogAddress,
    pub head: LogAddress,
    pub safe_head: LogAddress,
    pub read_only: LogAddress,
    pub safe_read_only: LogAddress,
    pub flushed_until: LogAddress,
    pub tail: LogAddress,
}
/// Maintaining boundary snapshots and unreleased reservations for the same control lock,Avoid observing conflicting boundaries.
#[derive(Default)]
struct LogState {
    frontiers: Frontiers,
    reservations: usize,
    flush: Option<Arc<()>>,
    reclaim: Option<(PageId, Generation)>,
}
struct LogControl {
    state: std::sync::RwLock<LogState>,
    // Keep the derived restriction with the control allocation instead of
    // shifting the page pool, record table, and enclosing engine fields.
    mutable_floor: crate::sync::AtomicU64,
    identity: crate::sync::InstanceId,
}
impl LogControl {
    fn new(state: LogState) -> Result<Self, Error> {
        let floor = state
            .frontiers
            .begin
            .max(state.frontiers.read_only)
            .max(state.frontiers.head);
        Ok(Self {
            state: std::sync::RwLock::new(state),
            mutable_floor: crate::sync::AtomicU64::new(floor.0),
            identity: crate::sync::InstanceId::new()?,
        })
    }
}
impl std::ops::Deref for LogControl {
    type Target = std::sync::RwLock<LogState>;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
struct ReservationActivity<'a>(&'a std::sync::RwLock<LogState>);
impl Drop for ReservationActivity<'_> {
    fn drop(&mut self) {
        let mut state = self
            .0
            .write()
            .expect("The reservation count lock is not poisoned");
        state.reservations -= 1;
    }
}
/// All bytes are independently encoded,Does not contain page references or user views,Can be handed over to the device thread.
pub(crate) struct EncodedPage {
    pub generation: Generation,
    pub bytes: Vec<u8>,
}
/// The original record slot has not yet consumed the owned value,cannot be published directly.
pub(crate) struct RecordAllocation<'a, V: ValueLayout> {
    owner: &'a HybridLog<V>,
    value: value::PageValue<V>,
    activity: ReservationActivity<'a>,
}
impl<'a, V: ValueLayout> RecordAllocation<'a, V> {
    pub fn initialize(self, value: V::Owned) -> Result<RecordReservation<'a, V>, Error> {
        Ok(RecordReservation {
            owner: self.owner,
            value: self.value.initialize_owned(value)?,
            _activity: self.activity,
        })
    }
}
/// Value initialization completed but not entered into address table;To throw away means to give up,Not publishing half records.
pub(crate) struct RecordReservation<'a, V: ValueLayout> {
    owner: &'a HybridLog<V>,
    value: value::PageValue<V>,
    _activity: ReservationActivity<'a>,
}
impl<V: ValueLayout> RecordReservation<'_, V> {
    pub fn with_version(mut self, version: CheckpointVersion) -> Self {
        self.value = self.value.with_version(version);
        self
    }
    #[cfg(test)]
    pub fn address(&self) -> Result<LogAddress, Error> {
        self.value.address()
    }
}
/// Have a record reference,The short-term view remains PageValue Limitations on license to arbitrate.
pub(crate) struct RecordLease<V: ValueLayout> {
    value: Arc<value::PageValue<V>>,
    local: PhantomData<Rc<()>>,
}
impl<V: ValueLayout> RecordLease<V> {
    pub fn address(&self) -> Result<LogAddress, Error> {
        self.value.address()
    }
    pub fn tombstone_at_version(
        &self,
        version: CheckpointVersion,
        publish: impl FnOnce() -> Result<bool, Error>,
    ) -> Result<ValueAccess<Option<bool>>, Error> {
        self.value.tombstone_at_version(version, publish)
    }
    pub fn try_read_live<R>(
        &self,
        f: impl for<'a> FnOnce(V::Read<'a>) -> R,
    ) -> Result<ValueAccess<Option<R>>, Error> {
        self.value.try_read_live(f)
    }
    #[cfg(test)]
    pub fn version(&self) -> CheckpointVersion {
        self.value.version()
    }
    pub fn update_at_version<R>(
        &self,
        version: CheckpointVersion,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<ValueAccess<Option<R>>, Error> {
        self.value.update_at_version(version, f)
    }

    pub fn try_read<R>(
        &self,
        f: impl for<'a> FnOnce(V::Read<'a>) -> R,
    ) -> Result<ValueAccess<R>, Error> {
        self.value.try_read(f)
    }
    #[cfg(test)]
    pub fn read<R>(&self, f: impl for<'a> FnOnce(V::Read<'a>) -> R) -> Result<R, Error> {
        self.value.read(f)
    }
    #[cfg(test)]
    pub fn update<R>(
        &self,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        self.value.update(f)
    }
    #[cfg(test)]
    pub fn update_if_mutable<R>(
        &self,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<Option<R>, Error> {
        self.value.update_if_mutable(f)
    }
    pub fn is_tombstone(&self) -> bool {
        self.value.is_tombstone()
    }
    pub fn key(&self) -> &[u8] {
        self.value.key()
    }
    pub fn previous(&self) -> Option<LogAddress> {
        self.value.previous()
    }
    #[cfg(test)]
    pub fn generation(&self) -> Generation {
        self.value.generation()
    }
}
pub(crate) struct HybridLog<V: ValueLayout> {
    pool: page::PagePool,
    page_bytes: usize,
    state: Arc<LogControl>,
    layout: Arc<V>,
    records: crate::sync::Mutex<BTreeMap<LogAddress, Arc<value::PageValue<V>>>>,
}
impl<V: ValueLayout> HybridLog<V> {
    pub fn new(config: LogConfig, layout: Arc<V>) -> Result<Self, Error> {
        Ok(Self {
            pool: page::PagePool::new(config.page_bytes, config.memory_pages)?,
            page_bytes: config.page_bytes,
            state: Arc::new(LogControl::new(LogState::default())?),
            layout,
            records: crate::sync::Mutex::new(BTreeMap::new()),
        })
    }
    pub fn preallocate(&mut self) -> Result<(), Error> {
        self.pool.preallocate()
    }
    pub fn memory_usage(&self) -> Result<(usize, usize), Error> {
        self.pool.memory_usage()
    }
    /// The caller must first verify and install the old log material;Only cold log boundaries are established here,Do not perform recovery I/O.
    pub fn from_checkpoint(
        config: LogConfig,
        layout: Arc<V>,
        begin: LogAddress,
        end: LogAddress,
    ) -> Result<Self, Error> {
        begin.validate()?;
        end.validate()?;
        if begin > end || config.page_bytes == 0 || !end.0.is_multiple_of(config.page_bytes as u64)
        {
            return Err(Error::InvalidFormat(
                "Invalid recovery log range or tail alignment",
            ));
        }
        let pool = page::PagePool::new_at(
            config.page_bytes,
            config.memory_pages,
            PageId(end.0 / config.page_bytes as u64),
        )?;
        Ok(Self {
            pool,
            page_bytes: config.page_bytes,
            layout,
            state: Arc::new(LogControl::new(LogState {
                frontiers: Frontiers {
                    begin,
                    head: end,
                    safe_head: end,
                    read_only: end,
                    safe_read_only: end,
                    flushed_until: end,
                    tail: end,
                },
                ..Default::default()
            })?),
            records: crate::sync::Mutex::new(BTreeMap::new()),
        })
    }
    fn enter_reservation(&self) -> Result<ReservationActivity<'_>, Error> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
        state.reservations = state
            .reservations
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        Ok(ReservationActivity(&self.state))
    }
    /// The caller has verified record boundaries and obtained all business write quorum;Logical truncation does not declare physical deletion.
    pub fn publish_begin(&self, begin: LogAddress) -> Result<(), Error> {
        begin.validate()?;
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
        if begin < state.frontiers.begin || begin > self.pool.tail()? {
            return Err(Error::InvalidFormat(
                "logic begin Cannot go backwards or over the tail",
            ));
        }
        if state.reservations != 0 {
            return Err(Error::Busy);
        }
        self.publish_mutable_floor(begin);
        state.frontiers.begin = begin;
        Ok(())
    }
    /// The caller has stopped new flushes and drained existing writes;The complete old page has been logically invalidated,No need to rewrite it for recycling.
    /// The skipped brush range is located in begin before,Persistent credentials that do not constitute discarded data.
    pub fn discard_prefix(&self) -> Result<(), Error> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
        let floor =
            LogAddress(state.frontiers.begin.0 / self.page_bytes as u64 * self.page_bytes as u64);
        if state.flush.is_some() {
            return Err(Error::Busy);
        }
        self.publish_mutable_floor(floor);
        state.frontiers.read_only = state.frontiers.read_only.max(floor);
        state.frontiers.safe_read_only = state.frontiers.safe_read_only.max(floor);
        state.frontiers.flushed_until = state.frontiers.flushed_until.max(floor);
        Ok(())
    }
    pub fn frontiers(&self) -> Result<Frontiers, Error> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
        let mut result = state.frontiers;
        result.tail = self.pool.tail()?;
        Ok(result)
    }
    /// Called under the control write lock after validating the transition.
    /// Failed freezes keep their target restriction, just like read_only.
    fn publish_mutable_floor(&self, frontier: LogAddress) {
        self.state
            .mutable_floor
            .fetch_max(frontier.0, crate::sync::PUBLISH_ORDER);
    }
    #[cfg(test)]
    pub fn reserve(&self, value: V::Owned) -> Result<RecordReservation<'_, V>, Error> {
        let activity = self.enter_reservation()?;
        Ok(RecordReservation {
            _activity: activity,
            owner: self,
            value: value::PageValue::initialize(&self.pool, self.layout.clone(), value)?,
        })
    }
    pub fn tombstone_fits(&self, key_len: usize) -> Result<(), Error> {
        if key_len
            .checked_add(52)
            .is_none_or(|length| length > self.page_bytes || u32::try_from(length).is_err())
        {
            return Err(Error::CapacityExceeded);
        }
        Ok(())
    }
    pub fn reserve_tombstone(
        &self,
        key: &[u8],
        previous: Option<LogAddress>,
    ) -> Result<RecordReservation<'_, V>, Error> {
        let activity = self.enter_reservation()?;
        Ok(RecordReservation {
            _activity: activity,
            owner: self,
            value: value::PageValue::tombstone(&self.pool, self.layout.clone(), key, previous)?,
        })
    }
    pub fn record_fits(
        &self,
        key_len: usize,
        plan: crate::schema::value::ValuePlan,
    ) -> Result<(), Error> {
        let (_, total) = value::record_bytes(key_len, plan)?;
        if total > self.page_bytes || plan.alignment > self.page_bytes {
            return Err(Error::CapacityExceeded);
        }
        Ok(())
    }
    pub fn allocate_record(
        &self,
        key: &[u8],
        previous: Option<LogAddress>,
        plan: crate::schema::value::ValuePlan,
    ) -> Result<RecordAllocation<'_, V>, Error> {
        self.record_fits(key.len(), plan)?;
        let activity = self.enter_reservation()?;
        Ok(RecordAllocation {
            owner: self,
            value: value::PageValue::allocate_record(
                &self.pool,
                self.layout.clone(),
                key,
                previous,
                plan,
            )?,
            activity,
        })
    }
    pub fn find_mutable(
        &self,
        key: &[u8],
        mut head: Option<LogAddress>,
    ) -> Result<Option<RecordLease<V>>, Error> {
        while let Some(address) = head {
            if self.state.is_poisoned() {
                return Err(Error::InvalidState("Log boundary lock poisoning"));
            }
            self.pool.ensure_healthy()?;
            let floor = LogAddress(self.state.mutable_floor.load(crate::sync::PUBLISH_ORDER));
            // Intra-page logical truncation can lead to read-only boundaries,Cannot update expired old keys along the surviving chain head.
            if address < floor {
                return Ok(None);
            }
            let lease = match self.lease(address) {
                Ok(lease) => lease,
                Err(Error::RangeTruncated) if address < self.frontiers()?.head => return Ok(None),
                Err(error) => return Err(error),
            };
            if lease.key() == key {
                return Ok(Some(lease));
            }
            head = lease.previous();
            if head.is_some_and(|previous| previous >= address) {
                return Err(Error::InvalidFormat(
                    "The log precursor forms an illegal loop",
                ));
            }
        }
        Ok(None)
    }
    #[cfg(test)]
    pub fn reserve_record(
        &self,
        key: &[u8],
        previous: Option<LogAddress>,
        value: V::Owned,
    ) -> Result<RecordReservation<'_, V>, Error> {
        let activity = self.enter_reservation()?;
        Ok(RecordReservation {
            _activity: activity,
            owner: self,
            value: value::PageValue::initialize_record(
                &self.pool,
                self.layout.clone(),
                key,
                previous,
                value,
            )?,
        })
    }
    #[cfg(test)]
    pub fn find<K: crate::schema::KeyCodec>(
        &self,
        codec: &K,
        key: &K::Key,
        mut head: Option<LogAddress>,
    ) -> Result<Option<RecordLease<V>>, Error> {
        while let Some(address) = head {
            let lease = self.lease(address)?;
            if codec.equals_encoded(key, lease.key())? {
                return Ok(Some(lease));
            }
            head = lease.previous();
            if head.is_some_and(|previous| previous >= address) {
                return Err(Error::InvalidFormat(
                    "The log precursor forms an illegal loop",
                ));
            }
        }
        Ok(None)
    }
    pub fn finish_initialization(
        &self,
        reservation: RecordReservation<'_, V>,
    ) -> Result<LogAddress, Error> {
        self.with_initialization(reservation, Ok)
    }
    /// Preserve reservation count until synchronous release closure ends;Scanning will not lend target records that have not yet completed release determinations..
    pub fn with_initialization<R>(
        &self,
        reservation: RecordReservation<'_, V>,
        publish: impl FnOnce(LogAddress) -> Result<R, Error>,
    ) -> Result<R, Error> {
        if !std::ptr::eq(self, reservation.owner) {
            return Err(Error::InvalidState("Reserve other logs"));
        }
        let address = reservation.value.address()?;
        {
            let mut records = self
                .records
                .lock()
                .map_err(|_| Error::InvalidState("Record table lock poisoning"))?;
            if records.contains_key(&address) {
                return Err(Error::InvalidState(
                    "Record address is published repeatedly",
                ));
            }
            records.insert(address, Arc::new(reservation.value));
        }
        // Caller handling CAS Targets can be removed in case of conflict;Record table lock cannot be held here.
        let result = publish(address);
        drop(reservation._activity);
        result
    }
    pub fn lease(&self, address: LogAddress) -> Result<RecordLease<V>, Error> {
        address.validate()?;
        let records = self
            .records
            .lock()
            .map_err(|_| Error::InvalidState("Record table lock poisoning"))?;
        let value = records.get(&address).ok_or(Error::RangeTruncated)?.clone();
        Ok(RecordLease {
            value,
            local: PhantomData,
        })
    }
    #[cfg(test)]
    pub fn lease_generation(
        &self,
        address: LogAddress,
        generation: Generation,
    ) -> Result<RecordLease<V>, Error> {
        let lease = self.lease(address)?;
        if lease.generation() != generation {
            return Err(Error::RangeTruncated);
        }
        Ok(lease)
    }
    #[cfg(test)]
    pub fn abandon(&self, reservation: RecordReservation<'_, V>) -> Result<(), Error> {
        if !std::ptr::eq(self, reservation.owner) {
            return Err(Error::InvalidState("Reserve other logs"));
        }
        drop(reservation);
        Ok(())
    }
    /// The upper layer must first remove the index visibility;Old lease retention value,Disallow direct release of its allocation.
    pub fn retire(&self, address: LogAddress) -> Result<(), Error> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| Error::InvalidState("Record table lock poisoning"))?;
        records.get(&address).ok_or(Error::RangeTruncated)?.seal()?;
        let value = records.remove(&address).expect("Confirmed record exists");
        drop(records);
        drop(value);
        Ok(())
    }
    #[cfg(test)]
    pub fn release_page(&self, page: PageId, generation: Generation) -> Result<(), Error> {
        self.pool.release(page, generation)
    }
    /// Get page-aligned checkpoint tail;The caller must save the return bounds,Then promote read-only and disk flushing.
    /// Deny the existence of reserved moments;New requests can then only be appended to the next page,The footer of this page will not be backfilled.
    pub fn pad_tail(&self) -> Result<LogAddress, Error> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
        if state.reservations != 0 {
            return Err(Error::Busy);
        }
        self.pool.pad_tail()
    }
    pub fn advance_read_only(&self, target: LogAddress) -> Result<(), Error> {
        target.validate()?;
        if !target.0.is_multiple_of(self.page_bytes as u64) {
            return Err(Error::InvalidFormat(
                "Read-only boundaries must be aligned to the page",
            ));
        }
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
        if target < state.frontiers.read_only || target > self.pool.tail()? {
            return Err(Error::InvalidState(
                "Read-only boundaries cannot go backwards or past the end",
            ));
        }
        // including initializing,Reservations that have been initialized but not yet released.Can't just check the address table.
        if state.reservations != 0 {
            return Err(Error::Busy);
        }
        let records = self
            .records
            .lock()
            .map_err(|_| Error::InvalidState("Record table lock poisoning"))?;
        self.publish_mutable_floor(target);
        state.frontiers.read_only = target;
        for (_, value) in records.range(..target) {
            value.seal()?;
        }
        // of each record seal Arbitration with updated licenses;Keep target on failure,but does not push the boundaries of security.
        state.frontiers.safe_read_only = target;
        Ok(())
    }
    /// Only pages that are frozen and completely written to the normal log are allowed to be copied;Upper level maintenance actions must be excluded GC/Truncate.
    pub fn checkpoint_page(
        &self,
        page: PageId,
        route: crate::device::CompletionRoute,
    ) -> Result<(read_page::PageRead, LogAddress, LogAddress), Error> {
        let start = LogAddress::from_page_offset(page, 0, self.page_bytes as u64)?;
        let end = start.checked_add(self.page_bytes as u64)?;
        let frontiers = self.frontiers()?;
        if end <= frontiers.begin {
            return Err(Error::RangeTruncated);
        }
        if end > frontiers.safe_read_only || end > frontiers.flushed_until {
            return Err(Error::Busy);
        }
        Ok((
            read_page::PageRead::new(page, self.page_bytes, route)?,
            start.max(frontiers.begin),
            end,
        ))
    }
    pub fn decode_temporary(&self, encoded: &[u8]) -> Result<value::TemporaryValue<V>, Error> {
        value::TemporaryValue::decode(self.layout.clone(), encoded, self.page_bytes)
    }
    pub fn encode_page(
        &self,
        page: PageId,
        version: CheckpointVersion,
    ) -> Result<EncodedPage, Error> {
        let begin = LogAddress::from_page_offset(page, 0, self.page_bytes as u64)?;
        let end = begin.checked_add(self.page_bytes as u64)?;
        let (generation, values) = {
            let state = self
                .state
                .read()
                .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
            if end <= state.frontiers.begin {
                return Err(Error::RangeTruncated);
            }
            if end > state.frontiers.safe_read_only {
                return Err(Error::Busy);
            }
            let generation = self.pool.generation(page)?;
            let records = self
                .records
                .lock()
                .map_err(|_| Error::InvalidState("Record table lock poisoning"))?;
            let mut values = Vec::new();
            for (address, value) in records.range(begin.max(state.frontiers.begin)..end) {
                values.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                values.push((*address, value.clone()));
            }
            (generation, values)
        };
        // Log control lock or record table lock is not held during expert layout callback execution.
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(self.page_bytes)
            .map_err(|_| Error::OutOfMemory)?;
        payload.resize(self.page_bytes, 0);
        for (address, value) in values {
            let encoded = value.encode_record(version)?;
            let offset =
                usize::try_from(address.0 - begin.0).map_err(|_| Error::CapacityExceeded)?;
            let end = offset
                .checked_add(encoded.len())
                .ok_or(Error::CapacityExceeded)?;
            payload
                .get_mut(offset..end)
                .ok_or(Error::InvalidFormat("Record past logical page"))?
                .copy_from_slice(&encoded);
        }
        let bytes = crate::format::PageFrame {
            page,
            version,
            payload: &payload,
        }
        .encode()?;
        Ok(EncodedPage { generation, bytes })
    }
}
mod gate;
mod page;
pub(crate) mod scan;
mod value;

#[cfg(test)]
mod mutable_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builtin::AtomicU64Value;
    struct Resource(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for Resource {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    struct ResourceLayout {
        drops: Arc<std::sync::atomic::AtomicUsize>,
        destructors: Arc<std::sync::atomic::AtomicUsize>,
    }
    // SAFETY: Test layout is read-write aligned only under exclusive license Box;Failed initialization cleans itself up,success by drop_value clean up.
    unsafe impl ValueLayout for ResourceLayout {
        type Owned = bool;
        type Read<'a> = ();
        type Update<'a> = ();
        fn format_id(&self) -> FormatId {
            FormatId([1; 16])
        }
        fn plan(&self, _: &bool) -> Result<crate::schema::value::ValuePlan, Error> {
            Ok(crate::schema::value::ValuePlan {
                live_bytes: size_of::<Box<Resource>>(),
                encoded_bytes: 1,
                capacity: size_of::<Box<Resource>>(),
                alignment: align_of::<Box<Resource>>(),
            })
        }
        fn decode_owned(&self, bytes: &[u8]) -> Result<bool, Error> {
            if bytes != [0] {
                return Err(Error::Codec("Resource encoding is corrupted"));
            }
            Ok(false)
        }
        fn plan_decode(&self, bytes: &[u8]) -> Result<crate::schema::value::ValuePlan, Error> {
            if bytes != [0] {
                return Err(Error::Codec("Resource encoding is corrupted"));
            }
            self.plan(&false)
        }
        fn initialize(
            &self,
            p: crate::schema::value::InitPermit<'_>,
            fail: bool,
        ) -> Result<(), Error> {
            assert!(p.len() >= size_of::<Box<Resource>>());
            assert_eq!(
                p.as_ptr().as_ptr() as usize % align_of::<Box<Resource>>(),
                0
            );
            let pointer = p.as_ptr().cast::<Box<Resource>>().as_ptr();
            // SAFETY: License exclusive and size alignment checked,Slot not initialized.
            unsafe { pointer.write(Box::new(Resource(self.drops.clone()))) };
            if fail {
                // SAFETY: Just initialized Box Still exclusive to this call;After being removed, the slot returns to its uninitialized state..
                drop(unsafe { pointer.read() });
                return Err(Error::Codec("Failed after partial initialization"));
            }
            Ok(())
        }
        fn read<'a>(&'a self, _: crate::schema::value::ReadPermit<'a>) -> Result<(), Error> {
            Ok(())
        }
        fn update<'a>(&'a self, _: crate::schema::value::UpdatePermit<'a>) -> Result<(), Error> {
            Ok(())
        }
        fn stable_encoded_len(
            &self,
            _: crate::schema::value::StablePermit<'_>,
        ) -> Result<usize, Error> {
            Ok(1)
        }
        fn encode_stable(
            &self,
            _: crate::schema::value::StablePermit<'_>,
            output: &mut [u8],
        ) -> Result<(), Error> {
            if output.len() != 1 {
                return Err(Error::Codec("Length does not match"));
            }
            output[0] = 0;
            Ok(())
        }
        fn decode_initialize(
            &self,
            bytes: &[u8],
            p: crate::schema::value::InitPermit<'_>,
        ) -> Result<(), Error> {
            if bytes != [0] {
                return Err(Error::Codec("Corrupted encoding"));
            }
            self.initialize(p, false)
        }
        fn drop_value(&self, p: crate::schema::value::DropPermit<'_>) -> Result<(), Error> {
            self.destructors
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // SAFETY: Only for successfully initialized Box call,Last owner holds permission to destroy.
            unsafe { std::ptr::drop_in_place(p.as_ptr().cast::<Box<Resource>>().as_ptr()) };
            Ok(())
        }
    }
    #[test]
    fn the_original_record_slot_is_discarded_without_destroying_the_uninitialized_value_and_is_only_destroyed_once_after_successful_initialization()
     {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let drops = Arc::new(AtomicUsize::new(0));
        let destructors = Arc::new(AtomicUsize::new(0));
        let layout = Arc::new(ResourceLayout {
            drops: drops.clone(),
            destructors: destructors.clone(),
        });
        let plan = layout.plan(&false).unwrap();
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 128,
                memory_pages: 1,
                mutable_fraction: 0.5,
            },
            layout,
        )
        .unwrap();
        let allocation = log.allocate_record(b"k", None, plan).unwrap();
        assert!(log.lease(LogAddress(0)).is_err());
        drop(allocation);
        assert_eq!(destructors.load(Ordering::SeqCst), 0);
        log.release_page(PageId(0), Generation(0)).unwrap();
        let allocation = log.allocate_record(b"k", None, plan).unwrap();
        let address = log
            .finish_initialization(allocation.initialize(false).unwrap())
            .unwrap();
        log.retire(address).unwrap();
        assert_eq!(destructors.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn temporary_decoding_does_not_occupy_log_pages_and_resources_are_only_destroyed_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let log = log();
        for value in 0..8 {
            log.finish_initialization(log.reserve(value).unwrap())
                .unwrap();
        }
        assert!(log.reserve(9).is_err());
        let temporary = log.decode_temporary(&u64::MAX.to_le_bytes()).unwrap();
        assert_eq!(temporary.read(|v| v).unwrap(), u64::MAX);
        assert_eq!(log.frontiers().unwrap().tail, LogAddress(64));
        assert!(log.reserve(9).is_err());
        drop(log);
        assert_eq!(temporary.read(|v| v).unwrap(), u64::MAX);
        let drops = Arc::new(AtomicUsize::new(0));
        let destructors = Arc::new(AtomicUsize::new(0));
        let layout = Arc::new(ResourceLayout {
            drops: drops.clone(),
            destructors: destructors.clone(),
        });
        assert!(value::TemporaryValue::decode(layout.clone(), &[1], 64).is_err());
        assert!(value::TemporaryValue::decode(layout.clone(), &[0], 1).is_err());
        let decoded = value::TemporaryValue::decode(layout, &[0], 64).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(decoded);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(destructors.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn cold_recovery_log_bounds_are_consistent_and_invalid_tails_are_rejected() {
        let config = LogConfig {
            page_bytes: 256,
            memory_pages: 2,
            mutable_fraction: 0.5,
        };
        for (begin, end) in [(256, 0), (0, 255), (0, u64::MAX)] {
            assert!(
                HybridLog::from_checkpoint(
                    config.clone(),
                    Arc::new(AtomicU64Value),
                    LogAddress(begin),
                    LogAddress(end)
                )
                .is_err()
            );
        }
        let log = HybridLog::from_checkpoint(
            config,
            Arc::new(AtomicU64Value),
            LogAddress(128),
            LogAddress(2048),
        )
        .unwrap();
        let f = log.frontiers().unwrap();
        assert_eq!(f.begin, LogAddress(128));
        for boundary in [
            f.tail,
            f.head,
            f.safe_head,
            f.read_only,
            f.safe_read_only,
            f.flushed_until,
        ] {
            assert_eq!(boundary, LogAddress(2048));
        }
        assert!(log.lease(LogAddress(256)).is_err());
        assert!(matches!(log.evict_next(), Err(Error::Busy)));
        let address = log
            .finish_initialization(
                log.reserve_record(b"key", Some(LogAddress(256)), 17)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(address, LogAddress(2048));
        assert_eq!(log.lease(address).unwrap().read(|v| v).unwrap(), 17);
        assert_eq!(
            log.lease(address).unwrap().previous(),
            Some(LogAddress(256))
        );
    }
    #[test]
    fn checkpoint_filling_last_page_does_not_create_records_and_subsequent_allocations_do_not_backfill()
     {
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 256,
                memory_pages: 2,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
        )
        .unwrap();
        assert_eq!(log.pad_tail().unwrap(), LogAddress(0));
        let first = log
            .finish_initialization(log.reserve_record(b"a", None, 11).unwrap())
            .unwrap();
        let before = log.frontiers().unwrap();
        assert!(before.tail.0 < 256);
        let boundary = log.pad_tail().unwrap();
        assert_eq!(boundary, LogAddress(256));
        assert_eq!(log.pad_tail().unwrap(), boundary);
        let padded = log.frontiers().unwrap();
        assert_eq!(padded.read_only, before.read_only);
        assert_eq!(padded.safe_read_only, before.safe_read_only);
        assert_eq!(padded.flushed_until, before.flushed_until);
        assert!(log.encode_page(PageId(0), CheckpointVersion(0)).is_err());
        log.advance_read_only(boundary).unwrap();
        let encoded = log.encode_page(PageId(0), CheckpointVersion(0)).unwrap();
        let frame = crate::format::PageFrame::decode(&encoded.bytes, PageId(0), 256).unwrap();
        let records = frame.records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, first);
        assert_eq!(records[0].1.value, 11u64.to_le_bytes());
        assert!(
            frame.payload[before.tail.0 as usize..]
                .iter()
                .all(|b| *b == 0)
        );
        let next = log
            .finish_initialization(log.reserve_record(b"b", Some(first), 22).unwrap())
            .unwrap();
        assert_eq!(next, boundary);
        assert_eq!(log.frontiers().unwrap().safe_read_only, boundary);
    }
    #[test]
    fn active_reservations_reject_tail_page_filling_and_do_not_change_allocation_boundaries() {
        let log = log();
        let pending = log.reserve(11).unwrap();
        let before = log.frontiers().unwrap().tail;
        assert!(matches!(log.pad_tail(), Err(Error::Busy)));
        assert_eq!(log.frontiers().unwrap().tail, before);
        drop(pending);
        let end = log.pad_tail().unwrap();
        assert_eq!(end, LogAddress(64));
        log.advance_read_only(end).unwrap();
        assert_eq!(log.frontiers().unwrap().safe_read_only, end);
    }
    #[test]
    fn freeze_page_encoding_covers_alignment_gaps_and_abandoned_reservations() {
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 256,
                memory_pages: 2,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
        )
        .unwrap();
        drop(log.reserve_record(b"lost", None, 9).unwrap());
        let first = log
            .finish_initialization(log.reserve_record(b"a", None, 11).unwrap())
            .unwrap();
        let second = log
            .finish_initialization(log.reserve_record(b"b", Some(first), 22).unwrap())
            .unwrap();
        // The fourth slot is entered into the second page.,Leave zero padding at the end of the first page.
        let _next = log
            .finish_initialization(log.reserve_record(b"next", Some(second), 33).unwrap())
            .unwrap();
        assert!(log.encode_page(PageId(0), CheckpointVersion(0)).is_err());
        log.advance_read_only(LogAddress(256)).unwrap();
        let encoded = log.encode_page(PageId(0), CheckpointVersion(0)).unwrap();
        assert_eq!(encoded.generation, Generation(0));
        let frame = crate::format::PageFrame::decode(&encoded.bytes, PageId(0), 256).unwrap();
        assert_eq!(frame.page, PageId(0));
        let records = frame.records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, first);
        assert_eq!(records[0].1.value, 11u64.to_le_bytes());
        assert_eq!(records[1].0, second);
        assert_eq!(records[1].1.header.previous, Some(first));
        assert_eq!(log.frontiers().unwrap().flushed_until, LogAddress(0));
    }
    #[test]
    fn unreleased_reservations_prevent_freezing_and_can_be_advanced_after_abandonment() {
        let log = log();
        let pending = log.reserve(1).unwrap();
        for value in 2..=8 {
            log.finish_initialization(log.reserve(value).unwrap())
                .unwrap();
        }
        assert_eq!(log.frontiers().unwrap().tail, LogAddress(64));
        assert!(matches!(
            log.advance_read_only(LogAddress(64)),
            Err(Error::Busy)
        ));
        assert_eq!(log.frontiers().unwrap().safe_read_only, LogAddress(0));
        drop(pending);
        log.advance_read_only(LogAddress(64)).unwrap();
        let frontiers = log.frontiers().unwrap();
        assert_eq!(frontiers.read_only, LogAddress(64));
        assert_eq!(frontiers.safe_read_only, LogAddress(64));
        assert_eq!(frontiers.flushed_until, LogAddress(0));
        assert!(log.advance_read_only(LogAddress(0)).is_err());
        assert!(log.advance_read_only(LogAddress(65)).is_err());
        assert!(log.advance_read_only(LogAddress(128)).is_err());
        let lease = log.lease(LogAddress(8)).unwrap();
        assert_eq!(lease.read(|value| value).unwrap(), 2);
        assert!(lease.update(|_| Ok(())).is_err());
    }
    #[test]
    fn freezing_while_updating_does_not_advance_security_boundaries_and_retry_completes() {
        use std::sync::Barrier;
        let log = log();
        for value in 0..8 {
            log.finish_initialization(log.reserve(value).unwrap())
                .unwrap();
        }
        let entered = Barrier::new(2);
        let leave = Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let lease = log.lease(LogAddress(0)).unwrap();
                lease
                    .update(|_| {
                        entered.wait();
                        leave.wait();
                        Ok(())
                    })
                    .unwrap();
            });
            entered.wait();
            assert!(matches!(
                log.advance_read_only(LogAddress(64)),
                Err(Error::Busy)
            ));
            let frontiers = log.frontiers().unwrap();
            assert_eq!(frontiers.read_only, LogAddress(64));
            assert_eq!(frontiers.safe_read_only, LogAddress(0));
            leave.wait();
        });
        log.advance_read_only(LogAddress(64)).unwrap();
        assert_eq!(log.frontiers().unwrap().safe_read_only, LogAddress(64));
    }
    #[test]
    fn partial_initialization_failure_is_cleaned_up_by_itself_and_the_success_value_is_only_destroyed_once()
     {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let drops = Arc::new(AtomicUsize::new(0));
        let destructors = Arc::new(AtomicUsize::new(0));
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 64,
                memory_pages: 1,
                mutable_fraction: 0.5,
            },
            Arc::new(ResourceLayout {
                drops: drops.clone(),
                destructors: destructors.clone(),
            }),
        )
        .unwrap();
        assert!(log.reserve(true).is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(destructors.load(Ordering::SeqCst), 0);
        let address = log
            .finish_initialization(log.reserve(false).unwrap())
            .unwrap();
        let lease = log.lease(address).unwrap();
        log.retire(address).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(lease);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert_eq!(destructors.load(Ordering::SeqCst), 1);
        log.release_page(PageId(0), Generation(0)).unwrap();
    }
    #[test]
    fn variable_lookups_respect_in_page_truncation_and_freezing_and_old_leases_cannot_be_updated() {
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 256,
                memory_pages: 2,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
        )
        .unwrap();
        let first = log
            .finish_initialization(log.reserve_record(b"a", None, 11).unwrap())
            .unwrap();
        let second = log
            .finish_initialization(log.reserve_record(b"b", Some(first), 22).unwrap())
            .unwrap();
        let third = log
            .finish_initialization(log.reserve_record(b"c", Some(second), 33).unwrap())
            .unwrap();
        assert_eq!(
            log.find_mutable(b"a", Some(third))
                .unwrap()
                .unwrap()
                .read(|v| v)
                .unwrap(),
            11
        );
        log.publish_begin(second).unwrap();
        assert!(log.find_mutable(b"a", Some(third)).unwrap().is_none());
        let lease = log.find_mutable(b"b", Some(third)).unwrap().unwrap();
        assert_eq!(lease.read(|v| v).unwrap(), 22);
        let tail = log.pad_tail().unwrap();
        assert_eq!(tail, LogAddress(256));
        log.advance_read_only(tail).unwrap();
        assert!(log.find_mutable(b"c", Some(third)).unwrap().is_none());
        assert!(lease.update(|_| Ok(())).is_err());
        assert_eq!(lease.read(|v| v).unwrap(), 22);
    }
    #[test]
    fn variable_lookup_distinguishes_cold_history_from_missing_records() {
        let log = HybridLog::from_checkpoint(
            LogConfig {
                page_bytes: 256,
                memory_pages: 2,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
            LogAddress(128),
            LogAddress(2048),
        )
        .unwrap();
        assert!(
            log.find_mutable(b"old", Some(LogAddress(256)))
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            log.find_mutable(b"missing", Some(LogAddress(2048))),
            Err(Error::RangeTruncated)
        ));
        let first = log
            .finish_initialization(
                log.reserve_record(b"new", Some(LogAddress(256)), 17)
                    .unwrap(),
            )
            .unwrap();
        assert!(log.find_mutable(b"old", Some(first)).unwrap().is_none());
        assert_eq!(
            log.find_mutable(b"new", Some(first))
                .unwrap()
                .unwrap()
                .read(|v| v)
                .unwrap(),
            17
        );
    }
    fn log() -> HybridLog<AtomicU64Value> {
        HybridLog::new(
            LogConfig {
                page_bytes: 64,
                memory_pages: 1,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
        )
        .unwrap()
    }
    #[test]
    fn the_reservation_is_invisible_and_the_discard_leaves_no_address_record() {
        let log = log();
        let reservation = log.reserve(7).unwrap();
        let address = reservation.address().unwrap();
        assert!(log.lease(address).is_err());
        log.abandon(reservation).unwrap();
        assert!(log.lease(address).is_err());
        log.release_page(PageId(0), Generation(0)).unwrap();
    }
    #[test]
    fn can_be_leased_after_release_and_old_lease_prevents_destruction() {
        let log = log();
        let address = log.finish_initialization(log.reserve(7).unwrap()).unwrap();
        let lease = log.lease(address).unwrap();
        assert_eq!(lease.read(|v| v).unwrap(), 7);
        assert!(log.lease_generation(address, Generation(9)).is_err());
        log.retire(address).unwrap();
        assert!(log.lease(address).is_err());
        assert!(log.release_page(PageId(0), Generation(0)).is_err());
        assert_eq!(lease.read(|v| v).unwrap(), 7);
        assert!(lease.update(|_| Ok(())).is_err());
        drop(lease);
        log.release_page(PageId(0), Generation(0)).unwrap();
    }
    #[test]
    fn lease_outlives_log_but_reservation_cannot_be_released_across_logs() {
        let first = log();
        let second = log();
        assert!(
            second
                .finish_initialization(first.reserve(1).unwrap())
                .is_err()
        );
        let address = first
            .finish_initialization(first.reserve(2).unwrap())
            .unwrap();
        let lease = first.lease(address).unwrap();
        drop(first);
        assert_eq!(lease.read(|v| v).unwrap(), 2);
    }
}

pub(crate) mod flush;

mod evict;
pub(crate) mod lookup;
pub(crate) mod read_page;

#[cfg(test)]
mod lookup_identity_tests;
