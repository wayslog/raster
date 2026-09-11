//! 混合日志的页状态、访问许可与分配接口；不直接调用用户操作。
use crate::{config::LogConfig, schema::value::ValueLayout, types::*};
use std::{collections::BTreeMap, marker::PhantomData, rc::Rc, sync::Arc};

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
/// 同一控制锁维护边界快照与未发布预留，避免观察到互相矛盾的边界。
#[derive(Default)]
struct LogState {
    frontiers: Frontiers,
    reservations: usize,
    flush: Option<Arc<()>>,
    reclaim: Option<(PageId, Generation)>,
}
struct ReservationActivity<'a>(&'a crate::sync::Mutex<LogState>);
impl Drop for ReservationActivity<'_> {
    fn drop(&mut self) {
        let mut state = self.0.lock().expect("预留计数锁未中毒");
        state.reservations -= 1;
    }
}
/// 所有字节已独立编码，不含页面引用或用户视图，可以交给设备线程。
pub(crate) struct EncodedPage {
    pub generation: Generation,
    pub bytes: Vec<u8>,
}
/// 原始记录槽尚未消费拥有值，不能直接发布。
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
/// 已完成值初始化但未进入地址表；丢弃即放弃，不发布半记录。
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
/// 拥有记录引用，短期视图仍由 PageValue 的仲裁许可限制。
pub(crate) struct RecordLease<V: ValueLayout> {
    value: Arc<value::PageValue<V>>,
    local: PhantomData<Rc<()>>,
}
impl<V: ValueLayout> RecordLease<V> {
    #[cfg(test)]
    pub fn version(&self) -> CheckpointVersion {
        self.value.version()
    }
    pub fn update_at_version<R>(
        &self,
        version: CheckpointVersion,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<Option<R>, Error> {
        self.value.update_at_version(version, f)
    }

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
    state: Arc<crate::sync::Mutex<LogState>>,
    layout: Arc<V>,
    records: crate::sync::Mutex<BTreeMap<LogAddress, Arc<value::PageValue<V>>>>,
}
impl<V: ValueLayout> HybridLog<V> {
    pub fn new(config: LogConfig, layout: Arc<V>) -> Result<Self, Error> {
        Ok(Self {
            pool: page::PagePool::new(config.page_bytes, config.memory_pages)?,
            page_bytes: config.page_bytes,
            state: Arc::new(crate::sync::Mutex::new(LogState::default())),
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
    /// 调用者须先验证并安装旧日志材料；这里只建立冷日志边界，不执行恢复 I/O。
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
            return Err(Error::InvalidFormat("恢复日志范围或尾部对齐无效"));
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
            state: Arc::new(crate::sync::Mutex::new(LogState {
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
            })),
            records: crate::sync::Mutex::new(BTreeMap::new()),
        })
    }
    fn enter_reservation(&self) -> Result<ReservationActivity<'_>, Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
        state.reservations = state
            .reservations
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        Ok(ReservationActivity(&self.state))
    }
    /// 调用者已验证记录边界且取得全部业务写入仲裁；逻辑截断不声明物理删除。
    pub fn publish_begin(&self, begin: LogAddress) -> Result<(), Error> {
        begin.validate()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
        if begin < state.frontiers.begin || begin > self.pool.tail()? {
            return Err(Error::InvalidFormat("逻辑 begin 不能倒退或越过尾部"));
        }
        if state.reservations != 0 {
            return Err(Error::Busy);
        }
        state.frontiers.begin = begin;
        Ok(())
    }
    /// 调用者已停止新刷盘并排空既有写入；完整旧页已经逻辑作废，无需为了回收再写出。
    /// 跳过的刷盘范围位于 begin 之前，不构成被丢弃数据的持久化凭据。
    pub fn discard_prefix(&self) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
        let floor =
            LogAddress(state.frontiers.begin.0 / self.page_bytes as u64 * self.page_bytes as u64);
        if state.flush.is_some() {
            return Err(Error::Busy);
        }
        state.frontiers.read_only = state.frontiers.read_only.max(floor);
        state.frontiers.safe_read_only = state.frontiers.safe_read_only.max(floor);
        state.frontiers.flushed_until = state.frontiers.flushed_until.max(floor);
        Ok(())
    }
    pub fn frontiers(&self) -> Result<Frontiers, Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
        let mut result = state.frontiers;
        result.tail = self.pool.tail()?;
        Ok(result)
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
            let frontiers = self.frontiers()?;
            // 页内逻辑截断可以领先于只读边界，不能沿存活链头更新已失效的旧键。
            if address < frontiers.begin.max(frontiers.read_only).max(frontiers.head) {
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
                return Err(Error::InvalidFormat("日志前驱形成非法回路"));
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
                return Err(Error::InvalidFormat("日志前驱形成非法回路"));
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
    /// 同步发布闭包结束之前保留预留计数；扫描不会借出尚未完成发布判定的目标记录。
    pub fn with_initialization<R>(
        &self,
        reservation: RecordReservation<'_, V>,
        publish: impl FnOnce(LogAddress) -> Result<R, Error>,
    ) -> Result<R, Error> {
        if !std::ptr::eq(self, reservation.owner) {
            return Err(Error::InvalidState("预留属于其他日志"));
        }
        let address = reservation.value.address()?;
        {
            let mut records = self
                .records
                .lock()
                .map_err(|_| Error::InvalidState("记录表锁中毒"))?;
            if records.contains_key(&address) {
                return Err(Error::InvalidState("记录地址重复发布"));
            }
            records.insert(address, Arc::new(reservation.value));
        }
        // 调用者处理 CAS 冲突时可摘除目标；此处不能持有记录表锁。
        let result = publish(address);
        drop(reservation._activity);
        result
    }
    pub fn lease(&self, address: LogAddress) -> Result<RecordLease<V>, Error> {
        address.validate()?;
        let records = self
            .records
            .lock()
            .map_err(|_| Error::InvalidState("记录表锁中毒"))?;
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
            return Err(Error::InvalidState("预留属于其他日志"));
        }
        drop(reservation);
        Ok(())
    }
    /// 上层须先摘除索引可见性；旧租约保留值，禁止直接释放其分配。
    pub fn retire(&self, address: LogAddress) -> Result<(), Error> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| Error::InvalidState("记录表锁中毒"))?;
        records.get(&address).ok_or(Error::RangeTruncated)?.seal()?;
        let value = records.remove(&address).expect("已确认记录存在");
        drop(records);
        drop(value);
        Ok(())
    }
    #[cfg(test)]
    pub fn release_page(&self, page: PageId, generation: Generation) -> Result<(), Error> {
        self.pool.release(page, generation)
    }
    /// 取得页对齐的检查点尾部；调用者须保存返回边界，再推进只读及刷盘。
    /// 拒绝存在预留的时刻；新请求随后只能在下一页追加，不会回填该页尾。
    pub fn pad_tail(&self) -> Result<LogAddress, Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
        if state.reservations != 0 {
            return Err(Error::Busy);
        }
        self.pool.pad_tail()
    }
    pub fn advance_read_only(&self, target: LogAddress) -> Result<(), Error> {
        target.validate()?;
        if !target.0.is_multiple_of(self.page_bytes as u64) {
            return Err(Error::InvalidFormat("只读边界必须对齐到页"));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
        if target < state.frontiers.read_only || target > self.pool.tail()? {
            return Err(Error::InvalidState("只读边界不能倒退或超过尾部"));
        }
        // 包括正在初始化、初始化完毕但尚未发布的预留。不能仅检查地址表。
        if state.reservations != 0 {
            return Err(Error::Busy);
        }
        let records = self
            .records
            .lock()
            .map_err(|_| Error::InvalidState("记录表锁中毒"))?;
        state.frontiers.read_only = target;
        for (_, value) in records.range(..target) {
            value.seal()?;
        }
        // 每条记录的 seal 与更新许可仲裁；失败时保留目标，但不推进安全边界。
        state.frontiers.safe_read_only = target;
        Ok(())
    }
    /// 仅允许复制已冻结且完整写入普通日志的页；上层维护动作须排斥 GC/截断。
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
                .lock()
                .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
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
                .map_err(|_| Error::InvalidState("记录表锁中毒"))?;
            let mut values = Vec::new();
            for (address, value) in records.range(begin.max(state.frontiers.begin)..end) {
                values.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                values.push((*address, value.clone()));
            }
            (generation, values)
        };
        // 专家布局回调执行期间不持有日志控制锁或记录表锁。
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
                .ok_or(Error::InvalidFormat("记录越过逻辑页"))?
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
    // SAFETY: 测试布局只在独占许可下读写对齐的 Box；失败初始化自行清理，成功由 drop_value 清理。
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
                return Err(Error::Codec("资源编码损坏"));
            }
            Ok(false)
        }
        fn plan_decode(&self, bytes: &[u8]) -> Result<crate::schema::value::ValuePlan, Error> {
            if bytes != [0] {
                return Err(Error::Codec("资源编码损坏"));
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
            // SAFETY: 许可独占且尺寸对齐已检查，槽未初始化。
            unsafe { pointer.write(Box::new(Resource(self.drops.clone()))) };
            if fail {
                // SAFETY: 刚初始化的 Box 仍由本次调用独占；取走后槽恢复未初始化状态。
                drop(unsafe { pointer.read() });
                return Err(Error::Codec("部分初始化后失败"));
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
                return Err(Error::Codec("长度不符"));
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
                return Err(Error::Codec("编码损坏"));
            }
            self.initialize(p, false)
        }
        fn drop_value(&self, p: crate::schema::value::DropPermit<'_>) -> Result<(), Error> {
            self.destructors
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // SAFETY: 仅对成功初始化的 Box 调用，最后所有者持有销毁许可。
            unsafe { std::ptr::drop_in_place(p.as_ptr().cast::<Box<Resource>>().as_ptr()) };
            Ok(())
        }
    }
    #[test]
    fn 原始记录槽放弃不销毁未初始化值且成功初始化仅销毁一次() {
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
    fn 临时解码不占日志页且资源只销毁一次() {
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
    fn 冷恢复日志边界一致且拒绝无效尾部() {
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
    fn 检查点填充尾页不创建记录且后续分配不回填() {
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
    fn 活跃预留拒绝尾页填充且不改变分配边界() {
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
    fn 冻结页编码覆盖对齐间隙和已放弃预留() {
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
        // 第四条占槽进入第二页，第一页尾部保留零填充。
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
    fn 未发布预留阻止冻结且放弃后可以推进() {
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
    fn 正在更新时冻结不推进安全边界且重试可完成() {
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
    fn 部分初始化失败自行清理且成功值仅销毁一次() {
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
    fn 预留不可见且放弃不留下地址记录() {
        let log = log();
        let reservation = log.reserve(7).unwrap();
        let address = reservation.address().unwrap();
        assert!(log.lease(address).is_err());
        log.abandon(reservation).unwrap();
        assert!(log.lease(address).is_err());
        log.release_page(PageId(0), Generation(0)).unwrap();
    }
    #[test]
    fn 发布后可租用且旧租约阻止销毁() {
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
    fn 租约比日志存活更久但预留不能跨日志发布() {
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
