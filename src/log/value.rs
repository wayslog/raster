//! 页内值所有权：仅初始化成功才返回对象，所有视图在仲裁许可作用域内使用。
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

/// 磁盘值的独立只读活跃对象，生命周期与日志页池分离。
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
        let range = $owner.range.as_ref().expect("值范围存在");
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
            return Err(Error::InvalidFormat("前驱必须早于当前记录"));
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
            return Err(Error::InvalidState("值不能重复初始化"));
        }
        self.layout.initialize(permit!(self, InitPermit), value)?;
        self.initialized = true;
        Ok(self)
    }
    fn value_pointer(&self) -> std::ptr::NonNull<u8> {
        let pointer = self.range.as_ref().expect("值范围存在").pointer();
        // SAFETY: 构造时检查值偏移与容量在分配内，初始化后范围不移动或重叠。
        unsafe { std::ptr::NonNull::new_unchecked(pointer.as_ptr().add(self.value_offset)) }
    }
    pub fn key(&self) -> &[u8] {
        let offset = if self.value_offset == 0 { 0 } else { 48 };
        // SAFETY: 键在初始化前拷贝到独立前缀，发布后不可变，且与所有值许可范围不重叠。
        unsafe {
            std::slice::from_raw_parts(
                self.range
                    .as_ref()
                    .expect("值范围存在")
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
    /// 仅在记录发布前持有独占拥有权时设置，发布后版本不可变。
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
            return Err(Error::InvalidFormat("前驱必须早于墓碑"));
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
            return Err(Error::Codec("编码长度与计划不符"));
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
        self.range.as_ref().expect("值范围存在").generation()
    }
    pub fn address(&self) -> Result<LogAddress, Error> {
        self.range.as_ref().expect("值范围存在").address()
    }
    fn ready(&self) -> Result<(), Error> {
        if self.is_tombstone() {
            return Err(Error::InvalidState("墓碑不含活跃值"));
        }
        if self.failed.load(Ordering::SeqCst) {
            Err(Error::InvalidState("值访问已失败关闭"))
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
            ValueAccess::Ready(None) => Err(Error::InvalidState("墓碑不含活跃值")),
            ValueAccess::Contended => Ok(ValueAccess::Contended),
        }
    }
    /// 墓碑检查和用户值视图共享独占许可，避免并发删除被误报为布局错误。
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
    /// 发布与墓碑标记处于同一源许可内；争用、冻结和跨版本均不修改记录。
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
            return Err(Error::InvalidState("值访问已失败关闭"));
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
            .ok_or(Error::InvalidState("记录已停止更新"))
    }
    /// None 表示在用户回调执行前已冻结；检查与 seal 使用同一个仲裁门。
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
    /// 只有同版本操作可申请原地更新；不同版本在调用用户代码前返回 None。
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
    /// 冻结后的拥有型磁盘记录。值与内存布局分别编码，保持逻辑地址占槽不变。
    pub fn encode_record(&self, maximum_version: CheckpointVersion) -> Result<Vec<u8>, Error> {
        self.copy_record(Some(maximum_version))
    }
    /// 完整记录占槽长度，供扫描检查边界是否落在槽内。
    pub fn record_bytes(&self) -> usize {
        self.range.as_ref().expect("活跃记录持有分配").len()
    }
    /// 扫描短暂排除全部更新后复制编码，不修改 sealed 或日志边界。
    pub fn snapshot_record(&self) -> Result<Vec<u8>, Error> {
        self.copy_record(None)
    }
    fn copy_record(&self, maximum_version: Option<CheckpointVersion>) -> Result<Vec<u8>, Error> {
        let _gate = self.gate.try_replace()?;
        self.copy_record_locked(maximum_version)
    }
    /// 条件复制在源独占许可内取得当前字节并执行同步发布；闭包不能等待 I/O。
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
            return Err(Error::InvalidState("记录版本超过刷盘范围"));
        }
        if self.value_offset == 0
            || maximum_version.is_some() && !self.sealed.load(Ordering::SeqCst)
        {
            return Err(Error::InvalidState("刷盘需要已冻结的完整记录"));
        }
        let len = self.range.as_ref().expect("值范围存在").len();
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
                // 不确定是否完成销毁时保留分配，避免潜在外部资源仍引用已释放内存。
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
    fn 原地墓碑必须通过源许可版本冻结与发布判定() {
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
                value.tombstone_at_version(CheckpointVersion(3), || panic!("读许可尚未释放")),
                Ok(ValueAccess::Contended)
            ));
            release.wait();
        });
        assert!(matches!(
            value.tombstone_at_version(CheckpointVersion(4), || panic!("版本不符不能发布")),
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
                    value.tombstone_at_version(CheckpointVersion(3), || panic!("复制许可尚未释放")),
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
            value.try_read_live(|_| panic!("墓碑不得调用值回调")),
            Ok(ValueAccess::Ready(None))
        ));
        value.seal().unwrap();
        assert!(matches!(
            value.tombstone_at_version(CheckpointVersion(3), || panic!("冻结记录不得原地发布")),
            Ok(ValueAccess::Ready(None))
        ));
        let encoded = value.encode_record(CheckpointVersion(3)).unwrap();
        let record = crate::format::Record::decode(&encoded).unwrap();
        assert!(record.header.tombstone);
        assert!(record.value.is_empty());
        assert_eq!(record.header.version, CheckpointVersion(3));
    }
    #[test]
    fn 布局和操作返回繁忙错误不能伪装成许可争用() {
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
    fn 变长临时值按编码规划且拒绝超预算输入() {
        let layout = Arc::new(SerializedValue::new(ByteValueCodec));
        for bytes in [vec![], vec![0, 255, 128], vec![42; 17]] {
            let temporary = TemporaryValue::decode(layout.clone(), &bytes, 64).unwrap();
            assert_eq!(temporary.read(|v| v).unwrap(), bytes);
        }
        assert!(TemporaryValue::decode(layout, &[1; 65], 64).is_err());
        assert!(TemporaryValue::decode(Arc::new(AtomicU64Value), &[1; 7], 64).is_err());
    }
    #[test]
    fn 变长值缩短后按当前长度编码且保持占槽() {
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
    fn 原子值与墓碑稳定记录可被格式层解码() {
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
    fn 普通值初始化增长拒绝与稳定编码() {
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
    fn 原子布局跨线程更新与逻辑落盘() {
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
    fn 用户恐慌之后拒绝继续修改() {
        let pool = PagePool::new(256, 1).unwrap();
        let value = PageValue::initialize(&pool, Arc::new(AtomicU64Value), 0).unwrap();
        assert!(
            catch_unwind(AssertUnwindSafe(|| value.update::<()>(|v| {
                v.store(1, Ordering::SeqCst);
                panic!("修改后恐慌")
            })))
            .is_err()
        );
        assert!(value.read(|v| v).is_err());
        assert!(value.update(|_| Ok(())).is_err());
    }
    #[test]
    fn 损坏解码不返回可见值且范围可释放() {
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
