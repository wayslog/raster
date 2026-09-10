//! 页内值所有权：仅初始化成功才返回对象，所有视图在仲裁许可作用域内使用。
use super::{
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
    tombstone: bool,
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
    pub fn initialize(pool: &PagePool, layout: Arc<V>, value: V::Owned) -> Result<Self, Error> {
        let plan = layout.plan(&value)?.validate()?;
        let range = pool.reserve(plan.capacity.max(1), plan.alignment)?;
        let mut owner = Self {
            layout,
            range: Some(range),
            initialized: false,
            tombstone: false,
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
            tombstone: false,
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
        if self.initialized || self.tombstone {
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
            tombstone: true,
            value_offset: prefix,
            capacity: 0,
            key_len: key.len(),
            previous,
            version: CheckpointVersion(0),
            gate: MutationGate::default(),
            failed: AtomicBool::new(false),
            sealed: AtomicBool::new(true),
        })
    }
    pub fn is_tombstone(&self) -> bool {
        self.tombstone
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
            tombstone: false,
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
    pub fn generation(&self) -> Generation {
        self.range.as_ref().expect("值范围存在").generation()
    }
    pub fn address(&self) -> Result<LogAddress, Error> {
        self.range.as_ref().expect("值范围存在").address()
    }
    fn ready(&self) -> Result<(), Error> {
        if self.tombstone {
            return Err(Error::InvalidState("墓碑不含活跃值"));
        }
        if self.failed.load(Ordering::SeqCst) {
            Err(Error::InvalidState("值访问已失败关闭"))
        } else {
            Ok(())
        }
    }
    pub fn read<R>(&self, f: impl for<'a> FnOnce(V::Read<'a>) -> R) -> Result<R, Error> {
        self.ready()?;
        let _gate = self.gate.try_replace()?;
        self.ready()?;
        Ok(f(self.layout.read(permit!(self, ReadPermit))?))
    }
    pub fn update<R>(
        &self,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        self.update_if_mutable(f)?
            .ok_or(Error::InvalidState("记录已停止更新"))
    }
    /// None 表示在用户回调执行前已冻结；检查与 seal 使用同一个仲裁门。
    pub fn update_if_mutable<R>(
        &self,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<Option<R>, Error> {
        self.ready()?;
        if self.sealed.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let _shared;
        let _exclusive;
        if self.layout.concurrent_updates() {
            _shared = Some(self.gate.try_update()?);
            _exclusive = None;
        } else {
            _exclusive = Some(self.gate.try_replace()?);
            _shared = None;
        }
        self.ready()?;
        if self.sealed.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            f(self.layout.update(permit!(self, UpdatePermit))?)
        }));
        match result {
            Ok(value) => value.map(Some),
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
    ) -> Result<Option<R>, Error> {
        if self.version != version {
            return Ok(None);
        }
        self.update_if_mutable(f)
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
        use crate::format::{HEADER_BYTES, Record, RecordHeader};
        let _gate = self.gate.try_replace()?;
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
        let value_len = if self.tombstone {
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
        if !self.tombstone {
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
                tombstone: self.tombstone,
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
