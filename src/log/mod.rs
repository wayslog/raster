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
/// 共享引擎的运行期边界；Frontiers 是读取后的逻辑快照。
pub(crate) struct AtomicFrontiers {
    begin: crate::sync::AtomicU64,
    head: crate::sync::AtomicU64,
    safe_head: crate::sync::AtomicU64,
    read_only: crate::sync::AtomicU64,
    safe_read_only: crate::sync::AtomicU64,
    flushed_until: crate::sync::AtomicU64,
    tail: crate::sync::AtomicU64,
}
pub(crate) struct PageState {
    pub id: PageId,
    pub generation: Generation,
    pub frozen: bool,
    pub closed: bool,
    pub flushed: bool,
    pub readers: usize,
    pub io_references: usize,
}
/// 已完成值初始化但未进入地址表；丢弃即放弃，不发布半记录。
pub(crate) struct RecordReservation<'a, V: ValueLayout> {
    owner: &'a HybridLog<V>,
    value: value::PageValue<V>,
}
impl<V: ValueLayout> RecordReservation<'_, V> {
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
    pub fn read<R>(&self, f: impl for<'a> FnOnce(V::Read<'a>) -> R) -> Result<R, Error> {
        self.value.read(f)
    }
    pub fn update<R>(
        &self,
        f: impl for<'a> FnOnce(V::Update<'a>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        self.value.update(f)
    }
    pub fn generation(&self) -> Generation {
        self.value.generation()
    }
}
pub(crate) struct HybridLog<V: ValueLayout> {
    pool: page::PagePool,
    layout: Arc<V>,
    records: crate::sync::Mutex<BTreeMap<LogAddress, Arc<value::PageValue<V>>>>,
}
impl<V: ValueLayout> HybridLog<V> {
    pub fn new(config: LogConfig, layout: Arc<V>) -> Result<Self, Error> {
        Ok(Self {
            pool: page::PagePool::new(config.page_bytes, config.memory_pages)?,
            layout,
            records: crate::sync::Mutex::new(BTreeMap::new()),
        })
    }
    pub fn reserve(&self, value: V::Owned) -> Result<RecordReservation<'_, V>, Error> {
        Ok(RecordReservation {
            owner: self,
            value: value::PageValue::initialize(&self.pool, self.layout.clone(), value)?,
        })
    }
    pub fn finish_initialization(
        &self,
        reservation: RecordReservation<'_, V>,
    ) -> Result<LogAddress, Error> {
        if !std::ptr::eq(self, reservation.owner) {
            return Err(Error::InvalidState("预留属于其他日志"));
        }
        let address = reservation.value.address()?;
        let mut records = self
            .records
            .lock()
            .map_err(|_| Error::InvalidState("记录表锁中毒"))?;
        if records.contains_key(&address) {
            return Err(Error::InvalidState("记录地址重复发布"));
        }
        records.insert(address, Arc::new(reservation.value));
        Ok(address)
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
    pub fn release_page(&self, page: PageId, generation: Generation) -> Result<(), Error> {
        self.pool.release(page, generation)
    }
    pub fn advance_read_only(&self, _target: LogAddress) -> Result<(), Error> {
        Err(Error::unimplemented("log::advance"))
    }
    pub fn flush_step(&self, _budget: PollBudget) -> Result<Progress, Error> {
        Err(Error::unimplemented("log::flush"))
    }
}
mod gate;
mod page;
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
