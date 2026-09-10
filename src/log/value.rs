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

pub(crate) struct PageValue<V: ValueLayout> {
    layout: Arc<V>,
    range: Option<PageRange>,
    initialized: bool,
    gate: MutationGate,
    failed: AtomicBool,
    sealed: AtomicBool,
}
macro_rules! permit {
    ($owner:expr,$name:ident) => {{
        let range = $owner.range.as_ref().expect("值范围存在");
        $name {
            pointer: range.pointer(),
            length: range.len(),
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
            gate: MutationGate::default(),
            failed: AtomicBool::new(false),
            sealed: AtomicBool::new(false),
        };
        owner.layout.initialize(permit!(owner, InitPermit), value)?;
        owner.initialized = true;
        Ok(owner)
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
        self.ready()?;
        if self.sealed.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("记录已停止更新"));
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
            return Err(Error::InvalidState("记录已停止更新"));
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            f(self.layout.update(permit!(self, UpdatePermit))?)
        }));
        match result {
            Ok(value) => value,
            Err(panic) => {
                self.failed.store(true, Ordering::SeqCst);
                resume_unwind(panic)
            }
        }
    }
    pub fn seal(&self) -> Result<(), Error> {
        let _gate = self.gate.try_replace()?;
        self.sealed.store(true, Ordering::SeqCst);
        Ok(())
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
