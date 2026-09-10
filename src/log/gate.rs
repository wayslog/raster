//! 原子更新可共享，替换和普通字节访问必须独占；所有操作只尝试一次状态转换。
use crate::{
    sync::{AtomicU64, PUBLISH_ORDER},
    types::Error,
};
use std::{marker::PhantomData, rc::Rc};
const EXCLUSIVE: u64 = u64::MAX;

pub(crate) struct MutationGate {
    state: AtomicU64,
}
impl Default for MutationGate {
    fn default() -> Self {
        Self {
            state: AtomicU64::new(0),
        }
    }
}
pub(crate) struct ReplacementPermit<'a> {
    gate: &'a MutationGate,
    local: PhantomData<Rc<()>>,
}
pub(crate) struct SharedUpdatePermit<'a> {
    gate: &'a MutationGate,
    local: PhantomData<Rc<()>>,
}
impl MutationGate {
    /// 只允许能安全并发访问的原子布局使用；不授予普通值的可变引用。
    pub fn try_update(&self) -> Result<SharedUpdatePermit<'_>, Error> {
        let state = self.state.load(PUBLISH_ORDER);
        if state >= EXCLUSIVE - 1 {
            return Err(Error::Busy);
        }
        self.state
            .compare_exchange(state, state + 1, PUBLISH_ORDER, PUBLISH_ORDER)
            .map_err(|_| Error::Busy)?;
        Ok(SharedUpdatePermit {
            gate: self,
            local: PhantomData,
        })
    }
    /// 不支持持共享许可升级；读取普通字节、写入普通值及替换均走此入口。
    pub fn try_replace(&self) -> Result<ReplacementPermit<'_>, Error> {
        self.state
            .compare_exchange(0, EXCLUSIVE, PUBLISH_ORDER, PUBLISH_ORDER)
            .map_err(|_| Error::Busy)?;
        Ok(ReplacementPermit {
            gate: self,
            local: PhantomData,
        })
    }
}
impl Drop for SharedUpdatePermit<'_> {
    fn drop(&mut self) {
        self.gate.state.fetch_sub(1, PUBLISH_ORDER);
    }
}
impl Drop for ReplacementPermit<'_> {
    fn drop(&mut self) {
        self.gate.state.store(0, PUBLISH_ORDER);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 共享更新互容但与替换互斥() {
        let gate = MutationGate::default();
        let a = gate.try_update().unwrap();
        let b = gate.try_update().unwrap();
        assert!(gate.try_replace().is_err());
        drop(a);
        assert!(gate.try_replace().is_err());
        drop(b);
        let exclusive = gate.try_replace().unwrap();
        assert!(gate.try_replace().is_err());
        assert!(gate.try_update().is_err());
        drop(exclusive);
        assert!(gate.try_replace().is_ok());
    }
    #[test]
    fn 恐慌展开释放许可且遗忘不会开放仲裁() {
        let gate = MutationGate::default();
        let result = std::panic::catch_unwind(|| {
            let _p = gate.try_replace().unwrap();
            panic!("模拟用户恐慌");
        });
        assert!(result.is_err());
        assert!(gate.try_update().is_ok());
        std::mem::forget(gate.try_update().unwrap());
        assert!(gate.try_replace().is_err());
        // 此处仅验证仲裁许可释放；业务错误影响与引擎失败关闭仍由 P3 实现。
    }
    #[test]
    fn 真实线程的共享更新与替换不能重叠() {
        let gate = MutationGate::default();
        let entered = std::sync::Barrier::new(2);
        let exit = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let _p = gate.try_update().unwrap();
                entered.wait();
                exit.wait();
            });
            entered.wait();
            assert!(gate.try_replace().is_err());
            let second = gate.try_update().unwrap();
            drop(second);
            exit.wait();
        });
        assert!(gate.try_replace().is_ok());
    }
    #[test]
    fn 更新计数上限不能变成独占标记() {
        let gate = MutationGate {
            state: AtomicU64::new(EXCLUSIVE - 1),
        };
        assert!(gate.try_update().is_err());
        assert_eq!(gate.state.load(PUBLISH_ORDER), EXCLUSIVE - 1);
    }
}
