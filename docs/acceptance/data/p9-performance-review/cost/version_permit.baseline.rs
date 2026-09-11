//! 挂起期间保留逻辑版本登记，不保留记录租约或用户上下文引用。
use crate::types::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
#[derive(Default)]
pub(crate) struct VersionPermits {
    active: Arc<Mutex<BTreeMap<(u64, u64), usize>>>,
}
pub(crate) struct VersionPermit {
    active: Arc<Mutex<BTreeMap<(u64, u64), usize>>>,
    key: (u64, u64),
}
impl VersionPermits {
    pub fn reserve(
        &self,
        hash: KeyHash,
        version: CheckpointVersion,
    ) -> Result<VersionPermit, Error> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| Error::InvalidState("请求版本登记锁中毒"))?;
        // 阶段观察落后的调用不能插到已登记的新版本之前；拒绝发生在接受序号前。
        if active
            .range((hash.0, version.0)..=(hash.0, u64::MAX))
            .any(|(&(h, v), _)| h == hash.0 && v > version.0)
        {
            return Err(Error::Busy);
        }
        let key = (hash.0, version.0);
        let count = active
            .get(&key)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        active.insert(key, count);
        Ok(VersionPermit {
            active: self.active.clone(),
            key,
        })
    }
}
impl VersionPermit {
    pub fn ready(&self) -> Result<bool, Error> {
        let active = self
            .active
            .lock()
            .map_err(|_| Error::InvalidState("请求版本登记锁中毒"))?;
        Ok(active.range((self.key.0, 0)..self.key).next().is_none())
    }
}
impl Drop for VersionPermit {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock()
            && let Some(count) = active.get_mut(&self.key)
        {
            *count -= 1;
            if *count == 0 {
                active.remove(&self.key);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 旧版本全部退出后新版本才可执行且不同键互不阻塞() {
        let permits = VersionPermits::default();
        let old = permits.reserve(KeyHash(1), CheckpointVersion(0)).unwrap();
        let same = permits.reserve(KeyHash(1), CheckpointVersion(0)).unwrap();
        let new = permits.reserve(KeyHash(1), CheckpointVersion(1)).unwrap();
        let other = permits.reserve(KeyHash(2), CheckpointVersion(1)).unwrap();
        assert!(old.ready().unwrap());
        assert!(same.ready().unwrap());
        assert!(other.ready().unwrap());
        assert!(!new.ready().unwrap());
        assert!(matches!(
            permits.reserve(KeyHash(1), CheckpointVersion(0)),
            Err(Error::Busy)
        ));
        drop(old);
        assert!(!new.ready().unwrap());
        drop(same);
        assert!(new.ready().unwrap());
        drop(new);
        drop(other);
        assert!(permits.active.lock().unwrap().is_empty());
    }
}
