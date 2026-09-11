//! 访问安全 epoch；全部状态转换在同一控制锁下线性化，不执行回收回调。
use crate::{sync::Mutex, types::*};
use std::{marker::PhantomData, rc::Rc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParticipantId {
    owner: u64,
    pub slot: usize,
    pub generation: Generation,
}
pub(crate) struct EpochGuard<'a> {
    manager: &'a EpochManager,
    participant: ParticipantId,
    local: PhantomData<Rc<()>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeferredAction {
    ReleaseIndex(Generation),
}
struct Slot {
    generation: Generation,
    registered: bool,
    active: usize,
    epoch: EpochVersion,
}
struct State {
    current: EpochVersion,
    slots: Vec<Slot>,
    deferred: Vec<(EpochVersion, DeferredAction)>,
}
static NEXT_MANAGER: crate::sync::AtomicU64 = crate::sync::AtomicU64::new(0);
pub(crate) struct EpochManager {
    owner: u64,
    state: Mutex<State>,
}
impl EpochManager {
    pub fn new() -> Result<Self, Error> {
        let owner = NEXT_MANAGER
            .fetch_update(
                crate::sync::PUBLISH_ORDER,
                crate::sync::PUBLISH_ORDER,
                |n| n.checked_add(1),
            )
            .map_err(|_| Error::CapacityExceeded)?;
        Ok(Self {
            owner,
            state: Mutex::new(State {
                current: EpochVersion(0),
                slots: Vec::new(),
                deferred: Vec::new(),
            }),
        })
    }
}
impl EpochManager {
    fn lock(&self) -> Result<crate::sync::MutexGuard<'_, State>, Error> {
        self.state
            .lock()
            .map_err(|_| Error::InvalidState("epoch 控制锁中毒"))
    }
    pub fn register(&self) -> Result<ParticipantId, Error> {
        let mut state = self.lock()?;
        for (slot, entry) in state.slots.iter_mut().enumerate() {
            if !entry.registered && entry.generation.0 < u64::MAX {
                entry.generation.0 += 1;
                entry.registered = true;
                return Ok(ParticipantId {
                    owner: self.owner,
                    slot,
                    generation: entry.generation,
                });
            }
        }
        state.slots.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        let id = ParticipantId {
            owner: self.owner,
            slot: state.slots.len(),
            generation: Generation(0),
        };
        state.slots.push(Slot {
            generation: id.generation,
            registered: true,
            active: 0,
            epoch: EpochVersion(0),
        });
        Ok(id)
    }
    pub fn enter(&self, id: ParticipantId) -> Result<EpochGuard<'_>, Error> {
        if id.owner != self.owner {
            return Err(Error::InvalidState("参与者属于其他管理器"));
        }
        let mut state = self.lock()?;
        let current = state.current;
        let slot = slot(&mut state, id)?;
        let active = slot.active.checked_add(1).ok_or(Error::CapacityExceeded)?;
        if slot.active == 0 {
            slot.epoch = current;
        }
        slot.active = active;
        Ok(EpochGuard {
            manager: self,
            participant: id,
            local: PhantomData,
        })
    }
    /// 调用者先从可见结构摘除对象，再排队；不允许自行传入过旧 epoch。
    pub fn defer(&self, action: DeferredAction) -> Result<(), Error> {
        let mut state = self.lock()?;
        let current = state.current;
        state
            .deferred
            .try_reserve(1)
            .map_err(|_| Error::OutOfMemory)?;
        state.deferred.push((current, action));
        Ok(())
    }
    pub fn advance(&self) -> Result<EpochVersion, Error> {
        let mut state = self.lock()?;
        state.current = EpochVersion(
            state
                .current
                .0
                .checked_add(1)
                .ok_or(Error::CapacityExceeded)?,
        );
        Ok(state.current)
    }
    /// 只交付已越过安全边界的动作，实际销毁在控制锁外执行。
    pub fn collect(&self) -> Result<Vec<DeferredAction>, Error> {
        let mut state = self.lock()?;
        let oldest = state
            .slots
            .iter()
            .filter(|s| s.active != 0)
            .map(|s| s.epoch)
            .min_by_key(|e| e.0);
        let ready = |epoch: EpochVersion| oldest.is_none_or(|e| epoch.0 < e.0);
        let mut actions = Vec::new();
        actions
            .try_reserve_exact(state.deferred.iter().filter(|(e, _)| ready(*e)).count())
            .map_err(|_| Error::OutOfMemory)?;
        state.deferred.retain(|(epoch, action)| {
            if ready(*epoch) {
                actions.push(*action);
                false
            } else {
                true
            }
        });
        Ok(actions)
    }
    pub fn unregister(&self, id: ParticipantId) -> Result<(), Error> {
        if id.owner != self.owner {
            return Err(Error::InvalidState("参与者属于其他管理器"));
        }
        let mut state = self.lock()?;
        let slot = slot(&mut state, id)?;
        if slot.active != 0 {
            return Err(Error::Busy);
        }
        slot.registered = false;
        Ok(())
    }
}
fn slot(state: &mut State, id: ParticipantId) -> Result<&mut Slot, Error> {
    let slot = state
        .slots
        .get_mut(id.slot)
        .ok_or(Error::InvalidState("参与者槽不存在"))?;
    if !slot.registered || slot.generation != id.generation {
        return Err(Error::InvalidState("参与者代次失效"));
    }
    Ok(slot)
}
impl Drop for EpochGuard<'_> {
    fn drop(&mut self) {
        // 中毒时保守保留活动计数，所有后续管理操作拒绝，不冒险回收。
        if let Ok(mut state) = self.manager.state.lock()
            && let Ok(slot) = slot(&mut state, self.participant)
        {
            slot.active -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const ACTION: DeferredAction = DeferredAction::ReleaseIndex(Generation(4));
    #[test]
    fn 管理器身份隔离且真实线程退出后才交付动作() {
        let manager = EpochManager::new().unwrap();
        let other = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        let _other_id = other.register().unwrap();
        assert!(other.enter(id).is_err());
        assert!(other.unregister(id).is_err());
        let entered = std::sync::Barrier::new(2);
        let leave = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let guard = manager.enter(id).unwrap();
                entered.wait();
                leave.wait();
                drop(guard);
            });
            entered.wait();
            manager.defer(ACTION).unwrap();
            manager.advance().unwrap();
            assert!(manager.collect().unwrap().is_empty());
            leave.wait();
        });
        assert_eq!(manager.collect().unwrap(), vec![ACTION]);
        manager.unregister(id).unwrap();
    }

    #[test]
    fn 最慢读者和嵌套许可退出前不回收() {
        let manager = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        let first = manager.enter(id).unwrap();
        manager.advance().unwrap();
        let second = manager.enter(id).unwrap();
        manager.defer(ACTION).unwrap();
        manager.advance().unwrap();
        assert!(manager.collect().unwrap().is_empty());
        drop(first);
        assert!(manager.collect().unwrap().is_empty());
        assert!(manager.unregister(id).is_err());
        drop(second);
        assert_eq!(manager.collect().unwrap(), vec![ACTION]);
        assert!(manager.collect().unwrap().is_empty());
        manager.unregister(id).unwrap();
    }
    #[test]
    fn 新读者不阻碍旧对象且旧槽不能复用() {
        let manager = EpochManager::new().unwrap();
        let old = manager.register().unwrap();
        manager.unregister(old).unwrap();
        let new = manager.register().unwrap();
        assert_eq!(old.slot, new.slot);
        assert_ne!(old.generation, new.generation);
        assert!(manager.enter(old).is_err());
        assert!(manager.unregister(old).is_err());
        manager.defer(ACTION).unwrap();
        manager.advance().unwrap();
        let guard = manager.enter(new).unwrap();
        assert_eq!(manager.collect().unwrap(), vec![ACTION]);
        drop(guard);
    }
    #[test]
    fn 遗忘许可只阻碍回收且代次不回绕() {
        let manager = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        std::mem::forget(manager.enter(id).unwrap());
        manager.defer(ACTION).unwrap();
        manager.advance().unwrap();
        assert!(manager.collect().unwrap().is_empty());
        assert!(manager.unregister(id).is_err());
        let other = manager.register().unwrap();
        manager.unregister(other).unwrap();
        manager.lock().unwrap().slots[other.slot].generation = Generation(u64::MAX);
        assert_ne!(manager.register().unwrap().slot, other.slot);
        manager.lock().unwrap().current = EpochVersion(u64::MAX);
        assert!(manager.advance().is_err());
        assert!(manager.collect().unwrap().is_empty());
    }
    #[test]
    fn 穷举两个参与者与回收者的线性化交错() {
        // 每个转换持同一把锁；穷举三条操作流的全部交错，不模拟另一套 epoch 实现。
        fn schedules(prefix: &mut Vec<usize>, left: &mut [usize; 3], all: &mut Vec<Vec<usize>>) {
            if left.iter().all(|n| *n == 0) {
                all.push(prefix.clone());
                return;
            }
            for i in 0..3 {
                if left[i] != 0 {
                    left[i] -= 1;
                    prefix.push(i);
                    schedules(prefix, left, all);
                    prefix.pop();
                    left[i] += 1;
                }
            }
        }
        let mut all = Vec::new();
        schedules(&mut Vec::new(), &mut [2, 2, 3], &mut all);
        assert_eq!(all.len(), 210);
        for sequence in all {
            let manager = EpochManager::new().unwrap();
            let ids = [manager.register().unwrap(), manager.register().unwrap()];
            let mut guards = [None, None];
            let mut steps = [0; 3];
            let mut readers_at_retirement = [false; 2];
            let mut retired = false;
            let mut delivered = 0;
            for actor in sequence {
                if actor < 2 {
                    if steps[actor] == 0 {
                        guards[actor] = Some(manager.enter(ids[actor]).unwrap());
                    } else {
                        guards[actor].take();
                        readers_at_retirement[actor] = false;
                    }
                } else {
                    match steps[actor] {
                        0 => {
                            manager.defer(ACTION).unwrap();
                            retired = true;
                            readers_at_retirement = [guards[0].is_some(), guards[1].is_some()];
                        }
                        1 => {
                            manager.advance().unwrap();
                        }
                        _ => {}
                    }
                }
                steps[actor] += 1;
                let ready = manager.collect().unwrap();
                if !ready.is_empty() {
                    assert!(retired);
                    assert!(!readers_at_retirement.iter().any(|x| *x));
                    delivered += ready.len();
                }
            }
            delivered += manager.collect().unwrap().len();
            assert_eq!(delivered, 1);
        }
    }
}
