//! 会话登记、双版本上下文与顶层动作仲裁；与安全回收 epoch 分开。
use crate::types::*;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Action {
    CheckpointFull,
    CheckpointIndex,
    CheckpointLog,
    Recover,
    Gc,
    GrowIndex,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum Phase {
    Rest,
    PrepareIndex,
    IndexSnapshot,
    Prepare,
    InProgress,
    WaitPending,
    WaitFlush,
    Publish,
    GcIo,
    GcIndex,
    GrowPrepare,
    GrowCopy,
    Failed,
}
pub(crate) struct SystemState {
    pub action: Option<Action>,
    pub phase: Phase,
    pub version: CheckpointVersion,
}
pub(crate) struct SessionCut {
    pub session: SessionId,
    pub last_accepted: Option<Serial>,
    pub old_pending: usize,
}
struct Registration {
    active: bool,
    last_accepted: Option<Serial>,
}
struct Registry {
    sessions: BTreeMap<SessionId, Registration>,
    closed: bool,
}
pub(crate) struct Coordinator {
    state: crate::sync::Mutex<SystemState>,
    registry: crate::sync::Mutex<Registry>,
    max_sessions: usize,
}
impl Coordinator {
    pub fn new(max_sessions: usize) -> Result<Self, Error> {
        if max_sessions == 0 {
            return Err(Error::InvalidConfig {
                field: "session.max_sessions",
                reason: "会话容量须非零",
            });
        }
        Ok(Self {
            state: crate::sync::Mutex::new(SystemState {
                action: None,
                phase: Phase::Rest,
                version: CheckpointVersion(0),
            }),
            registry: crate::sync::Mutex::new(Registry {
                sessions: BTreeMap::new(),
                closed: false,
            }),
            max_sessions,
        })
    }
    pub fn last_accepted(&self, id: SessionId) -> Result<Option<Serial>, Error> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        let entry = registry
            .sessions
            .get(&id)
            .filter(|e| e.active)
            .ok_or(Error::InvalidState("会话未注册"))?;
        Ok(entry.last_accepted)
    }
    /// 只在其他拒绝条件已检查后调用；拒绝保持原序号。
    pub fn accept_serial(&self, id: SessionId, serial: Serial) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.closed {
            return Err(Error::InvalidState("存储已关闭"));
        }
        let entry = registry
            .sessions
            .get_mut(&id)
            .filter(|e| e.active)
            .ok_or(Error::InvalidState("会话未注册"))?;
        if entry.last_accepted.is_some_and(|last| serial <= last) {
            return Err(Error::InvalidState("操作序号必须严格递增"));
        }
        entry.last_accepted = Some(serial);
        Ok(())
    }
    pub fn shutdown(&self) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.sessions.values().any(|e| e.active) {
            return Err(Error::Busy);
        }
        registry.closed = true;
        Ok(())
    }

    pub fn start_action(&self, _action: Action) -> Result<MaintenanceId, Error> {
        Err(Error::unimplemented("coordination::start_action"))
    }
    pub fn enroll(&self, session: SessionId) -> Result<(), Error> {
        session.validate()?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.closed {
            return Err(Error::InvalidState("存储已关闭"));
        }
        if registry
            .sessions
            .get(&session)
            .is_some_and(|entry| entry.active)
        {
            return Err(Error::Busy);
        }
        if registry.sessions.values().filter(|e| e.active).count() >= self.max_sessions {
            return Err(Error::CapacityExceeded);
        }
        registry
            .sessions
            .entry(session)
            .and_modify(|entry| entry.active = true)
            .or_insert(Registration {
                active: true,
                last_accepted: None,
            });
        Ok(())
    }
    pub fn acknowledge(&self, _cut: SessionCut, _phase: Phase) -> Result<(), Error> {
        Err(Error::unimplemented("coordination::acknowledge"))
    }
    pub fn fail_action(&self, _cause: Error) -> Result<(), Error> {
        Err(Error::unimplemented("coordination::fail"))
    }
    pub fn leave(&self, session: SessionId) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        let entry = registry
            .sessions
            .get_mut(&session)
            .filter(|e| e.active)
            .ok_or(Error::InvalidState("会话未注册"))?;
        entry.active = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 重复注册容量和关闭拒绝不改变状态() {
        let c = Coordinator::new(1).unwrap();
        let a = SessionId([1; 16]);
        let b = SessionId([2; 16]);
        c.enroll(a).unwrap();
        assert!(matches!(c.enroll(a), Err(Error::Busy)));
        assert!(matches!(c.enroll(b), Err(Error::CapacityExceeded)));
        assert!(matches!(c.shutdown(), Err(Error::Busy)));
        c.accept_serial(a, Serial(7)).unwrap();
        c.leave(a).unwrap();
        c.enroll(b).unwrap();
        c.leave(b).unwrap();
        c.shutdown().unwrap();
        c.shutdown().unwrap();
        assert!(c.enroll(a).is_err());
    }
    #[test]
    fn 序号可跳号但不能回退且重新注册保留进度() {
        let c = Coordinator::new(2).unwrap();
        let a = SessionId([1; 16]);
        let b = SessionId([2; 16]);
        c.enroll(a).unwrap();
        c.enroll(b).unwrap();
        for n in [0, 7, 19] {
            c.accept_serial(a, Serial(n)).unwrap();
        }
        for n in [0, 18, 19] {
            assert!(c.accept_serial(a, Serial(n)).is_err());
            assert_eq!(c.last_accepted(a).unwrap(), Some(Serial(19)));
        }
        assert_eq!(c.last_accepted(b).unwrap(), None);
        c.leave(a).unwrap();
        assert!(c.accept_serial(a, Serial(20)).is_err());
        c.enroll(a).unwrap();
        assert_eq!(c.last_accepted(a).unwrap(), Some(Serial(19)));
        c.accept_serial(a, Serial(20)).unwrap();
    }
    #[test]
    fn 注册与关闭竞争只有一致的终态() {
        for _ in 0..32 {
            let c = Coordinator::new(1).unwrap();
            let barrier = std::sync::Barrier::new(2);
            let id = SessionId([1; 16]);
            let (enroll, shutdown) = std::thread::scope(|s| {
                let a = s.spawn(|| {
                    barrier.wait();
                    c.enroll(id)
                });
                let b = s.spawn(|| {
                    barrier.wait();
                    c.shutdown()
                });
                (a.join().unwrap(), b.join().unwrap())
            });
            assert_ne!(enroll.is_ok(), shutdown.is_ok());
            if enroll.is_ok() {
                c.leave(id).unwrap();
                c.shutdown().unwrap();
            }
            assert!(c.enroll(id).is_err());
        }
    }
}
