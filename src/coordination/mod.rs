//! 会话登记、双版本上下文与顶层动作仲裁；与安全回收 epoch 分开。
use crate::types::*;
use std::collections::BTreeMap;
mod action;
use action::ActiveAction;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    CheckpointFull,
    CheckpointIndex,
    CheckpointLog,
    Recover,
    Gc,
    GrowIndex,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SystemState {
    pub id: Option<MaintenanceId>,
    pub action: Option<Action>,
    pub phase: Phase,
    pub version: CheckpointVersion,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SessionCut {
    pub session: SessionId,
    pub last_accepted: Option<Serial>,
    pub old_pending: usize,
}
struct Registration {
    active: bool,
    last_accepted: Option<Serial>,
    recovered: Option<(Serial, CheckpointVersion)>,
}
struct Registry {
    system: SystemState,
    action: Option<ActiveAction>,
    next_action: u64,
    sessions: BTreeMap<SessionId, Registration>,
    closed: bool,
}
pub(crate) struct Coordinator {
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
            registry: crate::sync::Mutex::new(Registry {
                system: SystemState {
                    id: None,
                    action: None,
                    phase: Phase::Rest,
                    version: CheckpointVersion(0),
                },
                action: None,
                next_action: 0,
                sessions: BTreeMap::new(),
                closed: false,
            }),
            max_sessions,
        })
    }
    pub fn from_checkpoint(
        max_sessions: usize,
        version: CheckpointVersion,
        sessions: &[(SessionId, Serial)],
    ) -> Result<Self, Error> {
        let mut coordinator = Self::new(max_sessions)?;
        let registry = coordinator
            .registry
            .get_mut()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        registry.system.version =
            CheckpointVersion(version.0.checked_add(1).ok_or(Error::CapacityExceeded)?);
        for &(id, serial) in sessions {
            id.validate()?;
            if registry
                .sessions
                .insert(
                    id,
                    Registration {
                        active: false,
                        last_accepted: Some(serial),
                        recovered: Some((serial, version)),
                    },
                )
                .is_some()
            {
                return Err(Error::InvalidFormat("恢复会话重复"));
            }
        }
        Ok(coordinator)
    }
    pub fn resume(
        &self,
        session: SessionId,
    ) -> Result<(CheckpointVersion, Serial, CheckpointVersion), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.closed || registry.system.phase == Phase::Failed {
            return Err(Error::InvalidState("存储已关闭或失败"));
        }
        if registry.action.is_some() {
            return Err(Error::Busy);
        }
        if registry
            .sessions
            .values()
            .filter(|entry| entry.active)
            .count()
            >= self.max_sessions
        {
            return Err(Error::CapacityExceeded);
        }
        let version = registry.system.version;
        let entry = registry
            .sessions
            .get_mut(&session)
            .ok_or(Error::InvalidState("没有该会话的恢复进度"))?;
        if entry.active {
            return Err(Error::Busy);
        }
        let (serial, durable_version) = entry
            .recovered
            .ok_or(Error::InvalidState("会话不属于本次恢复集合"))?;
        entry.active = true;
        Ok((version, serial, durable_version))
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
    pub fn accept_serial(
        &self,
        id: SessionId,
        serial: Serial,
        version: CheckpointVersion,
    ) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.closed || registry.system.phase == Phase::Failed {
            return Err(Error::InvalidState("存储已关闭或协调动作失败"));
        }
        if version != registry.system.version {
            return Err(Error::Busy);
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
        if registry.action.is_some() && registry.system.phase != Phase::Failed {
            return Err(Error::Busy);
        }
        registry.closed = true;
        Ok(())
    }

    pub fn enroll(&self, session: SessionId) -> Result<CheckpointVersion, Error> {
        session.validate()?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.closed || registry.system.phase == Phase::Failed {
            return Err(Error::InvalidState("存储已关闭或协调动作失败"));
        }
        if registry.action.is_some() {
            return Err(Error::Busy);
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
        if registry
            .sessions
            .get(&session)
            .is_some_and(|entry| entry.recovered.is_some())
        {
            return Err(Error::InvalidState(
                "持久会话必须通过 continue_session 恢复",
            ));
        }
        registry
            .sessions
            .entry(session)
            .and_modify(|entry| entry.active = true)
            .or_insert(Registration {
                active: true,
                last_accepted: None,
                recovered: None,
            });
        Ok(registry.system.version)
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
        if let Some(action) = &mut registry.action
            && action.participants.contains_key(&session)
        {
            action
                .failure
                .get_or_insert_with(|| std::sync::Arc::new(Error::SessionAbandoned));
            registry.system.phase = Phase::Failed;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 恢复进度注册遵守容量且拒绝重复身份与版本耗尽() {
        let a = SessionId([1; 16]);
        let b = SessionId([2; 16]);
        assert!(Coordinator::from_checkpoint(1, CheckpointVersion(u64::MAX), &[]).is_err());
        assert!(
            Coordinator::from_checkpoint(
                1,
                CheckpointVersion(3),
                &[(a, Serial(7)), (a, Serial(9))]
            )
            .is_err()
        );
        let c = Coordinator::from_checkpoint(
            1,
            CheckpointVersion(3),
            &[(a, Serial(7)), (b, Serial(19))],
        )
        .unwrap();
        assert!(c.enroll(a).is_err());
        assert_eq!(
            c.resume(a).unwrap(),
            (CheckpointVersion(4), Serial(7), CheckpointVersion(3))
        );
        assert!(c.resume(b).is_err());
        assert!(c.accept_serial(a, Serial(7), CheckpointVersion(4)).is_err());
        c.accept_serial(a, Serial(11), CheckpointVersion(4))
            .unwrap();
        c.leave(a).unwrap();
        assert_eq!(
            c.resume(b).unwrap(),
            (CheckpointVersion(4), Serial(19), CheckpointVersion(3))
        );
    }
    #[test]
    fn 重复注册容量和关闭拒绝不改变状态() {
        let c = Coordinator::new(1).unwrap();
        let a = SessionId([1; 16]);
        let b = SessionId([2; 16]);
        c.enroll(a).unwrap();
        assert!(matches!(c.enroll(a), Err(Error::Busy)));
        assert!(matches!(c.enroll(b), Err(Error::CapacityExceeded)));
        assert!(matches!(c.shutdown(), Err(Error::Busy)));
        c.accept_serial(a, Serial(7), CheckpointVersion(0)).unwrap();
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
            c.accept_serial(a, Serial(n), CheckpointVersion(0)).unwrap();
        }
        for n in [0, 18, 19] {
            assert!(c.accept_serial(a, Serial(n), CheckpointVersion(0)).is_err());
            assert_eq!(c.last_accepted(a).unwrap(), Some(Serial(19)));
        }
        assert_eq!(c.last_accepted(b).unwrap(), None);
        c.leave(a).unwrap();
        assert!(
            c.accept_serial(a, Serial(20), CheckpointVersion(0))
                .is_err()
        );
        c.enroll(a).unwrap();
        assert_eq!(c.last_accepted(a).unwrap(), Some(Serial(19)));
        c.accept_serial(a, Serial(20), CheckpointVersion(0))
            .unwrap();
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
