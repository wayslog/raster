//! 动作状态和会话注册共用同一锁；阶段确认不等价于材料持久化。
use super::*;
use std::sync::Arc;
#[derive(Default)]
pub(super) struct Participant {
    departed: bool,
    acknowledged: Option<Phase>,
    cut: Option<SessionCut>,
}
pub(super) struct ActiveAction {
    id: MaintenanceId,
    pub participants: BTreeMap<SessionId, Participant>,
    pub failure: Option<Arc<Error>>,
    completed: Vec<SessionCut>,
}
fn barrier(phase: Phase) -> bool {
    matches!(
        phase,
        Phase::PrepareIndex
            | Phase::Prepare
            | Phase::InProgress
            | Phase::WaitPending
            | Phase::GrowPrepare
    )
}
fn next_phase(action: Action, phase: Phase) -> Option<Phase> {
    use Action::*;
    use Phase::*;
    match (action, phase) {
        (CheckpointFull | CheckpointIndex, PrepareIndex) => Some(IndexSnapshot),
        (CheckpointFull, IndexSnapshot) => Some(Prepare),
        (CheckpointIndex, IndexSnapshot) => Some(WaitFlush),
        (CheckpointFull | CheckpointLog, Prepare) => Some(InProgress),
        (CheckpointFull | CheckpointLog, InProgress) => Some(WaitPending),
        (CheckpointFull | CheckpointLog, WaitPending) => Some(WaitFlush),
        (CheckpointFull | CheckpointIndex | CheckpointLog | Recover, WaitFlush) => Some(Publish),
        (Gc, GcIo) => Some(GcIndex),
        (Gc, GcIndex) => Some(Publish),
        (GrowIndex, GrowPrepare) => Some(GrowCopy),
        (GrowIndex, GrowCopy) => Some(Publish),
        (Compact, Compacting) => Some(Publish),
        (ReleaseCheckpoint, ReclaimCheckpoint) => Some(Publish),
        _ => None,
    }
}
impl Coordinator {
    pub fn snapshot(&self) -> Result<SystemState, Error> {
        Ok(self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?
            .system)
    }
    pub fn start_action(&self, kind: Action) -> Result<MaintenanceId, Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.closed || registry.system.phase == Phase::Failed {
            return Err(Error::InvalidState("存储已关闭或协调动作失败"));
        }
        if registry.action.is_some()
            || kind == Action::Recover && registry.sessions.values().any(|s| s.active)
        {
            return Err(Error::Busy);
        }
        let next = registry
            .next_action
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        let id = MaintenanceId(registry.next_action);
        // 活跃但无业务的会话也必须参与；不得用其操作次数推断已退出。
        let participants = registry
            .sessions
            .iter()
            .filter(|(_, s)| s.active)
            .map(|(id, _)| (*id, Participant::default()))
            .collect();
        let completed = registry
            .sessions
            .iter()
            .filter(|(_, s)| !s.active)
            .map(|(id, s)| SessionCut {
                session: *id,
                last_accepted: s.last_accepted,
                old_pending: 0,
            })
            .collect();
        registry.action = Some(ActiveAction {
            id,
            participants,
            failure: None,
            completed,
        });
        registry.next_action = next;
        registry.system.id = Some(id);
        registry.system.action = Some(kind);
        registry.system.phase = match kind {
            Action::CheckpointFull | Action::CheckpointIndex => Phase::PrepareIndex,
            Action::CheckpointLog => Phase::Prepare,
            Action::Recover => Phase::WaitFlush,
            Action::Gc => Phase::GcIo,
            Action::GrowIndex => Phase::GrowPrepare,
            Action::Compact => Phase::Compacting,
            Action::ReleaseCheckpoint => Phase::ReclaimCheckpoint,
        };
        Ok(id)
    }
    /// 阶段和动作 ID 必须都匹配；重复确认只能保持同一切分，不能篡改旧序号。
    pub fn acknowledge(
        &self,
        id: MaintenanceId,
        cut: SessionCut,
        phase: Phase,
    ) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.system.phase != phase || !barrier(phase) {
            return Err(Error::InvalidState("确认阶段不匹配"));
        }
        let registered = registry
            .sessions
            .get(&cut.session)
            .filter(|s| s.active)
            .ok_or(Error::InvalidState("会话未注册"))?;
        if cut.last_accepted > registered.last_accepted {
            return Err(Error::InvalidState("会话切分超过已接受序号"));
        }
        if phase == Phase::WaitPending && cut.old_pending != 0 {
            return Err(Error::Busy);
        }
        let action = registry
            .action
            .as_mut()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("维护动作不匹配"))?;
        let participant = action
            .participants
            .get_mut(&cut.session)
            .ok_or(Error::InvalidState("会话不属于阶段参与集合"))?;
        if matches!(phase, Phase::InProgress | Phase::WaitPending) {
            if let Some(old) = participant.cut {
                if old.last_accepted != cut.last_accepted || cut.old_pending > old.old_pending {
                    return Err(Error::InvalidState("会话旧版本切分发生变化"));
                }
            } else if phase == Phase::WaitPending {
                return Err(Error::InvalidState("会话尚未登记版本切分"));
            }
            participant.cut = Some(cut);
        }
        participant.acknowledged = Some(phase);
        Ok(())
    }
    /// 驱动者完成该阶段实际工作后调用；本方法只验证状态与参与者屏障。
    pub fn advance(&self, id: MaintenanceId, expected: Phase) -> Result<SystemState, Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.system.phase != expected {
            return Err(Error::InvalidState("推进阶段不匹配"));
        }
        let action = registry
            .action
            .as_ref()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("维护动作不匹配"))?;
        if barrier(expected)
            && action
                .participants
                .values()
                .any(|p| !p.departed && p.acknowledged != Some(expected))
        {
            return Err(Error::Busy);
        }
        let next = next_phase(
            registry
                .system
                .action
                .ok_or(Error::InvalidState("无维护动作"))?,
            expected,
        )
        .ok_or(Error::InvalidState("阶段不能直接推进"))?;
        if next == Phase::InProgress {
            registry.system.version = CheckpointVersion(
                registry
                    .system
                    .version
                    .0
                    .checked_add(1)
                    .ok_or(Error::CapacityExceeded)?,
            );
        }
        registry.system.phase = next;
        Ok(registry.system)
    }
    /// Publish 的持久化结果由实际维护驱动者确认后，才能释放动作占用。
    pub fn finish_action(&self, id: MaintenanceId) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        if registry.system.phase != Phase::Publish
            || registry.action.as_ref().is_none_or(|a| a.id != id)
        {
            return Err(Error::InvalidState("维护动作尚未到发布阶段"));
        }
        registry.action = None;
        registry.system.id = None;
        registry.system.action = None;
        registry.system.phase = Phase::Rest;
        Ok(())
    }
    pub fn fail_action(&self, id: MaintenanceId, cause: Error) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        let action = registry
            .action
            .as_mut()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("维护动作不匹配"))?;
        action.failure.get_or_insert_with(|| Arc::new(cause));
        registry.system.phase = Phase::Failed;
        Ok(())
    }
    pub fn action_failure(&self, id: MaintenanceId) -> Result<Option<Arc<Error>>, Error> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        Ok(registry
            .action
            .as_ref()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("维护动作不匹配"))?
            .failure
            .clone())
    }
    /// 只有拥有会话的线程排空两个上下文后调用；退出参与者保留切分而不删除。
    pub fn leave_drained(
        &self,
        current: (CheckpointVersion, SessionCut),
        previous: Option<(CheckpointVersion, SessionCut)>,
    ) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        let session = current.1.session;
        if current.1.old_pending != 0
            || previous.is_some_and(|(_, cut)| cut.old_pending != 0 || cut.session != session)
        {
            return Err(Error::Busy);
        }
        let registered = registry
            .sessions
            .get(&session)
            .filter(|s| s.active)
            .ok_or(Error::InvalidState("会话未注册"))?;
        if current.1.last_accepted != registered.last_accepted {
            return Err(Error::InvalidState("关闭会话的已接受序号不匹配"));
        }
        let system = registry.system;
        if let Some(action) = &mut registry.action {
            let participant = action
                .participants
                .get_mut(&session)
                .ok_or(Error::InvalidState("会话不属于动作参与集合"))?;
            if action.failure.is_none() {
                let version = if matches!(
                    system.action,
                    Some(Action::CheckpointFull | Action::CheckpointLog)
                ) && matches!(
                    system.phase,
                    Phase::InProgress | Phase::WaitPending | Phase::WaitFlush | Phase::Publish
                ) {
                    CheckpointVersion(
                        system
                            .version
                            .0
                            .checked_sub(1)
                            .ok_or(Error::InvalidState("检查点旧版本不存在"))?,
                    )
                } else {
                    system.version
                };
                let cut = if current.0 == version {
                    current.1
                } else {
                    previous
                        .filter(|(v, _)| *v == version)
                        .ok_or(Error::InvalidState("关闭会话缺少旧版本切分"))?
                        .1
                };
                if participant
                    .cut
                    .is_some_and(|old| old.last_accepted != cut.last_accepted)
                {
                    return Err(Error::InvalidState("关闭会话改变已固定切分"));
                }
                participant.cut = Some(cut);
            }
            participant.departed = true;
        }
        registry
            .sessions
            .get_mut(&session)
            .expect("已验证会话存在")
            .active = false;
        Ok(())
    }
    pub fn cuts(&self, id: MaintenanceId) -> Result<Vec<SessionCut>, Error> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("会话注册表锁中毒"))?;
        let action = registry
            .action
            .as_ref()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("维护动作不匹配"))?;
        let mut cuts = action.completed.clone();
        for participant in action.participants.values() {
            cuts.push(participant.cut.ok_or(Error::Busy)?);
        }
        cuts.sort_unstable_by_key(|cut| cut.session);
        Ok(cuts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cut(session: SessionId, old_pending: usize) -> SessionCut {
        SessionCut {
            session,
            last_accepted: Some(Serial(7)),
            old_pending,
        }
    }
    #[test]
    fn 接受序号与版本校验原子进行且拒绝不消费序号() {
        let c = Coordinator::new(1).unwrap();
        let session = SessionId([1; 16]);
        c.enroll(session).unwrap();
        c.accept_serial(session, Serial(7), CheckpointVersion(0))
            .unwrap();
        let id = c.start_action(Action::CheckpointLog).unwrap();
        c.acknowledge(id, cut(session, 0), Phase::Prepare).unwrap();
        c.advance(id, Phase::Prepare).unwrap();
        for version in [0, 2] {
            assert!(matches!(
                c.accept_serial(session, Serial(9), CheckpointVersion(version)),
                Err(Error::Busy)
            ));
            assert_eq!(c.last_accepted(session).unwrap(), Some(Serial(7)));
        }
        c.accept_serial(session, Serial(9), CheckpointVersion(1))
            .unwrap();
        c.acknowledge(id, cut(session, 0), Phase::InProgress)
            .unwrap();
        assert_eq!(c.cuts(id).unwrap()[0].last_accepted, Some(Serial(7)));
    }
    #[test]
    fn 完整动作逐阶段确认且旧请求未排空不能发布() {
        let c = Coordinator::new(2).unwrap();
        let sessions = [SessionId([1; 16]), SessionId([2; 16])];
        for session in sessions {
            c.enroll(session).unwrap();
            c.accept_serial(session, Serial(7), CheckpointVersion(0))
                .unwrap();
        }
        let id = c.start_action(Action::CheckpointFull).unwrap();
        assert!(matches!(c.start_action(Action::Gc), Err(Error::Busy)));
        assert!(matches!(c.enroll(SessionId([3; 16])), Err(Error::Busy)));
        assert!(c.finish_action(id).is_err());
        for phase in [Phase::PrepareIndex, Phase::Prepare, Phase::InProgress] {
            assert!(matches!(c.advance(id, phase), Err(Error::Busy)));
            c.acknowledge(id, cut(sessions[0], 1), phase).unwrap();
            assert!(matches!(c.advance(id, phase), Err(Error::Busy)));
            c.acknowledge(id, cut(sessions[1], 1), phase).unwrap();
            c.advance(id, phase).unwrap();
            if phase == Phase::PrepareIndex {
                c.advance(id, Phase::IndexSnapshot).unwrap();
            }
        }
        assert_eq!(c.snapshot().unwrap().version, CheckpointVersion(1));
        assert!(matches!(
            c.acknowledge(id, cut(sessions[0], 1), Phase::WaitPending),
            Err(Error::Busy)
        ));
        let mut invalid = cut(sessions[0], 0);
        invalid.last_accepted = Some(Serial(6));
        assert!(c.acknowledge(id, invalid, Phase::WaitPending).is_err());
        for session in sessions {
            c.acknowledge(id, cut(session, 0), Phase::WaitPending)
                .unwrap();
        }
        assert_eq!(c.cuts(id).unwrap(), sessions.map(|s| cut(s, 0)));
        c.advance(id, Phase::WaitPending).unwrap();
        c.advance(id, Phase::WaitFlush).unwrap();
        c.finish_action(id).unwrap();
        assert_eq!(c.snapshot().unwrap().phase, Phase::Rest);
        let next = c.start_action(Action::CheckpointLog).unwrap();
        assert_ne!(next, id);
        assert!(c.fail_action(id, Error::SessionAbandoned).is_err());
        assert!(
            c.acknowledge(id, cut(sessions[0], 0), Phase::Prepare)
                .is_err()
        );
    }
    #[test]
    fn 仅索引动作不递增日志版本且恢复要求没有活跃会话() {
        let c = Coordinator::new(1).unwrap();
        let session = SessionId([1; 16]);
        c.enroll(session).unwrap();
        assert!(matches!(c.start_action(Action::Recover), Err(Error::Busy)));
        c.leave(session).unwrap();
        let id = c.start_action(Action::CheckpointIndex).unwrap();
        for phase in [Phase::PrepareIndex, Phase::IndexSnapshot, Phase::WaitFlush] {
            c.advance(id, phase).unwrap();
        }
        assert_eq!(c.snapshot().unwrap().version, CheckpointVersion(0));
        c.finish_action(id).unwrap();
        let recovery = c.start_action(Action::Recover).unwrap();
        assert!(matches!(c.shutdown(), Err(Error::Busy)));
        c.advance(recovery, Phase::WaitFlush).unwrap();
        c.finish_action(recovery).unwrap();
        c.shutdown().unwrap();
    }
    #[test]
    fn 动作中放弃参与会话保留首个失败并禁止新成功动作() {
        let c = Coordinator::new(1).unwrap();
        let session = SessionId([1; 16]);
        c.enroll(session).unwrap();
        let id = c.start_action(Action::CheckpointLog).unwrap();
        c.leave(session).unwrap();
        assert!(matches!(
            &*c.action_failure(id).unwrap().unwrap(),
            Error::SessionAbandoned
        ));
        c.fail_action(id, Error::Codec("第二次错误")).unwrap();
        assert!(matches!(
            &*c.action_failure(id).unwrap().unwrap(),
            Error::SessionAbandoned
        ));
        assert!(c.start_action(Action::CheckpointFull).is_err());
        assert!(c.enroll(session).is_err());
        assert!(c.finish_action(id).is_err());
        c.shutdown().unwrap();
    }
    #[test]
    fn 注册与阶段开始竞争不会漏记参与者() {
        for _ in 0..32 {
            let c = Coordinator::new(1).unwrap();
            let barrier = std::sync::Barrier::new(2);
            let session = SessionId([1; 16]);
            let (enroll, id) = std::thread::scope(|scope| {
                let registration = scope.spawn(|| {
                    barrier.wait();
                    c.enroll(session)
                });
                let action = scope.spawn(|| {
                    barrier.wait();
                    c.start_action(Action::CheckpointLog).unwrap()
                });
                (registration.join().unwrap(), action.join().unwrap())
            });
            let first = c.advance(id, Phase::Prepare);
            if enroll.is_ok() {
                assert!(matches!(first, Err(Error::Busy)));
                c.acknowledge(
                    id,
                    SessionCut {
                        session,
                        last_accepted: None,
                        old_pending: 0,
                    },
                    Phase::Prepare,
                )
                .unwrap();
                c.advance(id, Phase::Prepare).unwrap();
            } else {
                assert!(matches!(enroll, Err(Error::Busy)));
                first.unwrap();
            }
        }
    }
    #[test]
    fn 版本耗尽拒绝推进并保持原状态() {
        let c = Coordinator::new(1).unwrap();
        c.registry.lock().unwrap().system.version = CheckpointVersion(u64::MAX);
        let id = c.start_action(Action::CheckpointLog).unwrap();
        let before = c.snapshot().unwrap();
        assert!(matches!(
            c.advance(id, Phase::Prepare),
            Err(Error::CapacityExceeded)
        ));
        assert_eq!(c.snapshot().unwrap(), before);
    }
}
