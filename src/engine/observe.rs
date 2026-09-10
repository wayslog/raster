//! 会话只确认自己的上下文；材料 I/O 和阶段推进仍由动作驱动者负责。
use super::{Engine, SessionRuntime};
use crate::{
    coordination::{Phase, SystemState},
    schema::Schema,
    types::*,
};
use std::sync::atomic::Ordering;
impl<S: Schema> Engine<S> {
    pub(crate) fn observe_session(&self, session: &mut SessionRuntime) -> Result<bool, Error> {
        let mut changed = false;
        // 每次观察有固定上限；并发动作不断变化时让调用者下一轮继续。
        for _ in 0..8 {
            let state = self.coordinator.snapshot()?;
            if self.failed.load(Ordering::SeqCst) {
                if state.id.is_none() {
                    return Ok(changed);
                }
                if let Some(id) = state.id
                    && state.phase != Phase::Failed
                {
                    self.coordinator
                        .fail_action(id, Error::InvalidState("引擎已失败关闭"))?;
                }
                return Err(Error::InvalidState("引擎已失败关闭"));
            }
            if state.phase == Phase::Failed {
                self.failed.store(true, Ordering::SeqCst);
                return Err(Error::InvalidState("协调动作已失败"));
            }
            let result = self.observe_at(session, state, &mut changed);
            match result {
                Ok(()) => return Ok(changed),
                Err(error) => {
                    if self.coordinator.snapshot()? != state {
                        continue;
                    }
                    if matches!(error, Error::Busy) {
                        return Ok(changed);
                    }
                    return Err(error);
                }
            }
        }
        Ok(changed)
    }
    fn observe_at(
        &self,
        session: &mut SessionRuntime,
        state: SystemState,
        changed: &mut bool,
    ) -> Result<(), Error> {
        let cut_version = match state.phase {
            Phase::InProgress | Phase::WaitPending => {
                *changed |= session.switch_version(state.version)?;
                Some(CheckpointVersion(
                    state
                        .version
                        .0
                        .checked_sub(1)
                        .ok_or(Error::InvalidState("旧版本不存在"))?,
                ))
            }
            Phase::PrepareIndex | Phase::Prepare | Phase::GrowPrepare => Some(state.version),
            _ => None,
        };
        if session.current.version != state.version {
            return Err(Error::InvalidState("会话尚未观察到当前动作版本"));
        }
        if let Some(version) = cut_version {
            self.coordinator.acknowledge(
                state.id.ok_or(Error::InvalidState("阶段缺少动作标识"))?,
                session.cut(version)?,
                state.phase,
            )?;
        }
        Ok(())
    }
}
