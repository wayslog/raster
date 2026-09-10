//! 维护轮询只推进设备和全局屏障，不执行其他会话的用户回调。
use super::Engine;
use crate::{coordination::Phase, schema::Schema, types::*};
use std::sync::atomic::Ordering;
impl<S: Schema> Engine<S> {
    pub(crate) fn poll_maintenance(&self, budget: PollBudget) -> Result<Progress, Error> {
        let result = self.maintenance_step(budget);
        if result.is_err() {
            self.failed.store(true, Ordering::SeqCst);
            self.fail_checkpoint()?;
            let state = self.coordinator.snapshot()?;
            if let Some(id) = state.id
                && state.phase != Phase::Failed
            {
                self.coordinator
                    .fail_action(id, Error::InvalidState("维护推进失败"))?;
            }
        }
        result
    }
    fn maintenance_step(&self, budget: PollBudget) -> Result<Progress, Error> {
        if self.failed.load(Ordering::SeqCst) || self.coordinator.snapshot()?.phase == Phase::Failed
        {
            return Err(Error::InvalidState("引擎或维护动作已失败"));
        }
        self.io.poll(&*self.storage.device, budget)?;
        let mut advanced = self.progress_storage()?;
        let mut completed = 0;
        for _ in 0..budget.0.get() {
            let (checkpoint_progress, checkpoint_completed) = self.progress_checkpoint()?;
            advanced |= checkpoint_progress;
            completed += usize::from(checkpoint_completed);
            let state = self.coordinator.snapshot()?;
            let Some(id) = state.id else {
                break;
            };
            // 这里只跨过参与者屏障；索引快照、刷盘和发布必须由实际材料驱动者完成。
            if !matches!(
                state.phase,
                Phase::PrepareIndex
                    | Phase::Prepare
                    | Phase::InProgress
                    | Phase::WaitPending
                    | Phase::GrowPrepare
            ) {
                break;
            }
            match self.coordinator.advance(id, state.phase) {
                Ok(_) => advanced = true,
                Err(Error::Busy) => break,
                Err(error) => {
                    if self.coordinator.snapshot()? != state {
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        let state = self.coordinator.snapshot()?;
        if state.phase == Phase::Failed {
            return Err(Error::InvalidState("维护动作已失败"));
        }
        Ok(Progress {
            completed,
            remaining: usize::from(state.id.is_some()),
            phase_advanced: advanced,
        })
    }
}
