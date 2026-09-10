//! 完成路由、重试与阶段推进的统一接口；不能用最大完成序号代替持久化进度。
use crate::{device::IoCompletion, types::*};

pub(crate) enum ResumeReason {
    Io(IoCompletion),
    EpochAdvanced,
    PhaseChanged,
    SpaceAvailable,
}
pub(crate) trait CompletionRouter {
    fn route(&mut self, completion: IoCompletion) -> Result<(), Error>;
    fn progress(&mut self, budget: PollBudget) -> Result<Progress, Error>;
}

impl<S: crate::schema::Schema> super::Engine<S> {
    pub(crate) fn poll_session(
        &self,
        session: &mut super::SessionRuntime,
        budget: PollBudget,
    ) -> Result<Progress, Error> {
        use super::pending::TaskStep;
        if let Err(error) = self.io.poll(&*self.storage.device, budget) {
            self.failed.store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(error);
        }
        let mut keys = Vec::new();
        keys.try_reserve_exact(session.pending())
            .map_err(|_| Error::OutOfMemory)?;
        keys.extend(session.current.tasks.keys().copied());
        if let Some(previous) = &session.previous {
            keys.extend(previous.tasks.keys().copied());
        }
        keys.sort_unstable();
        let start = session
            .poll_cursor
            .map_or(0, |cursor| keys.partition_point(|key| *key <= cursor));
        let mut completed = 0;
        for offset in 0..budget.0.get().min(keys.len()) {
            let key = keys[(start + offset) % keys.len()];
            session.poll_cursor = Some(key);
            let task = if let Some(task) = session.current.tasks.get_mut(&key) {
                task
            } else {
                session
                    .previous
                    .as_mut()
                    .and_then(|previous| previous.tasks.get_mut(&key))
                    .expect("已收集任务存在")
            };
            let step = match self.io.take(task.id()) {
                Ok(Some(completion)) => match task.on_io(completion) {
                    Ok(()) => task.step(PollBudget(
                        std::num::NonZeroUsize::new(1).expect("固定预算"),
                    )),
                    Err(cause) => TaskStep::Failed(OperationError {
                        cause,
                        effect: Effect::NotApplied,
                    }),
                },
                Ok(None) => task.step(PollBudget(
                    std::num::NonZeroUsize::new(1).expect("固定预算"),
                )),
                Err(cause) => TaskStep::Failed(OperationError {
                    cause,
                    effect: Effect::NotApplied,
                }),
            };
            let ended = match step {
                TaskStep::Complete => true,
                TaskStep::Failed(error) => {
                    task.abandon(error);
                    true
                }
                TaskStep::AwaitingIo | TaskStep::Retry => false,
            };
            if ended {
                if session.current.tasks.remove(&key).is_none()
                    && let Some(previous) = &mut session.previous
                {
                    previous.tasks.remove(&key);
                }
                completed += 1;
            }
        }
        Ok(Progress {
            completed,
            remaining: session.pending(),
            phase_advanced: false,
        })
    }
}
