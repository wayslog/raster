//! Synchronous work borrows its caller's engine; suspended work owns the engine.
//! Both paths run the same cleanup before releasing the remaining work fields.
use super::{
    Engine,
    pending::{PendingTask, TaskStep},
};
use crate::{device::IoCompletion, schema::Schema, types::*};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

pub(super) trait TaskWork<S: Schema>: 'static {
    fn id(&self) -> RequestId;
    fn serial(&self) -> Serial;
    fn version(&self) -> CheckpointVersion;
    fn on_io(&mut self, engine: &Arc<Engine<S>>, completion: IoCompletion) -> Result<(), Error>;
    fn step(&mut self, engine: &Arc<Engine<S>>, budget: PollBudget) -> TaskStep;
    fn abandon(&mut self, engine: &Arc<Engine<S>>, error: OperationError);
    fn cleanup(&mut self, engine: &Arc<Engine<S>>);
}

pub(super) struct BorrowedTask<'a, S: Schema, W: TaskWork<S>> {
    work: Option<W>,
    engine: &'a Arc<Engine<S>>,
}
impl<'a, S: Schema, W: TaskWork<S>> BorrowedTask<'a, S, W> {
    pub fn new(engine: &'a Arc<Engine<S>>, work: W) -> Self {
        Self {
            work: Some(work),
            engine,
        }
    }

    pub fn into_owned(mut self) -> OwnedTask<S, W> {
        // Clone before transferring the work. On an unwind before transfer,
        // the borrowed guard still owns cleanup. Afterward only OwnedTask does.
        let engine = self.engine.clone();
        let work = self
            .work
            .take()
            .expect("task work has not been transferred");
        OwnedTask { work, engine }
    }
}
impl<S: Schema, W: TaskWork<S>> Deref for BorrowedTask<'_, S, W> {
    type Target = W;
    fn deref(&self) -> &W {
        self.work
            .as_ref()
            .expect("task work has not been transferred")
    }
}
impl<S: Schema, W: TaskWork<S>> DerefMut for BorrowedTask<'_, S, W> {
    fn deref_mut(&mut self) -> &mut W {
        self.work
            .as_mut()
            .expect("task work has not been transferred")
    }
}
impl<S: Schema, W: TaskWork<S>> Drop for BorrowedTask<'_, S, W> {
    fn drop(&mut self) {
        if let Some(work) = &mut self.work {
            work.cleanup(self.engine);
        }
    }
}

pub(super) struct OwnedTask<S: Schema, W: TaskWork<S>> {
    // Work fields are destroyed before the final engine owner can be released.
    work: W,
    engine: Arc<Engine<S>>,
}
impl<S: Schema, W: TaskWork<S>> PendingTask for OwnedTask<S, W> {
    fn id(&self) -> RequestId {
        self.work.id()
    }
    fn serial(&self) -> Serial {
        self.work.serial()
    }
    fn version(&self) -> CheckpointVersion {
        self.work.version()
    }
    fn on_io(&mut self, completion: IoCompletion) -> Result<(), Error> {
        self.work.on_io(&self.engine, completion)
    }
    fn step(&mut self, budget: PollBudget) -> TaskStep {
        self.work.step(&self.engine, budget)
    }
    fn abandon(&mut self, error: OperationError) {
        self.work.abandon(&self.engine, error);
    }
}
impl<S: Schema, W: TaskWork<S>> Drop for OwnedTask<S, W> {
    fn drop(&mut self) {
        self.work.cleanup(&self.engine);
    }
}
