//! 只管理访问安全 epoch，不认识检查点阶段或业务持久化序号。
use crate::types::*;
use std::{marker::PhantomData, rc::Rc};

pub(crate) struct ParticipantId {
    pub slot: usize,
    pub generation: Generation,
}
pub(crate) struct EpochGuard<'a> {
    manager: &'a EpochManager,
    participant: ParticipantId,
    local: PhantomData<Rc<()>>,
}
pub(crate) enum DeferredAction {
    RecyclePage(PageId, Generation),
    ReleaseIndex(Generation),
    AdvanceReadOnly(LogAddress),
}
pub(crate) struct EpochManager {
    current: crate::sync::AtomicU64,
}
impl EpochManager {
    pub fn register(&self) -> Result<ParticipantId, Error> {
        Err(Error::unimplemented("epoch::register"))
    }
    pub fn enter(&self, _id: ParticipantId) -> Result<EpochGuard<'_>, Error> {
        Err(Error::unimplemented("epoch::enter"))
    }
    pub fn defer(&self, _epoch: EpochVersion, _action: DeferredAction) -> Result<(), Error> {
        Err(Error::unimplemented("epoch::defer"))
    }
    pub fn advance(&self) -> Result<EpochVersion, Error> {
        Err(Error::unimplemented("epoch::advance"))
    }
    pub fn unregister(&self, _id: ParticipantId) -> Result<(), Error> {
        Err(Error::unimplemented("epoch::unregister"))
    }
}
