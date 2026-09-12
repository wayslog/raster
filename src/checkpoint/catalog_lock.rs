//! Cross-instance quorum for checkpoint directories;Open the lock file independently for each action,The holding will be released after the closing is completed..
use crate::{device::*, storage::SegmentedStorage, types::*};
use std::sync::Arc;
#[derive(Clone, Copy)]
enum Phase {
    Acquire,
    Held,
    Close,
    Closed,
    Failed,
}
pub(crate) struct CatalogLock {
    owner: Arc<()>,
    route: CompletionRoute,
    mode: FileLockMode,
    phase: Phase,
    pending: Option<IoId>,
    file: Option<FileId>,
    contended: bool,
}
impl CatalogLock {
    pub fn new(
        storage: &SegmentedStorage,
        route: CompletionRoute,
        mode: FileLockMode,
    ) -> Result<Self, Error> {
        if !storage.device.capabilities().supports_file_locks {
            return Err(Error::UnsupportedDurability);
        }
        Ok(Self {
            owner: storage.identity.clone(),
            route,
            mode,
            phase: Phase::Acquire,
            pending: None,
            file: None,
            contended: false,
        })
    }
    pub fn held(&self) -> bool {
        matches!(self.phase, Phase::Held)
    }
    pub fn closed(&self) -> bool {
        matches!(self.phase, Phase::Closed)
    }
    pub fn exclusive_handle(&self, storage: &SegmentedStorage) -> Result<FileId, Error> {
        self.check_owner(storage)?;
        if !self.held() || self.mode != FileLockMode::Exclusive {
            return Err(Error::InvalidState(
                "Directory enumeration requires an exclusive lock to be held continuously",
            ));
        }
        Ok(self.file.expect("Obtained exclusive lock"))
    }
    pub fn take_contention(&mut self) -> bool {
        std::mem::take(&mut self.contended)
    }
    /// Only if the lock has not been accepted or has been received Busy Cancel when complete,Cannot be discarded and locked in transit.
    pub fn cancel_unacquired(&mut self) -> Result<(), Error> {
        if !matches!(self.phase, Phase::Acquire) || self.pending.is_some() {
            return Err(Error::InvalidState(
                "Directory lock is still pending or acquired",
            ));
        }
        self.phase = Phase::Closed;
        Ok(())
    }
    pub fn release(&mut self) -> Result<(), Error> {
        match self.phase {
            Phase::Held => {
                self.phase = Phase::Close;
                Ok(())
            }
            Phase::Close | Phase::Closed => Ok(()),
            _ => Err(Error::InvalidState(
                "Directory locks that have not been acquired or have failed cannot be released",
            )),
        }
    }
    fn check_owner(&self, storage: &SegmentedStorage) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.owner, &storage.identity) {
            return Err(Error::InvalidState(
                "Directory lock belongs to other storage",
            ));
        }
        Ok(())
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        self.check_owner(storage)?;
        if self.pending.is_some() {
            return Ok(None);
        }
        let operation = match self.phase {
            Phase::Acquire => IoOperation::TryLock {
                path: "checkpoint.lock".into(),
                mode: self.mode,
            },
            Phase::Close => IoOperation::Close(self.file.expect("directory lock holds handle")),
            Phase::Held | Phase::Closed => return Ok(None),
            Phase::Failed => return Err(Error::InvalidState("Directory lock has failed")),
        };
        match storage.device.submit(IoRequest {
            route: self.route,
            operation,
        }) {
            Ok(id) => {
                self.pending = Some(id);
                Ok(Some(id))
            }
            Err(rejected) => {
                if !matches!(rejected.reason, Error::Busy) {
                    self.phase = Phase::Failed;
                }
                Err(rejected.reason)
            }
        }
    }
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Error> {
        self.check_owner(storage)?;
        if self.pending != Some(completion.id)
            || self.route != completion.route
            || completion.buffer.is_some()
        {
            return Err(Error::InvalidState(
                "Directory lock completion identity or buffering error",
            ));
        }
        self.pending = None;
        match (self.phase, completion.result) {
            (Phase::Acquire, Ok(IoOutcome::Locked(file))) => {
                self.file = Some(file);
                self.phase = Phase::Held;
            }
            (Phase::Acquire, Err(Error::Busy)) => {
                self.contended = true;
            }
            (Phase::Close, Ok(IoOutcome::Done) | Err(Error::RangeTruncated)) => {
                self.file = None;
                self.phase = Phase::Closed;
            }
            (_, Err(error)) => {
                self.phase = Phase::Failed;
                return Err(error);
            }
            _ => {
                self.phase = Phase::Failed;
                return Err(Error::InvalidState("Directory lock completion type error"));
            }
        }
        Ok(())
    }
}
// Drop Do not submit I/O.Failed tasks must be retained by the owner to the device shutdown;Device file table holds unacknowledged handle.
