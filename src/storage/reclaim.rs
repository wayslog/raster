//! Work section recycling intention:Remove the protected mapping first,Close again,Delete and sync directories;Keep current step on failure.
use super::*;
use crate::{device::*, engine::io_hub::CompletionHub};
#[derive(Clone, Debug)]
pub(crate) struct SegmentCandidate {
    pub number: u64,
    pub generation: Generation,
}
impl SegmentedStorage {
    /// boundary is the first physical byte that must be reserved;Only enumerate complete old segments that are still bound to the current instance.
    pub fn reclaim_candidates(&self, boundary: u64) -> Result<Vec<SegmentCandidate>, Error> {
        let segments = self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("segment_map_lock_poisoned"))?;
        let limit = boundary / self.segment_bytes;
        let mut result = Vec::new();
        for (&number, binding) in segments.range(..limit) {
            if binding.file.is_some() {
                result.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                result.push(SegmentCandidate {
                    number,
                    generation: binding.generation,
                });
            }
        }
        Ok(result)
    }
}
#[derive(Clone, Copy)]
enum Stage {
    Close,
    Remove,
    Sync,
    Done,
}
pub(crate) struct SegmentDelete {
    owner: Arc<()>,
    hub: Arc<CompletionHub>,
    id: RequestId,
    file: FileId,
    path: PathBuf,
    directory: PathBuf,
    stage: Stage,
    inflight: Option<IoId>,
}
impl SegmentDelete {
    /// All potentially failed routes and path preparations precede the failure of the mapping;After the failure, the task holds the old handle and the old generation path.
    pub fn detach(
        storage: &SegmentedStorage,
        hub: Arc<CompletionHub>,
        store: StoreId,
        candidate: SegmentCandidate,
    ) -> Result<Self, Error> {
        let path = storage.segment_path(candidate.number, candidate.generation);
        let directory = storage.segment_directory();
        let id = hub.reserve(SessionId(store.0))?;
        let file = match storage.invalidate(candidate.number, candidate.generation) {
            Ok((file, _)) => file,
            Err(error) => {
                hub.release(id)?;
                return Err(error);
            }
        };
        Ok(Self {
            owner: storage.identity.clone(),
            hub,
            id,
            file,
            path,
            directory,
            stage: Stage::Close,
            inflight: None,
        })
    }
    pub fn has_inflight(&self) -> bool {
        self.inflight.is_some()
    }
    /// false Indicates that it is still to be submitted or completed,true Only returned after deletion directory synchronization is successful.
    pub fn step(&mut self, storage: &SegmentedStorage) -> Result<bool, Error> {
        if !Arc::ptr_eq(&self.owner, &storage.identity) {
            return Err(Error::InvalidState("Delete tasks belong to other storage"));
        }
        if let Some(completion) = self.hub.take(self.id)? {
            if Some(completion.id) != self.inflight
                || completion.route != CompletionHub::route(self.id)
                || completion.buffer.is_some()
            {
                return Err(Error::InvalidState(
                    "Delete completion identity or buffer type error",
                ));
            }
            self.inflight = None;
            let done = match completion.result {
                Ok(IoOutcome::Done) => true,
                // After exclusive removal, the old handle has no other users.;Closed or deleted. It is safe to continue to the next step..
                Err(Error::RangeTruncated) if matches!(self.stage, Stage::Close) => true,
                Err(Error::Io(ref error))
                    if matches!(self.stage, Stage::Remove)
                        && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    true
                }
                Err(error) => return Err(error),
                _ => return Err(Error::InvalidState("Delete completion result type error")),
            };
            if done {
                self.stage = match self.stage {
                    Stage::Close => Stage::Remove,
                    Stage::Remove => Stage::Sync,
                    Stage::Sync => Stage::Done,
                    Stage::Done => {
                        return Err(Error::InvalidState("Deletion of duplicates completed"));
                    }
                };
            }
            return Ok(matches!(self.stage, Stage::Done));
        }
        if self.inflight.is_some() {
            return Ok(false);
        }
        let operation = match self.stage {
            Stage::Close => IoOperation::Close(self.file),
            Stage::Remove => IoOperation::RemoveFile(self.path.clone()),
            Stage::Sync => IoOperation::SyncDirectory(self.directory.clone()),
            Stage::Done => return Ok(true),
        };
        match storage.device.submit(IoRequest {
            route: CompletionHub::route(self.id),
            operation,
        }) {
            Ok(id) => self.inflight = Some(id),
            Err(rejected) if matches!(rejected.reason, Error::Busy) => {}
            Err(rejected) => return Err(rejected.reason),
        }
        Ok(false)
    }
}
impl Drop for SegmentDelete {
    fn drop(&mut self) {
        let _ = self.hub.release(self.id);
    }
}
