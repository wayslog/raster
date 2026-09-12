//! Segmented file open task;New never silently overwrites existing files,Late opening first cleans up the handle and then fails.
use super::SegmentedStorage;
use crate::{device::*, types::*};
use std::sync::Arc;
#[derive(Clone, Copy)]
enum Stage {
    Directory,
    Open,
    Close,
    Done,
}
pub(crate) struct SegmentOpen {
    owner: Arc<crate::sync::InstanceId>,
    number: u64,
    generation: Generation,
    create_new: bool,
    route: CompletionRoute,
    pending: Option<IoId>,
    stage: Stage,
    cleanup: Option<FileId>,
    failure: Option<Error>,
    result: Option<Result<FileId, Error>>,
}
impl SegmentOpen {
    pub fn new(
        storage: &SegmentedStorage,
        number: u64,
        generation: Generation,
        create_new: bool,
        route: CompletionRoute,
    ) -> Result<Self, Error> {
        let base = LogAddress(
            number
                .checked_mul(storage.segment_bytes)
                .ok_or(Error::CapacityExceeded)?,
        );
        base.validate()?;
        let bound = match storage.resolve(base) {
            Ok(location) if location.generation == generation => Some(location.file),
            Ok(_) => return Err(Error::RangeTruncated),
            Err(Error::RangeTruncated) => None,
            Err(error) => return Err(error),
        };
        Ok(Self {
            owner: storage.identity.clone(),
            number,
            generation,
            create_new,
            route,
            pending: None,
            stage: if bound.is_some() {
                Stage::Done
            } else if create_new {
                Stage::Directory
            } else {
                Stage::Open
            },
            cleanup: None,
            failure: None,
            result: bound.map(Ok),
        })
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        if !Arc::ptr_eq(&self.owner, &storage.identity) {
            return Err(Error::InvalidState("Open tasks belonging to other storage"));
        }
        if self.pending.is_some() {
            return Ok(None);
        }
        let operation = match self.stage {
            Stage::Directory => IoOperation::CreateDirectory(storage.segment_directory()),
            Stage::Open => IoOperation::Open {
                path: storage.segment_path(self.number, self.generation),
                create_new: self.create_new,
            },
            Stage::Close => IoOperation::Close(self.cleanup.expect("Close phase save handle")),
            Stage::Done => return Ok(None),
        };
        let id = storage
            .device
            .submit(IoRequest {
                route: self.route,
                operation,
            })
            .map_err(|rejected| rejected.reason)?;
        self.pending = Some(id);
        Ok(Some(id))
    }
    #[allow(
        clippy::result_large_err,
        reason = "The error route is returned as is and the opening is completed."
    )]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if !Arc::ptr_eq(&self.owner, &storage.identity)
            || self.pending != Some(completion.id)
            || self.route != completion.route
        {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("Open task completion identity mismatch"),
            });
        }
        self.pending = None;
        match (self.stage, completion.result) {
            (Stage::Directory, Ok(IoOutcome::Done)) => self.stage = Stage::Open,
            (Stage::Open, Ok(IoOutcome::Opened(file))) => {
                match storage.bind(self.number, self.generation, file) {
                    Ok(()) => {
                        self.stage = Stage::Done;
                        self.result = Some(Ok(file));
                    }
                    Err(error) => {
                        self.failure = Some(error);
                        self.cleanup = Some(file);
                        self.stage = Stage::Close;
                    }
                }
            }
            (Stage::Close, Ok(IoOutcome::Done)) => {
                self.cleanup = None;
                self.stage = Stage::Done;
                self.result = Some(Err(self
                    .failure
                    .take()
                    .expect("Save original binding error")));
            }
            (_, Err(error)) => {
                self.stage = Stage::Done;
                self.result = Some(Err(error));
            }
            _ => {
                self.stage = Stage::Done;
                self.result = Some(Err(Error::InvalidState("Open task completion type error")));
            }
        }
        Ok(())
    }
    pub fn has_resources(&self) -> bool {
        self.pending.is_some() || self.cleanup.is_some()
    }
    pub fn take_result(&mut self) -> Option<Result<FileId, Error>> {
        self.result.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::memory::MemoryDevice;
    fn storage() -> SegmentedStorage {
        SegmentedStorage::new(Arc::new(MemoryDevice::new(8, 1024).unwrap()), 128).unwrap()
    }
    fn complete(storage: &SegmentedStorage) -> IoCompletion {
        let mut out = vec![];
        storage
            .device
            .poll(PollBudget::default(), &mut out)
            .unwrap();
        assert_eq!(out.len(), 1);
        out.pop().unwrap()
    }
    fn run(storage: &SegmentedStorage, task: &mut SegmentOpen) -> Result<FileId, Error> {
        loop {
            if let Some(result) = task.take_result() {
                return result;
            }
            task.submit_next(storage)?;
            task.accept(storage, complete(storage))
                .map_err(|rejected| rejected.reason)?;
        }
    }
    #[test]
    fn create_a_new_one_and_bind_it_there_is_no_need_to_open_the_existing_mapping_again() {
        let storage = storage();
        let file = run(
            &storage,
            &mut SegmentOpen::new(&storage, 2, Generation(0), true, CompletionRoute(7)).unwrap(),
        )
        .unwrap();
        assert_eq!(storage.resolve(LogAddress(256)).unwrap().file, file);
        let mut reused =
            SegmentOpen::new(&storage, 2, Generation(0), true, CompletionRoute(8)).unwrap();
        assert!(reused.submit_next(&storage).unwrap().is_none());
        assert_eq!(reused.take_result().unwrap().unwrap(), file);
    }
    #[test]
    fn if_the_open_is_late_and_the_generation_expires_the_unbound_handle_must_be_closed_first() {
        let storage = storage();
        let mut task =
            SegmentOpen::new(&storage, 0, Generation(0), true, CompletionRoute(7)).unwrap();
        task.submit_next(&storage).unwrap();
        task.accept(&storage, complete(&storage))
            .map_err(|r| r.reason)
            .unwrap();
        task.submit_next(&storage).unwrap();
        let done = complete(&storage);
        let file = match &done.result {
            Ok(IoOutcome::Opened(file)) => *file,
            _ => panic!("open"),
        };
        storage
            .bind(
                0,
                Generation(0),
                FileId {
                    slot: 999,
                    generation: Generation(0),
                },
            )
            .unwrap();
        storage.invalidate(0, Generation(0)).unwrap();
        task.accept(&storage, done).map_err(|r| r.reason).unwrap();
        assert!(task.take_result().is_none());
        assert!(run(&storage, &mut task).is_err());
        storage
            .device
            .submit(IoRequest {
                route: CompletionRoute(0),
                operation: IoOperation::Read {
                    file,
                    offset: 0,
                    buffer: AlignedBuffer::new_zeroed(1, 8).unwrap(),
                },
            })
            .unwrap();
        assert!(complete(&storage).result.is_err());
        assert!(storage.resolve(LogAddress(0)).is_err());
    }
}
