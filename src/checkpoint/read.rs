//! Recovery files are read bounded by declared length;Verify EOF and hand over the bytes only after closing.
use crate::{device::*, storage::SegmentedStorage, types::*};
use std::{path::PathBuf, sync::Arc};
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Open,
    Read,
    Close,
    Done,
}
pub(crate) struct ReadSpec<'a> {
    pub token: CheckpointToken,
    pub name: &'a str,
    pub bytes: u64,
    pub limit: usize,
    pub chunk: usize,
    pub route: CompletionRoute,
}
pub(crate) struct MaterialRead {
    owner: Arc<crate::sync::InstanceId>,
    path: PathBuf,
    expected: usize,
    chunk: usize,
    route: CompletionRoute,
    bytes: Vec<u8>,
    stage: Stage,
    file: Option<FileId>,
    pending: Option<(IoId, usize)>,
    failure: Option<Error>,
    result: Option<Result<Vec<u8>, Error>>,
}
impl MaterialRead {
    pub fn belongs_to(&self, storage: &SegmentedStorage) -> bool {
        Arc::ptr_eq(&self.owner, &storage.identity)
    }

    pub fn new(storage: &SegmentedStorage, spec: ReadSpec<'_>) -> Result<Self, Error> {
        let caps = storage.device.capabilities();
        if !caps.supports_files || caps.transfer_alignment != 1 {
            return Err(Error::UnsupportedDurability);
        }
        if spec.chunk == 0 || !caps.memory_alignment.is_power_of_two() {
            return Err(Error::InvalidConfig {
                field: "recovery.chunk",
                reason: "The read block must be non-zero and the memory alignment must be valid",
            });
        }
        let expected = usize::try_from(spec.bytes).map_err(|_| Error::CapacityExceeded)?;
        if expected > spec.limit {
            return Err(Error::CapacityExceeded);
        }
        // The extra byte is only used for EOF detection,Do not enter result distribution.
        spec.bytes.checked_add(1).ok_or(Error::CapacityExceeded)?;
        let path = storage.checkpoint_path(spec.token, spec.name)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(expected)
            .map_err(|_| Error::OutOfMemory)?;
        Ok(Self {
            owner: storage.identity.clone(),
            path,
            expected,
            chunk: spec.chunk,
            route: spec.route,
            bytes,
            stage: Stage::Open,
            file: None,
            pending: None,
            failure: None,
            result: None,
        })
    }
    fn fail(&mut self, error: Error) {
        self.failure.get_or_insert(error);
        if self.file.is_some() && self.stage != Stage::Close {
            self.stage = Stage::Close;
        } else {
            self.stage = Stage::Done;
            self.result = Some(Err(self.failure.take().expect("Save first error")));
        }
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        if !Arc::ptr_eq(&self.owner, &storage.identity) {
            return Err(Error::InvalidState(
                "Resume reads belonging to other storage",
            ));
        }
        if self.pending.is_some() || self.stage == Stage::Done {
            return Ok(None);
        }
        let mut length = 0;
        let operation = match self.stage {
            Stage::Open => IoOperation::Open {
                path: self.path.clone(),
                create_new: false,
            },
            Stage::Read => {
                length = self
                    .expected
                    .saturating_sub(self.bytes.len())
                    .min(self.chunk)
                    .max(1);
                let buffer = match AlignedBuffer::new_zeroed(
                    length,
                    storage.device.capabilities().memory_alignment,
                ) {
                    Ok(buffer) => buffer,
                    Err(error) => {
                        self.fail(error);
                        return Ok(None);
                    }
                };
                IoOperation::Read {
                    file: self.file.expect("Read an open file"),
                    offset: self.bytes.len() as u64,
                    buffer,
                }
            }
            Stage::Close => IoOperation::Close(self.file.expect("Close retained handle")),
            Stage::Done => return Ok(None),
        };
        match storage.device.submit(IoRequest {
            route: self.route,
            operation,
        }) {
            Ok(id) => {
                self.pending = Some((id, length));
                Ok(Some(id))
            }
            Err(rejected) => match rejected.reason {
                Error::Busy => Err(Error::Busy),
                error => {
                    self.fail(error);
                    Ok(None)
                }
            },
        }
    }
    #[allow(
        clippy::result_large_err,
        reason = "Error completion returns buffer intact"
    )]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if !Arc::ptr_eq(&self.owner, &storage.identity)
            || self.pending.is_none_or(|(id, _)| id != completion.id)
            || completion.route != self.route
        {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("Recovery read completion identity mismatch"),
            });
        }
        let (_, length) = self.pending.take().expect("Matched status in transit");
        match (self.stage, completion.result) {
            (Stage::Open, Ok(IoOutcome::Opened(file))) => {
                self.file = Some(file);
                if completion.buffer.is_some() {
                    self.fail(Error::InvalidState(
                        "Opening file for reading returns unexpected buffering",
                    ));
                } else {
                    self.stage = Stage::Read;
                }
            }
            (Stage::Read, Ok(IoOutcome::Transferred(read))) => {
                if read > length || completion.buffer.as_ref().is_none_or(|b| b.len() != length) {
                    self.fail(Error::InvalidState("Read length or return buffer mismatch"));
                } else if self.bytes.len() == self.expected {
                    if read == 0 {
                        self.stage = Stage::Close;
                    } else {
                        self.fail(Error::InvalidFormat("Recover files with trailing bytes"));
                    }
                } else if read == 0 {
                    self.fail(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
                } else {
                    self.bytes.extend_from_slice(
                        &completion.buffer.expect("Buffering verified").as_slice()[..read],
                    );
                }
            }
            (Stage::Close, Ok(IoOutcome::Done)) if completion.buffer.is_none() => {
                self.file = None;
                self.stage = Stage::Done;
                self.result = Some(match self.failure.take() {
                    Some(error) => Err(error),
                    None => Ok(std::mem::take(&mut self.bytes)),
                });
            }
            (_, Err(error)) => self.fail(error),
            _ => self.fail(Error::InvalidState("Recovery file completion type error")),
        }
        Ok(())
    }
    pub fn take_result(&mut self) -> Option<Result<Vec<u8>, Error>> {
        self.result.take()
    }
    /// If there is still a handle after the failure, the driver must continue to clean up or install the device. shutdown take over,Drop Do not submit I/O.
    pub fn has_resources(&self) -> bool {
        self.pending.is_some() || self.file.is_some()
    }
}
