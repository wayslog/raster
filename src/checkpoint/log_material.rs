//! Copy frozen and completed log pages as independent sync material,Memory overhead is limited to a single page.
use super::material::{MaterialWrite, SyncedFile};
use crate::{
    device::{CompletionRoute, IoCompletion},
    format::{Kind, Material, checksum},
    log::{HybridLog, read_page::PageRead},
    schema::ValueLayout,
    storage::SegmentedStorage,
    types::*,
};
use std::sync::Arc;

pub(crate) struct LogMaterialSpec {
    pub token: CheckpointToken,
    pub page: PageId,
    pub id: u64,
    pub chunk: usize,
    pub route: CompletionRoute,
}
pub(crate) struct LogMaterialFile {
    pub descriptor: Material,
    pub file: SyncedFile,
}
enum Stage {
    Read(PageRead),
    Write(MaterialWrite),
    Done,
}
pub(crate) struct LogMaterialWrite {
    owner: Arc<()>,
    spec: LogMaterialSpec,
    descriptor: Material,
    stage: Stage,
    ended: bool,
    result: Option<Result<LogMaterialFile, Error>>,
}
impl LogMaterialWrite {
    /// The parent directory is first DirectoryPrepare reserved.Entire replication period truncated by maintenance action exclusion log.
    pub fn new<V: ValueLayout>(
        storage: &SegmentedStorage,
        log: &HybridLog<V>,
        spec: LogMaterialSpec,
    ) -> Result<Self, Error> {
        let caps = storage.device.capabilities();
        if !caps.supports_files || !caps.supports_file_sync || caps.transfer_alignment != 1 {
            return Err(Error::UnsupportedDurability);
        }
        if spec.chunk == 0 || !caps.memory_alignment.is_power_of_two() {
            return Err(Error::InvalidConfig {
                field: "checkpoint.chunk",
                reason: "Block size must be non-zero and device memory alignment must be valid",
            });
        }
        // Verify target identity and path before reading source file.
        storage.checkpoint_path(
            spec.token,
            &SegmentedStorage::checkpoint_material_name(spec.id, Generation(0)),
        )?;
        let (read, begin, end) = log.checkpoint_page(spec.page, spec.route)?;
        Ok(Self {
            owner: storage.identity.clone(),
            descriptor: Material {
                id: spec.id,
                generation: Generation(0),
                kind: Kind::Log,
                begin,
                end,
                bytes: 0,
                checksum: 0,
            },
            spec,
            stage: Stage::Read(read),
            ended: false,
            result: None,
        })
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        if !Arc::ptr_eq(&self.owner, &storage.identity) {
            return Err(Error::InvalidState("Log material belongs to other storage"));
        }
        if self.ended {
            return Ok(None);
        }
        let progress = self.progress(storage);
        match progress {
            Err(Error::Busy) => Err(Error::Busy),
            Err(error) => {
                self.ended = true;
                self.result = Some(Err(error));
                // Keep intra-task handle when material close fails,Recycling by device shutdown process.
                if !self.has_resources() {
                    self.stage = Stage::Done;
                }
                Ok(None)
            }
            result => result,
        }
    }
    fn progress(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        match &mut self.stage {
            Stage::Read(read) => {
                if read.has_inflight() {
                    return Ok(None);
                }
                if let Some(page) = read.finish(storage)? {
                    let bytes = page.into_checkpoint_bytes()?;
                    self.descriptor.bytes = bytes.len() as u64;
                    self.descriptor.checksum = checksum(&bytes);
                    let name =
                        SegmentedStorage::checkpoint_material_name(self.spec.id, Generation(0));
                    self.stage = Stage::Write(MaterialWrite::new(
                        storage,
                        self.spec.token,
                        &name,
                        bytes,
                        self.spec.chunk,
                        self.spec.route,
                    )?);
                    Ok(None)
                } else {
                    read.submit_next(storage)
                }
            }
            Stage::Write(write) => {
                if let Some(result) = write.take_synced() {
                    let file = result?;
                    self.result = Some(Ok(LogMaterialFile {
                        descriptor: self.descriptor.clone(),
                        file,
                    }));
                    self.ended = true;
                    self.stage = Stage::Done;
                    Ok(None)
                } else {
                    write.submit_next(storage)
                }
            }
            Stage::Done => Ok(None),
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
        if !Arc::ptr_eq(&self.owner, &storage.identity) || self.ended {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("Log material completion identity mismatch"),
            });
        }
        match &mut self.stage {
            Stage::Read(read) => read.accept(storage, completion),
            Stage::Write(write) => write.accept(storage, completion),
            Stage::Done => Err(Rejected {
                request: completion,
                reason: Error::InvalidState("Log material has ended"),
            }),
        }
    }
    pub fn has_resources(&self) -> bool {
        match &self.stage {
            Stage::Write(write) => write.has_resources(),
            Stage::Read(read) => read.has_inflight(),
            Stage::Done => false,
        }
    }
    pub fn take_result(&mut self) -> Option<Result<LogMaterialFile, Error>> {
        self.result.take()
    }
}
