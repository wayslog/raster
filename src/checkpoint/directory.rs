//! Checkpoint directory exclusive reservation and parent directory synchronization;Successful reservation does not mean that there are recoverable commits.
use super::material::MaterialWrite;
use crate::{device::*, storage::SegmentedStorage, types::*};
use std::{path::PathBuf, sync::Arc};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Namespace,
    RootSync,
    Token,
    Claim,
    TokenSync,
    NamespaceSync,
    Done,
}
/// Non-clonable reserved credentials,It can only be obtained after completely synchronizing the directory hierarchy..
pub(crate) struct PreparedDirectory {
    pub(super) owner: Arc<crate::sync::InstanceId>,
    pub(super) store: StoreId,
    pub(super) token: CheckpointToken,
}
pub(crate) struct DirectoryPrepare {
    owner: Arc<crate::sync::InstanceId>,
    store: StoreId,
    token: CheckpointToken,
    path: PathBuf,
    route: CompletionRoute,
    stage: Stage,
    pending: Option<IoId>,
    claim: Option<MaterialWrite>,
    result: Option<Result<PreparedDirectory, Error>>,
}
impl DirectoryPrepare {
    pub fn new(
        storage: &SegmentedStorage,
        store: StoreId,
        token: CheckpointToken,
        route: CompletionRoute,
    ) -> Result<Self, Error> {
        store.validate()?;
        let caps = storage.device.capabilities();
        if !caps.supports_files
            || !caps.supports_file_sync
            || !caps.supports_directory_sync
            || !caps.supports_atomic_publish
        {
            return Err(Error::UnsupportedDurability);
        }
        let path = storage
            .checkpoint_path(token, "owner")?
            .parent()
            .expect("fixed directory hierarchy")
            .to_path_buf();
        // owner is an exclusively reserved record,Not a commit ID;Never automatically delete or reuse existing token.
        let mut marker = b"RCLM\x01\x00\x00\x00".to_vec();
        marker.extend_from_slice(&store.0);
        marker.extend_from_slice(&token.0);
        let checksum = crate::format::checksum(&marker);
        marker.extend_from_slice(&checksum.to_le_bytes());
        let claim = MaterialWrite::new(storage, token, "owner", marker, 4096, route)?;
        Ok(Self {
            owner: storage.identity.clone(),
            store,
            token,
            path,
            route,
            stage: Stage::Namespace,
            pending: None,
            claim: Some(claim),
            result: None,
        })
    }
    fn check_owner(&self, storage: &SegmentedStorage) -> Result<(), Error> {
        if Arc::ptr_eq(&self.owner, &storage.identity) {
            Ok(())
        } else {
            Err(Error::InvalidState(
                "Directory reservation belongs to other storage",
            ))
        }
    }
    fn fail(&mut self, error: Error) {
        self.stage = Stage::Done;
        self.result = Some(Err(error));
    }
    fn finish_claim(&mut self) {
        if let Some(result) = self.claim.as_mut().and_then(MaterialWrite::take_result) {
            match result {
                Ok(_) => {
                    self.claim = None;
                    self.stage = Stage::TokenSync;
                }
                Err(error) => self.fail(error),
            }
        }
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        self.check_owner(storage)?;
        if self.pending.is_some() || self.stage == Stage::Done {
            return Ok(None);
        }
        if self.stage == Stage::Claim {
            self.finish_claim();
            if self.stage == Stage::Claim {
                return self
                    .claim
                    .as_mut()
                    .expect("Reserved tasks exist")
                    .submit_next(storage);
            }
            if self.stage == Stage::Done {
                return Ok(None);
            }
        }
        let operation = match self.stage {
            Stage::Namespace => IoOperation::CreateDirectory("checkpoints".into()),
            Stage::RootSync => IoOperation::SyncDirectory(PathBuf::new()),
            Stage::Token => IoOperation::CreateDirectory(self.path.clone()),
            Stage::TokenSync => IoOperation::SyncDirectory(self.path.clone()),
            Stage::NamespaceSync => IoOperation::SyncDirectory("checkpoints".into()),
            Stage::Claim | Stage::Done => unreachable!("Subtask stage has been processed"),
        };
        match storage.device.submit(IoRequest {
            route: self.route,
            operation,
        }) {
            Ok(id) => {
                self.pending = Some(id);
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
        reason = "Mistaken identity restitution still complete"
    )]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if self.check_owner(storage).is_err() {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("Directory reservation belongs to other storage"),
            });
        }
        if self.stage == Stage::Claim {
            self.claim
                .as_mut()
                .expect("Reserved tasks exist")
                .accept(storage, completion)?;
            self.finish_claim();
            return Ok(());
        }
        if self.pending != Some(completion.id) || completion.route != self.route {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("Directory reservation completion identity mismatch"),
            });
        }
        self.pending = None;
        match completion.result {
            Ok(IoOutcome::Done) if completion.buffer.is_none() => {
                self.stage = match self.stage {
                    Stage::Namespace => Stage::RootSync,
                    Stage::RootSync => Stage::Token,
                    Stage::Token => Stage::Claim,
                    Stage::TokenSync => Stage::NamespaceSync,
                    Stage::NamespaceSync => Stage::Done,
                    _ => {
                        self.fail(Error::InvalidState("Directory reservation phase mismatch"));
                        return Ok(());
                    }
                };
                if self.stage == Stage::Done {
                    self.result = Some(Ok(PreparedDirectory {
                        owner: self.owner.clone(),
                        store: self.store,
                        token: self.token,
                    }));
                }
            }
            Err(error) => self.fail(error),
            _ => self.fail(Error::InvalidState(
                "Directory reservation completion type error",
            )),
        }
        Ok(())
    }
    pub fn take_result(&mut self) -> Option<Result<PreparedDirectory, Error>> {
        self.result.take()
    }
    #[cfg(test)]
    pub fn has_resources(&self) -> bool {
        self.pending.is_some()
            || self
                .claim
                .as_ref()
                .is_some_and(MaterialWrite::has_resources)
    }
}
