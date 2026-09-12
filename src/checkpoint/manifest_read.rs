//! Submit evidence-driven inventory reading;Success only means the list is credible,Materials still need to be verified item by item.
use super::read::{MaterialRead, ReadSpec};
use crate::{
    device::{CompletionRoute, IoCompletion},
    format::{Commit, Manifest},
    storage::SegmentedStorage,
    types::*,
};
pub(crate) struct ManifestRead {
    store: StoreId,
    token: CheckpointToken,
    route: CompletionRoute,
    chunk: usize,
    limit: usize,
    commit: Option<Commit>,
    read: MaterialRead,
    done: bool,
    result: Option<Result<Manifest, Error>>,
}
impl ManifestRead {
    pub fn new(
        storage: &SegmentedStorage,
        store: StoreId,
        token: CheckpointToken,
        route: CompletionRoute,
        chunk: usize,
    ) -> Result<Self, Error> {
        Self::bounded(storage, store, token, route, chunk, false, usize::MAX)
    }
    pub fn bounded(
        storage: &SegmentedStorage,
        store: StoreId,
        token: CheckpointToken,
        route: CompletionRoute,
        chunk: usize,
        retired: bool,
        limit: usize,
    ) -> Result<Self, Error> {
        store.validate()?;
        token.validate()?;
        Ok(Self {
            store,
            token,
            route,
            chunk,
            limit,
            commit: None,
            read: MaterialRead::new(
                storage,
                ReadSpec {
                    token,
                    name: if retired { "commit.released" } else { "commit" },
                    bytes: 56,
                    limit: 56,
                    chunk,
                    route,
                },
            )?,
            done: false,
            result: None,
        })
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        // Verify the stored identity of the reader every time,Including none I/O parsing stage.
        if !self.read.belongs_to(storage) {
            return Err(Error::InvalidState(
                "Manifest reads belong to other storage",
            ));
        }
        if self.done {
            return Ok(None);
        }
        if let Some(result) = self.read.take_result() {
            if let Err(error) = result.and_then(|bytes| self.accept_bytes(storage, bytes)) {
                self.done = true;
                self.result = Some(Err(error));
            }
            return Ok(None);
        }
        self.read.submit_next(storage)
    }
    fn accept_bytes(&mut self, storage: &SegmentedStorage, bytes: Vec<u8>) -> Result<(), Error> {
        if let Some(commit) = &self.commit {
            self.result = Some(Ok(commit.verify(&bytes)?));
            self.done = true;
        } else {
            let commit = Commit::decode(&bytes)?;
            if commit.store != self.store || commit.token != self.token {
                return Err(Error::InvalidFormat(
                    "Submit identity does not match requested recovery point",
                ));
            }
            // Commit::decode List length limited,Reject corrupted oversized allocation declaration.
            let length =
                usize::try_from(commit.manifest_bytes).map_err(|_| Error::CapacityExceeded)?;
            self.read = MaterialRead::new(
                storage,
                ReadSpec {
                    token: self.token,
                    name: "manifest",
                    bytes: commit.manifest_bytes,
                    limit: length.min(self.limit),
                    chunk: self.chunk,
                    route: self.route,
                },
            )?;
            self.commit = Some(commit);
        }
        Ok(())
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
        self.read.accept(storage, completion)
    }
    pub fn has_resources(&self) -> bool {
        self.read.has_resources()
    }
    pub fn manifest_bytes(&self) -> Option<u64> {
        self.commit.as_ref().map(|commit| commit.manifest_bytes)
    }
    pub fn take_result(&mut self) -> Option<Result<Manifest, Error>> {
        self.result.take()
    }
}
