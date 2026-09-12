//! Runtime retention directory for submitted checkpoints;Persistent evidence remains immutable commit and manifest.
use crate::{
    format::{Kind, Manifest},
    types::*,
};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RetentionRecord {
    pub store: StoreId,
    pub token: CheckpointToken,
    pub base_index: CheckpointToken,
    pub kind: Kind,
    pub version: CheckpointVersion,
    pub begin: LogAddress,
    pub end: LogAddress,
    pub material_count: usize,
    pub material_bytes: u64,
}
#[derive(Default)]
pub(crate) struct RetentionCatalog {
    store: Option<StoreId>,
    records: BTreeMap<CheckpointToken, RetentionRecord>,
}
impl RetentionCatalog {
    pub fn retire(&mut self, token: CheckpointToken) {
        self.records.remove(&token);
    }
    /// The caller must have published synchronously or read the manifest from a verified recovery collection;This function does not prove disk commit.
    pub fn record_committed(&mut self, manifest: &Manifest) -> Result<(), Error> {
        manifest.validate()?;
        if self.store.is_some_and(|store| store != manifest.store) {
            return Err(Error::InvalidState(
                "Reserved directory does not accept other storage",
            ));
        }
        if self.records.contains_key(&manifest.token) {
            return Err(Error::InvalidState(
                "Checkpoint retention record duplicates",
            ));
        }
        let material_bytes = manifest.materials.iter().try_fold(0u64, |sum, material| {
            sum.checked_add(material.bytes)
                .ok_or(Error::CapacityExceeded)
        })?;
        let record = RetentionRecord {
            store: manifest.store,
            token: manifest.token,
            base_index: manifest.base_index,
            kind: manifest.kind,
            version: manifest.version,
            begin: manifest.begin,
            end: manifest.end,
            material_count: manifest.materials.len(),
            material_bytes,
        };
        self.records.insert(manifest.token, record);
        self.store = Some(manifest.store);
        Ok(())
    }
    #[cfg(test)]
    pub fn records(&self) -> impl Iterator<Item = &RetentionRecord> {
        self.records.values()
    }
    /// List only known references for this process;Disk not listed token The default remains,Deletion cannot be authorized based on this.
    #[cfg(test)]
    pub fn references_token(&self, token: CheckpointToken) -> bool {
        self.records
            .values()
            .any(|record| record.token == token || record.base_index == token)
    }
}
