//! 材料凭据匹配后写清单、提交标识并发布；最终目录同步之前不得成功。
use super::{
    directory::PreparedDirectory,
    material::{MaterialWrite, SyncedFile, WrittenMaterial},
};
use crate::{
    device::*,
    format::{Commit, Manifest},
    storage::SegmentedStorage,
    types::*,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PublishedCommit {
    pub store: StoreId,
    pub token: CheckpointToken,
    pub manifest: WrittenMaterial,
}
pub(crate) struct CommitPublish {
    directory: PreparedDirectory,
    route: CompletionRoute,
    files: VecDeque<MaterialWrite>,
    operations: VecDeque<IoOperation>,
    pending: Option<IoId>,
    visible: bool,
    done: bool,
    receipt: PublishedCommit,
    result: Option<Result<PublishedCommit, Error>>,
}
impl CommitPublish {
    pub fn new(
        storage: &SegmentedStorage,
        directory: PreparedDirectory,
        manifest: Manifest,
        materials: Vec<SyncedFile>,
        chunk: usize,
        route: CompletionRoute,
    ) -> Result<Self, Error> {
        manifest.validate()?;
        if !Arc::ptr_eq(&directory.owner, &storage.identity)
            || directory.store != manifest.store
            || directory.token != manifest.token
        {
            return Err(Error::InvalidState("清单与目录预留身份不匹配"));
        }
        let mut by_name = BTreeMap::new();
        for material in materials {
            if !Arc::ptr_eq(&material.owner, &storage.identity) || material.token != directory.token
            {
                return Err(Error::InvalidState("材料属于其他存储或检查点"));
            }
            if by_name.insert(material.name.clone(), material).is_some() {
                return Err(Error::InvalidState("材料凭据重复"));
            }
        }
        if by_name.len() != manifest.materials.len() {
            return Err(Error::InvalidFormat("材料凭据数量不匹配"));
        }
        for material in &manifest.materials {
            let name = SegmentedStorage::checkpoint_material_name(material.id, material.generation);
            let file = by_name
                .remove(&name)
                .ok_or(Error::InvalidFormat("缺少清单材料凭据"))?;
            if file.digest.bytes != material.bytes || file.digest.checksum != material.checksum {
                return Err(Error::InvalidFormat("材料长度或校验值与清单不匹配"));
            }
        }
        let encoded = manifest.encode()?;
        let digest = WrittenMaterial {
            bytes: encoded.len() as u64,
            checksum: crate::format::checksum(&encoded),
        };
        let commit = Commit {
            store: manifest.store,
            token: manifest.token,
            manifest_bytes: digest.bytes,
            manifest_checksum: digest.checksum,
        }
        .encode()?;
        let files = VecDeque::from([
            MaterialWrite::new(storage, manifest.token, "manifest", encoded, chunk, route)?,
            MaterialWrite::new(
                storage,
                manifest.token,
                "commit.pending",
                commit,
                chunk,
                route,
            )?,
        ]);
        let operations = VecDeque::from(storage.publish_plan(manifest.token)?);
        let receipt = PublishedCommit {
            store: manifest.store,
            token: manifest.token,
            manifest: digest,
        };
        Ok(Self {
            directory,
            route,
            files,
            operations,
            pending: None,
            visible: false,
            done: false,
            receipt,
            result: None,
        })
    }
    fn check_owner(&self, storage: &SegmentedStorage) -> Result<(), Error> {
        if Arc::ptr_eq(&self.directory.owner, &storage.identity) {
            Ok(())
        } else {
            Err(Error::InvalidState("提交发布属于其他存储"))
        }
    }
    fn fail(&mut self, error: Error) {
        self.done = true;
        self.result = Some(Err(error));
    }
    fn finish_files(&mut self) {
        while let Some(result) = self.files.front_mut().and_then(MaterialWrite::take_result) {
            match result {
                Ok(_) => {
                    self.files.pop_front();
                }
                Err(error) => {
                    self.fail(error);
                    break;
                }
            }
        }
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        self.check_owner(storage)?;
        if self.done || self.pending.is_some() {
            return Ok(None);
        }
        self.finish_files();
        if self.done {
            return Ok(None);
        }
        if let Some(file) = self.files.front_mut() {
            return file.submit_next(storage);
        }
        let operation = self
            .operations
            .pop_front()
            .ok_or(Error::InvalidState("提交发布没有待执行操作"))?;
        let rename = matches!(operation, IoOperation::Rename { .. });
        match storage.device.submit(IoRequest {
            route: self.route,
            operation,
        }) {
            Ok(id) => {
                self.pending = Some(id);
                self.visible |= rename;
                Ok(Some(id))
            }
            Err(rejected) => match rejected.reason {
                Error::Busy => {
                    self.operations.push_front(rejected.request.operation);
                    Err(Error::Busy)
                }
                error => {
                    self.fail(error);
                    Ok(None)
                }
            },
        }
    }
    #[allow(clippy::result_large_err, reason = "错误身份归还原完成与缓冲")]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if self.check_owner(storage).is_err() {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("提交发布属于其他存储"),
            });
        }
        if let Some(file) = self.files.front_mut() {
            file.accept(storage, completion)?;
            self.finish_files();
            return Ok(());
        }
        if self.pending != Some(completion.id) || completion.route != self.route {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("提交发布完成身份不匹配"),
            });
        }
        self.pending = None;
        match completion.result {
            Ok(IoOutcome::Done) if completion.buffer.is_none() => {
                if self.operations.is_empty() {
                    self.done = true;
                    self.result = Some(Ok(self.receipt));
                }
            }
            Err(error) => self.fail(error),
            _ => self.fail(Error::InvalidState("提交发布完成类型错误")),
        }
        Ok(())
    }
    pub fn take_result(&mut self) -> Option<Result<PublishedCommit, Error>> {
        self.result.take()
    }
    /// 重命名一旦被设备接受，就保守视为可能可见；失败时不能擅自删除目录。
    pub fn may_be_visible(&self) -> bool {
        self.visible
    }
    pub fn has_resources(&self) -> bool {
        self.pending.is_some() || self.files.iter().any(MaterialWrite::has_resources)
    }
}
