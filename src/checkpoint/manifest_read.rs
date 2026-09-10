//! 提交证据驱动清单读取；成功仅表示清单可信，材料仍须逐项验证。
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
        store.validate()?;
        token.validate()?;
        Ok(Self {
            store,
            token,
            route,
            chunk,
            commit: None,
            read: MaterialRead::new(
                storage,
                ReadSpec {
                    token,
                    name: "commit",
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
        // 每次都校验读取者的存储身份，包括无 I/O 的解析阶段。
        if !self.read.belongs_to(storage) {
            return Err(Error::InvalidState("清单读取属于其他存储"));
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
                return Err(Error::InvalidFormat("提交身份与请求的恢复点不匹配"));
            }
            // Commit::decode 已限制清单长度，拒绝损坏的超大分配声明。
            let length =
                usize::try_from(commit.manifest_bytes).map_err(|_| Error::CapacityExceeded)?;
            self.read = MaterialRead::new(
                storage,
                ReadSpec {
                    token: self.token,
                    name: "manifest",
                    bytes: commit.manifest_bytes,
                    limit: length,
                    chunk: self.chunk,
                    route: self.route,
                },
            )?;
            self.commit = Some(commit);
        }
        Ok(())
    }
    #[allow(clippy::result_large_err, reason = "错误完成原样归还缓冲")]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        self.read.accept(storage, completion)
    }
    #[cfg(test)]
    pub fn has_resources(&self) -> bool {
        self.read.has_resources()
    }
    pub fn take_result(&mut self) -> Option<Result<Manifest, Error>> {
        self.result.take()
    }
}
