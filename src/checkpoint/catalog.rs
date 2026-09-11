//! 在同一独占目录锁内有界枚举提交及引用；未读取完整目录前不授权任何删除。
use super::{catalog_lock::CatalogLock, manifest_read::ManifestRead};
use crate::{
    api::maintenance::RecoverySet,
    device::*,
    format::{Kind, Manifest},
    storage::SegmentedStorage,
    types::*,
};
use std::{collections::BTreeMap, sync::Arc};
pub(crate) struct CatalogOptions {
    pub store: StoreId,
    pub target: CheckpointToken,
    pub max_tokens: usize,
    pub max_bytes: usize,
    pub chunk: usize,
    pub route: CompletionRoute,
}
pub(crate) struct Catalog {
    pub target: Manifest,
    pub retired: bool,
    pub blockers: Vec<RecoverySet>,
}
enum Stage {
    Root,
    Directory,
    Manifest,
    Finish,
}
pub(crate) struct CatalogRead {
    owner: Arc<()>,
    guard: FileId,
    route: CompletionRoute,
    store: StoreId,
    target: CheckpointToken,
    tokens: Vec<CheckpointToken>,
    position: usize,
    stage: Stage,
    pending: Option<IoId>,
    reader: Option<ManifestRead>,
    current_retired: bool,
    remaining: usize,
    max_tokens: usize,
    target_manifest: Option<Manifest>,
    target_retired: bool,
    nodes: BTreeMap<CheckpointToken, (Kind, CheckpointToken)>,
    blockers: Vec<RecoverySet>,
    chunk: usize,
    result: Option<Catalog>,
}
impl CatalogRead {
    pub fn new(
        storage: &SegmentedStorage,
        lock: &CatalogLock,
        options: CatalogOptions,
    ) -> Result<Self, Error> {
        let CatalogOptions {
            store,
            target,
            max_tokens,
            max_bytes,
            chunk,
            route,
        } = options;
        store.validate()?;
        target.validate()?;
        if !storage.device.capabilities().supports_directory_listing {
            return Err(Error::UnsupportedDurability);
        }
        Ok(Self {
            owner: storage.identity.clone(),
            guard: lock.exclusive_handle(storage)?,
            route,
            store,
            target,
            tokens: Vec::new(),
            position: 0,
            stage: Stage::Root,
            pending: None,
            reader: None,
            current_retired: false,
            remaining: max_bytes,
            max_tokens,
            target_manifest: None,
            target_retired: false,
            nodes: BTreeMap::new(),
            blockers: Vec::new(),
            chunk,
            result: None,
        })
    }
    fn check(&self, storage: &SegmentedStorage, lock: &CatalogLock) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.owner, &storage.identity)
            || lock.exclusive_handle(storage)? != self.guard
        {
            return Err(Error::InvalidState("目录读取期间更换或释放了独占锁"));
        }
        Ok(())
    }
    fn charge(&mut self, bytes: usize) -> Result<(), Error> {
        self.remaining = self
            .remaining
            .checked_sub(bytes)
            .ok_or(Error::CapacityExceeded)?;
        Ok(())
    }
    fn directory(
        &mut self,
        storage: &SegmentedStorage,
        entries: Vec<DirectoryEntry>,
    ) -> Result<(), Error> {
        let names = entries.iter().try_fold(0usize, |sum, entry| {
            sum.checked_add(entry.name.as_encoded_bytes().len())
                .ok_or(Error::CapacityExceeded)
        })?;
        self.charge(names)?;
        match self.stage {
            Stage::Root => {
                self.tokens
                    .try_reserve_exact(entries.len())
                    .map_err(|_| Error::OutOfMemory)?;
                for entry in entries {
                    if entry.kind != DirectoryEntryKind::Directory {
                        return Err(Error::InvalidFormat("检查点命名空间包含非目录或链接"));
                    }
                    self.tokens.push(parse_token(&entry.name)?);
                }
                let at = self
                    .tokens
                    .iter()
                    .position(|token| *token == self.target)
                    .ok_or_else(|| Error::Io(std::io::ErrorKind::NotFound.into()))?;
                self.tokens.swap(0, at);
                self.stage = Stage::Directory;
            }
            Stage::Directory => {
                let mut committed = false;
                let mut retired = false;
                let mut owner = false;
                let mut manifest = false;
                for entry in entries {
                    if entry.kind != DirectoryEntryKind::File {
                        return Err(Error::InvalidFormat("检查点目录包含非文件或链接"));
                    }
                    match entry.name.to_str() {
                        Some("commit") => committed = true,
                        Some("commit.released") => retired = true,
                        Some("owner") => owner = true,
                        Some("manifest") => manifest = true,
                        Some("commit.pending") => {}
                        Some(name) if material_name(name) => {}
                        _ => {
                            return Err(Error::InvalidFormat(
                                "检查点目录包含未知对象，不能授权释放",
                            ));
                        }
                    }
                }
                if committed && retired {
                    return Err(Error::InvalidFormat("检查点同时存在有效和失效提交"));
                }
                if !committed && !retired {
                    if self.position == 0 {
                        return Err(Error::InvalidFormat("目标检查点尚未提交"));
                    }
                    self.position += 1;
                    return Ok(());
                }
                if !owner || !manifest {
                    return Err(Error::InvalidFormat("已提交检查点缺少清单或预留标识"));
                }
                self.current_retired = retired;
                self.charge(56)?;
                self.reader = Some(ManifestRead::bounded(
                    storage,
                    self.store,
                    self.tokens[self.position],
                    self.route,
                    self.chunk,
                    retired,
                    self.remaining,
                )?);
                self.stage = Stage::Manifest;
            }
            _ => return Err(Error::InvalidState("目录完成阶段不匹配")),
        }
        Ok(())
    }
    fn record(&mut self, manifest: Manifest) -> Result<(), Error> {
        if !self.current_retired {
            self.nodes
                .insert(manifest.token, (manifest.kind, manifest.base_index));
        }
        if self.position == 0 {
            self.target_retired = self.current_retired;
            self.target_manifest = Some(manifest);
        } else if !self.current_retired
            && manifest.kind == Kind::Log
            && manifest.base_index == self.target
        {
            crate::format::match_recovery(
                self.target_manifest.as_ref().expect("先读目标"),
                &manifest,
            )?;
            self.blockers
                .try_reserve(1)
                .map_err(|_| Error::OutOfMemory)?;
            self.blockers.push(RecoverySet {
                store: self.store,
                index: self.target,
                log: manifest.token,
            });
        }
        self.position += 1;
        self.stage = Stage::Directory;
        Ok(())
    }
    pub fn has_resources(&self) -> bool {
        self.pending.is_some()
            || self
                .reader
                .as_ref()
                .is_some_and(ManifestRead::has_resources)
    }
    pub fn step(
        &mut self,
        storage: &SegmentedStorage,
        lock: &CatalogLock,
        completion: Option<IoCompletion>,
    ) -> Result<bool, Error> {
        self.check(storage, lock)?;
        if let Some(reader) = &mut self.reader {
            if let Some(completion) = completion {
                reader.accept(storage, completion).map_err(|r| r.reason)?;
            }
            if let Some(result) = reader.take_result() {
                let manifest = result?;
                let bytes = usize::try_from(reader.manifest_bytes().expect("清单已验证"))
                    .map_err(|_| Error::CapacityExceeded)?;
                self.charge(bytes)?;
                self.reader = None;
                self.record(manifest)?;
                return Ok(true);
            }
            return match reader.submit_next(storage) {
                Ok(id) => Ok(id.is_some()),
                Err(Error::Busy) => Ok(false),
                Err(error) => Err(error),
            };
        }
        if let Some(completion) = completion {
            if self.pending != Some(completion.id)
                || completion.route != self.route
                || completion.buffer.is_some()
            {
                return Err(Error::InvalidState("目录枚举完成身份或缓冲错误"));
            }
            self.pending = None;
            let IoOutcome::Directory(entries) = completion.result? else {
                return Err(Error::InvalidState("目录枚举完成类型错误"));
            };
            self.directory(storage, entries)?;
            return Ok(true);
        }
        if self.pending.is_some() {
            return Ok(false);
        }
        if matches!(self.stage, Stage::Directory) && self.position == self.tokens.len() {
            for &(kind, base) in self.nodes.values() {
                if kind == Kind::Log
                    && !self
                        .nodes
                        .get(&base)
                        .is_some_and(|(kind, _)| *kind != Kind::Log)
                {
                    return Err(Error::InvalidFormat("有效日志检查点的基准索引缺失或失效"));
                }
            }
            self.result = Some(Catalog {
                target: self.target_manifest.take().expect("目标清单存在"),
                retired: self.target_retired,
                blockers: std::mem::take(&mut self.blockers),
            });
            self.stage = Stage::Finish;
            return Ok(true);
        }
        let (path, max_entries) = match self.stage {
            Stage::Root => (std::path::PathBuf::from("checkpoints"), self.max_tokens),
            Stage::Directory => (
                storage
                    .checkpoint_path(self.tokens[self.position], "commit")?
                    .parent()
                    .expect("固定目录")
                    .to_path_buf(),
                crate::format::MAX_MANIFEST_ITEMS + 5,
            ),
            Stage::Finish => return Ok(false),
            Stage::Manifest => return Err(Error::InvalidState("目录清单读取者缺失")),
        };
        match storage.device.submit(IoRequest {
            route: self.route,
            operation: IoOperation::ReadDirectory {
                path,
                max_entries,
                max_name_bytes: self.remaining,
            },
        }) {
            Ok(id) => {
                self.pending = Some(id);
                Ok(true)
            }
            Err(rejected) if matches!(rejected.reason, Error::Busy) => Ok(false),
            Err(rejected) => Err(rejected.reason),
        }
    }
    pub fn take_result(
        &mut self,
        storage: &SegmentedStorage,
        lock: &CatalogLock,
    ) -> Result<Option<Catalog>, Error> {
        self.check(storage, lock)?;
        Ok(self.result.take())
    }
}
fn parse_token(name: &std::ffi::OsStr) -> Result<CheckpointToken, Error> {
    let name = name
        .to_str()
        .ok_or(Error::InvalidFormat("检查点目录名不是规范 token"))?;
    if name.len() != 32
        || !name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::InvalidFormat("检查点目录名不是规范 token"));
    }
    let mut token = [0; 16];
    for (i, value) in token.iter_mut().enumerate() {
        *value = u8::from_str_radix(&name[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::InvalidFormat("检查点 token 无效"))?;
    }
    let token = CheckpointToken(token);
    token.validate()?;
    Ok(token)
}
fn material_name(name: &str) -> bool {
    name.len() == 42
        && name.as_bytes()[16] == b'-'
        && name.ends_with(".material")
        && name[..16]
            .bytes()
            .chain(name[17..33].bytes())
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
