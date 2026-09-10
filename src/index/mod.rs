//! 内部内存索引接口，不依赖 Value 或设备；扩容与旧表回收尚未实现。
use crate::{config::IndexConfig, types::*};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexHead {
    Empty,
    Log(LogAddress),
    Cache(CacheAddress),
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct EntrySnapshot {
    pub bucket: usize,
    pub tag: u16,
    pub head: IndexHead,
    pub table_generation: Generation,
}
pub(crate) struct IndexImage {
    pub generation: Generation,
    pub entries: Vec<EntrySnapshot>,
}
pub(crate) enum PublishResult {
    Published,
    Conflict(EntrySnapshot),
}
pub(crate) struct MemIndex {
    config: IndexConfig,
    generation: crate::sync::AtomicU64,
}
impl MemIndex {
    pub fn locate(&self, _hash: KeyHash) -> Result<Option<EntrySnapshot>, Error> {
        Err(Error::unimplemented("index::locate"))
    }
    /// 调用者必须已完成记录初始化；mutable 源记录还须有替换许可。
    pub fn compare_publish(
        &self,
        _expected: EntrySnapshot,
        _head: IndexHead,
    ) -> Result<PublishResult, Error> {
        Err(Error::unimplemented("index::publish"))
    }
    pub fn snapshot(&self) -> Result<IndexImage, Error> {
        Err(Error::unimplemented("index::snapshot"))
    }
    pub fn restore(&mut self, _image: IndexImage) -> Result<(), Error> {
        Err(Error::unimplemented("index::restore"))
    }
    pub fn grow_step(&self, _budget: PollBudget) -> Result<Progress, Error> {
        Err(Error::unimplemented("index::grow"))
    }
}
