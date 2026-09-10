//! 压缩复用的条件复制接口；复制可变源须与原地更新共同仲裁。
use super::Engine;
use crate::{index::EntrySnapshot, schema::Schema, types::*};

pub(crate) struct ConditionalCopy {
    pub expected: EntrySnapshot,
    pub source: LogAddress,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
pub(crate) enum CopyResult {
    Copied(LogAddress),
    Obsolete,
    Retry,
}
impl<S: Schema> Engine<S> {
    pub(crate) fn conditional_copy(&self, _request: ConditionalCopy) -> Result<CopyResult, Error> {
        Err(Error::unimplemented("engine::conditional_copy"))
    }
}
