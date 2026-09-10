//! 磁盘字段的逻辑草图，不是 repr(C) 内存镜像；版本和偏移尚未冻结。
use crate::types::*;

pub(crate) struct RecordHeader {
    pub previous: Option<LogAddress>,
    pub version: CheckpointVersion,
    pub key_bytes: u32,
    pub value_bytes: u32,
    pub capacity_bytes: u32,
    pub tombstone: bool,
    pub invalid: bool,
    pub final_record: bool,
}
pub(crate) struct Manifest {
    pub store: StoreId,
    pub key_format: FormatId,
    pub value_format: FormatId,
    pub hash: HashDescriptor,
    pub version: CheckpointVersion,
    pub begin: LogAddress,
    pub end: LogAddress,
    pub session_progress: Vec<(SessionId, Serial)>,
}
impl RecordHeader {
    pub fn decode(_bytes: &[u8]) -> Result<Self, Error> {
        Err(Error::unimplemented("format::record_decode"))
    }
    pub fn encode(&self, _output: &mut [u8]) -> Result<usize, Error> {
        Err(Error::unimplemented("format::record_encode"))
    }
}
impl Manifest {
    pub fn decode(_bytes: &[u8]) -> Result<Self, Error> {
        Err(Error::unimplemented("format::manifest_decode"))
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        Err(Error::unimplemented("format::manifest_encode"))
    }
}
