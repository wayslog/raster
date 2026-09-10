//! 显式磁盘编码；开发期 v1 在 P5 恢复协议验收后冻结。
use crate::types::*;

mod record;
mod wire;

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
impl Manifest {
    pub fn decode(_bytes: &[u8]) -> Result<Self, Error> {
        Err(Error::unimplemented("format::manifest_decode"))
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        Err(Error::unimplemented("format::manifest_encode"))
    }
}
