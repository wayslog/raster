//! 分段与检查点命名空间；路径操作在设备接口完成，不由核心直接操作文件。
use crate::{
    device::{Device, FileId, IoOperation},
    types::*,
};
use std::{path::PathBuf, sync::Arc};

pub(crate) struct SegmentLocation {
    pub file: FileId,
    pub offset: u64,
    pub generation: Generation,
}
pub(crate) struct SegmentedStorage {
    pub device: Arc<dyn Device>,
    pub root: PathBuf,
    pub segment_bytes: u64,
}
impl SegmentedStorage {
    pub fn resolve(&self, _address: LogAddress) -> Result<SegmentLocation, Error> {
        Err(Error::unimplemented("storage::resolve"))
    }
    pub fn checkpoint_path(
        &self,
        _token: CheckpointToken,
        _object: &str,
    ) -> Result<PathBuf, Error> {
        Err(Error::unimplemented("storage::checkpoint_path"))
    }
    pub fn publish_plan(&self, _token: CheckpointToken) -> Result<Vec<IoOperation>, Error> {
        Err(Error::unimplemented("storage::publish"))
    }
}
