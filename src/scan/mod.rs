//! 内部扫描游标持有短期页缓冲和截断代次，不长期向用户借出日志指针。
use crate::{device::AlignedBuffer, types::*};

pub(crate) struct ScanCursor {
    pub next: LogAddress,
    pub end: LogAddress,
    pub truncation_generation: Generation,
    pub buffers: Vec<AlignedBuffer>,
}
impl ScanCursor {
    pub fn next_encoded(&mut self) -> Result<Option<Vec<u8>>, Error> {
        Err(Error::unimplemented("scan::cursor"))
    }
}
