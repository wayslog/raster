//! 拥有型设备接口；不接收 Schema、用户回调或会话操作上下文。

mod buffer;
pub mod memory;
pub mod null;
pub mod thread_pool;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub mod uring;
#[cfg(target_os = "windows")]
pub mod windows;

use crate::types::{Deadline, Error, Generation, IoId, PollBudget};
pub use buffer::AlignedBuffer;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileId {
    pub slot: u64,
    pub generation: Generation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompletionRoute(pub u64);
#[derive(Clone, Debug)]
pub struct DeviceOpenOptions {
    pub root: PathBuf,
    pub create_new: bool,
}
#[derive(Clone, Copy, Debug)]
pub struct DeviceCapabilities {
    pub memory_alignment: usize,
    pub transfer_alignment: usize,
    pub supports_file_sync: bool,
    pub supports_directory_sync: bool,
    pub supports_atomic_publish: bool,
}

pub trait DeviceFactory: Send + Sync + 'static {
    /// 创建尚未对外服务的设备；实现必须报告真实能力。
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error>;
}
pub trait Device: Send + Sync + 'static {
    fn capabilities(&self) -> DeviceCapabilities;
    /// 拒绝时归还整个请求；接受后设备负责终结并归还缓冲。
    #[allow(
        clippy::result_large_err,
        reason = "拒绝时必须原样归还请求，避免在容量不足路径再次分配"
    )]
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo>;
    /// 全局错误不能吞掉在途请求的终结事件。
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error>;
    /// 超时不能提前释放内核仍可访问的缓冲。
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct IoRequest {
    pub route: CompletionRoute,
    pub operation: IoOperation,
}
#[derive(Debug)]
pub enum IoOperation {
    Open {
        path: PathBuf,
        create_new: bool,
    },
    Read {
        file: FileId,
        offset: u64,
        buffer: AlignedBuffer,
    },
    Write {
        file: FileId,
        offset: u64,
        buffer: AlignedBuffer,
    },
    SyncFile {
        file: FileId,
        metadata: bool,
    },
    SetLen {
        file: FileId,
        length: u64,
    },
    CreateDirectory(PathBuf),
    SyncDirectory(PathBuf),
    Rename {
        source: PathBuf,
        destination: PathBuf,
    },
    RemoveFile(PathBuf),
    Close(FileId),
    Cancel(IoId),
}
#[derive(Debug)]
pub struct RejectedIo {
    pub request: IoRequest,
    pub reason: Error,
}
#[derive(Debug)]
pub struct IoCompletion {
    pub id: IoId,
    pub route: CompletionRoute,
    pub result: Result<IoOutcome, Error>,
    /// 即使请求失败，也必须归还它拥有的读写缓冲。
    pub buffer: Option<AlignedBuffer>,
}
#[derive(Debug)]
pub enum IoOutcome {
    Opened(FileId),
    Transferred(usize),
    Done,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[allow(dead_code)] // P4.2 工作线程队列正在接入。
mod local_files;
