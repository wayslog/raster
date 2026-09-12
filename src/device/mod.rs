//! Owned device interface;Do not accept Schema,User callback or session action context.

mod buffer;
pub mod memory;
mod metadata;
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
    /// Whether to support basic file reading and writing;Separate from persistent synchronization capabilities.
    pub supports_files: bool,
    pub memory_alignment: usize,
    pub transfer_alignment: usize,
    pub supports_file_sync: bool,
    pub supports_directory_sync: bool,
    pub supports_atomic_publish: bool,
    /// Enumerable directories by number of entries and name byte budget,Result owns data and concurrent snapshots are not guaranteed.
    pub supports_directory_listing: bool,
    /// Independently open cross-instance file locks on handles;Cannot be faked with in-process mutex.
    pub supports_file_locks: bool,
}

pub trait DeviceFactory: Send + Sync + 'static {
    /// Create a device that has not yet been exposed to external services;Implementations must report true capabilities.
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error>;
}
pub trait Device: Send + Sync + 'static {
    fn capabilities(&self) -> DeviceCapabilities;
    /// Return the entire request on rejection;After acceptance, the device is responsible for finalizing and returning the buffer.
    #[allow(
        clippy::result_large_err,
        reason = "In the event of rejection, the request must be returned intact,Avoid reallocation on insufficient paths"
    )]
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo>;
    /// Global errors cannot swallow the final event of requests in transit.
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error>;
    /// Timeouts cannot prematurely release buffers that are still accessible to the kernel..
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
    /// An empty path represents the device root directory;Overall failure beyond either budget,Do not return incomplete directories.
    ReadDirectory {
        path: PathBuf,
        max_entries: usize,
        max_name_bytes: usize,
    },
    /// Open or create a lock file without truncation and attempt to lock it once;
    /// contention is returned as Busy through completion.
    /// Return successfully Locked,The handle must be retained until Close;Lock files cannot be deleted or replaced within the agreement.
    TryLock {
        path: PathBuf,
        mode: FileLockMode,
    },
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
    /// Even if the request fails,It must also return the read and write buffers it owns.
    pub buffer: Option<AlignedBuffer>,
}
#[derive(Debug)]
pub enum IoOutcome {
    Opened(FileId),
    /// Independent lock handles can only be closed,cannot be used for data I/O;Device successful shutdown will also release it.
    Locked(FileId),
    Directory(Vec<DirectoryEntry>),
    Transferred(usize),
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileLockMode {
    Shared,
    Exclusive,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectoryEntryKind {
    File,
    Directory,
    Symlink,
    Other,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// single file name,Reserve non UTF-8 bytes;Does not contain parent path,Also doesn't follow symlinks.
    pub name: std::ffi::OsString,
    pub kind: DirectoryEntryKind,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod local_files;
