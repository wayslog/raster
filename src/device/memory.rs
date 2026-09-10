//! 有界、由 poll 推进的内存设备；不提供进程重启或掉电持久化保证。
use super::*;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};
#[derive(Clone, Debug, Default)]
pub struct MemoryDeviceFactory;
impl DeviceFactory for MemoryDeviceFactory {
    fn open(&self, _options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(MemoryDevice::new(1024, 64 * 1024 * 1024)?))
    }
}
struct Queued {
    id: IoId,
    request: IoRequest,
}
struct State {
    closed: bool,
    next_io: u64,
    next_file: u64,
    pending: VecDeque<Queued>,
    ready: VecDeque<IoCompletion>,
    files: BTreeMap<PathBuf, Vec<u8>>,
    handles: BTreeMap<u64, PathBuf>,
}
pub struct MemoryDevice {
    capacity: usize,
    max_bytes: usize,
    state: Mutex<State>,
}
impl MemoryDevice {
    pub fn new(capacity: usize, max_bytes: usize) -> Result<Self, Error> {
        if capacity == 0 || max_bytes == 0 {
            return Err(Error::InvalidConfig {
                field: "memory_device",
                reason: "队列和字节容量必须非零",
            });
        }
        Ok(Self {
            capacity,
            max_bytes,
            state: Mutex::new(State {
                closed: false,
                next_io: 0,
                next_file: 0,
                pending: VecDeque::new(),
                ready: VecDeque::new(),
                files: BTreeMap::new(),
                handles: BTreeMap::new(),
            }),
        })
    }
    fn execute(&self, state: &mut State, queued: Queued) -> IoCompletion {
        let IoRequest { route, operation } = queued.request;
        let mut returned = None;
        let result = (|| match operation {
            IoOperation::Open { path, create_new } => {
                if path.as_os_str().is_empty()
                    || path.is_absolute()
                    || path
                        .components()
                        .any(|p| !matches!(p, std::path::Component::Normal(_)))
                {
                    return Err(Error::InvalidFormat("内存文件路径须为规范相对路径"));
                }
                if create_new && state.files.contains_key(&path) {
                    return Err(Error::Io(std::io::Error::from(
                        std::io::ErrorKind::AlreadyExists,
                    )));
                }
                if !create_new && !state.files.contains_key(&path) {
                    return Err(Error::Io(std::io::Error::from(
                        std::io::ErrorKind::NotFound,
                    )));
                }
                if state.handles.len() >= self.capacity
                    || (!state.files.contains_key(&path) && state.files.len() >= self.capacity)
                {
                    return Err(Error::CapacityExceeded);
                }
                let id = state.next_file;
                let next = id.checked_add(1).ok_or(Error::CapacityExceeded)?;
                state.files.entry(path.clone()).or_default();
                state.handles.insert(id, path);
                state.next_file = next;
                Ok(IoOutcome::Opened(FileId {
                    slot: id,
                    generation: Generation(0),
                }))
            }
            IoOperation::Read {
                file,
                offset,
                mut buffer,
            } => {
                let result = (|| {
                    let path = handle(state, file)?;
                    let data = state.files.get(path).ok_or(Error::RangeTruncated)?;
                    let offset = usize::try_from(offset).map_err(|_| Error::CapacityExceeded)?;
                    let len = buffer.len().min(data.len().saturating_sub(offset));
                    if len > 0 {
                        buffer.as_mut_slice()[..len].copy_from_slice(&data[offset..offset + len]);
                    }
                    Ok(IoOutcome::Transferred(len))
                })();
                returned = Some(buffer);
                result
            }
            IoOperation::Write {
                file,
                offset,
                buffer,
            } => {
                let result = (|| {
                    let path = handle(state, file)?.clone();
                    let offset = usize::try_from(offset).map_err(|_| Error::CapacityExceeded)?;
                    let end = offset
                        .checked_add(buffer.len())
                        .ok_or(Error::CapacityExceeded)?;
                    let old = state.files.get(&path).ok_or(Error::RangeTruncated)?.len();
                    self.budget(state, end.saturating_sub(old))?;
                    let data = state.files.get_mut(&path).expect("文件已检查");
                    if end > old {
                        data.try_reserve(end - old)
                            .map_err(|_| Error::OutOfMemory)?;
                        data.resize(end, 0);
                    }
                    data[offset..end].copy_from_slice(buffer.as_slice());
                    Ok(IoOutcome::Transferred(buffer.len()))
                })();
                returned = Some(buffer);
                result
            }
            IoOperation::SetLen { file, length } => {
                let path = handle(state, file)?.clone();
                let len = usize::try_from(length).map_err(|_| Error::CapacityExceeded)?;
                let old = state.files.get(&path).ok_or(Error::RangeTruncated)?.len();
                self.budget(state, len.saturating_sub(old))?;
                let data = state.files.get_mut(&path).expect("文件已检查");
                if len > old {
                    data.try_reserve(len - old)
                        .map_err(|_| Error::OutOfMemory)?;
                }
                data.resize(len, 0);
                Ok(IoOutcome::Done)
            }
            IoOperation::Close(file) => {
                handle(state, file)?;
                state.handles.remove(&file.slot);
                Ok(IoOutcome::Done)
            }
            IoOperation::SyncFile { file, .. } => {
                handle(state, file)?;
                Err(Error::UnsupportedDurability)
            }
            _ => Err(Error::unimplemented("memory::命名空间与取消")),
        })();
        IoCompletion {
            id: queued.id,
            route,
            result,
            buffer: returned,
        }
    }
    fn budget(&self, state: &State, growth: usize) -> Result<(), Error> {
        let used = state
            .files
            .values()
            .try_fold(0usize, |n, data| n.checked_add(data.len()))
            .ok_or(Error::CapacityExceeded)?;
        if used.checked_add(growth).is_none_or(|n| n > self.max_bytes) {
            return Err(Error::CapacityExceeded);
        }
        Ok(())
    }
}
fn handle(state: &State, file: FileId) -> Result<&PathBuf, Error> {
    if file.generation != Generation(0) {
        return Err(Error::RangeTruncated);
    }
    state.handles.get(&file.slot).ok_or(Error::RangeTruncated)
}
impl Device for MemoryDevice {
    fn capabilities(&self) -> DeviceCapabilities {
        DeviceCapabilities {
            memory_alignment: 1,
            transfer_alignment: 1,
            supports_file_sync: false,
            supports_directory_sync: false,
            supports_atomic_publish: false,
        }
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return Err(RejectedIo {
                    request,
                    reason: Error::InvalidState("内存设备锁中毒"),
                });
            }
        };
        if state.closed || state.pending.len() + state.ready.len() >= self.capacity {
            return Err(RejectedIo {
                request,
                reason: if state.closed {
                    Error::InvalidState("设备已关闭")
                } else {
                    Error::Busy
                },
            });
        }
        let Some(next) = state.next_io.checked_add(1) else {
            return Err(RejectedIo {
                request,
                reason: Error::CapacityExceeded,
            });
        };
        if state.pending.try_reserve(1).is_err() {
            return Err(RejectedIo {
                request,
                reason: Error::OutOfMemory,
            });
        }
        let id = IoId(state.next_io);
        state.next_io = next;
        state.pending.push_back(Queued { id, request });
        Ok(id)
    }
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("内存设备锁中毒"))?;
        let count = budget.0.get().min(state.pending.len() + state.ready.len());
        output.try_reserve(count).map_err(|_| Error::OutOfMemory)?;
        for _ in 0..count {
            if let Some(completion) = state.ready.pop_front() {
                output.push(completion);
            } else if let Some(queued) = state.pending.pop_front() {
                output.push(self.execute(&mut state, queued));
            }
        }
        Ok(())
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("内存设备锁中毒"))?;
        state.closed = true;
        while !state.pending.is_empty() {
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            state.ready.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
            let queued = state.pending.pop_front().expect("队列非空");
            let completed = self.execute(&mut state, queued);
            state.ready.push_back(completed);
        }
        Ok(())
    }
}
