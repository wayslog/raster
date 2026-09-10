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
/// 绑定到下一次成功接受的请求，拒绝不会消耗该故障。
#[derive(Clone, Copy, Debug)]
pub enum MemoryFault {
    Fail(std::io::ErrorKind),
    Short(usize),
}
struct Queued {
    fault: Option<MemoryFault>,
    cancelled: bool,
    id: IoId,
    request: IoRequest,
}
struct State {
    closed: bool,
    fault: Option<MemoryFault>,
    reverse: bool,
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
                fault: None,
                reverse: false,
                next_io: 0,
                next_file: 0,
                pending: VecDeque::new(),
                ready: VecDeque::new(),
                files: BTreeMap::new(),
                handles: BTreeMap::new(),
            }),
        })
    }
    pub fn inject_next(&self, fault: MemoryFault) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("内存设备锁中毒"))?;
        if state.fault.is_some() {
            return Err(Error::Busy);
        }
        state.fault = Some(fault);
        Ok(())
    }
    /// 测试用反向执行队列，用于确定地制造乱序；不会执行用户业务回调。
    pub fn set_reverse(&self, reverse: bool) -> Result<(), Error> {
        self.state
            .lock()
            .map_err(|_| Error::InvalidState("内存设备锁中毒"))?
            .reverse = reverse;
        Ok(())
    }
    fn execute(&self, state: &mut State, queued: Queued) -> IoCompletion {
        let IoRequest { route, operation } = queued.request;
        let fault = if queued.cancelled {
            Some(std::io::ErrorKind::Interrupted)
        } else {
            match queued.fault {
                Some(MemoryFault::Fail(kind)) => Some(kind),
                _ => None,
            }
        };
        if let Some(kind) = fault {
            let buffer = match operation {
                IoOperation::Read { buffer, .. } | IoOperation::Write { buffer, .. } => {
                    Some(buffer)
                }
                _ => None,
            };
            return IoCompletion {
                id: queued.id,
                route,
                result: Err(Error::Io(std::io::Error::from(kind))),
                buffer,
            };
        }
        let limit = match queued.fault {
            Some(MemoryFault::Short(limit)) => limit,
            _ => usize::MAX,
        };
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
                    let len = buffer
                        .len()
                        .min(limit)
                        .min(data.len().saturating_sub(offset));
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
                    let transferred = buffer.len().min(limit);
                    if transferred == 0 {
                        return Ok(IoOutcome::Transferred(0));
                    }
                    let end = offset
                        .checked_add(transferred)
                        .ok_or(Error::CapacityExceeded)?;
                    let old = state.files.get(&path).ok_or(Error::RangeTruncated)?.len();
                    self.budget(state, end.saturating_sub(old))?;
                    let data = state.files.get_mut(&path).expect("文件已检查");
                    if end > old {
                        data.try_reserve(end - old)
                            .map_err(|_| Error::OutOfMemory)?;
                        data.resize(end, 0);
                    }
                    data[offset..end].copy_from_slice(&buffer.as_slice()[..transferred]);
                    Ok(IoOutcome::Transferred(transferred))
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
            IoOperation::Cancel(target) => {
                if let Some(pending) = state.pending.iter_mut().find(|p| p.id == target) {
                    pending.cancelled = true;
                }
                Ok(IoOutcome::Done)
            }
            _ => Err(Error::unimplemented("memory::命名空间")),
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
        let fault = state.fault.take();
        state.pending.push_back(Queued {
            id,
            request,
            fault,
            cancelled: false,
        });
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
            } else if let Some(queued) = if state.reverse {
                state.pending.pop_back()
            } else {
                state.pending.pop_front()
            } {
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
            let queued = if state.reverse {
                state.pending.pop_back()
            } else {
                state.pending.pop_front()
            }
            .expect("队列非空");
            let completed = self.execute(&mut state, queued);
            state.ready.push_back(completed);
        }
        Ok(())
    }
}
