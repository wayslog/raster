//! Bounded,by poll Advance memory device;No process restart or power-off persistence guarantee is provided.
use super::*;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Mutex,
};
#[derive(Clone, Debug, Default)]
pub struct MemoryDeviceFactory;
impl DeviceFactory for MemoryDeviceFactory {
    fn open(&self, _options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(MemoryDevice::new(1024, 64 * 1024 * 1024)?))
    }
}
/// Bind to the next successfully accepted request,Rejection does not consume the fault.
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
    files: BTreeMap<PathBuf, u64>,
    objects: BTreeMap<u64, Vec<u8>>,
    directories: BTreeSet<PathBuf>,
    handles: BTreeMap<u64, u64>,
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
                reason: "Queue and byte capacity must be non-zero",
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
                objects: BTreeMap::new(),
                directories: BTreeSet::new(),
                handles: BTreeMap::new(),
            }),
        })
    }
    pub fn inject_next(&self, fault: MemoryFault) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("memory_device_lock_poisoned"))?;
        if state.fault.is_some() {
            return Err(Error::Busy);
        }
        state.fault = Some(fault);
        Ok(())
    }
    /// Reverse execution queue for testing,Used to deterministically create chaos;User business callbacks will not be executed.
    pub fn set_reverse(&self, reverse: bool) -> Result<(), Error> {
        self.state
            .lock()
            .map_err(|_| Error::InvalidState("memory_device_lock_poisoned"))?
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
                valid_path(&path)?;
                parent_exists(state, &path)?;
                if state.directories.contains(&path) {
                    return Err(Error::InvalidFormat("path is directory"));
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
                    || (!state.files.contains_key(&path) && state.objects.len() >= self.capacity)
                {
                    return Err(Error::CapacityExceeded);
                }
                let id = state.next_file;
                let next = id.checked_add(1).ok_or(Error::CapacityExceeded)?;
                let object = if let Some(object) = state.files.get(&path) {
                    *object
                } else {
                    state.objects.insert(id, Vec::new());
                    state.files.insert(path, id);
                    id
                };
                state.handles.insert(id, object);
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
                    let data = state.objects.get(&path).ok_or(Error::RangeTruncated)?;
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
                    let path = handle(state, file)?;
                    let offset = usize::try_from(offset).map_err(|_| Error::CapacityExceeded)?;
                    let transferred = buffer.len().min(limit);
                    if transferred == 0 {
                        return Ok(IoOutcome::Transferred(0));
                    }
                    let end = offset
                        .checked_add(transferred)
                        .ok_or(Error::CapacityExceeded)?;
                    let old = state.objects.get(&path).ok_or(Error::RangeTruncated)?.len();
                    self.budget(state, end.saturating_sub(old))?;
                    let data = state.objects.get_mut(&path).expect("File checked");
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
                let path = handle(state, file)?;
                let len = usize::try_from(length).map_err(|_| Error::CapacityExceeded)?;
                let old = state.objects.get(&path).ok_or(Error::RangeTruncated)?.len();
                self.budget(state, len.saturating_sub(old))?;
                let data = state.objects.get_mut(&path).expect("File checked");
                if len > old {
                    data.try_reserve(len - old)
                        .map_err(|_| Error::OutOfMemory)?;
                }
                data.resize(len, 0);
                Ok(IoOutcome::Done)
            }
            IoOperation::Close(file) => {
                let object = handle(state, file)?;
                state.handles.remove(&file.slot);
                collect_object(state, object);
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
            IoOperation::CreateDirectory(path) => {
                valid_path(&path)?;
                parent_exists(state, &path)?;
                if state.files.contains_key(&path) {
                    return Err(Error::InvalidFormat("Directory path is occupied by a file"));
                }
                if !state.directories.contains(&path) && state.directories.len() >= self.capacity {
                    return Err(Error::CapacityExceeded);
                }
                state.directories.insert(path);
                Ok(IoOutcome::Done)
            }
            IoOperation::SyncDirectory(path) => {
                if !path.as_os_str().is_empty() {
                    valid_path(&path)?;
                    if !state.directories.contains(&path) {
                        return Err(Error::Io(std::io::Error::from(
                            std::io::ErrorKind::NotFound,
                        )));
                    }
                }
                Err(Error::UnsupportedDurability)
            }
            IoOperation::Rename {
                source,
                destination,
            } => {
                valid_path(&source)?;
                valid_path(&destination)?;
                parent_exists(state, &destination)?;
                if state.directories.contains(&destination) {
                    return Err(Error::InvalidFormat("The target path is a directory"));
                }
                let object = *state
                    .files
                    .get(&source)
                    .ok_or(Error::Io(std::io::Error::from(
                        std::io::ErrorKind::NotFound,
                    )))?;
                if source != destination {
                    state.files.remove(&source);
                    let old = state.files.insert(destination, object);
                    if let Some(old) = old {
                        collect_object(state, old);
                    }
                }
                Ok(IoOutcome::Done)
            }
            IoOperation::RemoveFile(path) => {
                valid_path(&path)?;
                let object = state
                    .files
                    .remove(&path)
                    .ok_or(Error::Io(std::io::Error::from(
                        std::io::ErrorKind::NotFound,
                    )))?;
                collect_object(state, object);
                Ok(IoOutcome::Done)
            }
            IoOperation::TryLock { .. } => Err(Error::UnsupportedDurability),
            IoOperation::ReadDirectory {
                path,
                max_entries,
                max_name_bytes,
            } => {
                if !path.as_os_str().is_empty() {
                    valid_path(&path)?;
                    if !state.directories.contains(&path) {
                        return Err(Error::Io(std::io::Error::from(
                            std::io::ErrorKind::NotFound,
                        )));
                    }
                }
                let mut result = metadata::DirectorySnapshot::new(max_entries, max_name_bytes);
                for (name, kind) in state
                    .files
                    .keys()
                    .map(|name| (name, DirectoryEntryKind::File))
                    .chain(
                        state
                            .directories
                            .iter()
                            .map(|name| (name, DirectoryEntryKind::Directory)),
                    )
                {
                    if name.parent() == Some(path.as_path()) {
                        result.push(DirectoryEntry {
                            name: name
                                .file_name()
                                .expect("Memory path verified")
                                .to_os_string(),
                            kind,
                        })?;
                    }
                }
                Ok(result.finish())
            }
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
            .objects
            .values()
            .try_fold(0usize, |n, data| n.checked_add(data.len()))
            .ok_or(Error::CapacityExceeded)?;
        if used.checked_add(growth).is_none_or(|n| n > self.max_bytes) {
            return Err(Error::CapacityExceeded);
        }
        Ok(())
    }
}
fn handle(state: &State, file: FileId) -> Result<u64, Error> {
    if file.generation != Generation(0) {
        return Err(Error::RangeTruncated);
    }
    state
        .handles
        .get(&file.slot)
        .copied()
        .ok_or(Error::RangeTruncated)
}
fn valid_path(path: &std::path::Path) -> Result<(), Error> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|p| !matches!(p, std::path::Component::Normal(_)))
    {
        return Err(Error::InvalidFormat(
            "The memory path must be a canonical relative path",
        ));
    }
    Ok(())
}
fn parent_exists(state: &State, path: &std::path::Path) -> Result<(), Error> {
    if path
        .parent()
        .is_some_and(|parent| !parent.as_os_str().is_empty() && !state.directories.contains(parent))
    {
        return Err(Error::Io(std::io::Error::from(
            std::io::ErrorKind::NotFound,
        )));
    }
    Ok(())
}
fn collect_object(state: &mut State, object: u64) {
    if !state.files.values().any(|id| *id == object)
        && !state.handles.values().any(|id| *id == object)
    {
        state.objects.remove(&object);
    }
}
impl Device for MemoryDevice {
    fn capabilities(&self) -> DeviceCapabilities {
        DeviceCapabilities {
            supports_files: true,
            memory_alignment: 1,
            transfer_alignment: 1,
            supports_file_sync: false,
            supports_directory_sync: false,
            supports_atomic_publish: false,
            supports_directory_listing: true,
            supports_file_locks: false,
        }
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return Err(RejectedIo {
                    request,
                    reason: Error::InvalidState("memory_device_lock_poisoned"),
                });
            }
        };
        if state.closed || state.pending.len() + state.ready.len() >= self.capacity {
            return Err(RejectedIo {
                request,
                reason: if state.closed {
                    Error::InvalidState("Device is turned off")
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
            .map_err(|_| Error::InvalidState("memory_device_lock_poisoned"))?;
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
            .map_err(|_| Error::InvalidState("memory_device_lock_poisoned"))?;
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
            .expect("Queue is not empty");
            let completed = self.execute(&mut state, queued);
            state.ready.push_back(completed);
        }
        Ok(())
    }
}
