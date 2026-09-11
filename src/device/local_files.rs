//! 工作线程使用的根目录能力与文件句柄表；不执行会话回调。
use super::*;
use cap_std::{
    ambient_authority,
    fs::{Dir, OpenOptions},
};
use std::{
    collections::BTreeMap,
    fs::File,
    os::unix::fs::FileExt,
    sync::{Arc, Mutex},
};
struct Files {
    next: u64,
    open: BTreeMap<u64, Handle>,
}
enum Handle {
    Data(Arc<File>),
    // 不克隆锁句柄；关闭时显式解锁，避免并发 fork 的临时继承延长锁寿命。
    Lock { file: File },
}
impl Handle {
    fn unlock(&self) -> Result<(), Error> {
        if let Self::Lock { file } = self {
            file.unlock()?;
        }
        Ok(())
    }
}
pub(super) struct LocalFiles {
    root: Dir,
    files: Mutex<Files>,
    limit: usize,
}
impl LocalFiles {
    pub fn new(options: DeviceOpenOptions, limit: usize) -> Result<Self, Error> {
        if limit == 0 {
            return Err(Error::CapacityExceeded);
        }
        if options.create_new {
            std::fs::create_dir_all(&options.root)?;
        }
        let root = Dir::open_ambient_dir(&options.root, ambient_authority())?;
        Ok(Self {
            root,
            files: Mutex::new(Files {
                next: 0,
                open: BTreeMap::new(),
            }),
            limit,
        })
    }
    fn file(&self, id: FileId) -> Result<Arc<File>, Error> {
        if id.generation != Generation(0) {
            return Err(Error::RangeTruncated);
        }
        let files = self
            .files
            .lock()
            .map_err(|_| Error::InvalidState("文件表锁中毒"))?;
        match files.open.get(&id.slot) {
            Some(Handle::Data(file)) => Ok(file.clone()),
            Some(Handle::Lock { .. }) => Err(Error::InvalidState("锁句柄不能用于数据 I/O")),
            None => Err(Error::RangeTruncated),
        }
    }
    /// 工作线程全部退出后调用；保留设备对象也不应继续占有操作系统文件锁。
    pub fn close_all(&self) -> Result<(), Error> {
        let mut files = self
            .files
            .lock()
            .map_err(|_| Error::InvalidState("文件表锁中毒"))?;
        while let Some((&id, handle)) = files.open.first_key_value() {
            handle.unlock()?;
            files.open.remove(&id);
        }
        Ok(())
    }
    pub fn execute(&self, id: IoId, request: IoRequest) -> IoCompletion {
        let IoRequest { route, operation } = request;
        let mut returned = None;
        let result = (|| match operation {
            IoOperation::Open { path, create_new } => {
                valid(&path)?;
                let mut files = self
                    .files
                    .lock()
                    .map_err(|_| Error::InvalidState("文件表锁中毒"))?;
                if files.open.len() >= self.limit {
                    return Err(Error::CapacityExceeded);
                }
                let next = files.next.checked_add(1).ok_or(Error::CapacityExceeded)?;
                let mut options = OpenOptions::new();
                options.read(true).write(true).create_new(create_new);
                let file = self.root.open_with(path, &options)?.into_std();
                let id = FileId {
                    slot: files.next,
                    generation: Generation(0),
                };
                files.next = next;
                files.open.insert(id.slot, Handle::Data(Arc::new(file)));
                Ok(IoOutcome::Opened(id))
            }
            IoOperation::TryLock { path, mode } => {
                valid(&path)?;
                // 锁文件是固定仲裁对象；不能通过符号链接指向会被其他协议替换的材料。
                match self.root.symlink_metadata(&path) {
                    Ok(metadata) if !metadata.is_file() => {
                        return Err(Error::InvalidFormat("锁路径不是普通文件"));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                let mut files = self
                    .files
                    .lock()
                    .map_err(|_| Error::InvalidState("文件表锁中毒"))?;
                if files.open.len() >= self.limit {
                    return Err(Error::CapacityExceeded);
                }
                let next = files.next.checked_add(1).ok_or(Error::CapacityExceeded)?;
                let mut options = OpenOptions::new();
                options.read(true).write(true).create(true);
                let file = self.root.open_with(path, &options)?.into_std();
                if !file.metadata()?.is_file() {
                    return Err(Error::InvalidFormat("锁目标不是普通文件"));
                }
                let result = match mode {
                    FileLockMode::Shared => file.try_lock_shared(),
                    FileLockMode::Exclusive => file.try_lock(),
                };
                result.map_err(|error| match error {
                    std::fs::TryLockError::WouldBlock => Error::Busy,
                    std::fs::TryLockError::Error(error) => Error::Io(error),
                })?;
                let id = FileId {
                    slot: files.next,
                    generation: Generation(0),
                };
                files.next = next;
                files.open.insert(id.slot, Handle::Lock { file });
                Ok(IoOutcome::Locked(id))
            }
            IoOperation::ReadDirectory {
                path,
                max_entries,
                max_name_bytes,
            } => {
                let entries = if path.as_os_str().is_empty() {
                    self.root.entries()?
                } else {
                    valid(&path)?;
                    self.root.read_dir(path)?
                };
                let mut result = metadata::DirectorySnapshot::new(max_entries, max_name_bytes);
                for entry in entries {
                    let entry = entry?;
                    let kind = entry.file_type()?;
                    let kind = if kind.is_symlink() {
                        DirectoryEntryKind::Symlink
                    } else if kind.is_file() {
                        DirectoryEntryKind::File
                    } else if kind.is_dir() {
                        DirectoryEntryKind::Directory
                    } else {
                        DirectoryEntryKind::Other
                    };
                    result.push(DirectoryEntry {
                        name: entry.file_name(),
                        kind,
                    })?;
                }
                Ok(result.finish())
            }
            IoOperation::Read {
                file,
                offset,
                mut buffer,
            } => {
                let result = (|| {
                    offset
                        .checked_add(buffer.len() as u64)
                        .ok_or(Error::CapacityExceeded)?;
                    let file = self.file(file)?;
                    loop {
                        match file.read_at(buffer.as_mut_slice(), offset) {
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                            result => {
                                break result.map(IoOutcome::Transferred).map_err(Error::from);
                            }
                        }
                    }
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
                    offset
                        .checked_add(buffer.len() as u64)
                        .ok_or(Error::CapacityExceeded)?;
                    let file = self.file(file)?;
                    loop {
                        match file.write_at(buffer.as_slice(), offset) {
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                            result => {
                                break result.map(IoOutcome::Transferred).map_err(Error::from);
                            }
                        }
                    }
                })();
                returned = Some(buffer);
                result
            }
            IoOperation::SetLen { file, length } => {
                self.file(file)?.set_len(length)?;
                Ok(IoOutcome::Done)
            }
            IoOperation::SyncFile { file, metadata } => {
                let file = self.file(file)?;
                if metadata {
                    file.sync_all()?;
                } else {
                    file.sync_data()?;
                }
                Ok(IoOutcome::Done)
            }
            IoOperation::Close(file) => {
                if file.generation != Generation(0) {
                    return Err(Error::RangeTruncated);
                }
                let mut files = self
                    .files
                    .lock()
                    .map_err(|_| Error::InvalidState("文件表锁中毒"))?;
                files
                    .open
                    .get(&file.slot)
                    .ok_or(Error::RangeTruncated)?
                    .unlock()?;
                files.open.remove(&file.slot);
                Ok(IoOutcome::Done)
            }
            IoOperation::CreateDirectory(path) => {
                valid(&path)?;
                self.root.create_dir_all(path)?;
                Ok(IoOutcome::Done)
            }
            IoOperation::SyncDirectory(path) => {
                let directory = if path.as_os_str().is_empty() {
                    self.root.open(".")?
                } else {
                    valid(&path)?;
                    self.root.open(path)?
                };
                if !directory.metadata()?.is_dir() {
                    return Err(Error::InvalidFormat("同步目标不是目录"));
                }
                directory.sync_all()?;
                Ok(IoOutcome::Done)
            }
            IoOperation::Rename {
                source,
                destination,
            } => {
                valid(&source)?;
                valid(&destination)?;
                self.root.rename(source, &self.root, destination)?;
                Ok(IoOutcome::Done)
            }
            IoOperation::RemoveFile(path) => {
                valid(&path)?;
                self.root.remove_file(path)?;
                Ok(IoOutcome::Done)
            }
            IoOperation::Cancel(_) => Err(Error::InvalidState("取消应由请求队列处理")),
        })();
        IoCompletion {
            id,
            route,
            result,
            buffer: returned,
        }
    }
}
fn valid(path: &std::path::Path) -> Result<(), Error> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|p| !matches!(p, std::path::Component::Normal(_)))
    {
        return Err(Error::InvalidFormat("文件路径须相对于设备根目录"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn execute(files: &LocalFiles, op: IoOperation) -> IoCompletion {
        files.execute(
            IoId(1),
            IoRequest {
                route: CompletionRoute(2),
                operation: op,
            },
        )
    }
    #[test]
    fn 实际偏移读写同步重开与越界路径拒绝() {
        let root = std::env::temp_dir().join(format!(
            "raster-local-{}",
            crate::types::StoreId::generate()
                .unwrap()
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ));
        let files = LocalFiles::new(
            DeviceOpenOptions {
                root: root.clone(),
                create_new: true,
            },
            8,
        )
        .unwrap();
        let IoOutcome::Opened(file) = execute(
            &files,
            IoOperation::Open {
                path: "数据".into(),
                create_new: true,
            },
        )
        .result
        .unwrap() else {
            panic!("文件打开")
        };
        let mut buffer = AlignedBuffer::new_zeroed(3, 8).unwrap();
        buffer.as_mut_slice().copy_from_slice(b"abc");
        execute(
            &files,
            IoOperation::Write {
                file,
                offset: 2,
                buffer,
            },
        )
        .result
        .unwrap();
        execute(
            &files,
            IoOperation::SyncFile {
                file,
                metadata: true,
            },
        )
        .result
        .unwrap();
        execute(&files, IoOperation::Close(file)).result.unwrap();
        let IoOutcome::Opened(file) = execute(
            &files,
            IoOperation::Open {
                path: "数据".into(),
                create_new: false,
            },
        )
        .result
        .unwrap() else {
            panic!("文件重开")
        };
        let done = execute(
            &files,
            IoOperation::Read {
                file,
                offset: 0,
                buffer: AlignedBuffer::new_zeroed(8, 8).unwrap(),
            },
        );
        assert!(matches!(done.result, Ok(IoOutcome::Transferred(5))));
        assert_eq!(&done.buffer.unwrap().as_slice()[..5], b"\0\0abc");
        for path in ["../逃逸", "/绝对"] {
            assert!(
                execute(
                    &files,
                    IoOperation::Open {
                        path: path.into(),
                        create_new: true
                    }
                )
                .result
                .is_err()
            );
        }
        std::os::unix::fs::symlink(std::env::temp_dir(), root.join("链接")).unwrap();
        assert!(
            execute(
                &files,
                IoOperation::Open {
                    path: "链接/不应创建".into(),
                    create_new: true
                }
            )
            .result
            .is_err()
        );
        execute(&files, IoOperation::SyncDirectory(PathBuf::new()))
            .result
            .unwrap();
        drop(files);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn 描述符副本仍存活时关闭及设备清理显式解除原锁() {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let root = Directory(std::env::temp_dir().join(format!(
            "raster-lock-clone-{:x?}",
            crate::types::StoreId::generate().unwrap().0
        )));
        let make = || {
            LocalFiles::new(
                DeviceOpenOptions {
                    root: root.0.clone(),
                    create_new: true,
                },
                8,
            )
            .unwrap()
        };
        let first = make();
        let second = make();
        let acquire = |files: &LocalFiles| {
            let IoOutcome::Locked(id) = execute(
                files,
                IoOperation::TryLock {
                    path: "lock".into(),
                    mode: FileLockMode::Exclusive,
                },
            )
            .result
            .unwrap() else {
                panic!("文件锁未取得")
            };
            id
        };
        let duplicate = |files: &LocalFiles, id: FileId| {
            let table = files.files.lock().unwrap();
            let Handle::Lock { file } = table.open.get(&id.slot).unwrap() else {
                panic!("句柄不是文件锁")
            };
            file.try_clone().unwrap()
        };
        for shutdown in [false, true] {
            let id = acquire(&first);
            // 模拟 fork 或复制句柄仍保留同一打开文件描述；关闭必须主动解锁。
            let duplicate = duplicate(&first, id);
            assert!(matches!(
                execute(
                    &second,
                    IoOperation::TryLock {
                        path: "lock".into(),
                        mode: FileLockMode::Exclusive
                    }
                )
                .result,
                Err(Error::Busy)
            ));
            if shutdown {
                first.close_all().unwrap();
            } else {
                execute(&first, IoOperation::Close(id)).result.unwrap();
            }
            let other = acquire(&second);
            execute(&second, IoOperation::Close(other)).result.unwrap();
            drop(duplicate);
        }
    }
}
