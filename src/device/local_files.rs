//! Root directory capabilities and file handle tables used by worker threads;Session callbacks are not executed.
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
    // Do not clone lock handle;Explicitly unlock when closing,avoid concurrency fork Temporary inheritance extends lock life.
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
            .map_err(|_| Error::InvalidState("file_table_lock_poisoned"))?;
        match files.open.get(&id.slot) {
            Some(Handle::Data(file)) => Ok(file.clone()),
            Some(Handle::Lock { .. }) => Err(Error::InvalidState(
                "a lock handle cannot be used for data I/O",
            )),
            None => Err(Error::RangeTruncated),
        }
    }
    /// Called after all worker threads have exited;Retained device objects should also not continue to hold operating system file locks.
    pub fn close_all(&self) -> Result<(), Error> {
        let mut files = self
            .files
            .lock()
            .map_err(|_| Error::InvalidState("file_table_lock_poisoned"))?;
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
                    .map_err(|_| Error::InvalidState("file_table_lock_poisoned"))?;
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
                // The lock file is a fixed arbitration object;Symbolic links may not point to material that would be replaced by another protocol.
                match self.root.symlink_metadata(&path) {
                    Ok(metadata) if !metadata.is_file() => {
                        return Err(Error::InvalidFormat(
                            "The lock path is not an ordinary file",
                        ));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                let mut files = self
                    .files
                    .lock()
                    .map_err(|_| Error::InvalidState("file_table_lock_poisoned"))?;
                if files.open.len() >= self.limit {
                    return Err(Error::CapacityExceeded);
                }
                let next = files.next.checked_add(1).ok_or(Error::CapacityExceeded)?;
                let mut options = OpenOptions::new();
                options.read(true).write(true).create(true);
                let file = self.root.open_with(path, &options)?.into_std();
                if !file.metadata()?.is_file() {
                    return Err(Error::InvalidFormat(
                        "the lock target is not a regular file",
                    ));
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
                    .map_err(|_| Error::InvalidState("file_table_lock_poisoned"))?;
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
                    return Err(Error::InvalidFormat(
                        "The synchronization target is not a directory",
                    ));
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
            IoOperation::Cancel(_) => Err(Error::InvalidState(
                "Cancellation should be handled by the request queue",
            )),
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
        return Err(Error::InvalidFormat(
            "File paths must be relative to the device root directory",
        ));
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
    fn actual_offset_read_and_write_synchronization_reopening_and_out_of_bound_path_rejection() {
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
                path: "data".into(),
                create_new: true,
            },
        )
        .result
        .unwrap() else {
            panic!("file open")
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
                path: "data".into(),
                create_new: false,
            },
        )
        .result
        .unwrap() else {
            panic!("File reopen")
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
        for path in ["../escape", "/Absolutely"] {
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
        std::os::unix::fs::symlink(std::env::temp_dir(), root.join("link")).unwrap();
        assert!(
            execute(
                &files,
                IoOperation::Open {
                    path: "link/should not be created".into(),
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
    fn close_and_device_cleanup_explicitly_release_the_original_lock_while_the_descriptor_copy_is_still_alive()
     {
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
                panic!("File lock not obtained")
            };
            id
        };
        let duplicate = |files: &LocalFiles, id: FileId| {
            let table = files.files.lock().unwrap();
            let Handle::Lock { file } = table.open.get(&id.slot).unwrap() else {
                panic!("Handle is not a file lock")
            };
            file.try_clone().unwrap()
        };
        for shutdown in [false, true] {
            let id = acquire(&first);
            // Simulation fork or the copy handle still retains the same open file description;Close must be unlocked actively.
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
