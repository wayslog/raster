//! Retain native metadata primitives required for collection release;Not successfully impersonating checkpoint release with directory listing or lock.
#![cfg(any(target_os = "linux", target_os = "macos"))]
use raster::{device::*, types::*};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(10))
}
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "raster-metadata-{:x?}",
            StoreId::generate().unwrap().0
        )))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn native(path: &Path) -> Box<dyn Device> {
    thread_pool::ThreadPoolDeviceFactory {
        workers: 2,
        queue_capacity: 8,
    }
    .open(DeviceOpenOptions {
        root: path.to_path_buf(),
        create_new: true,
    })
    .unwrap()
}
fn take(device: &dyn Device, id: IoId) -> IoCompletion {
    let until = deadline();
    loop {
        assert!(!until.expired(), "Device completion timeout");
        let mut out = Vec::new();
        device.poll(PollBudget::default(), &mut out).unwrap();
        if let Some(done) = out.pop() {
            assert!(out.is_empty());
            assert_eq!(done.id, id);
            assert_eq!(done.route, CompletionRoute(77));
            return done;
        }
        std::thread::yield_now();
    }
}
fn execute(device: &dyn Device, operation: IoOperation) -> Result<IoOutcome, Error> {
    let id = device
        .submit(IoRequest {
            route: CompletionRoute(77),
            operation,
        })
        .unwrap();
    let done = take(device, id);
    assert!(done.buffer.is_none());
    done.result
}
fn close(device: &dyn Device, file: FileId) {
    assert!(matches!(
        execute(device, IoOperation::Close(file)),
        Ok(IoOutcome::Done)
    ));
}
fn create(device: &dyn Device, path: PathBuf) {
    let IoOutcome::Opened(file) = execute(
        device,
        IoOperation::Open {
            path,
            create_new: true,
        },
    )
    .unwrap() else {
        panic!("File not open")
    };
    close(device, file);
}
fn list(
    device: &dyn Device,
    path: &str,
    entries: usize,
    names: usize,
) -> Result<Vec<DirectoryEntry>, Error> {
    let outcome = execute(
        device,
        IoOperation::ReadDirectory {
            path: path.into(),
            max_entries: entries,
            max_name_bytes: names,
        },
    )?;
    match outcome {
        IoOutcome::Directory(entries) => Ok(entries),
        _ => panic!("Catalog result type error"),
    }
}
fn lock(device: &dyn Device, mode: FileLockMode) -> Result<FileId, Error> {
    match execute(
        device,
        IoOperation::TryLock {
            path: "catalog.lock".into(),
            mode,
        },
    )? {
        IoOutcome::Locked(file) => Ok(file),
        _ => panic!("File lock result type error"),
    }
}
#[test]
fn native_and_in_memory_directory_enumeration_fails_overall_with_reserved_names_and_over_budget() {
    use std::os::unix::ffi::OsStringExt;
    let root = Directory::new();
    for device in [
        native(&root.0),
        Box::new(memory::MemoryDevice::new(32, 4096).unwrap()) as Box<dyn Device>,
    ] {
        assert!(device.capabilities().supports_directory_listing);
        assert!(list(&*device, "", 0, 0).unwrap().is_empty());
        execute(
            &*device,
            IoOperation::CreateDirectory("subdirectory".into()),
        )
        .unwrap();
        create(&*device, "abc".into());
        let raw_name = OsString::from_vec(vec![0xff, b'x']);
        let name = match execute(
            &*device,
            IoOperation::Open {
                path: PathBuf::from(&raw_name),
                create_new: true,
            },
        ) {
            Ok(IoOutcome::Opened(file)) => {
                close(&*device, file);
                raw_name
            }
            // APFS reject illegal UTF-8 file name,Equipment must be transmitted as is,Instead of creating alias after replacing bytes.
            Err(Error::Io(error))
                if cfg!(target_os = "macos") && error.raw_os_error() == Some(92) =>
            {
                create(&*device, "legal name".into());
                OsString::from("legal name")
            }
            result => panic!("File name creation result error:{result:?}"),
        };
        create(&*device, "subdirectory/subfile".into());
        let mut expected = vec![
            DirectoryEntry {
                name: "abc".into(),
                kind: DirectoryEntryKind::File,
            },
            DirectoryEntry {
                name,
                kind: DirectoryEntryKind::File,
            },
            DirectoryEntry {
                name: "subdirectory".into(),
                kind: DirectoryEntryKind::Directory,
            },
        ];
        expected.sort_by(|a, b| a.name.cmp(&b.name));
        let bytes = expected
            .iter()
            .map(|entry| entry.name.as_encoded_bytes().len())
            .sum();
        assert_eq!(list(&*device, "", 3, bytes).unwrap(), expected);
        for (entries, names) in [(2, bytes), (3, bytes - 1), (0, bytes), (3, 0)] {
            assert!(matches!(
                list(&*device, "", entries, names),
                Err(Error::CapacityExceeded)
            ));
        }
        assert_eq!(
            list(&*device, "subdirectory", 1, 64).unwrap(),
            vec![DirectoryEntry {
                name: "subfile".into(),
                kind: DirectoryEntryKind::File
            }]
        );
        assert!(list(&*device, "missing", 10, 100).is_err());
        for path in ["../escape", "/Absolutely"] {
            assert!(list(&*device, path, 10, 100).is_err());
        }
        device.shutdown(deadline()).unwrap();
    }
    let memory = memory::MemoryDevice::new(4, 1024).unwrap();
    assert!(!memory.capabilities().supports_file_locks);
    assert!(matches!(
        lock(&memory, FileLockMode::Shared),
        Err(Error::UnsupportedDurability)
    ));
    assert!(list(&memory, "", 0, 0).unwrap().is_empty());
}
#[test]
fn the_enumeration_only_describes_symbolic_links_and_cannot_escape_from_the_root_directory_through_the_link()
 {
    let root = Directory::new();
    let outside = Directory::new();
    let device = native(&root.0);
    std::fs::create_dir_all(&outside.0).unwrap();
    std::fs::write(outside.0.join("external file"), b"outside").unwrap();
    std::os::unix::fs::symlink(&outside.0, root.0.join("link")).unwrap();
    assert_eq!(
        list(&*device, "", 1, 64).unwrap(),
        vec![DirectoryEntry {
            name: "link".into(),
            kind: DirectoryEntryKind::Symlink
        }]
    );
    assert!(list(&*device, "link", 10, 100).is_err());
    assert!(
        execute(
            &*device,
            IoOperation::TryLock {
                path: "link/external file".into(),
                mode: FileLockMode::Exclusive
            }
        )
        .is_err()
    );
    assert_eq!(
        std::fs::read(outside.0.join("external file")).unwrap(),
        b"outside"
    );
    create(&*device, "internal documents".into());
    std::os::unix::fs::symlink("internal documents", root.0.join("internal link")).unwrap();
    assert!(matches!(
        execute(
            &*device,
            IoOperation::TryLock {
                path: "internal link".into(),
                mode: FileLockMode::Shared,
            }
        ),
        Err(Error::InvalidFormat(_))
    ));
    device.shutdown(deadline()).unwrap();
}
#[test]
fn independent_handles_share_exclusive_mutual_exclusion_and_competition_does_not_exhaust_the_handle_table()
 {
    let root = Directory::new();
    let first = native(&root.0);
    let second = native(&root.0);
    assert!(first.capabilities().supports_file_locks);
    std::fs::write(
        root.0.join("catalog.lock"),
        "should not be truncated".as_bytes(),
    )
    .unwrap();
    let shared1 = lock(&*first, FileLockMode::Shared).unwrap();
    let shared2 = lock(&*first, FileLockMode::Shared).unwrap();
    let shared3 = lock(&*second, FileLockMode::Shared).unwrap();
    for _ in 0..32 {
        assert!(matches!(
            lock(&*second, FileLockMode::Exclusive),
            Err(Error::Busy)
        ));
    }
    close(&*first, shared1);
    close(&*first, shared2);
    assert!(matches!(
        lock(&*first, FileLockMode::Exclusive),
        Err(Error::Busy)
    ));
    close(&*second, shared3);
    let exclusive = lock(&*first, FileLockMode::Exclusive).unwrap();
    assert!(matches!(
        lock(&*first, FileLockMode::Shared),
        Err(Error::Busy)
    ));
    assert!(matches!(
        lock(&*second, FileLockMode::Shared),
        Err(Error::Busy)
    ));
    assert!(matches!(
        execute(
            &*first,
            IoOperation::SetLen {
                file: exclusive,
                length: 0
            }
        ),
        Err(Error::InvalidState(_))
    ));
    close(&*first, exclusive);
    assert!(matches!(
        execute(&*first, IoOperation::Close(exclusive)),
        Err(Error::RangeTruncated)
    ));
    let exclusive = lock(&*second, FileLockMode::Exclusive).unwrap();
    close(&*second, exclusive);
    assert_eq!(
        std::fs::read(root.0.join("catalog.lock")).unwrap(),
        "should not be truncated".as_bytes()
    );
    first.shutdown(deadline()).unwrap();
    second.shutdown(deadline()).unwrap();
}
#[test]
fn accepted_locks_are_held_until_closed_and_the_device_is_shut_down_to_release_unacquired_locks() {
    let root = Directory::new();
    let first = native(&root.0);
    let second = native(&root.0);
    let file = lock(&*first, FileLockMode::Exclusive).unwrap();
    assert!(matches!(
        lock(&*second, FileLockMode::Exclusive),
        Err(Error::Busy)
    ));
    close(&*first, file);
    let id = first
        .submit(IoRequest {
            route: CompletionRoute(77),
            operation: IoOperation::TryLock {
                path: "catalog.lock".into(),
                mode: FileLockMode::Exclusive,
            },
        })
        .unwrap();
    // shutdown Drain accepted locks,Close the file table again;first The object is still alive.
    first.shutdown(deadline()).unwrap();
    let result = take(&*first, id).result;
    assert!(
        matches!(result, Ok(IoOutcome::Locked(_))),
        "Turn off drain results:{result:?}"
    );
    let file = lock(&*second, FileLockMode::Exclusive).unwrap();
    close(&*second, file);
    first.shutdown(deadline()).unwrap();
    second.shutdown(deadline()).unwrap();
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn cross_process_lock_competition_and_abnormal_exit_release_handle() {
    const NAME: &str = "cross_process_lock_competition_and_abnormal_exit_release_handle";
    if let Some(root) = std::env::var_os("RASTER_METADATA_CHILD_ROOT") {
        let device = native(Path::new(&root));
        if std::env::var_os("RASTER_METADATA_CHILD_HOLD").is_some() {
            let _file = lock(&*device, FileLockMode::Exclusive).unwrap();
            std::fs::write(Path::new(&root).join("ready"), b"ready").unwrap();
            let mut byte = [0];
            std::io::Read::read_exact(&mut std::io::stdin(), &mut byte).unwrap();
        } else {
            assert!(matches!(
                lock(&*device, FileLockMode::Exclusive),
                Err(Error::Busy)
            ));
            device.shutdown(deadline()).unwrap();
        }
        return;
    }
    let root = Directory::new();
    let device = native(&root.0);
    let held = lock(&*device, FileLockMode::Exclusive).unwrap();
    let command = || {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", NAME, "--nocapture"])
            .env("RASTER_METADATA_CHILD_ROOT", &root.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        command
    };
    assert!(command().status().unwrap().success());
    close(&*device, held);
    let mut child = ChildGuard(
        command()
            .env("RASTER_METADATA_CHILD_HOLD", "1")
            .spawn()
            .unwrap(),
    );
    let until = deadline();
    while !root.0.join("ready").exists() {
        assert!(
            !until.expired(),
            "The child process did not acquire the file lock"
        );
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "Child process exits early"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(matches!(
        lock(&*device, FileLockMode::Exclusive),
        Err(Error::Busy)
    ));
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    let file = lock(&*device, FileLockMode::Exclusive).unwrap();
    close(&*device, file);
    device.shutdown(deadline()).unwrap();
}
