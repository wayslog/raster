//! 保留集合释放所需的原生元数据原语；不以目录列表或锁成功冒充检查点释放。
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
        assert!(!until.expired(), "设备完成超时");
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
        panic!("未打开文件")
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
        _ => panic!("目录结果类型错误"),
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
        _ => panic!("文件锁结果类型错误"),
    }
}
#[test]
fn 原生及内存目录枚举保留名称且超预算整体失败() {
    use std::os::unix::ffi::OsStringExt;
    let root = Directory::new();
    for device in [
        native(&root.0),
        Box::new(memory::MemoryDevice::new(32, 4096).unwrap()) as Box<dyn Device>,
    ] {
        assert!(device.capabilities().supports_directory_listing);
        assert!(list(&*device, "", 0, 0).unwrap().is_empty());
        execute(&*device, IoOperation::CreateDirectory("子目录".into())).unwrap();
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
            // APFS 拒绝非法 UTF-8 文件名，设备须原样传播，而不是替换字节后创建别名。
            Err(Error::Io(error))
                if cfg!(target_os = "macos") && error.raw_os_error() == Some(92) =>
            {
                create(&*device, "合法名称".into());
                OsString::from("合法名称")
            }
            result => panic!("文件名创建结果错误：{result:?}"),
        };
        create(&*device, "子目录/子文件".into());
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
                name: "子目录".into(),
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
            list(&*device, "子目录", 1, 64).unwrap(),
            vec![DirectoryEntry {
                name: "子文件".into(),
                kind: DirectoryEntryKind::File
            }]
        );
        assert!(list(&*device, "缺失", 10, 100).is_err());
        for path in ["../逃逸", "/绝对"] {
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
fn 枚举只描述符号链接且不能经链接逃出根目录() {
    let root = Directory::new();
    let outside = Directory::new();
    let device = native(&root.0);
    std::fs::create_dir_all(&outside.0).unwrap();
    std::fs::write(outside.0.join("外部文件"), b"outside").unwrap();
    std::os::unix::fs::symlink(&outside.0, root.0.join("链接")).unwrap();
    assert_eq!(
        list(&*device, "", 1, 64).unwrap(),
        vec![DirectoryEntry {
            name: "链接".into(),
            kind: DirectoryEntryKind::Symlink
        }]
    );
    assert!(list(&*device, "链接", 10, 100).is_err());
    assert!(
        execute(
            &*device,
            IoOperation::TryLock {
                path: "链接/外部文件".into(),
                mode: FileLockMode::Exclusive
            }
        )
        .is_err()
    );
    assert_eq!(
        std::fs::read(outside.0.join("外部文件")).unwrap(),
        b"outside"
    );
    create(&*device, "内部文件".into());
    std::os::unix::fs::symlink("内部文件", root.0.join("内部链接")).unwrap();
    assert!(matches!(
        execute(
            &*device,
            IoOperation::TryLock {
                path: "内部链接".into(),
                mode: FileLockMode::Shared,
            }
        ),
        Err(Error::InvalidFormat(_))
    ));
    device.shutdown(deadline()).unwrap();
}
#[test]
fn 独立句柄共享独占互斥且竞争不耗尽句柄表() {
    let root = Directory::new();
    let first = native(&root.0);
    let second = native(&root.0);
    assert!(first.capabilities().supports_file_locks);
    std::fs::write(root.0.join("catalog.lock"), "不应截断".as_bytes()).unwrap();
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
        "不应截断".as_bytes()
    );
    first.shutdown(deadline()).unwrap();
    second.shutdown(deadline()).unwrap();
}
#[test]
fn 已接受锁持有至关闭且设备关闭释放未收取的锁完成() {
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
    // shutdown 排空已经接受的加锁，再关闭文件表；first 对象仍然存活。
    first.shutdown(deadline()).unwrap();
    let result = take(&*first, id).result;
    assert!(
        matches!(result, Ok(IoOutcome::Locked(_))),
        "关闭排空结果：{result:?}"
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
fn 跨进程锁竞争且异常退出释放句柄() {
    const NAME: &str = "跨进程锁竞争且异常退出释放句柄";
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
        assert!(!until.expired(), "子进程没有取得文件锁");
        assert!(child.0.try_wait().unwrap().is_none(), "子进程提前退出");
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
