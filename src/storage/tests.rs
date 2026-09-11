//! 真实设备验证段代次隔离及提交命名操作，不替代 P5 检查点协议。
use super::*;
use crate::device::{thread_pool::ThreadPoolDeviceFactory, *};
use std::time::{Duration, Instant};

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(5))
}
fn execute(s: &SegmentedStorage, operation: IoOperation) -> IoCompletion {
    let id = s
        .device
        .submit(IoRequest {
            route: CompletionRoute(9),
            operation,
        })
        .unwrap();
    let until = deadline();
    let mut out = vec![];
    while out.is_empty() {
        assert!(!until.expired(), "文件完成超时");
        s.device.poll(PollBudget::default(), &mut out).unwrap();
        std::thread::yield_now();
    }
    assert_eq!(out.len(), 1);
    let done = out.pop().unwrap();
    assert_eq!(done.id, id);
    done
}
fn open(s: &SegmentedStorage, path: PathBuf, create_new: bool) -> FileId {
    let IoOutcome::Opened(file) = execute(s, IoOperation::Open { path, create_new })
        .result
        .unwrap()
    else {
        panic!("文件打开结果类型错误")
    };
    file
}
fn storage() -> (Directory, SegmentedStorage) {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-segment-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let device = ThreadPoolDeviceFactory {
        workers: 2,
        queue_capacity: 16,
    }
    .open(DeviceOpenOptions {
        root: root.0.clone(),
        create_new: true,
    })
    .unwrap();
    let storage = SegmentedStorage::new(Arc::from(device), 16).unwrap();
    (root, storage)
}
#[test]
fn 旧段实际写入不会污染新代次且迟到结果被拒绝() {
    let (_root, s) = storage();
    execute(&s, IoOperation::CreateDirectory("segments".into()))
        .result
        .unwrap();
    let old_file = open(&s, s.segment_path(0, Generation(0)), true);
    s.bind(0, Generation(0), old_file).unwrap();
    let old = s.resolve(LogAddress(3)).unwrap();
    let (_, generation) = s.invalidate(0, Generation(0)).unwrap();
    let new_file = open(&s, s.segment_path(0, generation), true);
    s.bind(0, generation, new_file).unwrap();
    // 在新段绑定后才执行旧请求，覆盖最不利的迟到写入顺序。
    let mut buffer = AlignedBuffer::new_zeroed(3, 8).unwrap();
    buffer.as_mut_slice().copy_from_slice(b"old");
    let done = execute(
        &s,
        IoOperation::Write {
            file: old.file,
            offset: old.offset,
            buffer,
        },
    );
    assert!(matches!(done.result, Ok(IoOutcome::Transferred(3))));
    assert!(matches!(
        s.validate_completion(LogAddress(3), old),
        Err(Error::RangeTruncated)
    ));
    let current = s.resolve(LogAddress(3)).unwrap();
    s.validate_completion(LogAddress(3), current).unwrap();
    let read = execute(
        &s,
        IoOperation::Read {
            file: current.file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(16, 8).unwrap(),
        },
    );
    assert!(matches!(read.result, Ok(IoOutcome::Transferred(0))));
    execute(&s, IoOperation::Close(old_file)).result.unwrap();
    let failed = execute(
        &s,
        IoOperation::Write {
            file: old.file,
            offset: old.offset,
            buffer: done.buffer.unwrap(),
        },
    );
    assert!(failed.result.is_err());
    assert_eq!(failed.buffer.unwrap().as_slice(), b"old");
    s.device.shutdown(deadline()).unwrap();
}
#[test]
fn 提交材料同步后重命名并重开读取() {
    let (root, s) = storage();
    let token = CheckpointToken([3; 16]);
    let pending = s.checkpoint_path(token, "commit.pending").unwrap();
    execute(
        &s,
        IoOperation::CreateDirectory(pending.parent().unwrap().into()),
    )
    .result
    .unwrap();
    // 新建目录的父目录项也必须先同步；计划只负责最终发布两步。
    for directory in [PathBuf::new(), PathBuf::from("checkpoints")] {
        execute(&s, IoOperation::SyncDirectory(directory))
            .result
            .unwrap();
    }
    let file = open(&s, pending.clone(), true);
    let mut buffer = AlignedBuffer::new_zeroed(6, 8).unwrap();
    buffer.as_mut_slice().copy_from_slice(b"commit");
    assert!(matches!(
        execute(
            &s,
            IoOperation::Write {
                file,
                offset: 0,
                buffer
            }
        )
        .result,
        Ok(IoOutcome::Transferred(6))
    ));
    execute(
        &s,
        IoOperation::SyncFile {
            file,
            metadata: true,
        },
    )
    .result
    .unwrap();
    execute(&s, IoOperation::Close(file)).result.unwrap();
    for operation in s.publish_plan(token).unwrap() {
        execute(&s, operation).result.unwrap();
    }
    assert!(!root.0.join(pending).exists());
    assert_eq!(
        std::fs::read(root.0.join(s.checkpoint_path(token, "commit").unwrap())).unwrap(),
        b"commit"
    );
    s.device.shutdown(deadline()).unwrap();
}

#[test]
fn 新实例不能覆盖已有段而恢复模式可以显式打开() {
    use super::open::SegmentOpen;
    fn drive(storage: &SegmentedStorage, create_new: bool) -> Result<FileId, Error> {
        let mut task = SegmentOpen::new(storage, 0, Generation(0), create_new, CompletionRoute(9))?;
        loop {
            if let Some(result) = task.take_result() {
                return result;
            }
            task.submit_next(storage)?;
            let mut out = vec![];
            let until = deadline();
            while out.is_empty() {
                assert!(!until.expired());
                storage.device.poll(PollBudget::default(), &mut out)?;
                std::thread::yield_now();
            }
            task.accept(storage, out.pop().unwrap())
                .map_err(|r| r.reason)?;
        }
    }
    let (root, first) = storage();
    let file = drive(&first, true).unwrap();
    let mut buffer = AlignedBuffer::new_zeroed(3, 8).unwrap();
    buffer.as_mut_slice().copy_from_slice(b"old");
    execute(
        &first,
        IoOperation::Write {
            file,
            offset: 0,
            buffer,
        },
    )
    .result
    .unwrap();
    let device = ThreadPoolDeviceFactory {
        workers: 1,
        queue_capacity: 16,
    }
    .open(DeviceOpenOptions {
        root: root.0.clone(),
        create_new: false,
    })
    .unwrap();
    let second = SegmentedStorage::new(Arc::from(device), 16).unwrap();
    assert!(
        matches!(drive(&second, true), Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists)
    );
    let file = drive(&second, false).unwrap();
    let done = execute(
        &second,
        IoOperation::Read {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(3, 8).unwrap(),
        },
    );
    assert_eq!(done.buffer.unwrap().as_slice(), b"old");
    first.device.shutdown(deadline()).unwrap();
    second.device.shutdown(deadline()).unwrap();
}
