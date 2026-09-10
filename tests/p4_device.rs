//! 真实内存设备接口的接受、终结和缓冲归还。
use raster::{
    device::{memory::MemoryDevice, *},
    types::*,
};
use std::time::{Duration, Instant};
fn request(operation: IoOperation) -> IoRequest {
    IoRequest {
        route: CompletionRoute(7),
        operation,
    }
}
fn complete(device: &MemoryDevice, operation: IoOperation) -> IoCompletion {
    let id = device
        .submit(request(operation))
        .map_err(|r| r.reason)
        .unwrap();
    let mut output = vec![];
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert_eq!(output.len(), 1);
    let completion = output.pop().unwrap();
    assert_eq!(completion.id, id);
    assert_eq!(completion.route, CompletionRoute(7));
    completion
}
fn open(device: &MemoryDevice) -> FileId {
    match complete(
        device,
        IoOperation::Open {
            path: "数据".into(),
            create_new: true,
        },
    )
    .result
    .unwrap()
    {
        IoOutcome::Opened(file) => file,
        _ => panic!("应打开文件"),
    }
}
#[test]
fn 读写短读和错误均归还原缓冲且终结一次() {
    let device = MemoryDevice::new(8, 128).unwrap();
    let file = open(&device);
    let mut buffer = AlignedBuffer::new_zeroed(3, 8).unwrap();
    buffer.as_mut_slice().copy_from_slice(b"abc");
    let pointer = buffer.as_slice().as_ptr();
    let written = complete(
        &device,
        IoOperation::Write {
            file,
            offset: 2,
            buffer,
        },
    );
    assert!(matches!(written.result, Ok(IoOutcome::Transferred(3))));
    assert_eq!(written.buffer.unwrap().as_slice().as_ptr(), pointer);
    let mut buffer = AlignedBuffer::new_zeroed(8, 8).unwrap();
    buffer.as_mut_slice().fill(9);
    let read = complete(
        &device,
        IoOperation::Read {
            file,
            offset: 0,
            buffer,
        },
    );
    assert!(matches!(read.result, Ok(IoOutcome::Transferred(5))));
    assert_eq!(
        read.buffer.unwrap().as_slice(),
        [0, 0, b'a', b'b', b'c', 9, 9, 9]
    );
    let buffer = AlignedBuffer::new_zeroed(4, 8).unwrap();
    let pointer = buffer.as_slice().as_ptr();
    let failed = complete(
        &device,
        IoOperation::Write {
            file,
            offset: u64::MAX,
            buffer,
        },
    );
    assert!(failed.result.is_err());
    assert_eq!(failed.buffer.unwrap().as_slice().as_ptr(), pointer);
    let mut output = vec![];
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert!(output.is_empty());
}
#[test]
fn 队列满拒绝原样归还且不消费请求编号() {
    let device = MemoryDevice::new(1, 128).unwrap();
    let first = device
        .submit(request(IoOperation::Open {
            path: "一".into(),
            create_new: true,
        }))
        .unwrap();
    let rejected = device
        .submit(request(IoOperation::Open {
            path: "二".into(),
            create_new: true,
        }))
        .unwrap_err();
    assert!(matches!(rejected.reason, Error::Busy));
    assert_eq!(rejected.request.route, CompletionRoute(7));
    let mut output = vec![];
    device.poll(PollBudget::default(), &mut output).unwrap();
    let second = device.submit(rejected.request).unwrap();
    assert_eq!(second.0, first.0 + 1);
}
#[test]
fn 关闭排空但仍可收取完成且超时保留推进() {
    let device = MemoryDevice::new(8, 128).unwrap();
    device
        .submit(request(IoOperation::Open {
            path: "一".into(),
            create_new: true,
        }))
        .unwrap();
    assert!(matches!(
        device.shutdown(Deadline(Instant::now())),
        Err(Error::DeadlineExceeded)
    ));
    assert!(
        device
            .submit(request(IoOperation::Open {
                path: "二".into(),
                create_new: true
            }))
            .is_err()
    );
    device
        .shutdown(Deadline(Instant::now() + Duration::from_secs(1)))
        .unwrap();
    let mut output = vec![];
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert_eq!(output.len(), 1);
    assert!(output[0].result.is_ok());
    output.clear();
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert!(output.is_empty());
}
#[test]
fn 文件代次关闭容量和持久化能力拒绝() {
    let device = MemoryDevice::new(8, 4).unwrap();
    let file = open(&device);
    assert!(!device.capabilities().supports_file_sync);
    assert!(
        complete(&device, IoOperation::SetLen { file, length: 5 })
            .result
            .is_err()
    );
    assert!(
        complete(
            &device,
            IoOperation::SyncFile {
                file,
                metadata: true
            }
        )
        .result
        .is_err()
    );
    complete(&device, IoOperation::Close(file)).result.unwrap();
    let result = complete(
        &device,
        IoOperation::Read {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(1, 1).unwrap(),
        },
    );
    assert!(result.result.is_err());
    assert!(result.buffer.is_some());
}
#[test]
fn 可控短写短读和失败保持缓冲与数据边界() {
    use raster::device::memory::MemoryFault;
    let device = MemoryDevice::new(8, 128).unwrap();
    let file = open(&device);
    let mut buffer = AlignedBuffer::new_zeroed(4, 8).unwrap();
    buffer.as_mut_slice().copy_from_slice(b"abcd");
    let pointer = buffer.as_slice().as_ptr();
    device.inject_next(MemoryFault::Short(2)).unwrap();
    let done = complete(
        &device,
        IoOperation::Write {
            file,
            offset: 0,
            buffer,
        },
    );
    assert!(matches!(done.result, Ok(IoOutcome::Transferred(2))));
    assert_eq!(done.buffer.unwrap().as_slice().as_ptr(), pointer);
    device.inject_next(MemoryFault::Short(1)).unwrap();
    let done = complete(
        &device,
        IoOperation::Read {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(4, 8).unwrap(),
        },
    );
    assert!(matches!(done.result, Ok(IoOutcome::Transferred(1))));
    assert_eq!(done.buffer.unwrap().as_slice(), [b'a', 0, 0, 0]);
    device
        .inject_next(MemoryFault::Fail(std::io::ErrorKind::Other))
        .unwrap();
    let done = complete(
        &device,
        IoOperation::Write {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(4, 8).unwrap(),
        },
    );
    assert!(done.result.is_err());
    assert!(done.buffer.is_some());
    let done = complete(
        &device,
        IoOperation::Read {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(4, 8).unwrap(),
        },
    );
    assert_eq!(done.buffer.unwrap().as_slice(), [b'a', b'b', 0, 0]);
}
#[test]
fn 反向执行取消待执行请求与迟到取消都只终结一次() {
    let device = MemoryDevice::new(8, 128).unwrap();
    let file = open(&device);
    device.set_reverse(true).unwrap();
    let write = device
        .submit(request(IoOperation::Write {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(4, 8).unwrap(),
        }))
        .unwrap();
    let cancel = device.submit(request(IoOperation::Cancel(write))).unwrap();
    let mut output = vec![];
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].id, cancel);
    assert!(output[0].result.is_ok());
    assert_eq!(output[1].id, write);
    assert!(
        matches!(&output[1].result,Err(Error::Io(e)) if e.kind()==std::io::ErrorKind::Interrupted)
    );
    assert!(output[1].buffer.is_some());
    assert!(complete(&device, IoOperation::Cancel(write)).result.is_ok());
    output.clear();
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert!(output.is_empty());
    let done = complete(
        &device,
        IoOperation::Read {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(4, 8).unwrap(),
        },
    );
    assert!(matches!(done.result, Ok(IoOutcome::Transferred(0))));
}
#[test]
fn 拒绝不消耗下一次故障且多个请求反序归还() {
    use raster::device::memory::MemoryFault;
    let device = MemoryDevice::new(1, 128).unwrap();
    device
        .submit(request(IoOperation::Open {
            path: "数据".into(),
            create_new: true,
        }))
        .unwrap();
    device
        .inject_next(MemoryFault::Fail(std::io::ErrorKind::PermissionDenied))
        .unwrap();
    let rejected = device
        .submit(request(IoOperation::Open {
            path: "其他".into(),
            create_new: true,
        }))
        .unwrap_err();
    let mut output = vec![];
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert!(output[0].result.is_ok());
    device.submit(rejected.request).unwrap();
    output.clear();
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert!(
        matches!(&output[0].result,Err(Error::Io(e)) if e.kind()==std::io::ErrorKind::PermissionDenied)
    );
    let device = MemoryDevice::new(4, 128).unwrap();
    device.set_reverse(true).unwrap();
    let a = device
        .submit(request(IoOperation::Open {
            path: "甲".into(),
            create_new: true,
        }))
        .unwrap();
    let b = device
        .submit(request(IoOperation::Open {
            path: "乙".into(),
            create_new: true,
        }))
        .unwrap();
    output.clear();
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert_eq!(output.iter().map(|c| c.id).collect::<Vec<_>>(), vec![b, a]);
}
#[test]
fn 重命名覆盖与删除不改变已有句柄的文件身份() {
    let device = MemoryDevice::new(16, 128).unwrap();
    complete(&device, IoOperation::CreateDirectory("目录".into()))
        .result
        .unwrap();
    let make = |path: &str| match complete(
        &device,
        IoOperation::Open {
            path: path.into(),
            create_new: true,
        },
    )
    .result
    .unwrap()
    {
        IoOutcome::Opened(file) => file,
        _ => panic!("预期文件"),
    };
    let a = make("目录/甲");
    let b = make("目录/乙");
    for (file, value) in [(a, 1), (b, 2)] {
        let mut buffer = AlignedBuffer::new_zeroed(1, 1).unwrap();
        buffer.as_mut_slice()[0] = value;
        complete(
            &device,
            IoOperation::Write {
                file,
                offset: 0,
                buffer,
            },
        )
        .result
        .unwrap();
    }
    complete(
        &device,
        IoOperation::Rename {
            source: "目录/甲".into(),
            destination: "目录/乙".into(),
        },
    )
    .result
    .unwrap();
    for (file, expected) in [(a, 1), (b, 2)] {
        let done = complete(
            &device,
            IoOperation::Read {
                file,
                offset: 0,
                buffer: AlignedBuffer::new_zeroed(1, 1).unwrap(),
            },
        );
        assert_eq!(done.buffer.unwrap().as_slice(), [expected]);
    }
    complete(&device, IoOperation::RemoveFile("目录/乙".into()))
        .result
        .unwrap();
    assert!(
        complete(
            &device,
            IoOperation::Open {
                path: "目录/乙".into(),
                create_new: false
            }
        )
        .result
        .is_err()
    );
    assert_eq!(
        complete(
            &device,
            IoOperation::Read {
                file: a,
                offset: 0,
                buffer: AlignedBuffer::new_zeroed(1, 1).unwrap()
            }
        )
        .buffer
        .unwrap()
        .as_slice(),
        [1]
    );
    assert!(
        complete(&device, IoOperation::SyncDirectory("目录".into()))
            .result
            .is_err()
    );
    for path in ["../外部", "/绝对", "缺父/文件"] {
        assert!(
            complete(
                &device,
                IoOperation::Open {
                    path: path.into(),
                    create_new: true
                }
            )
            .result
            .is_err()
        );
    }
}
#[test]
fn 删除的开放对象仍计入预算直到最后句柄关闭() {
    let device = MemoryDevice::new(8, 4).unwrap();
    let old = open(&device);
    complete(
        &device,
        IoOperation::SetLen {
            file: old,
            length: 4,
        },
    )
    .result
    .unwrap();
    complete(&device, IoOperation::RemoveFile("数据".into()))
        .result
        .unwrap();
    let new = open(&device);
    assert!(
        complete(
            &device,
            IoOperation::SetLen {
                file: new,
                length: 4
            }
        )
        .result
        .is_err()
    );
    complete(&device, IoOperation::Close(old)).result.unwrap();
    complete(
        &device,
        IoOperation::SetLen {
            file: new,
            length: 4,
        },
    )
    .result
    .unwrap();
}
#[test]
fn 取消与执行竞争保留两个独立的一次终结() {
    for _ in 0..32 {
        let device = MemoryDevice::new(8, 128).unwrap();
        let file = open(&device);
        device.set_reverse(true).unwrap();
        let target = device
            .submit(request(IoOperation::Read {
                file,
                offset: 0,
                buffer: AlignedBuffer::new_zeroed(1, 1).unwrap(),
            }))
            .unwrap();
        let barrier = std::sync::Barrier::new(2);
        let (mut output, cancel) = std::thread::scope(|scope| {
            let a = scope.spawn(|| {
                barrier.wait();
                let mut output = vec![];
                device
                    .poll(
                        PollBudget(std::num::NonZeroUsize::new(1).unwrap()),
                        &mut output,
                    )
                    .unwrap();
                output
            });
            let b = scope.spawn(|| {
                barrier.wait();
                device.submit(request(IoOperation::Cancel(target))).unwrap()
            });
            (a.join().unwrap(), b.join().unwrap())
        });
        device.poll(PollBudget::default(), &mut output).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output.iter().filter(|c| c.id == target).count(), 1);
        assert_eq!(output.iter().filter(|c| c.id == cancel).count(), 1);
        let target = output.iter().find(|c| c.id == target).unwrap();
        assert!(target.buffer.is_some());
        assert!(
            matches!(&target.result, Ok(IoOutcome::Transferred(0)))
                || matches!(&target.result,Err(Error::Io(e)) if e.kind()==std::io::ErrorKind::Interrupted)
        );
    }
}
#[test]
fn 空设备拒绝原样归还且不承诺恢复() {
    let device = raster::device::null::NullDeviceFactory
        .open(DeviceOpenOptions {
            root: "不使用".into(),
            create_new: true,
        })
        .unwrap();
    let buffer = AlignedBuffer::new_zeroed(4, 8).unwrap();
    let pointer = buffer.as_slice().as_ptr();
    let rejected = device
        .submit(request(IoOperation::Write {
            file: FileId {
                slot: 0,
                generation: Generation(0),
            },
            offset: 0,
            buffer,
        }))
        .unwrap_err();
    let IoOperation::Write { buffer, .. } = rejected.request.operation else {
        panic!("应归还写请求")
    };
    assert_eq!(buffer.as_slice().as_ptr(), pointer);
    assert!(!device.capabilities().supports_file_sync);
    let mut output = vec![];
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert!(output.is_empty());
}
