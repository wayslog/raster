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
