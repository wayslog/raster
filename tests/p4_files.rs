//! Linux/macOS 原生工作线程设备集成验证。
#![cfg(any(target_os = "linux", target_os = "macos"))]
use raster::{
    device::{thread_pool::ThreadPoolDeviceFactory, *},
    types::*,
};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
                "raster-worker-{}",
                StoreId::generate()
                    .unwrap()
                    .0
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            )))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(5))
}
fn request(operation: IoOperation) -> IoRequest {
    IoRequest {
        route: CompletionRoute(8),
        operation,
    }
}
fn collect(device: &dyn Device, count: usize) -> Vec<IoCompletion> {
    let until = deadline();
    let mut out = vec![];
    while out.len() < count {
        assert!(!until.expired(), "设备完成超时");
        device.poll(PollBudget::default(), &mut out).unwrap();
        std::thread::yield_now();
    }
    out
}
fn execute(device: &dyn Device, operation: IoOperation) -> IoCompletion {
    let id = device.submit(request(operation)).unwrap();
    let mut out = collect(device, 1);
    let done = out.pop().unwrap();
    assert_eq!(done.id, id);
    assert_eq!(done.route, CompletionRoute(8));
    done
}
#[test]
fn 原生工作线程偏移写入同步重开与关闭后收取完成() {
    let root = Directory::new();
    let factory = ThreadPoolDeviceFactory {
        workers: 3,
        queue_capacity: 16,
    };
    let device = factory
        .open(DeviceOpenOptions {
            root: root.0.clone(),
            create_new: true,
        })
        .unwrap();
    let IoOutcome::Opened(file) = execute(
        &*device,
        IoOperation::Open {
            path: "数据".into(),
            create_new: true,
        },
    )
    .result
    .unwrap() else {
        panic!("打开")
    };
    let mut ids = vec![];
    for i in 0..8 {
        let mut buffer = AlignedBuffer::new_zeroed(4, 8).unwrap();
        buffer.as_mut_slice().fill(i as u8);
        ids.push(
            device
                .submit(request(IoOperation::Write {
                    file,
                    offset: i * 4,
                    buffer,
                }))
                .unwrap(),
        );
    }
    let out = collect(&*device, 8);
    assert_eq!(out.len(), 8);
    for id in ids {
        assert_eq!(out.iter().filter(|c| c.id == id).count(), 1);
    }
    assert!(
        out.iter()
            .all(|c| matches!(c.result, Ok(IoOutcome::Transferred(4))) && c.buffer.is_some())
    );
    execute(
        &*device,
        IoOperation::SyncFile {
            file,
            metadata: true,
        },
    )
    .result
    .unwrap();
    execute(&*device, IoOperation::Close(file)).result.unwrap();
    device.shutdown(deadline()).unwrap();
    drop(device);
    let device = factory
        .open(DeviceOpenOptions {
            root: root.0.clone(),
            create_new: false,
        })
        .unwrap();
    let IoOutcome::Opened(file) = execute(
        &*device,
        IoOperation::Open {
            path: "数据".into(),
            create_new: false,
        },
    )
    .result
    .unwrap() else {
        panic!("重开")
    };
    device
        .submit(request(IoOperation::Read {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(40, 8).unwrap(),
        }))
        .unwrap();
    device.shutdown(deadline()).unwrap();
    let mut out = collect(&*device, 1);
    let done = out.pop().unwrap();
    assert!(matches!(done.result, Ok(IoOutcome::Transferred(32))));
    for (i, chunk) in done.buffer.unwrap().as_slice()[..32].chunks(4).enumerate() {
        assert_eq!(chunk, [i as u8; 4]);
    }
    assert!(device.submit(request(IoOperation::Close(file))).is_err());
}
#[test]
fn 完成未收取仍占队列容量且失败归还原缓冲() {
    let root = Directory::new();
    let device = ThreadPoolDeviceFactory {
        workers: 1,
        queue_capacity: 1,
    }
    .open(DeviceOpenOptions {
        root: root.0.clone(),
        create_new: true,
    })
    .unwrap();
    device
        .submit(request(IoOperation::Open {
            path: "数据".into(),
            create_new: true,
        }))
        .unwrap();
    assert!(matches!(
        device.submit(request(IoOperation::Open {
            path: "其他".into(),
            create_new: true
        })),
        Err(RejectedIo {
            reason: Error::Busy,
            ..
        })
    ));
    collect(&*device, 1);
    let buffer = AlignedBuffer::new_zeroed(8, 8).unwrap();
    let pointer = buffer.as_slice().as_ptr();
    let done = execute(
        &*device,
        IoOperation::Read {
            file: FileId {
                slot: 999,
                generation: Generation(0),
            },
            offset: 0,
            buffer,
        },
    );
    assert!(done.result.is_err());
    assert_eq!(done.buffer.unwrap().as_slice().as_ptr(), pointer);
    device.shutdown(deadline()).unwrap();
}
