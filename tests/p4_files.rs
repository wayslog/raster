//! Linux/macOS Native worker thread device integration verification.
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
        assert!(!until.expired(), "Device completion timeout");
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
fn native_worker_thread_offset_writing_synchronizes_reopening_and_closing_after_collection_is_completed()
 {
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
            path: "data".into(),
            create_new: true,
        },
    )
    .result
    .unwrap() else {
        panic!("open")
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
            path: "data".into(),
            create_new: false,
        },
    )
    .result
    .unwrap() else {
        panic!("reopen")
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
fn the_queue_capacity_is_still_occupied_if_the_collection_is_completed_and_the_original_buffer_is_returned_if_it_fails()
 {
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
            path: "data".into(),
            create_new: true,
        }))
        .unwrap();
    assert!(matches!(
        device.submit(request(IoOperation::Open {
            path: "Others".into(),
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

#[test]
fn canceling_and_closing_competition_still_returns_the_buffer_item_by_item_and_only_completes_it_once()
 {
    let root = Directory::new();
    let device = ThreadPoolDeviceFactory {
        workers: 2,
        queue_capacity: 128,
    }
    .open(DeviceOpenOptions {
        root: root.0.clone(),
        create_new: true,
    })
    .unwrap();
    let IoOutcome::Opened(file) = execute(
        &*device,
        IoOperation::Open {
            path: "competition".into(),
            create_new: true,
        },
    )
    .result
    .unwrap() else {
        panic!("open")
    };
    let mut writes = vec![];
    let mut cancels = vec![];
    for i in 0..32 {
        let mut buffer = AlignedBuffer::new_zeroed(4, 8).unwrap();
        buffer.as_mut_slice().fill(i as u8);
        let pointer = buffer.as_slice().as_ptr();
        let id = device
            .submit(request(IoOperation::Write {
                file,
                offset: i * 4,
                buffer,
            }))
            .unwrap();
        writes.push((id, pointer, i as u8));
        cancels.push(device.submit(request(IoOperation::Cancel(id))).unwrap());
    }
    // Resources in transit cannot be destroyed after timeout;Immediate success is also allowed when the thread has exited.
    match device.shutdown(Deadline(Instant::now())) {
        Ok(()) | Err(Error::DeadlineExceeded) => {}
        Err(error) => panic!("Close returns unexpected error:{error:?}"),
    }
    device.shutdown(deadline()).unwrap();
    let out = collect(&*device, 64);
    assert_eq!(out.len(), 64);
    for (id, pointer, value) in writes {
        let matches: Vec<_> = out.iter().filter(|c| c.id == id).collect();
        assert_eq!(matches.len(), 1);
        let done = matches[0];
        match &done.result {
            Ok(IoOutcome::Transferred(4)) => {}
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {}
            other => panic!("write final error:{other:?}"),
        }
        let buffer = done.buffer.as_ref().unwrap();
        assert_eq!(buffer.as_slice().as_ptr(), pointer);
        assert_eq!(buffer.as_slice(), [value; 4]);
    }
    for id in cancels {
        let matches: Vec<_> = out.iter().filter(|c| c.id == id).collect();
        assert_eq!(matches.len(), 1);
        assert!(matches!(matches[0].result, Ok(IoOutcome::Done)));
    }
    let mut extra = vec![];
    device.poll(PollBudget::default(), &mut extra).unwrap();
    assert!(extra.is_empty());
}
