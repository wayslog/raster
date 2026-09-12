//! Acceptance of real memory device interfaces,Finalization and buffered return.
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
            path: "data".into(),
            create_new: true,
        },
    )
    .result
    .unwrap()
    {
        IoOutcome::Opened(file) => file,
        _ => panic!("File should be opened"),
    }
}
#[test]
fn read_and_write_short_reads_and_errors_are_returned_to_the_original_buffer_and_terminated_once() {
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
fn if_the_queue_is_full_the_request_number_will_not_be_returned_and_the_request_number_will_not_be_consumed()
 {
    let device = MemoryDevice::new(1, 128).unwrap();
    let first = device
        .submit(request(IoOperation::Open {
            path: "one".into(),
            create_new: true,
        }))
        .unwrap();
    let rejected = device
        .submit(request(IoOperation::Open {
            path: "two".into(),
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
fn turn_off_draining_but_still_collect_completion_and_timeout_to_preserve_advancement() {
    let device = MemoryDevice::new(8, 128).unwrap();
    device
        .submit(request(IoOperation::Open {
            path: "one".into(),
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
                path: "two".into(),
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
fn file_generation_closure_capacity_and_persistence_denied() {
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
fn controlled_short_writes_short_reads_and_failures_to_maintain_buffering_and_data_boundaries() {
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
fn reverse_execution_cancellation_of_pending_requests_and_late_cancellation_are_terminated_only_once()
 {
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
fn rejection_does_not_consume_the_next_failure_and_multiple_requests_are_returned_in_reverse_order()
{
    use raster::device::memory::MemoryFault;
    let device = MemoryDevice::new(1, 128).unwrap();
    device
        .submit(request(IoOperation::Open {
            path: "data".into(),
            create_new: true,
        }))
        .unwrap();
    device
        .inject_next(MemoryFault::Fail(std::io::ErrorKind::PermissionDenied))
        .unwrap();
    let rejected = device
        .submit(request(IoOperation::Open {
            path: "Others".into(),
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
            path: "A".into(),
            create_new: true,
        }))
        .unwrap();
    let b = device
        .submit(request(IoOperation::Open {
            path: "B".into(),
            create_new: true,
        }))
        .unwrap();
    output.clear();
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert_eq!(output.iter().map(|c| c.id).collect::<Vec<_>>(), vec![b, a]);
}
#[test]
fn renaming_overwriting_and_deletion_do_not_change_the_file_identity_of_the_existing_handle() {
    let device = MemoryDevice::new(16, 128).unwrap();
    complete(&device, IoOperation::CreateDirectory("directory".into()))
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
        _ => panic!("expected file"),
    };
    let a = make("directory/A");
    let b = make("directory/B");
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
            source: "directory/A".into(),
            destination: "directory/B".into(),
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
    complete(&device, IoOperation::RemoveFile("directory/B".into()))
        .result
        .unwrap();
    assert!(
        complete(
            &device,
            IoOperation::Open {
                path: "directory/B".into(),
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
        complete(&device, IoOperation::SyncDirectory("directory".into()))
            .result
            .is_err()
    );
    for path in ["../external", "/Absolutely", "Missing father/file"] {
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
fn deleted_open_objects_are_still_included_in_the_budget_until_the_last_handle_is_closed() {
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
    complete(&device, IoOperation::RemoveFile("data".into()))
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
fn cancellation_and_execution_competition_preserve_two_independent_finalizations() {
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
fn empty_equipment_refuses_to_be_returned_as_is_and_no_promise_is_made_to_restore_it() {
    let device = raster::device::null::NullDeviceFactory
        .open(DeviceOpenOptions {
            root: "Not used".into(),
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
        panic!("Write request should be returned")
    };
    assert_eq!(buffer.as_slice().as_ptr(), pointer);
    assert!(!device.capabilities().supports_file_sync);
    let mut output = vec![];
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert!(output.is_empty());
}
