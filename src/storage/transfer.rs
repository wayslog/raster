//! Owned segment transfer,There is only one request in transit at a time;Complete unified routing by the upper layer.
use super::{SegmentLocation, SegmentedStorage};
use crate::{device::*, types::*};
#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    Read,
    Write,
}
struct Inflight {
    id: IoId,
    address: LogAddress,
    location: SegmentLocation,
    length: usize,
}
pub(crate) struct SegmentTransfer {
    direction: Direction,
    storage: Option<std::sync::Arc<()>>,
    start: LogAddress,
    bytes: Vec<u8>,
    cursor: usize,
    route: CompletionRoute,
    inflight: Option<Inflight>,
    bindings: Vec<(LogAddress, SegmentLocation)>,
    ended: bool,
    result: Option<Result<(), Error>>,
}
impl SegmentTransfer {
    /// start is the page frame physical byte stream offset,Not recording logical address.Segments must be opened and bound beforehand.
    pub fn write(start: u64, bytes: Vec<u8>, route: CompletionRoute) -> Result<Self, Error> {
        LogAddress(start)
            .checked_add(u64::try_from(bytes.len()).map_err(|_| Error::CapacityExceeded)?)?;
        let empty = bytes.is_empty();
        Ok(Self {
            direction: Direction::Write,
            storage: None,
            start: LogAddress(start),
            bytes,
            cursor: 0,
            route,
            inflight: None,
            bindings: Vec::new(),
            ended: empty,
            result: empty.then_some(Ok(())),
        })
    }
    pub fn read(start: u64, length: usize, route: CompletionRoute) -> Result<Self, Error> {
        LogAddress(start)
            .checked_add(u64::try_from(length).map_err(|_| Error::CapacityExceeded)?)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| Error::OutOfMemory)?;
        bytes.resize(length, 0);
        let mut transfer = Self::write(start, bytes, route)?;
        transfer.direction = Direction::Read;
        Ok(transfer)
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        if self.ended || self.inflight.is_some() {
            return Ok(None);
        }
        self.validate_bindings(storage)?;
        let address = self.start.checked_add(self.cursor as u64)?;
        let location = storage.resolve(address)?;
        let length = (self.bytes.len() - self.cursor)
            .min(usize::try_from(storage.segment_bytes - location.offset).unwrap_or(usize::MAX));
        let caps = storage.device.capabilities();
        if caps.transfer_alignment != 1 {
            return Err(Error::UnsupportedDurability);
        }
        let mut buffer = AlignedBuffer::new_zeroed(length, caps.memory_alignment)?;
        if self.direction == Direction::Write {
            buffer
                .as_mut_slice()
                .copy_from_slice(&self.bytes[self.cursor..self.cursor + length]);
        }
        let operation = match self.direction {
            Direction::Read => IoOperation::Read {
                file: location.file,
                offset: location.offset,
                buffer,
            },
            Direction::Write => IoOperation::Write {
                file: location.file,
                offset: location.offset,
                buffer,
            },
        };
        // Complete all internal expansion before submission;After acceptance, requests in transit will not be lost due to record binding failure..
        self.bindings
            .try_reserve(1)
            .map_err(|_| Error::OutOfMemory)?;
        let id = storage
            .device
            .submit(IoRequest {
                route: self.route,
                operation,
            })
            .map_err(|rejected| rejected.reason)?;
        if self.bindings.last().is_none_or(|(previous, _)| {
            previous.0 / storage.segment_bytes != address.0 / storage.segment_bytes
        }) {
            self.bindings.push((address, location));
        }
        self.storage = Some(storage.identity.clone());
        self.inflight = Some(Inflight {
            id,
            address,
            location,
            length,
        });
        Ok(Some(id))
    }
    /// Return the completion item unchanged if the route does not match,The task is still waiting for the original request;The matching item is consumed exactly once.
    #[allow(
        clippy::result_large_err,
        reason = "The error route is returned intact to the completion buffer."
    )]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if self
            .storage
            .as_ref()
            .is_some_and(|identity| !std::sync::Arc::ptr_eq(identity, &storage.identity))
            || self
                .inflight
                .as_ref()
                .is_none_or(|pending| pending.id != completion.id || completion.route != self.route)
        {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("Fragment transfer complete route mismatch"),
            });
        }
        let pending = self.inflight.take().expect("Matched items in transit");
        let result = (|| {
            storage.validate_completion(pending.address, pending.location)?;
            if completion
                .buffer
                .as_ref()
                .is_none_or(|buffer| buffer.len() != pending.length)
            {
                return Err(Error::InvalidState(
                    "Transfer completed without returning correct buffer",
                ));
            }
            let count = match completion.result? {
                IoOutcome::Transferred(0) => {
                    return Err(Error::Io(
                        match self.direction {
                            Direction::Read => std::io::ErrorKind::UnexpectedEof,
                            Direction::Write => std::io::ErrorKind::WriteZero,
                        }
                        .into(),
                    ));
                }
                IoOutcome::Transferred(count) if count <= pending.length => count,
                _ => {
                    return Err(Error::InvalidState(
                        "Transfer completion length or type is invalid",
                    ));
                }
            };
            if self.direction == Direction::Read {
                let buffer = completion
                    .buffer
                    .as_ref()
                    .expect("Checked completion buffer");
                self.bytes[self.cursor..self.cursor + count]
                    .copy_from_slice(&buffer.as_slice()[..count]);
            }
            self.cursor += count;
            if self.cursor == self.bytes.len() {
                for (address, location) in &self.bindings {
                    storage.validate_completion(*address, *location)?;
                }
            }
            Ok(())
        })();
        if result.is_err() || self.cursor == self.bytes.len() {
            self.ended = true;
            self.result = Some(result);
        }
        Ok(())
    }
    pub fn next_address(&self) -> Result<Option<LogAddress>, Error> {
        if self.ended || self.inflight.is_some() {
            return Ok(None);
        }
        self.start.checked_add(self.cursor as u64).map(Some)
    }
    pub fn has_inflight(&self) -> bool {
        self.inflight.is_some()
    }
    pub fn validate_bindings(&self, storage: &SegmentedStorage) -> Result<(), Error> {
        if self
            .storage
            .as_ref()
            .is_some_and(|identity| !std::sync::Arc::ptr_eq(identity, &storage.identity))
        {
            return Err(Error::InvalidState("Transfer belongs to other storage"));
        }
        for (address, location) in &self.bindings {
            storage.validate_completion(*address, *location)?;
        }
        Ok(())
    }
    pub fn take_result(&mut self) -> Option<Result<(), Error>> {
        self.result.take()
    }
    pub fn take_read_result(&mut self) -> Option<Result<Vec<u8>, Error>> {
        if self.direction != Direction::Read {
            return None;
        }
        self.result
            .take()
            .map(|result| result.map(|()| std::mem::take(&mut self.bytes)))
    }
    #[cfg(test)]
    pub fn completed_bytes(&self) -> usize {
        self.cursor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::memory::{MemoryDevice, MemoryFault};
    use std::sync::Arc;
    fn execute(device: &dyn Device, operation: IoOperation) -> IoCompletion {
        device
            .submit(IoRequest {
                route: CompletionRoute(0),
                operation,
            })
            .unwrap();
        let mut out = vec![];
        device.poll(PollBudget::default(), &mut out).unwrap();
        assert_eq!(out.len(), 1);
        out.pop().unwrap()
    }
    fn setup() -> (Arc<MemoryDevice>, SegmentedStorage, Vec<FileId>) {
        let device = Arc::new(MemoryDevice::new(16, 1024).unwrap());
        let storage = SegmentedStorage::new(device.clone(), 16).unwrap();
        execute(&*device, IoOperation::CreateDirectory("segments".into()))
            .result
            .unwrap();
        let mut files = vec![];
        for number in 0..3 {
            let IoOutcome::Opened(file) = execute(
                &*device,
                IoOperation::Open {
                    path: storage.segment_path(number, Generation(0)),
                    create_new: true,
                },
            )
            .result
            .unwrap() else {
                panic!("open")
            };
            storage.bind(number, Generation(0), file).unwrap();
            files.push(file);
        }
        (device, storage, files)
    }
    fn completion(device: &dyn Device) -> IoCompletion {
        let mut out = vec![];
        device.poll(PollBudget::default(), &mut out).unwrap();
        assert_eq!(out.len(), 1);
        out.pop().unwrap()
    }
    #[test]
    fn cross_segment_short_read_returns_complete_owned_bytes_and_an_error_is_reported_when_exceeding_the_file()
     {
        let (device, storage, _) = setup();
        let bytes: Vec<u8> = (0..30).collect();
        let mut write = SegmentTransfer::write(14, bytes.clone(), CompletionRoute(7)).unwrap();
        while write.completed_bytes() < bytes.len() {
            write.submit_next(&storage).unwrap();
            write
                .accept(&storage, completion(&*device))
                .map_err(|r| r.reason)
                .unwrap();
        }
        write.take_result().unwrap().unwrap();
        let mut read = SegmentTransfer::read(14, 30, CompletionRoute(8)).unwrap();
        device.inject_next(MemoryFault::Short(1)).unwrap();
        let mut requests = 0;
        loop {
            read.submit_next(&storage).unwrap();
            read.accept(&storage, completion(&*device))
                .map_err(|r| r.reason)
                .unwrap();
            requests += 1;
            if let Some(result) = read.take_read_result() {
                assert_eq!(result.unwrap(), bytes);
                break;
            }
        }
        assert_eq!(requests, 4);
        assert!(read.take_read_result().is_none());
        let mut read = SegmentTransfer::read(43, 2, CompletionRoute(9)).unwrap();
        read.submit_next(&storage).unwrap();
        read.accept(&storage, completion(&*device))
            .map_err(|r| r.reason)
            .unwrap();
        assert!(read.take_read_result().is_none());
        read.submit_next(&storage).unwrap();
        read.accept(&storage, completion(&*device))
            .map_err(|r| r.reason)
            .unwrap();
        assert!(
            matches!(read.take_read_result(), Some(Err(Error::Io(error))) if error.kind() == std::io::ErrorKind::UnexpectedEof)
        );
    }
    #[test]
    fn accepted_to_stop_resuming_after_write_failure_and_deliver_only_one_error() {
        let (device, storage, _) = setup();
        let mut task = SegmentTransfer::write(0, vec![5; 24], CompletionRoute(7)).unwrap();
        device
            .inject_next(MemoryFault::Fail(std::io::ErrorKind::PermissionDenied))
            .unwrap();
        task.submit_next(&storage).unwrap();
        task.accept(&storage, completion(&*device))
            .map_err(|r| r.reason)
            .unwrap();
        assert!(
            matches!(task.take_result(), Some(Err(Error::Io(error))) if error.kind() == std::io::ErrorKind::PermissionDenied)
        );
        assert!(task.take_result().is_none());
        assert!(task.submit_next(&storage).unwrap().is_none());
        assert_eq!(task.completed_bytes(), 0);
    }
    #[test]
    fn the_short_write_across_three_segments_is_resumed_from_the_actual_offset_and_the_complete_bytes_are_consistent()
     {
        let (device, storage, files) = setup();
        let bytes: Vec<u8> = (0..30).collect();
        let mut task = SegmentTransfer::write(14, bytes.clone(), CompletionRoute(7)).unwrap();
        device.inject_next(MemoryFault::Short(1)).unwrap();
        let mut requests = 0;
        while task.completed_bytes() < bytes.len() {
            assert!(task.submit_next(&storage).unwrap().is_some());
            assert!(task.submit_next(&storage).unwrap().is_none());
            task.accept(&storage, completion(&*device))
                .map_err(|r| r.reason)
                .unwrap();
            requests += 1;
        }
        assert_eq!(requests, 4);
        task.take_result().unwrap().unwrap();
        assert!(task.take_result().is_none());
        assert!(task.submit_next(&storage).unwrap().is_none());
        let mut stored = vec![];
        for file in files {
            let done = execute(
                &*device,
                IoOperation::Read {
                    file,
                    offset: 0,
                    buffer: AlignedBuffer::new_zeroed(16, 8).unwrap(),
                },
            );
            let IoOutcome::Transferred(n) = done.result.unwrap() else {
                panic!("read")
            };
            stored.extend_from_slice(&done.buffer.unwrap().as_slice()[..n]);
        }
        assert_eq!(&stored[..14], [0; 14]);
        assert_eq!(&stored[14..], bytes);
    }
    #[test]
    fn zero_writes_and_invalid_generation_termination_but_error_routing_does_not_consume_the_original_completion()
     {
        let (device, storage, _) = setup();
        let mut task = SegmentTransfer::write(0, vec![1; 8], CompletionRoute(7)).unwrap();
        device.inject_next(MemoryFault::Short(0)).unwrap();
        task.submit_next(&storage).unwrap();
        let mut done = completion(&*device);
        done.route = CompletionRoute(8);
        let mut done = task.accept(&storage, done).unwrap_err().request;
        assert!(task.take_result().is_none());
        done.route = CompletionRoute(7);
        task.accept(&storage, done).map_err(|r| r.reason).unwrap();
        assert!(
            matches!(task.take_result(), Some(Err(Error::Io(error))) if error.kind() == std::io::ErrorKind::WriteZero)
        );
        assert_eq!(task.completed_bytes(), 0);
        assert!(task.submit_next(&storage).unwrap().is_none());
        let mut task = SegmentTransfer::write(0, vec![2; 8], CompletionRoute(9)).unwrap();
        task.submit_next(&storage).unwrap();
        storage.invalidate(0, Generation(0)).unwrap();
        task.accept(&storage, completion(&*device))
            .map_err(|r| r.reason)
            .unwrap();
        assert!(matches!(
            task.take_result(),
            Some(Err(Error::RangeTruncated))
        ));
        assert_eq!(task.completed_bytes(), 0);
    }
}
