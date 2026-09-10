//! 拥有型分段写入，每次仅有一个在途请求；完成由上层统一路由。
use super::{SegmentLocation, SegmentedStorage};
use crate::{device::*, types::*};
struct Inflight {
    id: IoId,
    address: LogAddress,
    location: SegmentLocation,
    length: usize,
}
pub(crate) struct SegmentWrite {
    start: LogAddress,
    bytes: Vec<u8>,
    cursor: usize,
    route: CompletionRoute,
    inflight: Option<Inflight>,
    bindings: Vec<(LogAddress, SegmentLocation)>,
    ended: bool,
    result: Option<Result<(), Error>>,
}
impl SegmentWrite {
    /// start 是页帧物理字节流偏移，不是记录逻辑地址。段必须预先打开并绑定。
    pub fn new(start: u64, bytes: Vec<u8>, route: CompletionRoute) -> Result<Self, Error> {
        LogAddress(start)
            .checked_add(u64::try_from(bytes.len()).map_err(|_| Error::CapacityExceeded)?)?;
        let empty = bytes.is_empty();
        Ok(Self {
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
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        if self.ended || self.inflight.is_some() {
            return Ok(None);
        }
        for (address, location) in &self.bindings {
            storage.validate_completion(*address, *location)?;
        }
        let address = self.start.checked_add(self.cursor as u64)?;
        let location = storage.resolve(address)?;
        let length = (self.bytes.len() - self.cursor)
            .min(usize::try_from(storage.segment_bytes - location.offset).unwrap_or(usize::MAX));
        let caps = storage.device.capabilities();
        if caps.transfer_alignment != 1 {
            return Err(Error::UnsupportedDurability);
        }
        let mut buffer = AlignedBuffer::new_zeroed(length, caps.memory_alignment)?;
        buffer
            .as_mut_slice()
            .copy_from_slice(&self.bytes[self.cursor..self.cursor + length]);
        // 提交前完成所有内部扩容；接受后不会因记录绑定失败丢失在途请求。
        self.bindings
            .try_reserve(1)
            .map_err(|_| Error::OutOfMemory)?;
        let id = storage
            .device
            .submit(IoRequest {
                route: self.route,
                operation: IoOperation::Write {
                    file: location.file,
                    offset: location.offset,
                    buffer,
                },
            })
            .map_err(|rejected| rejected.reason)?;
        if self.bindings.last().is_none_or(|(previous, _)| {
            previous.0 / storage.segment_bytes != address.0 / storage.segment_bytes
        }) {
            self.bindings.push((address, location));
        }
        self.inflight = Some(Inflight {
            id,
            address,
            location,
            length,
        });
        Ok(Some(id))
    }
    /// 路由不匹配时原样归还完成项，任务仍等待原请求；匹配项恰好消费一次。
    #[allow(clippy::result_large_err, reason = "错误路由原样归还完成缓冲")]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if self
            .inflight
            .as_ref()
            .is_none_or(|pending| pending.id != completion.id || completion.route != self.route)
        {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("分段写入完成路由不匹配"),
            });
        }
        let pending = self.inflight.take().expect("已匹配在途项");
        let result = (|| {
            storage.validate_completion(pending.address, pending.location)?;
            if completion
                .buffer
                .as_ref()
                .is_none_or(|buffer| buffer.len() != pending.length)
            {
                return Err(Error::InvalidState("写入完成未归还正确缓冲"));
            }
            let count = match completion.result? {
                IoOutcome::Transferred(0) => {
                    return Err(Error::Io(std::io::ErrorKind::WriteZero.into()));
                }
                IoOutcome::Transferred(count) if count <= pending.length => count,
                _ => return Err(Error::InvalidState("写入完成长度或类型无效")),
            };
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
    pub fn has_inflight(&self) -> bool {
        self.inflight.is_some()
    }
    pub fn validate_bindings(&self, storage: &SegmentedStorage) -> Result<(), Error> {
        for (address, location) in &self.bindings {
            storage.validate_completion(*address, *location)?;
        }
        Ok(())
    }
    pub fn take_result(&mut self) -> Option<Result<(), Error>> {
        self.result.take()
    }
    pub fn completed_bytes(&self) -> usize {
        self.cursor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::memory::{MemoryDevice, MemoryFault};
    use std::{path::PathBuf, sync::Arc};
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
        let storage = SegmentedStorage::new(device.clone(), PathBuf::new(), 16).unwrap();
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
                panic!("打开")
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
    fn 已接受写入失败后停止续传并只交付一次错误() {
        let (device, storage, _) = setup();
        let mut task = SegmentWrite::new(0, vec![5; 24], CompletionRoute(7)).unwrap();
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
    fn 跨三段短写从实际偏移续传且完整字节一致() {
        let (device, storage, files) = setup();
        let bytes: Vec<u8> = (0..30).collect();
        let mut task = SegmentWrite::new(14, bytes.clone(), CompletionRoute(7)).unwrap();
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
                panic!("读取")
            };
            stored.extend_from_slice(&done.buffer.unwrap().as_slice()[..n]);
        }
        assert_eq!(&stored[..14], [0; 14]);
        assert_eq!(&stored[14..], bytes);
    }
    #[test]
    fn 零写和失效代次终结但错误路由不消费原完成() {
        let (device, storage, _) = setup();
        let mut task = SegmentWrite::new(0, vec![1; 8], CompletionRoute(7)).unwrap();
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
        let mut task = SegmentWrite::new(0, vec![2; 8], CompletionRoute(9)).unwrap();
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
