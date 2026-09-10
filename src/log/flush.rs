//! 单页顺序刷盘任务；写入完成不等于文件同步或检查点持久化。
use super::*;
use crate::{
    device::{CompletionRoute, IoCompletion},
    storage::{SegmentedStorage, transfer::SegmentTransfer},
};
pub(crate) struct PageFlush {
    control: Arc<crate::sync::Mutex<LogState>>,
    token: Arc<()>,
    storage: Arc<()>,
    route: CompletionRoute,
    page: PageId,
    generation: Generation,
    write: SegmentTransfer,
    opening: Option<crate::storage::open::SegmentOpen>,
    error: Option<Error>,
    terminal: bool,
}
impl PageFlush {
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        self.check_storage(storage)?;
        if self.terminal || self.error.is_some() {
            return Ok(None);
        }
        if let Some(opening) = &mut self.opening {
            if let Some(result) = opening.take_result() {
                self.error = result.err();
                self.opening = None;
                if self.error.is_some() {
                    return Ok(None);
                }
            } else {
                return opening.submit_next(storage);
            }
        }
        self.write.validate_bindings(storage)?;
        if let Some(address) = self.write.next_address()? {
            match storage.resolve(address) {
                Ok(_) => {}
                Err(Error::RangeTruncated) => {
                    let number = address.0 / storage.segment_bytes;
                    let mut opening = crate::storage::open::SegmentOpen::new(
                        storage,
                        number,
                        storage.generation(number)?,
                        true,
                        self.route,
                    )?;
                    if let Some(result) = opening.take_result() {
                        result?;
                    } else {
                        self.opening = Some(opening);
                        return self
                            .opening
                            .as_mut()
                            .expect("刚创建打开任务")
                            .submit_next(storage);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        self.write.submit_next(storage)
    }
    fn check_storage(&self, storage: &SegmentedStorage) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.storage, &storage.identity) {
            return Err(Error::InvalidState("刷盘属于其他存储"));
        }
        Ok(())
    }
    #[allow(clippy::result_large_err, reason = "错误存储路由须归还完成缓冲")]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if let Err(reason) = self.check_storage(storage) {
            return Err(Rejected {
                request: completion,
                reason,
            });
        }
        if let Some(opening) = &mut self.opening {
            opening.accept(storage, completion)?;
            if let Some(result) = opening.take_result() {
                if let Err(error) = result {
                    self.error = Some(error);
                }
                self.opening = None;
            }
            Ok(())
        } else {
            self.write.accept(storage, completion)
        }
    }
}
impl Drop for PageFlush {
    fn drop(&mut self) {
        // 在途写入仍由设备持有；未接管其终结前不能放行另一份可能不同版本的页写入。
        if !self.write.has_inflight()
            && self
                .opening
                .as_ref()
                .is_none_or(|opening| !opening.has_resources())
            && let Ok(mut state) = self.control.lock()
            && state
                .flush
                .as_ref()
                .is_some_and(|token| Arc::ptr_eq(token, &self.token))
        {
            state.flush = None;
        }
    }
}
impl<V: ValueLayout> HybridLog<V> {
    pub fn begin_flush(
        &self,
        storage: &SegmentedStorage,
        route: CompletionRoute,
        version: CheckpointVersion,
    ) -> Result<PageFlush, Error> {
        let page = {
            let state = self
                .state
                .lock()
                .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
            if state.flush.is_some() {
                return Err(Error::Busy);
            }
            state
                .frontiers
                .flushed_until
                .page_offset(self.page_bytes as u64)?
                .0
        };
        let encoded = self.encode_page(page, version)?;
        let start = crate::format::PageFrame::physical_offset(page, self.page_bytes)?;
        let write = SegmentTransfer::write(start, encoded.bytes, route)?;
        let token = Arc::new(());
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
        if state.flush.is_some()
            || state
                .frontiers
                .flushed_until
                .page_offset(self.page_bytes as u64)?
                .0
                != page
        {
            return Err(Error::Busy);
        }
        state.flush = Some(token.clone());
        Ok(PageFlush {
            control: self.state.clone(),
            token,
            storage: storage.identity.clone(),
            route,
            page,
            generation: encoded.generation,
            write,
            opening: None,
            error: None,
            terminal: false,
        })
    }
    /// false 表示仍待完成；只有所有字节写入且身份匹配才推进连续边界。
    pub fn finish_flush(
        &self,
        storage: &SegmentedStorage,
        task: &mut PageFlush,
    ) -> Result<bool, Error> {
        if !Arc::ptr_eq(&self.state, &task.control) {
            return Err(Error::InvalidState("刷盘属于其他日志"));
        }
        task.check_storage(storage)?;
        if task.terminal {
            return Err(Error::InvalidState("刷盘已终结"));
        }
        let Some(result) = task
            .error
            .take()
            .map(Err)
            .or_else(|| task.write.take_result())
        else {
            return Ok(false);
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
        if !state
            .flush
            .as_ref()
            .is_some_and(|token| Arc::ptr_eq(token, &task.token))
        {
            return Err(Error::InvalidState("刷盘任务已失效"));
        }
        task.terminal = true;
        state.flush = None;
        result?;
        task.write.validate_bindings(storage)?;
        if self.pool.generation(task.page)? != task.generation {
            return Err(Error::RangeTruncated);
        }
        let begin = LogAddress::from_page_offset(task.page, 0, self.page_bytes as u64)?;
        let end = begin.checked_add(self.page_bytes as u64)?;
        if state.frontiers.flushed_until != begin || end > state.frontiers.safe_read_only {
            return Err(Error::InvalidState("刷盘边界不连续或未冻结"));
        }
        state.frontiers.flushed_until = end;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        device::{
            memory::{MemoryDevice, MemoryFault},
            *,
        },
        schema::builtin::AtomicU64Value,
    };
    use std::path::PathBuf;
    fn complete(device: &dyn Device) -> IoCompletion {
        let mut out = vec![];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while out.is_empty() {
            assert!(std::time::Instant::now() < deadline, "等待设备完成超时");
            device.poll(PollBudget::default(), &mut out).unwrap();
            std::thread::yield_now();
        }
        assert_eq!(out.len(), 1);
        out.pop().unwrap()
    }
    fn execute(device: &dyn Device, operation: IoOperation) -> IoCompletion {
        device
            .submit(IoRequest {
                route: CompletionRoute(0),
                operation,
            })
            .unwrap();
        complete(device)
    }
    fn log() -> HybridLog<AtomicU64Value> {
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 256,
                memory_pages: 2,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
        )
        .unwrap();
        for key in 0..4 {
            log.finish_initialization(log.reserve_record(&[key], None, key as u64).unwrap())
                .unwrap();
        }
        log.advance_read_only(LogAddress(256)).unwrap();
        log
    }
    fn storage() -> (Arc<MemoryDevice>, SegmentedStorage, Vec<FileId>) {
        storage_with_size(128)
    }
    fn storage_with_size(segment_bytes: u64) -> (Arc<MemoryDevice>, SegmentedStorage, Vec<FileId>) {
        let device = Arc::new(MemoryDevice::new(16, 4096).unwrap());
        let storage = SegmentedStorage::new(device.clone(), PathBuf::new(), segment_bytes).unwrap();
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
    fn flush(log: &HybridLog<AtomicU64Value>, storage: &SegmentedStorage) {
        let mut task = log
            .begin_flush(storage, CompletionRoute(71), CheckpointVersion(0))
            .unwrap();
        loop {
            task.submit_next(storage).unwrap();
            task.accept(storage, complete(&*storage.device))
                .map_err(|r| r.reason)
                .unwrap();
            if log.finish_flush(storage, &mut task).unwrap() {
                break;
            }
        }
    }
    #[test]
    fn 混合链按预算跨内存与磁盘查找且墓碑遮蔽旧值() {
        use crate::log::lookup::LookupStep;
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 256,
                memory_pages: 4,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
        )
        .unwrap();
        let (device, storage, _) = storage_with_size(4096);
        let mut head = None;
        for key in 0..9 {
            let reservation = if key == 5 {
                log.reserve_tombstone(&[0], head).unwrap()
            } else {
                log.reserve_record(&[key], head, key as u64).unwrap()
            };
            head = Some(log.finish_initialization(reservation).unwrap());
        }
        log.advance_read_only(LogAddress(512)).unwrap();
        for _ in 0..2 {
            flush(&log, &storage);
            assert_eq!(log.evict_next().unwrap().completed, 1);
        }
        for (key, expected, expected_reads) in [
            (1, Some(1), 2),
            (8, Some(8), 0),
            (0, None, 1),
            (99, None, 2),
        ] {
            let mut lookup = log
                .lookup(
                    &storage,
                    vec![key],
                    head,
                    CheckpointVersion(0),
                    CompletionRoute(77),
                )
                .unwrap();
            let mut reads = 0;
            let mut ended = false;
            for _ in 0..100 {
                match lookup
                    .step(
                        &log,
                        &storage,
                        PollBudget(std::num::NonZeroUsize::new(1).unwrap()),
                    )
                    .unwrap()
                {
                    LookupStep::Continue => {}
                    LookupStep::AwaitingIo => {
                        lookup
                            .accept(&storage, complete(&*device))
                            .map_err(|r| r.reason)
                            .unwrap();
                        reads += 1;
                    }
                    LookupStep::Resident(value) => {
                        assert_eq!(Some(value.read(|v| v).unwrap()), expected);
                        ended = true;
                        break;
                    }
                    LookupStep::Decoded(value) => {
                        assert_eq!(Some(value.read(|v| v).unwrap()), expected);
                        ended = true;
                        break;
                    }
                    LookupStep::Tombstone => {
                        assert_eq!(key, 0);
                        ended = true;
                        break;
                    }
                    LookupStep::Missing => {
                        assert_eq!(key, 99);
                        ended = true;
                        break;
                    }
                }
            }
            assert!(ended, "预算推进未终结");
            assert_eq!(reads, expected_reads);
            assert!(lookup.step(&log, &storage, PollBudget::default()).is_err());
        }
    }
    #[test]
    fn 淘汰后的页通过跨段短读恢复且损坏帧被拒绝() {
        use crate::log::read_page::{PageRead, ReadPage};
        let log = log();
        let (device, storage, files) = storage();
        flush(&log, &storage);
        assert_eq!(log.evict_next().unwrap().completed, 1);
        assert!(log.lease(LogAddress(0)).is_err());
        let read = |device: &MemoryDevice| -> Result<ReadPage, Error> {
            let mut task =
                PageRead::new(PageId(0), 256, CheckpointVersion(0), CompletionRoute(19))?;
            loop {
                task.submit_next(&storage)?;
                task.accept(&storage, complete(device))
                    .map_err(|r| r.reason)?;
                if let Some(page) = task.finish(&storage)? {
                    return Ok(page);
                }
            }
        };
        device.inject_next(MemoryFault::Short(3)).unwrap();
        let page = read(&device).unwrap();
        let temporary = log
            .decode_temporary(page.record(LogAddress(72)).unwrap().value)
            .unwrap();
        assert_eq!(temporary.read(|v| v).unwrap(), 1);
        assert_eq!(page.record(LogAddress(0)).unwrap().key, [0]);
        assert_eq!(
            page.record(LogAddress(72)).unwrap().value,
            1u64.to_le_bytes()
        );
        assert!(page.record(LogAddress(1)).is_err());
        assert!(page.record(LogAddress(256)).is_err());
        let mut buffer = AlignedBuffer::new_zeroed(1, 8).unwrap();
        buffer.as_mut_slice()[0] = b'X';
        execute(
            &*device,
            IoOperation::Write {
                file: files[0],
                offset: 0,
                buffer,
            },
        )
        .result
        .unwrap();
        assert!(matches!(read(&device), Err(Error::InvalidFormat(_))));
    }
    #[test]
    fn 旧租约阻止安全回收而新分配只能复用新代次() {
        let log = log();
        let (_, storage, _) = storage();
        let lease = log.lease(LogAddress(0)).unwrap();
        assert!(matches!(log.evict_next(), Err(Error::Busy)));
        flush(&log, &storage);
        let progress = log.evict_next().unwrap();
        assert_eq!(progress.remaining, 1);
        assert_eq!(log.frontiers().unwrap().head, LogAddress(256));
        assert_eq!(log.frontiers().unwrap().safe_head, LogAddress(0));
        assert!(log.lease(LogAddress(0)).is_err());
        assert_eq!(lease.read(|v| v).unwrap(), 0);
        assert!(lease.update(|_| Ok(())).is_err());
        drop(lease);
        assert_eq!(log.evict_next().unwrap().completed, 1);
        assert_eq!(log.frontiers().unwrap().safe_head, LogAddress(256));
        for key in 4..7 {
            log.finish_initialization(log.reserve_record(&[key], None, key as u64).unwrap())
                .unwrap();
        }
        assert_eq!(
            log.lease(LogAddress(512)).unwrap().generation(),
            Generation(1)
        );
        assert!(log.lease_generation(LogAddress(0), Generation(0)).is_err());
    }
    #[test]
    fn 两页内存跨越多次窗口且所有刷盘页可重读() {
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 256,
                memory_pages: 2,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
        )
        .unwrap();
        let (device, storage, files) = storage_with_size(4096);
        for key in 0..30 {
            log.finish_initialization(log.reserve_record(&[key], None, key as u64).unwrap())
                .unwrap();
            let frontiers = log.frontiers().unwrap();
            let boundary = LogAddress(frontiers.tail.0 / 256 * 256);
            if boundary > frontiers.flushed_until {
                log.advance_read_only(boundary).unwrap();
                flush(&log, &storage);
                assert_eq!(log.evict_next().unwrap().completed, 1);
            }
        }
        assert_eq!(log.frontiers().unwrap().safe_head, LogAddress(9 * 256));
        let done = execute(
            &*device,
            IoOperation::Read {
                file: files[0],
                offset: 0,
                buffer: AlignedBuffer::new_zeroed(4096, 8).unwrap(),
            },
        );
        let IoOutcome::Transferred(length) = done.result.unwrap() else {
            panic!("读取")
        };
        assert_eq!(length, 9 * (256 + 36));
        let buffer = done.buffer.unwrap();
        for (page, bytes) in buffer.as_slice()[..length].chunks(292).enumerate() {
            let frame = crate::format::PageFrame::decode(bytes, PageId(page as u64), 256).unwrap();
            for (offset, (_, record)) in frame.records().unwrap().iter().enumerate() {
                let key = page * 3 + offset;
                assert_eq!(record.key, [key as u8]);
                assert_eq!(record.value, (key as u64).to_le_bytes());
            }
        }
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn 原生工作线程刷盘后重读三段并解码页帧() {
        struct Directory(PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let root = Directory(std::env::temp_dir().join(format!(
            "raster-flush-{:x?}",
            StoreId::generate().unwrap().0
        )));
        let device = crate::device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }
        .open(DeviceOpenOptions {
            root: root.0.clone(),
            create_new: true,
        })
        .unwrap();
        let storage = SegmentedStorage::new(Arc::from(device), root.0.clone(), 128).unwrap();
        let log = log();
        let mut task = log
            .begin_flush(&storage, CompletionRoute(77), CheckpointVersion(0))
            .unwrap();
        loop {
            task.submit_next(&storage).unwrap();
            task.accept(&storage, complete(&*storage.device))
                .map_err(|r| r.reason)
                .unwrap();
            if log.finish_flush(&storage, &mut task).unwrap() {
                break;
            }
        }
        storage
            .device
            .shutdown(Deadline(
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            ))
            .unwrap();
        let mut bytes = vec![];
        for number in 0..3 {
            bytes.extend(
                std::fs::read(root.0.join(storage.segment_path(number, Generation(0)))).unwrap(),
            );
        }
        let frame = crate::format::PageFrame::decode(&bytes, PageId(0), 256).unwrap();
        assert_eq!(frame.records().unwrap().len(), 3);
        assert_eq!(log.frontiers().unwrap().flushed_until, LogAddress(256));
    }
    #[test]
    fn 三段短写全部完成后才推进刷盘边界() {
        let log = log();
        let (device, storage, files) = storage();
        let mut task = log
            .begin_flush(&storage, CompletionRoute(7), CheckpointVersion(0))
            .unwrap();
        assert!(matches!(
            log.begin_flush(&storage, CompletionRoute(8), CheckpointVersion(0)),
            Err(Error::Busy)
        ));
        assert!(!log.finish_flush(&storage, &mut task).unwrap());
        device.inject_next(MemoryFault::Short(7)).unwrap();
        let mut writes = 0;
        loop {
            assert!(task.submit_next(&storage).unwrap().is_some());
            assert_eq!(log.frontiers().unwrap().flushed_until, LogAddress(0));
            task.accept(&storage, complete(&*device))
                .map_err(|r| r.reason)
                .unwrap();
            writes += 1;
            if log.finish_flush(&storage, &mut task).unwrap() {
                break;
            }
        }
        assert_eq!(writes, 4);
        assert_eq!(log.frontiers().unwrap().flushed_until, LogAddress(256));
        assert_eq!(log.frontiers().unwrap().head, LogAddress(0));
        assert!(log.finish_flush(&storage, &mut task).is_err());
        let mut bytes = vec![];
        for file in files {
            let done = execute(
                &*device,
                IoOperation::Read {
                    file,
                    offset: 0,
                    buffer: AlignedBuffer::new_zeroed(128, 8).unwrap(),
                },
            );
            let IoOutcome::Transferred(n) = done.result.unwrap() else {
                panic!("读取")
            };
            bytes.extend_from_slice(&done.buffer.unwrap().as_slice()[..n]);
        }
        let frame = crate::format::PageFrame::decode(&bytes, PageId(0), 256).unwrap();
        let records = frame.records().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[2].1.value, 2u64.to_le_bytes());
    }
    #[test]
    fn 失败与错误日志不得推进或消费其他任务() {
        let first = log();
        let second = log();
        let (device, storage, _) = storage();
        let mut task = first
            .begin_flush(&storage, CompletionRoute(7), CheckpointVersion(0))
            .unwrap();
        assert!(second.finish_flush(&storage, &mut task).is_err());
        device
            .inject_next(MemoryFault::Fail(std::io::ErrorKind::Other))
            .unwrap();
        task.submit_next(&storage).unwrap();
        task.accept(&storage, complete(&*device))
            .map_err(|r| r.reason)
            .unwrap();
        assert!(first.finish_flush(&storage, &mut task).is_err());
        assert_eq!(first.frontiers().unwrap().flushed_until, LogAddress(0));
        // 无在途 I/O 后允许上层显式开启一次新刷盘，不自行重试。
        assert!(
            first
                .begin_flush(&storage, CompletionRoute(8), CheckpointVersion(0))
                .is_ok()
        );
    }
    #[test]
    fn 所有传输结束后段失效仍拒绝边界推进() {
        let log = log();
        let (device, storage, _) = storage();
        let mut task = log
            .begin_flush(&storage, CompletionRoute(7), CheckpointVersion(0))
            .unwrap();
        for _ in 0..3 {
            task.submit_next(&storage).unwrap();
            task.accept(&storage, complete(&*device))
                .map_err(|r| r.reason)
                .unwrap();
        }
        storage.invalidate(0, Generation(0)).unwrap();
        assert!(matches!(
            log.finish_flush(&storage, &mut task),
            Err(Error::RangeTruncated)
        ));
        assert_eq!(log.frontiers().unwrap().flushed_until, LogAddress(0));
    }
}
