//! 压缩的逐步物理扫描：最多一页读取，跨轮询仅保存拥有字节和物理段租约。
use crate::{
    engine::{Engine, io_hub::CompletionHub},
    format::{PageFrame, Record},
    log::read_page::PageRead,
    scan::page::PageCursor,
    schema::Schema,
    storage::{SegmentReadLease, SegmentedStorage},
    types::*,
};
use std::sync::Arc;

pub(crate) enum ScanStep {
    Record(LogAddress, Vec<u8>),
    Pending,
    End,
}
struct Reading {
    hub: Arc<CompletionHub>,
    id: RequestId,
    page: PageRead,
    _lease: SegmentReadLease,
    begin: LogAddress,
    end: LogAddress,
    validation: bool,
}
impl Drop for Reading {
    fn drop(&mut self) {
        // 正常释放先归还在途完成；异常销毁只撤销历史路由，不夺回设备拥有的缓冲。
        let _ = self.hub.release(self.id);
    }
}
pub(crate) struct Scan {
    next: LogAddress,
    end: LogAddress,
    validate_end: bool,
    reading: Option<Reading>,
    cursor: Option<(PageCursor, LogAddress)>,
}
impl Scan {
    pub fn new<S: Schema>(
        engine: &Engine<S>,
        begin: LogAddress,
        end: LogAddress,
    ) -> Result<Self, Error> {
        let frontiers = engine.log.validate_scan_range(begin, end)?;
        Ok(Self {
            next: begin,
            end,
            // 在开始搬迁之前检查冷页内的结束边界，避免发现截断记录时已经搬迁部分范围。
            validate_end: begin != end
                && end < frontiers.head
                && !end.0.is_multiple_of(engine.config.log.page_bytes as u64),
            reading: None,
            cursor: None,
        })
    }
    pub fn has_inflight(&self) -> bool {
        self.reading
            .as_ref()
            .is_some_and(|reading| reading.page.has_inflight())
    }
    /// 一个步骤最多提交一段 I/O 或产出一条记录；不等待设备或执行循环式同步扫描。
    pub fn step<S: Schema>(&mut self, engine: &Engine<S>) -> Result<ScanStep, Error> {
        if self.next < engine.log.frontiers()?.begin {
            return Err(Error::RangeTruncated);
        }
        if let Some(reading) = &mut self.reading {
            if let Some(completion) = reading.hub.take(reading.id)? {
                reading
                    .page
                    .accept(&engine.storage, completion)
                    .map_err(|rejected| rejected.reason)?;
            }
            if let Some(page) = reading.page.finish(&engine.storage)? {
                let cursor = page.into_scan(reading.begin, reading.end)?;
                if reading.validation {
                    self.validate_end = false;
                } else {
                    self.cursor = Some((cursor, reading.end));
                }
                self.reading = None;
            } else {
                match reading.page.submit_next(&engine.storage) {
                    Ok(_) | Err(Error::Busy) => {}
                    Err(error) => return Err(error),
                }
                return Ok(ScanStep::Pending);
            }
        }
        if self.validate_end {
            let page_bytes = engine.config.log.page_bytes as u64;
            let begin = LogAddress(self.end.0 / page_bytes * page_bytes);
            self.begin_read(engine, begin, self.end, true)?;
            return Ok(ScanStep::Pending);
        }
        if let Some((cursor, end)) = &mut self.cursor {
            if let Some((address, bytes)) = cursor.next_encoded()? {
                self.next =
                    address.checked_add(Record::decode(&bytes)?.header.encoded_len()? as u64)?;
                return Ok(ScanStep::Record(address, bytes));
            }
            self.next = *end;
            self.cursor = None;
        }
        if self.next == self.end {
            return Ok(ScanStep::End);
        }
        let page_bytes = engine.config.log.page_bytes as u64;
        let limit = LogAddress(self.next.0 / page_bytes * page_bytes)
            .checked_add(page_bytes)?
            .min(self.end);
        if self.next < engine.log.frontiers()?.head {
            self.begin_read(engine, self.next, limit, false)?;
            return Ok(ScanStep::Pending);
        }
        match engine.log.snapshot_next(self.next, limit) {
            Ok(Some((address, bytes))) => {
                self.next =
                    address.checked_add(Record::decode(&bytes)?.header.encoded_len()? as u64)?;
                Ok(ScanStep::Record(address, bytes))
            }
            Ok(None) => {
                self.next = limit;
                Ok(ScanStep::Pending)
            }
            Err(Error::Busy) => Ok(ScanStep::Pending),
            Err(Error::RangeTruncated) if self.next >= engine.log.frontiers()?.begin => {
                Ok(ScanStep::Pending)
            }
            Err(error) => Err(error),
        }
    }
    fn begin_read<S: Schema>(
        &mut self,
        engine: &Engine<S>,
        begin: LogAddress,
        end: LogAddress,
        validation: bool,
    ) -> Result<(), Error> {
        let page_bytes = engine.config.log.page_bytes;
        let page = begin.page_offset(page_bytes as u64)?.0;
        let lease = engine.storage.lease_read(
            PageFrame::physical_offset(page, page_bytes)?,
            PageFrame::encoded_size(page_bytes)?,
        )?;
        let id = engine.io.reserve(SessionId(engine.id.0))?;
        let read = match PageRead::new(page, page_bytes, CompletionHub::route(id)) {
            Ok(read) => read,
            Err(error) => {
                engine.io.release(id)?;
                return Err(error);
            }
        };
        self.reading = Some(Reading {
            hub: engine.io.clone(),
            id,
            page: read,
            _lease: lease,
            begin,
            end,
            validation,
        });
        Ok(())
    }
    /// 收尾只消费已经提交的读取，不继续短传输或后续页；成功后可释放所属压缩动作。
    pub fn drain(&mut self, storage: &SegmentedStorage) -> Result<bool, Error> {
        if let Some(reading) = &mut self.reading {
            if let Some(completion) = reading.hub.take(reading.id)? {
                reading
                    .page
                    .accept(storage, completion)
                    .map_err(|rejected| rejected.reason)?;
            }
            if reading.page.has_inflight() {
                return Ok(false);
            }
        }
        self.reading = None;
        self.cursor = None;
        Ok(true)
    }
}
