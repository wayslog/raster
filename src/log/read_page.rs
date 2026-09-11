//! 磁盘页读取只携带拥有型字节；完整帧校验后才允许按逻辑地址查看记录。
use crate::{
    device::{CompletionRoute, IoCompletion},
    format::{PageFrame, Record},
    storage::{SegmentReadLease, SegmentedStorage, transfer::SegmentTransfer},
    types::*,
};
pub(crate) struct PageRead {
    page: PageId,
    page_bytes: usize,
    transfer: SegmentTransfer,
    protection: Option<(std::sync::Arc<()>, SegmentReadLease)>,
}
pub(crate) struct ReadPage {
    page: PageId,
    page_bytes: usize,
    bytes: Vec<u8>,
}
impl ReadPage {
    /// 消费完成页并交给扫描器；游标保留完整拥有缓冲，不借用设备或日志槽位。
    pub fn into_scan(
        self,
        begin: LogAddress,
        end: LogAddress,
    ) -> Result<crate::scan::page::PageCursor, Error> {
        crate::scan::page::PageCursor::new(self.bytes, self.page, self.page_bytes, begin, end)
    }

    /// 检查点复制整个页之前校验全部记录，不能只校验帧外壳。
    pub fn into_checkpoint_bytes(self) -> Result<Vec<u8>, Error> {
        PageFrame::decode(&self.bytes, self.page, self.page_bytes)?.records()?;
        Ok(self.bytes)
    }

    pub fn record(&self, address: LogAddress) -> Result<Record<'_>, Error> {
        if address.page_offset(self.page_bytes as u64)?.0 != self.page {
            return Err(Error::InvalidFormat("记录属于其他页"));
        }
        let frame = PageFrame::decode(&self.bytes, self.page, self.page_bytes)?;
        let (_, record) = frame
            .records()?
            .into_iter()
            .find(|(at, _)| *at == address)
            .ok_or(Error::InvalidFormat("地址不是记录起点"))?;
        if record.header.invalid {
            return Err(Error::InvalidFormat("地址指向无效记录"));
        }
        // frame 的 payload 借用来自 self.bytes，返回的记录不借用临时帧本身。
        let offset = usize::try_from(address.0 % self.page_bytes as u64)
            .map_err(|_| Error::CapacityExceeded)?;
        let length = record.header.encoded_len()?;
        Record::decode(&frame.payload[offset..offset + length])
    }
}
impl PageRead {
    pub fn has_inflight(&self) -> bool {
        self.transfer.has_inflight()
    }

    pub fn new(page: PageId, page_bytes: usize, route: CompletionRoute) -> Result<Self, Error> {
        let start = PageFrame::physical_offset(page, page_bytes)?;
        let length = PageFrame::encoded_size(page_bytes)?;
        Ok(Self {
            page,
            page_bytes,
            transfer: SegmentTransfer::read(start, length, route)?,
            protection: None,
        })
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        if self
            .protection
            .as_ref()
            .is_some_and(|(owner, _)| !std::sync::Arc::ptr_eq(owner, &storage.identity))
        {
            return Err(Error::InvalidState("受保护页读取属于其他存储"));
        }
        if self.transfer.next_address()?.is_none() {
            return Ok(None);
        }
        if self.protection.is_none() {
            let start = PageFrame::physical_offset(self.page, self.page_bytes)?;
            let length = PageFrame::encoded_size(self.page_bytes)?;
            // 一次保护整个帧：短读与跨段续传之间也不能使尚未提交的后续段失效。
            self.protection = Some((storage.identity.clone(), storage.lease_read(start, length)?));
        }
        self.transfer.submit_next(storage)
    }
    #[allow(clippy::result_large_err, reason = "错误完成路由原样归还缓冲")]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        self.transfer.accept(storage, completion)
    }
    pub fn finish(&mut self, storage: &SegmentedStorage) -> Result<Option<ReadPage>, Error> {
        self.transfer.validate_bindings(storage)?;
        let Some(result) = self.transfer.take_read_result() else {
            return Ok(None);
        };
        // 到达终结结果说明设备已归还所有在途缓冲；后续页解析只依赖拥有型字节。
        self.protection = None;
        let bytes = result?;
        // 页帧上界不等于请求版本；新旧记录可共页，操作先后关系由引擎版本许可保证。
        PageFrame::decode(&bytes, self.page, self.page_bytes)?;
        Ok(Some(ReadPage {
            page: self.page,
            page_bytes: self.page_bytes,
            bytes,
        }))
    }
}
