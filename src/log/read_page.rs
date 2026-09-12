//! Disk page reads carry only owned bytes;Viewing records by logical address is only allowed after complete frame verification..
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
    /// Consume the completed page and hand it over to the scanner;Cursor retains full ownership buffer,No borrowing of devices or log slots.
    pub fn into_scan(
        self,
        begin: LogAddress,
        end: LogAddress,
    ) -> Result<crate::scan::page::PageCursor, Error> {
        crate::scan::page::PageCursor::new(self.bytes, self.page, self.page_bytes, begin, end)
    }

    /// Checkpoint verifies all records before copying the entire page,Can't just check the frame shell.
    pub fn into_checkpoint_bytes(self) -> Result<Vec<u8>, Error> {
        PageFrame::decode(&self.bytes, self.page, self.page_bytes)?.records()?;
        Ok(self.bytes)
    }

    pub fn record(&self, address: LogAddress) -> Result<Record<'_>, Error> {
        if address.page_offset(self.page_bytes as u64)?.0 != self.page {
            return Err(Error::InvalidFormat("Records belong to other pages"));
        }
        let frame = PageFrame::decode(&self.bytes, self.page, self.page_bytes)?;
        let (_, record) = frame
            .records()?
            .into_iter()
            .find(|(at, _)| *at == address)
            .ok_or(Error::InvalidFormat(
                "The address is not the starting point of the record",
            ))?;
        if record.header.invalid {
            return Err(Error::InvalidFormat("Address points to invalid record"));
        }
        // frame of payload Borrowed from self.bytes,The returned record does not borrow the temporary frame itself.
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
            return Err(Error::InvalidState(
                "Protected page reads belong to other storage",
            ));
        }
        if self.transfer.next_address()?.is_none() {
            return Ok(None);
        }
        if self.protection.is_none() {
            let start = PageFrame::physical_offset(self.page, self.page_bytes)?;
            let length = PageFrame::encoded_size(self.page_bytes)?;
            // Protect entire frame at once:Between short reading and cross-segment resuming, unsubmitted subsequent segments cannot be invalidated..
            self.protection = Some((storage.identity.clone(), storage.lease_read(start, length)?));
        }
        self.transfer.submit_next(storage)
    }
    #[allow(
        clippy::result_large_err,
        reason = "Error completion routing returns the buffer as is"
    )]
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
        // The arrival of the final result indicates that the device has returned all buffers in transit.;Subsequent page parsing relies only on owning bytes.
        self.protection = None;
        let bytes = result?;
        // The page frame upper bound is not equal to the requested version;Old and new records can be shared on the same page,The order of operations is guaranteed by the engine version license..
        PageFrame::decode(&bytes, self.page, self.page_bytes)?;
        Ok(Some(ReadPage {
            page: self.page,
            page_bytes: self.page_bytes,
            bytes,
        }))
    }
}
