//! Scanning is independent of session barrier registration; a closed read continues
//! to occupy its quota until the device returns it.
use super::{Engine, io_hub::CompletionHub};
use crate::{
    api::scan::{Buffering, RecordScanner, ScanOptions, ScannedRecord},
    log::read_page::PageRead,
    scan::{page::PageCursor, record::decode_record},
    schema::Schema,
    types::*,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

#[derive(Default)]
pub(crate) struct ScanRegistry(Mutex<Registry>);
#[derive(Default)]
struct Registry {
    next: u64,
    scans: BTreeMap<u64, Arc<ScanHandle>>,
}
pub(crate) struct ScanHandle {
    abandoned: AtomicBool,
    state: Mutex<ScanState>,
}
struct ScanState {
    next: LogAddress,
    end: LogAddress,
    mode: Buffering,
    pages: Vec<ScanPage>,
    closing: bool,
    closed: bool,
    finished: bool,
    failed: bool,
}
struct ScanPage {
    page: PageId,
    begin: LogAddress,
    end: LogAddress,
    id: Option<RequestId>,
    read: Option<PageRead>,
    lease: Option<crate::storage::SegmentReadLease>,
    cursor: Option<PageCursor>,
    error: Option<Error>,
}
impl ScanRegistry {
    fn insert(&self, state: Arc<ScanHandle>, limit: usize) -> Result<u64, Error> {
        let mut registry = self
            .0
            .lock()
            .map_err(|_| Error::InvalidState("Scan registration lock poisoning"))?;
        if registry.scans.len() >= limit {
            return Err(Error::Busy);
        }
        let id = registry.next;
        registry.next = id.checked_add(1).ok_or(Error::CapacityExceeded)?;
        registry.scans.insert(id, state);
        Ok(id)
    }
    fn entries(&self) -> Result<Vec<(u64, Arc<ScanHandle>)>, Error> {
        let registry = self
            .0
            .lock()
            .map_err(|_| Error::InvalidState("Scan registration lock poisoning"))?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(registry.scans.len())
            .map_err(|_| Error::OutOfMemory)?;
        entries.extend(
            registry
                .scans
                .iter()
                .map(|(&id, state)| (id, state.clone())),
        );
        Ok(entries)
    }
    fn remove(&self, id: u64) -> Result<(), Error> {
        self.0
            .lock()
            .map_err(|_| Error::InvalidState("Scan registration lock poisoning"))?
            .scans
            .remove(&id);
        Ok(())
    }
}
impl Buffering {
    fn frames(self) -> usize {
        match self {
            Self::Unbuffered => 1,
            Self::SinglePage => 2,
            Self::DoublePage => 3,
        }
    }
}
impl ScanPage {
    fn new<S: Schema>(
        engine: &Engine<S>,
        page: PageId,
        begin: LogAddress,
        end: LogAddress,
    ) -> Result<Self, Error> {
        let start = crate::format::PageFrame::physical_offset(page, engine.config.log.page_bytes)?;
        let length = crate::format::PageFrame::encoded_size(engine.config.log.page_bytes)?;
        let lease = engine.storage.lease_read(start, length)?;
        let id = engine.io.reserve(SessionId(engine.id.0))?;
        let read = match PageRead::new(page, engine.config.log.page_bytes, CompletionHub::route(id))
        {
            Ok(read) => read,
            Err(error) => {
                engine.io.release(id)?;
                return Err(error);
            }
        };
        Ok(Self {
            page,
            begin,
            end,
            id: Some(id),
            read: Some(read),
            lease: Some(lease),
            cursor: None,
            error: None,
        })
    }
    /// At most one request in transit per slot;Cancellation only charges completed,Subsequent segmented reads are no longer committed.
    fn progress<S: Schema>(&mut self, engine: &Engine<S>, cancel: bool) -> Result<(), Error> {
        let Some(read) = &mut self.read else {
            return Ok(());
        };
        let id = self
            .id
            .ok_or(Error::InvalidState("Scan read missing completion mailbox"))?;
        if let Some(completion) = engine.io.take(id)? {
            read.accept(&engine.storage, completion)
                .map_err(|rejected| rejected.reason)?;
        }
        if !cancel && self.error.is_none() {
            match read.finish(&engine.storage) {
                Ok(Some(page)) => match page.into_scan(self.begin, self.end) {
                    Ok(cursor) => self.cursor = Some(cursor),
                    Err(error) => self.error = Some(error),
                },
                Ok(None) => match read.submit_next(&engine.storage) {
                    Ok(_) | Err(Error::Busy) => {}
                    Err(error) => self.error = Some(error),
                },
                Err(error) => self.error = Some(error),
            }
        }
        if !read.has_inflight() && (cancel || self.cursor.is_some() || self.error.is_some()) {
            engine.io.release(id)?;
            self.id = None;
            self.read = None;
            self.lease = None;
        }
        Ok(())
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn open_scan(
        self: &Arc<Self>,
        options: ScanOptions,
    ) -> Result<RecordScanner<S>, Error> {
        let done = self.shutdown_state.try_lock().map_err(lock_error)?;
        if *done
            || self.shutdown_requested.load(Ordering::SeqCst)
            || self.failed.load(Ordering::SeqCst)
        {
            return Err(Error::InvalidState(
                "Engine shuts down or fails,Unable to register for scan",
            ));
        }
        self.scan_available()?;
        self.io.poll(&*self.storage.device, PollBudget::default())?;
        self.progress_scans()?;
        self.log.validate_scan_range(options.begin, options.end)?;
        let deadline = self.scan_deadline()?;
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(options.buffering.frames())
            .map_err(|_| Error::OutOfMemory)?;
        let state = Arc::new(ScanHandle {
            abandoned: AtomicBool::new(false),
            state: Mutex::new(ScanState {
                next: options.begin,
                end: options.end,
                mode: options.buffering,
                pages,
                closing: false,
                closed: false,
                finished: false,
                failed: false,
            }),
        });
        let registration = self
            .scans
            .insert(state.clone(), self.config.scan.max_scanners)?;
        let scanner = RecordScanner {
            engine: self.clone(),
            registration,
            state: state.clone(),
        };
        drop(done);
        // Register first and then perform boundary reads that may wait;Concurrency shutdown You will see active scanning.
        let validation = (|| {
            let mut state = state.state.try_lock().map_err(lock_error)?;
            if state.next != state.end {
                let page_bytes = self.config.log.page_bytes as u64;
                let mut checked = None;
                for boundary in [state.next, state.end] {
                    let frontiers = self.log.validate_scan_range(state.next, state.end)?;
                    if boundary < frontiers.head && boundary.0 % page_bytes != 0 {
                        let page = boundary.page_offset(page_bytes)?.0;
                        if checked == Some(page) {
                            continue;
                        }
                        let base = LogAddress::from_page_offset(page, 0, page_bytes)?;
                        let end = base.checked_add(page_bytes)?.min(state.end);
                        let begin = base.max(state.next);
                        state.pages.push(ScanPage::new(self, page, begin, end)?);
                        loop {
                            self.scan_poll()?;
                            state.pages[0].progress(self, false)?;
                            if let Some(error) = state.pages[0].error.take() {
                                return Err(error);
                            }
                            if state.pages[0].cursor.is_some() {
                                break;
                            }
                            if deadline.expired() {
                                return Err(Error::DeadlineExceeded);
                            }
                            std::thread::yield_now();
                        }
                        state.pages.clear();
                        checked = Some(page);
                    }
                }
                self.log.validate_scan_range(state.next, state.end)?;
            }
            Ok(())
        })();
        validation?;
        Ok(scanner)
    }
    fn scan_available(&self) -> Result<(), Error> {
        if self.failed.load(Ordering::SeqCst)
            || self.shutdown_requested.load(Ordering::SeqCst)
            || self.coordinator.snapshot()?.phase == crate::coordination::Phase::Failed
        {
            return Err(Error::InvalidState(
                "Engine shuts down or fails,Can't advance scan",
            ));
        }
        Ok(())
    }
    fn scan_deadline(&self) -> Result<Deadline, Error> {
        Instant::now()
            .checked_add(self.config.scan.timeout)
            .map(Deadline)
            .ok_or(Error::CapacityExceeded)
    }
    fn scan_poll(&self) -> Result<(), Error> {
        let result = self
            .io
            .poll(&*self.storage.device, PollBudget::default())
            .and_then(|_| self.progress_scans());
        if result.is_err() {
            self.failed.store(true, Ordering::SeqCst);
        }
        result
    }
    pub(crate) fn scan_next(
        &self,
        id: u64,
        shared: &ScanHandle,
    ) -> Result<Option<ScannedRecord<S>>, Error> {
        let mut state = shared.state.try_lock().map_err(lock_error)?;
        if state.closed && state.finished {
            return Ok(None);
        }
        if shared.abandoned.load(Ordering::SeqCst) || state.closing || state.closed || state.failed
        {
            return Err(Error::InvalidState("Scan is closed or failed"));
        }
        let deadline = self.scan_deadline()?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.scan_next_inner(id, &mut state, deadline)
        }))
        .unwrap_or_else(|_| {
            self.failed.store(true, Ordering::SeqCst);
            Err(Error::InvalidState("Scan execution panic"))
        });
        if let Err(error) = &result
            && !matches!(
                error,
                Error::Busy | Error::DeadlineExceeded | Error::OutOfMemory
            )
        {
            if matches!(error, Error::InvalidState(_)) {
                self.failed.store(true, Ordering::SeqCst);
            }
            state.failed = true;
            state.closing = true;
            // Only try non-blocking closing;In-flight slots remain in the registry,Subsequent polling will continue to empty the.
            let _ = self.close_scan_step(id, &mut state);
        }
        result
    }
    fn scan_next_inner(
        &self,
        id: u64,
        state: &mut ScanState,
        deadline: Deadline,
    ) -> Result<Option<ScannedRecord<S>>, Error> {
        loop {
            self.scan_available()?;
            if state.next == state.end {
                state.finished = true;
                state.closing = true;
                self.close_scan_step(id, state)?;
                return Ok(None);
            }
            let frontiers = self.log.frontiers()?;
            if state.next < frontiers.begin {
                return Err(Error::RangeTruncated);
            }
            let page_bytes = self.config.log.page_bytes as u64;
            let page = state.next.page_offset(page_bytes)?.0;
            let base = LogAddress::from_page_offset(page, 0, page_bytes)?;
            let page_end = base.checked_add(page_bytes)?.min(state.end);
            if state.next >= frontiers.head {
                match self.log.snapshot_next(state.next, page_end) {
                    Ok(Some((address, bytes))) => {
                        let record = decode_record(&*self.schema, address, &bytes)?;
                        if state.next < self.log.frontiers()?.begin {
                            return Err(Error::RangeTruncated);
                        }
                        state.next = address.checked_add(bytes.len() as u64)?;
                        return Ok(Some(record));
                    }
                    Ok(None) => {
                        if state.next < self.log.frontiers()?.begin {
                            return Err(Error::RangeTruncated);
                        }
                        state.next = page_end;
                        continue;
                    }
                    Err(Error::Busy) => {}
                    // head May advance before selecting a record;Next round turns to disk according to latest boundary.
                    Err(Error::RangeTruncated) => continue,
                    Err(error) => return Err(error),
                }
            } else {
                for offset in 0..state.mode.frames() {
                    let number = page
                        .0
                        .checked_add(offset as u64)
                        .ok_or(Error::CapacityExceeded)?;
                    let at = LogAddress::from_page_offset(PageId(number), 0, page_bytes)?;
                    if at >= state.end || at >= frontiers.head {
                        break;
                    }
                    if state.pages.iter().any(|slot| slot.page == PageId(number)) {
                        continue;
                    }
                    let end = at.checked_add(page_bytes)?.min(state.end);
                    state.pages.push(ScanPage::new(
                        self,
                        PageId(number),
                        at.max(state.next),
                        end,
                    )?);
                }
                self.scan_poll()?;
                for slot in &mut state.pages {
                    slot.progress(self, false)?;
                }
                let slot = state.pages.first_mut().ok_or(Error::InvalidState(
                    "Scan for missing pages on current disk",
                ))?;
                if slot.page != page {
                    return Err(Error::InvalidState("Scan page windows in wrong order"));
                }
                if let Some(error) = slot.error.take() {
                    return Err(error);
                }
                if let Some(cursor) = &mut slot.cursor {
                    let result = cursor.next_record(&*self.schema)?;
                    if state.next < self.log.frontiers()?.begin {
                        return Err(Error::RangeTruncated);
                    }
                    if result.is_some() {
                        state.next = cursor.position();
                        return Ok(result);
                    }
                    state.pages.remove(0);
                    state.next = page_end;
                    continue;
                }
            }
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            self.scan_poll()?;
            std::thread::yield_now();
        }
    }
    fn close_scan_step(&self, id: u64, state: &mut ScanState) -> Result<bool, Error> {
        if state.closed {
            return Ok(true);
        }
        for slot in &mut state.pages {
            slot.progress(self, true)?;
        }
        if state.pages.iter().any(|slot| slot.read.is_some()) {
            return Ok(false);
        }
        state.pages.clear();
        state.closed = true;
        self.scans.remove(id)?;
        Ok(true)
    }
    pub(crate) fn close_scan(&self, id: u64, shared: &ScanHandle) -> Result<(), Error> {
        shared.abandoned.store(true, Ordering::SeqCst);
        let deadline = self.scan_deadline()?;
        let mut state = shared.state.try_lock().map_err(lock_error)?;
        if state.closed {
            return Ok(());
        }
        state.closing = true;
        loop {
            self.scan_poll()?;
            if self.close_scan_step(id, &mut state)? {
                return Ok(());
            }
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            std::thread::yield_now();
        }
    }
    pub(crate) fn abandon_scan(&self, id: u64, shared: &ScanHandle) {
        // Even if the finishing thread holds the state lock temporarily,Can't be lost either Drop Notification.
        shared.abandoned.store(true, Ordering::SeqCst);
        if let Ok(mut state) = shared.state.try_lock() {
            state.closing = true;
            let _ = self.close_scan_step(id, &mut state);
        }
    }
    pub(crate) fn progress_scans(&self) -> Result<(), Error> {
        // Copy the registration reference first and then release the registration lock;Never wait within a registration lock to scan the lock or perform decoding.
        for (id, shared) in self.scans.entries()? {
            let mut state = match shared.state.try_lock() {
                Ok(state) => state,
                Err(TryLockError::WouldBlock) => continue,
                Err(_) => return Err(Error::InvalidState("Scan status lock poisoning")),
            };
            state.closing |= shared.abandoned.load(Ordering::SeqCst);
            if state.closing {
                self.close_scan_step(id, &mut state)?;
            } else {
                // Only advance byte reads of existing windows,Do not decode user values or generate additional readahead pages.
                // When the scanner is idle,Other session polls can also promptly return segment leases for completed pages..
                for slot in &mut state.pages {
                    slot.progress(self, false)?;
                }
            }
        }
        Ok(())
    }
    /// After the equipment is confirmed to be empty,Only then can the scan slots that cannot be ended normally due to device protocol errors be released..
    pub(crate) fn release_stopped_scans(&self) -> Result<(), Error> {
        for (id, shared) in self.scans.entries()? {
            let mut state = shared.state.try_lock().map_err(lock_error)?;
            for slot in &mut state.pages {
                if let Some(route) = slot.id.take() {
                    self.io.release(route)?;
                }
            }
            state.pages.clear();
            state.closed = true;
            state.closing = true;
            self.scans.remove(id)?;
        }
        Ok(())
    }
    pub(crate) fn drain_scans(&self, deadline: Deadline) -> Result<(), Error> {
        loop {
            let entries = self.scans.entries()?;
            if entries.is_empty() {
                return Ok(());
            }
            for (_, shared) in entries {
                let state = shared.state.try_lock().map_err(lock_error)?;
                if !state.closing && !shared.abandoned.load(Ordering::SeqCst) {
                    return Err(Error::Busy);
                }
            }
            self.scan_poll()?;
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            std::thread::yield_now();
        }
    }
}
fn lock_error<T>(error: TryLockError<T>) -> Error {
    match error {
        TryLockError::WouldBlock => Error::Busy,
        TryLockError::Poisoned(_) => Error::InvalidState("Scan status or close lock poisoning"),
    }
}

#[cfg(test)]
#[path = "scan_tests.rs"]
mod tests;
