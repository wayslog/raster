//! The background page promotes shared device mailboxes,but does not execute or migrate session user requests.
use super::{Engine, io_hub::CompletionHub};
use crate::{log::flush::PageFlush, schema::Schema, types::*};
#[derive(Default)]
pub(crate) struct StorageProgress {
    flush: Option<(RequestId, PageFlush)>,
}
impl StorageProgress {
    pub(crate) fn has_flush(&self) -> bool {
        self.flush.is_some()
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn progress_storage(&self) -> Result<bool, Error> {
        if !self.storage.device.capabilities().supports_files {
            return Ok(false);
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.storage_step()));
        match result {
            Ok(Ok(progress)) => Ok(progress),
            Ok(Err(error)) => {
                self.failed.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(error)
            }
            Err(_) => {
                self.failed.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(Error::InvalidState("Background log push panic"))
            }
        }
    }
    fn storage_step(&self) -> Result<bool, Error> {
        let mut state = match self.storage_progress.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(_) => return Err(Error::InvalidState("background_log_progress_lock_poisoned")),
        };
        if let Some((id, task)) = &mut state.flush {
            if let Some(completion) = self.io.take(*id)? {
                task.accept(&self.storage, completion)
                    .map_err(|rejected| rejected.reason)?;
            }
            if self.log.finish_flush(&self.storage, task)? {
                self.io.release(*id)?;
                state.flush = None;
                return match self.log.evict_next() {
                    Ok(_) | Err(Error::Busy) => Ok(true),
                    Err(error) => Err(error),
                };
            }
            return match task.submit_next(&self.storage) {
                Ok(_) | Err(Error::Busy) => Ok(false),
                Err(error) => Err(error),
            };
        }
        if self
            .shutdown_requested
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(false);
        }
        let frontiers = self.log.frontiers()?;
        if frontiers.head < frontiers.flushed_until || frontiers.safe_head < frontiers.head {
            match self.log.evict_next() {
                Ok(progress) if progress.phase_advanced => return Ok(true),
                Ok(_) | Err(Error::Busy) => {}
                Err(error) => return Err(error),
            }
        }
        // GC Responsible for crossing obsolete prefixes;Only flush existing writes,Avoid recoding data that has been logically discarded.
        if self.coordinator.snapshot()?.action == Some(crate::coordination::Action::Gc) {
            return Ok(false);
        }
        // Keep at least the current page variable,retain at most memory_pages - 1 page,Leave room for circulation.
        let mutable = ((self.config.log.memory_pages as f64 * self.config.log.mutable_fraction)
            .ceil() as u64)
            .max(1)
            .min((self.config.log.memory_pages - 1) as u64);
        let tail_page = frontiers.tail.0 / self.config.log.page_bytes as u64;
        let target_page = tail_page.saturating_add(1).saturating_sub(mutable);
        // Checkpoints can actively freeze the last page that is still in the mutable window;The brush target cannot return to the window calculation value.
        let target = LogAddress::from_page_offset(
            PageId(target_page),
            0,
            self.config.log.page_bytes as u64,
        )?
        .max(frontiers.read_only);
        if target <= frontiers.flushed_until {
            return Ok(false);
        }
        match self.log.advance_read_only(target) {
            Ok(()) => {}
            Err(Error::Busy) => return Ok(false),
            Err(error) => return Err(error),
        }
        // The background routing retains an additional mailbox,Not available to all users Pending slot full.
        let version = self.coordinator.snapshot()?.version;
        let id = self.io.reserve(SessionId(self.id.0))?;
        match self
            .log
            .begin_flush(&self.storage, CompletionHub::route(id), version)
        {
            Ok(task) => {
                state.flush = Some((id, task));
                Ok(true)
            }
            Err(error) => {
                self.io.release(id)?;
                match error {
                    Error::Busy => Ok(false),
                    error => Err(error),
                }
            }
        }
    }
}

impl<S: Schema> Engine<S> {
    /// Turn off Blocked registration of new sessions;Only complete the started background page,Do not encode new pages.
    pub(crate) fn drain_storage(&self, deadline: Deadline) -> Result<(), Error> {
        loop {
            let active = match self.storage_progress.try_lock() {
                Ok(state) => state.flush.is_some(),
                Err(std::sync::TryLockError::WouldBlock) => true,
                Err(_) => return Err(Error::InvalidState("background_log_progress_lock_poisoned")),
            };
            if !active || self.failed.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            self.io.poll(&*self.storage.device, PollBudget::default())?;
            self.progress_storage()?;
            std::thread::yield_now();
        }
    }
    /// only on device shutdown success,Called after all in-flight buffers have been returned.
    pub(crate) fn release_stopped_storage(&self) -> Result<(), Error> {
        let mut state = self
            .storage_progress
            .try_lock()
            .map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => Error::Busy,
                std::sync::TryLockError::Poisoned(_) => {
                    Error::InvalidState("background_log_progress_lock_poisoned")
                }
            })?;
        if let Some((id, task)) = &mut state.flush {
            task.discard_after_device_shutdown()?;
            self.io.release(*id)?;
            state.flush = None;
        }
        // Historical routes that were abandoned by the session may still have completed;The returned owned buffer is released here.
        self.io.discard_after_device_shutdown(&*self.storage.device)
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use crate::{
        RasterKV,
        config::Config,
        device::thread_pool::ThreadPoolDeviceFactory,
        schema::builtin::{AtomicU64Value, SchemaPair, U64Key},
    };
    #[test]
    fn the_checkpoint_has_frozen_and_the_last_page_continues_to_be_flushed_in_the_background_even_if_it_is_still_within_the_variable_window()
     {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let root = Directory(std::env::temp_dir().join(format!(
            "raster-checkpoint-flush-{:x?}",
            StoreId::generate().unwrap().0
        )));
        let mut config = Config::default();
        config.storage.root = root.0.clone();
        config.log.page_bytes = 4096;
        config.log.memory_pages = 4;
        config.log.mutable_fraction = 0.75;
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config)
            .device(Box::new(ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 16,
            }))
            .create()
            .unwrap();
        store
            .inner
            .log
            .finish_initialization(store.inner.log.reserve_record(b"key", None, 17).unwrap())
            .unwrap();
        let end = store.inner.log.pad_tail().unwrap();
        assert_eq!(end, LogAddress(4096));
        store.inner.log.advance_read_only(end).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while store.inner.log.frontiers().unwrap().flushed_until != end {
            assert!(
                std::time::Instant::now() < deadline,
                "The frozen last page has not been refreshed"
            );
            store.inner.poll_maintenance(PollBudget::default()).unwrap();
            std::thread::yield_now();
        }
        let path = root
            .0
            .join(store.inner.storage.segment_path(0, Generation(0)));
        let bytes = std::fs::read(path).unwrap();
        let frame = crate::format::PageFrame::decode(&bytes, PageId(0), 4096).unwrap();
        assert_eq!(frame.records().unwrap()[0].1.value, 17u64.to_le_bytes());
        store
            .shutdown(Deadline(
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            ))
            .unwrap();
    }
}
