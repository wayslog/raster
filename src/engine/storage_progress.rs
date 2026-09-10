//! 后台页推进共用设备邮箱，但不执行或迁移会话用户请求。
use super::{Engine, io_hub::CompletionHub};
use crate::{log::flush::PageFlush, schema::Schema, types::*};
#[derive(Default)]
pub(crate) struct StorageProgress {
    flush: Option<(RequestId, PageFlush)>,
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
                Err(Error::InvalidState("后台日志推进恐慌"))
            }
        }
    }
    fn storage_step(&self) -> Result<bool, Error> {
        let mut state = match self.storage_progress.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(_) => return Err(Error::InvalidState("后台日志推进锁中毒")),
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
        // 至少保留当前页可变，最多保留 memory_pages - 1 页，留出循环推进空间。
        let mutable = ((self.config.log.memory_pages as f64 * self.config.log.mutable_fraction)
            .ceil() as u64)
            .max(1)
            .min((self.config.log.memory_pages - 1) as u64);
        let tail_page = frontiers.tail.0 / self.config.log.page_bytes as u64;
        let target_page = tail_page.saturating_add(1).saturating_sub(mutable);
        let target = LogAddress::from_page_offset(
            PageId(target_page),
            0,
            self.config.log.page_bytes as u64,
        )?;
        if target <= frontiers.flushed_until {
            return Ok(false);
        }
        match self.log.advance_read_only(target) {
            Ok(()) => {}
            Err(Error::Busy) => return Ok(false),
            Err(error) => return Err(error),
        }
        // 后台路由保留一个额外邮箱，不能被所有用户 Pending 槽占满。
        let id = self.io.reserve(SessionId(self.id.0))?;
        match self.log.begin_flush(
            &self.storage,
            CompletionHub::route(id),
            CheckpointVersion(0),
        ) {
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
    /// 关闭已阻止注册新会话；只完成已启动的后台页，不编码新的页。
    pub(crate) fn drain_storage(&self, deadline: Deadline) -> Result<(), Error> {
        loop {
            let active = match self.storage_progress.try_lock() {
                Ok(state) => state.flush.is_some(),
                Err(std::sync::TryLockError::WouldBlock) => true,
                Err(_) => return Err(Error::InvalidState("后台日志推进锁中毒")),
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
    /// 仅在设备 shutdown 成功、所有在途缓冲归还后调用。
    pub(crate) fn release_stopped_storage(&self) -> Result<(), Error> {
        let mut state = self
            .storage_progress
            .try_lock()
            .map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => Error::Busy,
                std::sync::TryLockError::Poisoned(_) => Error::InvalidState("后台日志推进锁中毒"),
            })?;
        if let Some((id, task)) = &mut state.flush {
            task.discard_after_device_shutdown()?;
            self.io.release(*id)?;
            state.flush = None;
        }
        // 会话放弃的历史路由仍可能有完成；返回的拥有型缓冲在此释放。
        while self.io.poll(&*self.storage.device, PollBudget::default())? != 0 {}
        Ok(())
    }
}
