//! Recovery from a CPR checkpoint (FASTER `Recover`, `RecoverFromPage`,
//! `RestoreHybridLog`, `ContinueSession`).
use super::address::Address;
use super::checkpoint::{IndexMetadata, LogMetadata, SessionRecord};
use super::device::AlignedBuffer;
use super::guid::Guid;
use super::kv::{Key, Value};
use super::record::{Record, RecordInfo};
use super::session::Session;
use super::state::{Action, Phase, SystemState, record_newer};
use super::store::{FasterKv, Status};
use std::sync::mpsc;

/// What a successful recovery restored.
#[derive(Clone, Debug)]
pub struct RecoveryInfo {
    /// Version of the recovered checkpoint; the store continues at `version + 1`.
    pub version: u32,
    /// Sessions that can resume with `continue_session`.
    pub sessions: Vec<Guid>,
}

const INVALID_BIT: u64 = 1 << 61;

impl<K: Key, V: Value> FasterKv<K, V> {
    /// Restores a freshly created store, built on the same log device, from
    /// an index checkpoint and a hybrid-log checkpoint with the same version.
    /// Recovery requires exclusive access, including exclusion of scanners.
    ///
    /// ```compile_fail
    /// use raster::core::{FasterKv, StoreConfig, guid::Guid};
    /// let mut store = FasterKv::<u64, u64>::new(StoreConfig::default());
    /// let session = store.start_session();
    /// let _ = store.recover(Guid::new(), Guid::new());
    /// drop(session);
    /// ```
    ///
    /// ```compile_fail
    /// use raster::core::{FasterKv, StoreConfig, guid::Guid};
    /// let mut store = FasterKv::<u64, u64>::new(StoreConfig::default());
    /// let scanner = store.scan(store.hlog.begin.load(), store.hlog.tail());
    /// let _ = store.recover(Guid::new(), Guid::new());
    /// drop(scanner);
    /// ```
    /// Returns Aborted for a used log or storage still retained by deferred
    /// work. After a partial page load fails, retry with a fresh store.
    pub fn recover(&mut self, index_token: Guid, log_token: Guid) -> Result<RecoveryInfo, Status> {
        let dirs = self.dirs.as_ref().ok_or(Status::IoError)?;
        let index_dir = dirs.index(index_token);
        let log_dir = dirs.log(log_token);
        if !self.hlog.recovery_ready() {
            return Err(Status::Aborted);
        }
        let rest = self.state.load();
        let recovering = rest.start(Action::Recover);
        if rest.phase != Phase::Rest
            || rest.action != Action::None
            || !self.state.compare_exchange(rest, recovering)
        {
            return Err(Status::Aborted);
        }
        let result = self.recover_inner(index_dir, log_dir);
        let version = result
            .as_ref()
            .map_or(rest.version, |info| info.version + 1);
        self.state.store(SystemState::rest(version));
        result
    }

    fn recover_inner(
        &mut self,
        index_dir: std::path::PathBuf,
        log_dir: std::path::PathBuf,
    ) -> Result<RecoveryInfo, Status> {
        let io = |_| Status::IoError;
        let index_meta = IndexMetadata::read(&index_dir).map_err(io)?;
        let log_meta = LogMetadata::read(&log_dir).map_err(io)?;
        if index_meta.version != log_meta.version || log_meta.use_snapshot_file {
            return Err(Status::Corruption);
        }
        let begin = index_meta.log_begin_address;
        let start = index_meta.checkpoint_start_address;
        let end = log_meta.final_address;
        if ![begin, start, end]
            .into_iter()
            .all(|address| self.hlog.valid_record_boundary(address))
            || begin < Address::new(0, super::hlog::FIRST_VALID_OFFSET)
            || begin > start
            || start > end
        {
            return Err(Status::Corruption);
        }
        let mut tokens = std::collections::HashMap::new();
        for &guid in &log_meta.sessions {
            let record = SessionRecord::read(&log_dir, guid).map_err(|_| Status::Corruption)?;
            tokens.insert(guid, record.serial);
        }
        if tokens.len() != log_meta.sessions.len() {
            return Err(Status::Corruption);
        }
        self.index
            .read_checkpoint(&index_dir.join("ht.dat"), &index_dir.join("ofb.dat"))
            .map_err(|_| Status::Corruption)?;
        // An empty replay interval has no records to read, even when its
        // address has a nonzero offset in a page that was never written.
        if start < end {
            let mut page = start.page();
            while Address::new(page, 0) < end {
                self.recover_page(page, start, end, log_meta.version)?;
                page += 1;
            }
        }
        self.restore_hybrid_log(index_meta.log_begin_address, end)?;
        *self.continue_tokens.lock().unwrap() = tokens;
        Ok(RecoveryInfo {
            version: log_meta.version,
            sessions: log_meta.sessions,
        })
    }

    /// Reads `[page start, min(page end, end))` from the device.
    fn read_page_prefix(&self, page: u64, end: Address) -> Result<Vec<u8>, Status> {
        let page_start = Address::new(page, 0);
        let limit = if end.page() == page {
            end.offset()
        } else {
            self.hlog.page_size()
        };
        let (tx, rx) = mpsc::channel();
        self.hlog.read_async(
            page_start,
            limit as usize,
            Box::new(move |r| drop(tx.send(r))),
        );
        let bytes = rx
            .recv()
            .map_err(|_| Status::IoError)?
            .map_err(|_| Status::IoError)?;
        if bytes.len() != limit as usize {
            return Err(Status::IoError);
        }
        Ok(bytes)
    }

    /// Replays the records of `page` within `[start, end)` into the index and
    /// invalidates records newer than `version` (FASTER `RecoverFromPage`).
    fn recover_page(
        &self,
        page: u64,
        start: Address,
        end: Address,
        version: u32,
    ) -> Result<(), Status> {
        let mut input = self.read_page_prefix(page, end)?;
        let mut aligned = None;
        let bytes = if input.as_ptr().cast::<u64>().is_aligned() {
            input.as_mut_slice()
        } else {
            let buffer = aligned.insert(AlignedBuffer::new(
                input.len().next_multiple_of(super::device::SECTOR as usize),
            ));
            buffer.as_mut()[..input.len()].copy_from_slice(&input);
            &mut buffer.as_mut()[..input.len()]
        };
        let mut offset = if start.page() == page {
            start.offset() as usize
        } else {
            0
        };
        let mut changed = false;
        while offset + 8 <= bytes.len() {
            let header = u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("8 bytes"));
            if header == 0 {
                break;
            }
            // SAFETY: aligned page storage with a complete header at offset.
            let record = unsafe { Record::<K, V>::from_ptr(bytes.as_mut_ptr().add(offset)) };
            // SAFETY: required_bytes checks each header before inspecting it.
            let required =
                unsafe { record.required_bytes(bytes.len() - offset, bytes.len() - offset) }
                    .map_err(|_| Status::Corruption)?;
            if required > bytes.len() - offset {
                return Err(Status::Corruption);
            }
            // SAFETY: the size headers are available and bounded above.
            let size = unsafe { record.size() } as usize;
            if offset + size > bytes.len() {
                return Err(Status::Corruption);
            }
            let info = RecordInfo::from_word(header);
            if !info.invalid() {
                // SAFETY: the complete inline key is available in aligned memory.
                let key = unsafe { record.key() };
                let address = Address::new(page, offset as u64);
                let slot = self.index.find_or_create(key.hash());
                if !record_newer(info.version(), version) {
                    slot.store(address);
                } else {
                    let invalid = header | INVALID_BIT;
                    bytes[offset..offset + 8].copy_from_slice(&invalid.to_le_bytes());
                    changed = true;
                    if info.previous() < start {
                        slot.store(info.previous());
                    }
                }
            }
            offset += size;
        }
        if changed {
            self.write_page_prefix(page, bytes)?;
        }
        Ok(())
    }

    fn write_page_prefix(&self, page: u64, bytes: &[u8]) -> Result<(), Status> {
        let mut aligned =
            AlignedBuffer::new(bytes.len().next_multiple_of(super::device::SECTOR as usize));
        aligned.as_mut()[..bytes.len()].copy_from_slice(bytes);
        let (tx, rx) = mpsc::channel();
        let data = aligned.as_ptr();
        let len = aligned.len();
        let offset = self.hlog.device_offset(Address::new(page, 0));
        // SAFETY: `aligned` stays alive in the callback until the write ends.
        unsafe {
            self.hlog.write_async(
                offset,
                data,
                len,
                Box::new(move |r| {
                    drop(aligned);
                    drop(tx.send(r));
                }),
            )
        };
        rx.recv()
            .map_err(|_| Status::IoError)?
            .map_err(|_| Status::IoError)
    }

    /// Loads the last pages into memory and resets the log boundaries.
    fn restore_hybrid_log(&mut self, begin: Address, tail: Address) -> Result<(), Status> {
        if begin > tail || !self.hlog.valid_address(begin) || !self.hlog.valid_address(tail) {
            return Err(Status::Corruption);
        }
        if begin == tail {
            self.hlog.recovery_reset(begin, begin, tail);
            return Ok(());
        }
        let end_page = tail.page() + (tail.offset() > 0) as u64;
        // GC may have deleted every segment before begin. Only the retained
        // window can be read, even when it is smaller than the memory window.
        let start_page = end_page
            .saturating_sub(self.hlog.frames().saturating_sub(4).max(1))
            .max(begin.page());
        for page in start_page..end_page {
            let bytes = self.read_page_prefix(page, tail)?;
            self.hlog.load_page(page, &bytes);
        }
        let head = if start_page == 0 {
            Address::new(0, super::hlog::FIRST_VALID_OFFSET)
        } else {
            Address::new(start_page, 0)
        };
        self.hlog.recovery_reset(begin, head.max(begin), tail);
        Ok(())
    }

    /// Resumes a recovered session and returns it with its persisted serial
    /// number: every operation up to that serial survived the checkpoint.
    pub fn continue_session(&self, guid: Guid) -> Option<(Session<'_, K, V>, u64)> {
        let serial = self.continue_tokens.lock().unwrap().remove(&guid)?;
        Some((Session::new(self, guid, serial), serial))
    }
}

#[cfg(test)]
mod tests {
    use crate::core::address::Address;
    use crate::core::device::{Device, ReadCallback, WriteCallback};
    use crate::core::guid::Guid;
    use crate::core::hlog::LogConfig;
    use crate::core::state::{Action, SystemState};
    use crate::core::{FasterKv, Status, StoreConfig};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct NoReads(AtomicU64);

    impl Device for NoReads {
        unsafe fn write_async(&self, _: u64, _: *const u8, _: usize, done: WriteCallback) {
            done(Ok(()));
        }

        fn read_async(&self, _: u64, _: usize, done: ReadCallback) {
            self.0.fetch_add(1, Ordering::Relaxed);
            done(Err(std::io::Error::other("segment was truncated")));
        }
    }

    #[test]
    fn empty_recovery_window_never_reads_a_truncated_segment() {
        let device = Arc::new(NoReads::default());
        let mut store = FasterKv::<u64, AtomicU64>::with_device(
            StoreConfig {
                index_buckets: 16,
                log: LogConfig {
                    page_bits: 12,
                    memory_pages: 16,
                    mutable_fraction: 0.5,
                },
                ..Default::default()
            },
            device.clone(),
        );
        for tail in [Address::new(32, 0), Address::new(32, 72)] {
            assert_eq!(store.restore_hybrid_log(tail, tail), Ok(()));
            assert_eq!(store.hlog.begin.load(), tail);
            assert_eq!(store.hlog.head.load(), tail);
            assert_eq!(store.hlog.tail(), tail);
        }
        assert_eq!(device.0.load(Ordering::Relaxed), 0);
        assert_eq!(
            store.restore_hybrid_log(Address::new(33, 0), Address::new(32, 0)),
            Err(Status::Corruption)
        );
        for boundary in [Address::new(0, 4097), Address::new(1, 0).to_read_cache()] {
            assert_eq!(
                store.restore_hybrid_log(boundary, boundary),
                Err(Status::Corruption)
            );
        }
    }

    #[test]
    fn recovery_excludes_other_actions_and_new_sessions() {
        let dir = std::env::temp_dir().join(format!("raster-recover-excl-{}", std::process::id()));
        let mut store = FasterKv::<u64, AtomicU64>::new(StoreConfig {
            index_buckets: 64,
            checkpoint_dir: Some(dir),
            ..StoreConfig::default()
        });
        // An action that has started but not yet left REST.
        store.state.store(SystemState::rest(1).start(Action::Gc));
        assert_eq!(
            store.recover(Guid::new(), Guid::new()).err(),
            Some(Status::Aborted)
        );
        // Recovery itself stays in REST while it loads the log.
        store
            .state
            .store(SystemState::rest(1).start(Action::Recover));
        assert!(store.try_start_session().is_none());
        store.state.store(SystemState::rest(1));
        assert!(store.try_start_session().is_some());
    }

    #[test]
    fn recovery_rejects_used_and_partially_restored_logs() {
        let dir = std::env::temp_dir().join(format!("raster-recover-fresh-{}", Guid::new()));
        let config = StoreConfig {
            index_buckets: 16,
            log: LogConfig {
                page_bits: 12,
                memory_pages: 4,
                mutable_fraction: 0.5,
            },
            checkpoint_dir: Some(dir),
            ..Default::default()
        };
        let mut store = FasterKv::<u64, u64>::new(config.clone());
        // Metadata errors before a page load still allow a corrected retry.
        for _ in 0..2 {
            assert_eq!(
                store.recover(Guid::new(), Guid::new()).err(),
                Some(Status::IoError)
            );
        }
        let _ = store.hlog.allocate(8);
        assert_eq!(
            store.recover(Guid::new(), Guid::new()).err(),
            Some(Status::Aborted)
        );

        for reset in [false, true] {
            let mut store = FasterKv::<u64, u64>::new(config.clone());
            let start = store.hlog.tail();
            if reset {
                store.hlog.recovery_reset(start, start, start);
            } else {
                store.hlog.load_page(0, &[0; 64]);
            }
            assert_eq!(store.hlog.tail(), start);
            assert_eq!(
                store.recover(Guid::new(), Guid::new()).err(),
                Some(Status::Aborted)
            );
        }
    }

    #[test]
    fn recovery_rejects_unaligned_metadata_before_reading_pages() {
        use crate::core::checkpoint::{CheckpointDirs, IndexMetadata, LogMetadata};
        use crate::core::index::HashIndex;

        let root = std::env::temp_dir().join(format!("raster-recover-alignment-{}", Guid::new()));
        let dirs = CheckpointDirs { root: root.clone() };
        let token = Guid::new();
        dirs.create(token).unwrap();
        let ofb_count = HashIndex::new(16)
            .write_checkpoint(
                &dirs.index(token).join("ht.dat"),
                &dirs.index(token).join("ofb.dat"),
                &|address| address,
            )
            .unwrap();
        let device = Arc::new(NoReads::default());
        for [begin, start, end] in [[64, 65, 96], [65, 72, 96], [64, 64, 89]] {
            IndexMetadata {
                version: 1,
                table_size: 16,
                ofb_count,
                log_begin_address: Address::new(0, begin),
                checkpoint_start_address: Address::new(0, start),
            }
            .write(&dirs.index(token))
            .unwrap();
            LogMetadata {
                version: 1,
                flushed_address: Address::new(0, begin),
                final_address: Address::new(0, end),
                ..Default::default()
            }
            .write(&dirs.log(token))
            .unwrap();
            let mut store = FasterKv::<u64, u64>::with_device(
                StoreConfig {
                    index_buckets: 16,
                    checkpoint_dir: Some(root.clone()),
                    log: LogConfig {
                        page_bits: 12,
                        memory_pages: 4,
                        mutable_fraction: 0.5,
                    },
                    ..Default::default()
                },
                device.clone(),
            );
            assert_eq!(store.recover(token, token).err(), Some(Status::Corruption));
            assert_eq!(device.0.load(Ordering::Relaxed), 0);
            assert_eq!(store.hlog.tail(), Address::new(0, 64));
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
