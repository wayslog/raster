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
    pub fn recover(&self, index_token: Guid, log_token: Guid) -> Result<RecoveryInfo, Status> {
        let dirs = self.dirs.as_ref().ok_or(Status::IoError)?;
        let rest = self.state.load();
        let recovering = rest.start(Action::Recover);
        if rest.phase != Phase::Rest || !self.state.compare_exchange(rest, recovering) {
            return Err(Status::Aborted);
        }
        let result = self.recover_inner(dirs.index(index_token), dirs.log(log_token));
        let version = result
            .as_ref()
            .map_or(rest.version, |info| info.version + 1);
        self.state.store(SystemState::rest(version));
        result
    }

    fn recover_inner(
        &self,
        index_dir: std::path::PathBuf,
        log_dir: std::path::PathBuf,
    ) -> Result<RecoveryInfo, Status> {
        let io = |_| Status::IoError;
        let index_meta = IndexMetadata::read(&index_dir).map_err(io)?;
        let log_meta = LogMetadata::read(&log_dir).map_err(io)?;
        if index_meta.version != log_meta.version || log_meta.use_snapshot_file {
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
        let start = index_meta.checkpoint_start_address;
        let end = log_meta.final_address;
        let mut page = start.page();
        while Address::new(page, 0) < end {
            self.recover_page(page, start, end, log_meta.version)?;
            page += 1;
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
        rx.recv()
            .map_err(|_| Status::IoError)?
            .map_err(|_| Status::IoError)
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
        let bytes = self.read_page_prefix(page, end)?;
        let mut aligned =
            AlignedBuffer::new(bytes.len().next_multiple_of(super::device::SECTOR as usize));
        aligned.as_mut()[..bytes.len()].copy_from_slice(&bytes);
        let bytes = &mut aligned.as_mut()[..bytes.len()];
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
    fn restore_hybrid_log(&self, begin: Address, tail: Address) -> Result<(), Status> {
        let end_page = tail.page() + (tail.offset() > 0) as u64;
        let start_page = end_page.saturating_sub(self.hlog.frames().saturating_sub(4).max(1));
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
