//! Checkpoint artifacts and layout (FASTER `checkpoint_state.h`,
//! `file_system_disk.h`):
//!
//! ```text
//! <root>/index-checkpoints/<token>/{info.dat, ht.dat, ofb.dat}
//! <root>/cpr-checkpoints/<token>/{info.dat, <session-guid>.dat ...}
//! ```
//!
//! Files are little-endian fixed fields after a magic word, written to a
//! temporary name, synced and renamed so a crash never leaves a torn file.
use super::address::Address;
use super::guid::Guid;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const INDEX_MAGIC: u64 = 0x5241_5354_4552_4958; // "RASTERIX"
const LOG_MAGIC: u64 = 0x5241_5354_4552_4C47; // "RASTERLG"
const SESSION_MAGIC: u64 = 0x5241_5354_4552_5353; // "RASTERSS"

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexMetadata {
    pub version: u32,
    pub table_size: u64,
    pub ofb_count: u64,
    pub log_begin_address: Address,
    pub checkpoint_start_address: Address,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogMetadata {
    pub use_snapshot_file: bool,
    pub version: u32,
    pub flushed_address: Address,
    pub final_address: Address,
    pub sessions: Vec<Guid>,
}

/// Persisted state of one session: its serial at the version switch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionRecord {
    pub guid: Guid,
    pub serial: u64,
    pub version: u32,
}

#[derive(Clone, Debug)]
pub struct CheckpointDirs {
    pub root: PathBuf,
}

impl CheckpointDirs {
    pub fn index(&self, token: Guid) -> PathBuf {
        self.root.join("index-checkpoints").join(token.to_string())
    }
    pub fn log(&self, token: Guid) -> PathBuf {
        self.root.join("cpr-checkpoints").join(token.to_string())
    }
    /// Creates fresh, empty directories for `token`.
    pub fn create(&self, token: Guid) -> io::Result<()> {
        for dir in [self.index(token), self.log(token)] {
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir)?;
        }
        Ok(())
    }
}

struct Encoder(Vec<u8>);
impl Encoder {
    fn new(magic: u64) -> Self {
        Self(magic.to_le_bytes().to_vec())
    }
    fn u64(mut self, v: u64) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn u128(mut self, v: u128) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
}

struct Decoder<'a>(&'a [u8]);
impl Decoder<'_> {
    fn take<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        if self.0.len() < N {
            return Err(invalid("truncated checkpoint file"));
        }
        let (head, rest) = self.0.split_at(N);
        self.0 = rest;
        Ok(head.try_into().expect("length checked"))
    }
    fn u64(&mut self) -> io::Result<u64> {
        self.take::<8>().map(u64::from_le_bytes)
    }
    fn u128(&mut self) -> io::Result<u128> {
        self.take::<16>().map(u128::from_le_bytes)
    }
    fn magic(&mut self, magic: u64) -> io::Result<()> {
        if self.u64()? == magic {
            Ok(())
        } else {
            Err(invalid("unexpected checkpoint file type"))
        }
    }
    fn finish(self) -> io::Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(invalid("trailing bytes in checkpoint file"))
        }
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// Writes `bytes` to `path` atomically and durably.
pub fn write_durable(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = fs::File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

impl IndexMetadata {
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        let bytes = Encoder::new(INDEX_MAGIC)
            .u64(self.version as u64)
            .u64(self.table_size)
            .u64(self.ofb_count)
            .u64(self.log_begin_address.control())
            .u64(self.checkpoint_start_address.control())
            .0;
        write_durable(&dir.join("info.dat"), &bytes)
    }
    pub fn read(dir: &Path) -> io::Result<Self> {
        let bytes = fs::read(dir.join("info.dat"))?;
        let mut d = Decoder(&bytes);
        d.magic(INDEX_MAGIC)?;
        let meta = Self {
            version: d.u64()? as u32,
            table_size: d.u64()?,
            ofb_count: d.u64()?,
            log_begin_address: Address::from_control(d.u64()?),
            checkpoint_start_address: Address::from_control(d.u64()?),
        };
        d.finish()?;
        Ok(meta)
    }
}

impl LogMetadata {
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        let mut e = Encoder::new(LOG_MAGIC)
            .u64(self.use_snapshot_file as u64)
            .u64(self.version as u64)
            .u64(self.flushed_address.control())
            .u64(self.final_address.control())
            .u64(self.sessions.len() as u64);
        for guid in &self.sessions {
            e = e.u128(guid.0);
        }
        write_durable(&dir.join("info.dat"), &e.0)
    }
    pub fn read(dir: &Path) -> io::Result<Self> {
        let bytes = fs::read(dir.join("info.dat"))?;
        let mut d = Decoder(&bytes);
        d.magic(LOG_MAGIC)?;
        let use_snapshot_file = d.u64()? != 0;
        let version = d.u64()? as u32;
        let flushed_address = Address::from_control(d.u64()?);
        let final_address = Address::from_control(d.u64()?);
        let count = d.u64()?;
        let sessions = (0..count)
            .map(|_| d.u128().map(Guid))
            .collect::<io::Result<_>>()?;
        d.finish()?;
        Ok(Self {
            use_snapshot_file,
            version,
            flushed_address,
            final_address,
            sessions,
        })
    }
}

impl SessionRecord {
    fn path(dir: &Path, guid: Guid) -> PathBuf {
        dir.join(format!("{guid}.dat"))
    }
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        let bytes = Encoder::new(SESSION_MAGIC)
            .u128(self.guid.0)
            .u64(self.serial)
            .u64(self.version as u64)
            .0;
        write_durable(&Self::path(dir, self.guid), &bytes)
    }
    pub fn read(dir: &Path, guid: Guid) -> io::Result<Self> {
        let bytes = fs::read(Self::path(dir, guid))?;
        let mut d = Decoder(&bytes);
        d.magic(SESSION_MAGIC)?;
        let record = Self {
            guid: Guid(d.u128()?),
            serial: d.u64()?,
            version: d.u64()? as u32,
        };
        d.finish()?;
        if record.guid != guid {
            return Err(invalid("session file belongs to another session"));
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_round_trips_and_rejects_corruption() {
        let dirs = CheckpointDirs {
            root: std::env::temp_dir().join(format!("raster-ckpt-meta-{}", std::process::id())),
        };
        let token = Guid::new();
        dirs.create(token).unwrap();
        let index = IndexMetadata {
            version: 3,
            table_size: 1024,
            ofb_count: 9,
            log_begin_address: Address::new(0, 64),
            checkpoint_start_address: Address::new(7, 128),
        };
        index.write(&dirs.index(token)).unwrap();
        assert_eq!(IndexMetadata::read(&dirs.index(token)).unwrap(), index);
        let session = SessionRecord {
            guid: Guid::new(),
            serial: 42,
            version: 3,
        };
        let log = LogMetadata {
            use_snapshot_file: false,
            version: 3,
            flushed_address: Address::new(5, 0),
            final_address: Address::new(9, 72),
            sessions: vec![session.guid],
        };
        log.write(&dirs.log(token)).unwrap();
        session.write(&dirs.log(token)).unwrap();
        assert_eq!(LogMetadata::read(&dirs.log(token)).unwrap(), log);
        assert_eq!(
            SessionRecord::read(&dirs.log(token), session.guid).unwrap(),
            session
        );
        let info = dirs.log(token).join("info.dat");
        let mut bytes = fs::read(&info).unwrap();
        bytes.pop();
        fs::write(&info, bytes).unwrap();
        assert!(LogMetadata::read(&dirs.log(token)).is_err());
        fs::remove_dir_all(&dirs.root).unwrap();
    }
}
