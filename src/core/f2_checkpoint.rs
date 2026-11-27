//! Quiescent checkpoints of the hot log, cold data log, and cold index.

use super::{Address, F2Kv, Guid, Key, Status, Value};
use crate::core::auto_compact::AutoCompactionConfig;
use crate::core::checkpoint::{
    CheckpointDirs, Decoder, Encoder, IndexMetadata, LogMetadata, write_durable,
};
use crate::core::cold_store::ColdStore;
use crate::core::hlog::{HybridLog, LogConfig};
use crate::core::{FasterKv, ReadCacheConfig, StoreConfig};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const MAGIC: u64 = 0x5241_5354_4552_4632; // "RASTERF2"
const FORMAT: u64 = 1;

/// Buffered persistent F2 layout. The cold index stores 32 slots per block.
#[derive(Clone, Debug)]
pub struct F2Config {
    pub root: PathBuf,
    /// Directory relative to the F2 root.
    pub hot_dir: PathBuf,
    /// Directory relative to the F2 root, with separate data and index children.
    pub cold_dir: PathBuf,
    pub hot_index_buckets: u64,
    pub cold_index_buckets: u64,
    pub hot_log: LogConfig,
    pub cold_log: LogConfig,
    /// Page geometry of the cold index's own log and its in-memory window.
    pub cold_index_log: LogConfig,
    pub hot_read_cache: Option<ReadCacheConfig>,
    pub hot_compaction: Option<AutoCompactionConfig>,
    pub cold_compaction: Option<AutoCompactionConfig>,
}

impl F2Config {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            hot_dir: "hot".into(),
            cold_dir: "cold".into(),
            hot_index_buckets: 1 << 20,
            cold_index_buckets: 1 << 14,
            hot_log: LogConfig::default(),
            cold_log: LogConfig::default(),
            cold_index_log: LogConfig::default(),
            hot_read_cache: None,
            hot_compaction: None,
            cold_compaction: None,
        }
    }
}

#[derive(Default)]
pub(super) struct SessionHistory {
    pub(super) completed: BTreeMap<Guid, u64>,
    pub(super) resumable: BTreeMap<Guid, u64>,
}

pub(super) struct Persistence {
    root: PathBuf,
    pub(super) sessions: Mutex<SessionHistory>,
    recovering: bool,
    hot_compaction: Option<AutoCompactionConfig>,
    cold_compaction: Option<AutoCompactionConfig>,
}

impl Persistence {
    fn catalog(&self) -> PathBuf {
        self.root.join("f2-checkpoints")
    }
}

impl<K: Key, V: Value> F2Kv<K, V> {
    /// Opens buffered storage. An existing published generation must be
    /// recovered before this instance accepts sessions.
    pub fn open(config: F2Config) -> io::Result<Self> {
        if !config.hot_index_buckets.is_power_of_two() || config.hot_index_buckets > i32::MAX as u64
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid hot index bucket count",
            ));
        }
        let (root, hot, cold) = open_directories(&config)?;
        let persistence = Persistence {
            root,
            sessions: Mutex::default(),
            recovering: false,
            hot_compaction: config.hot_compaction,
            cold_compaction: config.cold_compaction,
        };
        let ready = !persistence.catalog().try_exists()?;
        let hot = FasterKv::open(StoreConfig {
            filepath: Some(hot.join("log")),
            checkpoint_dir: Some(hot.join("checkpoints")),
            index_buckets: config.hot_index_buckets,
            log: config.hot_log,
            read_cache: config.hot_read_cache,
            ..StoreConfig::default()
        })?;
        let cold = ColdStore::open(
            &cold,
            config.cold_index_buckets,
            config.cold_index_log,
            config.cold_log,
        )?;
        let mut store = Self::new(hot, cold);
        store.ready = ready;
        store.persistent = Some(persistence);
        Ok(store)
    }

    /// Starts the configured background compaction policy.
    pub fn spawn_configured_compaction(
        self: &Arc<Self>,
    ) -> Option<crate::core::auto_compact::AutoCompaction> {
        let p = self.persistent.as_ref()?;
        if p.hot_compaction.is_none() && p.cold_compaction.is_none() {
            return None;
        }
        Some(self.spawn_auto_compaction(p.hot_compaction, p.cold_compaction))
    }

    /// Persists all three logs at a drained boundary. The exclusive borrow
    /// excludes active user sessions and migration workers.
    pub fn checkpoint(&mut self) -> Result<Guid, Status> {
        if !self.ready {
            return Err(Status::Aborted);
        }
        let p = self.persistent.as_mut().ok_or(Status::InvalidArgument)?;
        let path = p.catalog();
        let old = Catalog::read(&path)?;
        let hot_token = checkpoint_hot(&self.hot)?;
        let cold_token = self.cold.checkpoint()?;
        let cold_index_token = self.cold.paired_index_token(cold_token)?;
        let hot_dirs = self.hot.dirs.as_ref().expect("persistent hot directories");
        let cold_dirs = self
            .cold
            .data_store()
            .dirs
            .as_ref()
            .expect("persistent cold directories");
        let cold_index = self.cold.cold_index();
        let hot = Component::read(hot_dirs, &self.hot.hlog, hot_token)?;
        let cold = Component::read(cold_dirs, &self.cold.data_store().hlog, cold_token)?;
        let index = Component::read(
            cold_index.checkpoint_dirs(),
            cold_index.log(),
            cold_index_token,
        )?;
        for (component, dirs, log) in [
            (hot, hot_dirs, &self.hot.hlog),
            (cold, cold_dirs, &self.cold.data_store().hlog),
            (index, cold_index.checkpoint_dirs(), cold_index.log()),
        ] {
            if component.begin != log.begin.load() || component.end != log.tail() {
                return Err(Status::Corruption);
            }
            log.sync_device().map_err(|_| Status::IoError)?;
            sync_checkpoint_dirs(dirs, &p.root).map_err(|_| Status::IoError)?;
        }
        let token = Guid::new();
        let catalog = Catalog {
            current: Generation {
                token,
                hot,
                cold,
                index,
                sessions: p.sessions.get_mut().unwrap().completed.clone(),
            },
            previous: old.map(|catalog| catalog.current),
        };
        catalog.write(&path)?;
        self.reclaim_checkpoint_prefix(&catalog);
        Ok(token)
    }

    /// Migrates the hot prefix and reports the joint checkpoint separately.
    pub fn compact_hot_with_checkpoint(
        &mut self,
        until: Address,
        threads: usize,
    ) -> Result<crate::core::CompactionResult, Status> {
        let mut result = self.compact_hot(until, threads)?;
        result.checkpoint = Some(self.checkpoint());
        Ok(result)
    }

    /// Compacts the cold prefix and reports the joint checkpoint separately.
    pub fn compact_cold_with_checkpoint(
        &mut self,
        until: Address,
        threads: usize,
    ) -> Result<crate::core::CompactionResult, Status> {
        let mut result = self.compact_cold(until, threads)?;
        result.checkpoint = Some(self.checkpoint());
        Ok(result)
    }

    /// Returns the currently published checkpoint token.
    pub fn latest_checkpoint(&self) -> Result<Guid, Status> {
        let p = self.persistent.as_ref().ok_or(Status::InvalidArgument)?;
        Catalog::read(&p.catalog())?
            .map(|catalog| catalog.current.token)
            .ok_or(Status::NotFound)
    }

    /// Restores the selected retained generation into a fresh instance.
    /// Resuming an older generation makes it current before allowing writes.
    pub fn recover(&mut self, token: Guid) -> Result<Vec<Guid>, Status> {
        let p = self.persistent.as_mut().ok_or(Status::InvalidArgument)?;
        if p.recovering || self.ready || !self.hot.hlog.recovery_ready() {
            return Err(Status::Aborted);
        }
        let path = p.catalog();
        let catalog = Catalog::read(&path)?.ok_or(Status::NotFound)?;
        let older = catalog.current.token != token;
        let generation = if older {
            catalog
                .previous
                .filter(|previous| previous.token == token)
                .ok_or(Status::NotFound)?
        } else {
            catalog.current
        };

        let hot_dirs = self.hot.dirs.as_ref().expect("persistent hot directories");
        let cold_dirs = self
            .cold
            .data_store()
            .dirs
            .as_ref()
            .expect("persistent cold directories");
        let index_dirs = self.cold.cold_index().checkpoint_dirs();
        generation.hot.verify(hot_dirs, &self.hot.hlog)?;
        generation
            .cold
            .verify(cold_dirs, &self.cold.data_store().hlog)?;
        generation
            .index
            .verify(index_dirs, self.cold.cold_index().log())?;
        if self.cold.paired_index_token(generation.cold.token)? != generation.index.token {
            return Err(Status::Corruption);
        }

        p.recovering = true;
        self.hot
            .recover(generation.hot.token, generation.hot.token)?;
        self.cold.recover(generation.cold.token)?;
        if older {
            Catalog {
                current: generation.clone(),
                previous: None,
            }
            .write(&path)?;
        }
        let sessions = generation.sessions.keys().copied().collect();
        let history = p.sessions.get_mut().unwrap();
        history.resumable = generation.sessions.clone();
        history.completed = generation.sessions;
        self.ready = true;
        Ok(sessions)
    }

    fn reclaim_checkpoint_prefix(&self, catalog: &Catalog) {
        let current = &catalog.current;
        let previous = catalog.previous.as_ref().unwrap_or(current);
        let index = self.cold.cold_index();
        for (log, begin) in [
            (&self.hot.hlog, current.hot.begin.min(previous.hot.begin)),
            (
                &self.cold.data_store().hlog,
                current.cold.begin.min(previous.cold.begin),
            ),
            (index.log(), current.index.begin.min(previous.index.begin)),
        ] {
            log.truncate(begin, Box::new(|_| {}));
        }
    }
}

fn open_directories(config: &F2Config) -> io::Result<(PathBuf, PathBuf, PathBuf)> {
    fn directory(path: &Path) -> io::Result<PathBuf> {
        fs::create_dir_all(path)?;
        path.canonicalize()
    }

    let root = directory(&config.root)?;
    for requested in [&config.hot_dir, &config.cold_dir] {
        let reserved = ["f2-checkpoints", "f2-checkpoints.tmp"];
        if requested
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
            || reserved.iter().any(|name| requested.starts_with(name))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "F2 storage paths must be relative and outside the catalog",
            ));
        }
    }
    let hot = directory(&root.join(&config.hot_dir))?;
    let cold = directory(&root.join(&config.cold_dir))?;
    let data = directory(&cold.join("data"))?;
    let index = directory(&cold.join("index"))?;
    let independent = [
        (&hot, &cold),
        (&hot, &data),
        (&hot, &index),
        (&data, &index),
    ]
    .iter()
    .all(|(a, b)| !a.starts_with(b) && !b.starts_with(a));
    if !independent
        || [&hot, &cold, &data, &index]
            .iter()
            .any(|path| path.as_path() == root || !path.starts_with(&root))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "F2 storage directories must be separate children of the root",
        ));
    }
    Ok((root, hot, cold))
}

fn checkpoint_hot<K: Key, V: Value>(store: &FasterKv<K, V>) -> Result<Guid, Status> {
    let mut session = store.try_start_session().ok_or(Status::Aborted)?;
    let result = Arc::new(Mutex::new(Status::Pending));
    let completed = result.clone();
    let token = store
        .checkpoint(
            None,
            Some(Arc::new(move |status, _| {
                *completed.lock().unwrap() = status;
            })),
        )
        .ok_or(Status::IoError)?;
    session.complete_pending(true);
    let status = *result.lock().unwrap();
    if status == Status::Ok {
        Ok(token)
    } else {
        Err(status)
    }
}

fn sync_checkpoint_dirs(dirs: &CheckpointDirs, root: &Path) -> io::Result<()> {
    for child in ["index-checkpoints", "cpr-checkpoints"] {
        fs::File::open(dirs.root.join(child))?.sync_all()?;
    }
    for dir in dirs
        .root
        .ancestors()
        .take_while(|dir| dir.starts_with(root))
    {
        fs::File::open(dir)?.sync_all()?;
    }
    if let Some(parent) = root.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Component {
    token: Guid,
    version: u64,
    page_bits: u64,
    begin: Address,
    end: Address,
}

impl Component {
    fn read(dirs: &CheckpointDirs, hlog: &HybridLog, token: Guid) -> Result<Self, Status> {
        let index = IndexMetadata::read(&dirs.index(token)).map_err(read_error)?;
        let log = LogMetadata::read(&dirs.log(token)).map_err(read_error)?;
        if log.use_snapshot_file
            || index.version != log.version
            || index.checkpoint_start_address != log.final_address
            || log.flushed_address < log.final_address
        {
            return Err(Status::Corruption);
        }
        Ok(Self {
            token,
            version: index.version.into(),
            page_bits: hlog.page_size().trailing_zeros().into(),
            begin: index.log_begin_address,
            end: log.final_address,
        })
    }

    fn verify(self, dirs: &CheckpointDirs, log: &HybridLog) -> Result<(), Status> {
        if self != Self::read(dirs, log, self.token)?
            || self.begin > self.end
            || !log.valid_record_boundary(self.begin)
            || !log.valid_record_boundary(self.end)
        {
            return Err(Status::Corruption);
        }
        Ok(())
    }

    fn encode(self, output: Encoder) -> Encoder {
        output
            .u128(self.token.0)
            .u64(self.version)
            .u64(self.page_bits)
            .u64(self.begin.control())
            .u64(self.end.control())
    }

    fn decode(input: &mut Decoder<'_>) -> io::Result<Self> {
        let token = Guid(input.u128()?);
        let version = input.u64()?;
        let page_bits = input.u64()?;
        let begin = input.u64()?;
        let end = input.u64()?;
        if begin > end || end > Address::MAX.control() {
            return Err(corruption());
        }
        Ok(Self {
            token,
            version,
            page_bits,
            begin: Address::from_control(begin),
            end: Address::from_control(end),
        })
    }
}

#[derive(Clone)]
struct Generation {
    token: Guid,
    hot: Component,
    cold: Component,
    index: Component,
    sessions: BTreeMap<Guid, u64>,
}

impl Generation {
    fn encode(&self, output: Encoder) -> Encoder {
        let output = self.hot.encode(output.u128(self.token.0));
        let output = self.cold.encode(output);
        let output = self.index.encode(output).u64(self.sessions.len() as u64);
        self.sessions.iter().fold(output, |output, (guid, serial)| {
            output.u128(guid.0).u64(*serial)
        })
    }

    fn decode(input: &mut Decoder<'_>) -> io::Result<Self> {
        let token = Guid(input.u128()?);
        let hot = Component::decode(input)?;
        let cold = Component::decode(input)?;
        let index = Component::decode(input)?;
        let mut sessions = BTreeMap::new();
        for _ in 0..input.u64()? {
            if sessions.insert(Guid(input.u128()?), input.u64()?).is_some() {
                return Err(corruption());
            }
        }
        Ok(Self {
            token,
            hot,
            cold,
            index,
            sessions,
        })
    }
}

struct Catalog {
    current: Generation,
    previous: Option<Generation>,
}

impl Catalog {
    fn read(path: &Path) -> Result<Option<Self>, Status> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(read_error(error)),
        };
        let mut input = Decoder::new(&bytes);
        let decoded = (|| {
            input.magic(MAGIC)?;
            if input.u64()? != FORMAT {
                return Err(corruption());
            }
            let count = input.u64()?;
            if !(1..=2).contains(&count) {
                return Err(corruption());
            }
            let current = Generation::decode(&mut input)?;
            let previous = if count == 2 {
                Some(Generation::decode(&mut input)?)
            } else {
                None
            };
            input.finish()?;
            Ok(Self { current, previous })
        })();
        decoded.map(Some).map_err(read_error)
    }

    fn write(&self, path: &Path) -> Result<(), Status> {
        let mut output = Encoder::new(MAGIC)
            .u64(FORMAT)
            .u64(if self.previous.is_some() { 2 } else { 1 });
        output = self.current.encode(output);
        if let Some(previous) = &self.previous {
            output = previous.encode(output);
        }
        write_durable(path, &output.into_bytes()).map_err(|_| Status::IoError)
    }
}

fn corruption() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid F2 checkpoint catalog")
}

fn read_error(error: io::Error) -> Status {
    if error.kind() == io::ErrorKind::InvalidData {
        Status::Corruption
    } else {
        Status::IoError
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::device::FileDevice;
    use crate::core::{KeyContext, UpsertContext, ValueSlot};

    type Store = F2Kv<u64, u64>;

    struct Fixture {
        config: F2Config,
    }

    impl Fixture {
        fn new() -> Self {
            let log = LogConfig {
                page_bits: 12,
                memory_pages: 4,
                mutable_fraction: 0.5,
            };
            Self {
                config: F2Config {
                    hot_index_buckets: 256,
                    cold_index_buckets: 256,
                    hot_log: log,
                    cold_log: log,
                    cold_index_log: log,
                    ..F2Config::new(std::env::temp_dir().join(format!("raster-f2-{}", Guid::new())))
                },
            }
        }

        fn open(&self) -> Store {
            Store::open(self.config.clone()).unwrap()
        }

        fn open_with_short_hot_segments(&self) -> Store {
            let mut store = self.open();
            let hot = self.config.root.join("hot");
            let device = FileDevice::open_with(hot.join("log"), 12, 2, false).unwrap();
            store.hot = FasterKv::with_device(
                StoreConfig {
                    checkpoint_dir: Some(hot.join("checkpoints")),
                    index_buckets: self.config.hot_index_buckets,
                    log: self.config.hot_log,
                    ..StoreConfig::default()
                },
                Arc::new(device),
            );
            store
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.config.root);
        }
    }

    #[derive(Clone)]
    struct Put(u64);

    // SAFETY: The context owns a complete fixed-size key.
    unsafe impl KeyContext<u64> for Put {
        fn key(&self) -> &u64 {
            &self.0
        }
    }

    // SAFETY: put initializes the fixed-size destination, and in-place
    // mutation is declined.
    unsafe impl UpsertContext<u64, u64> for Put {
        fn put(&mut self, slot: ValueSlot<'_, u64>) {
            slot.write(self.0);
        }

        fn put_atomic(&mut self, _: &u64) -> bool {
            false
        }
    }

    #[test]
    fn retains_the_previous_generation_until_the_next_publication() {
        let fixture = Fixture::new();
        let mut store = fixture.open_with_short_hot_segments();
        {
            let mut session = store.try_start_session().unwrap();
            for key in 0..1024 {
                assert_eq!(session.upsert(&mut Put(key), key + 1), Status::Ok);
            }
        }
        let first = store.checkpoint().unwrap();
        let segment = fixture.config.root.join("hot/log/log.0");
        assert!(segment.is_file());

        let cutoff = store.hot_compaction_limit();
        assert!(cutoff.page() > 0);
        let result = store.compact_hot_with_checkpoint(cutoff, 2).unwrap();
        assert_eq!(result.begin_address, cutoff);
        let second = result.checkpoint.unwrap().unwrap();
        assert!(segment.is_file());
        let third = store.checkpoint().unwrap();
        assert!(!segment.exists());
        drop(store);

        let mut reopened = fixture.open_with_short_hot_segments();
        assert_eq!(reopened.latest_checkpoint(), Ok(third));
        assert_eq!(reopened.recover(first), Err(Status::NotFound));
        reopened.recover(second).unwrap();
        assert_eq!(reopened.latest_checkpoint(), Ok(second));
    }

    #[test]
    fn component_checkpoint_failures_leave_the_catalog_visible() {
        let fixture = Fixture::new();
        let mut store = fixture.open();
        let previous = store.checkpoint().unwrap();
        for relative in [
            "hot/checkpoints/index-checkpoints",
            "cold/index/checkpoints/index-checkpoints",
            "cold/data/checkpoints/index-checkpoints",
        ] {
            let target = fixture.config.root.join(relative);
            let saved = target.with_extension("saved");
            fs::rename(&target, &saved).unwrap();
            fs::write(&target, []).unwrap();
            assert_eq!(store.checkpoint(), Err(Status::IoError));
            fs::remove_file(&target).unwrap();
            fs::rename(saved, target).unwrap();
            assert_eq!(store.latest_checkpoint(), Ok(previous));
        }
        drop(store);
        fixture.open().recover(previous).unwrap();
    }

    #[test]
    fn mixed_cold_index_token_rejects_the_generation() {
        let fixture = Fixture::new();
        let mut store = fixture.open();
        let previous = store.checkpoint().unwrap();
        let latest = store.checkpoint().unwrap();
        let catalog = Catalog::read(&store.persistent.as_ref().unwrap().catalog())
            .unwrap()
            .unwrap();
        let cold_dir = store.cold.data_store().dirs.as_ref().unwrap();
        fs::write(
            cold_dir
                .index(catalog.current.cold.token)
                .join("cold-index-token"),
            catalog.previous.unwrap().index.token.0.to_le_bytes(),
        )
        .unwrap();
        drop(store);

        let mut reopened = fixture.open();
        assert_eq!(reopened.recover(latest), Err(Status::Corruption));
        assert!(reopened.try_start_session().is_none());
        reopened.recover(previous).unwrap();
    }

    #[test]
    fn component_load_failure_poisons_the_recovery_instance() {
        let fixture = Fixture::new();
        let mut store = fixture.open();
        let latest = store.checkpoint().unwrap();
        let catalog = Catalog::read(&store.persistent.as_ref().unwrap().catalog())
            .unwrap()
            .unwrap();
        let path = store
            .cold
            .cold_index()
            .checkpoint_dirs()
            .index(catalog.current.index.token)
            .join("ht.dat");
        let original = fs::read(&path).unwrap();
        fs::remove_file(&path).unwrap();
        drop(store);

        let mut reopened = fixture.open();
        assert!(reopened.recover(latest).is_err());
        assert!(reopened.try_start_session().is_none());
        fs::write(path, original).unwrap();
        assert_eq!(reopened.recover(latest), Err(Status::Aborted));
        fixture.open().recover(latest).unwrap();
    }

    #[test]
    fn rejects_overlapping_layer_directories() {
        let fixture = Fixture::new();
        let mut config = fixture.config.clone();
        config.hot_dir = config.cold_dir.join("index");
        let result = Store::open(config);
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    }

    #[test]
    fn changed_page_geometry_rejects_checkpoint_before_loading() {
        let fixture = Fixture::new();
        let latest = fixture.open().checkpoint().unwrap();
        let mut config = fixture.config.clone();
        config.cold_index_log.page_bits += 1;
        let mut reopened = Store::open(config).unwrap();
        assert_eq!(reopened.recover(latest), Err(Status::Corruption));
        assert!(reopened.try_start_session().is_none());
    }
}
