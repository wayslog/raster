//! TOML configuration using the FASTER single-store field names.
use super::auto_compact::AutoCompactionConfig;
use super::hlog::LogConfig;
use super::read_cache::ReadCacheConfig;
use super::store::StoreConfig;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Toml(toml::de::Error),
    Invalid(String),
}
impl std::fmt::Display for ConfigError {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(out),
            Self::Toml(error) => error.fmt(out),
            Self::Invalid(error) => error.fmt(out),
        }
    }
}
impl std::error::Error for ConfigError {}
fn invalid(message: &str) -> ConfigError {
    ConfigError::Invalid(message.into())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    filepath: String,
    checkpoint_dir: Option<PathBuf>,
    hlog: Hlog,
    index: Index,
    read_cache: Option<Cache>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Hlog {
    in_mem_size_mb: u64,
    #[serde(default = "mutable_fraction")]
    mutable_fraction: f64,
    #[serde(default)]
    pre_allocate: bool,
    compaction: Option<Compaction>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Index {
    table_size: u64,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Cache {
    enabled: bool,
    in_mem_size_mb: u64,
    mutable_fraction: f64,
    pre_allocate: bool,
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            enabled: false,
            in_mem_size_mb: 256,
            mutable_fraction: 0.9,
            pre_allocate: false,
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Compaction {
    enabled: bool,
    hlog_size_budget_mb: Option<u64>,
    trigger_pct: f64,
    compact_pct: f64,
    max_compacted_size_mb: u64,
    check_interval_ms: u64,
    num_threads: usize,
}
impl Default for Compaction {
    fn default() -> Self {
        Self {
            enabled: false,
            hlog_size_budget_mb: None,
            trigger_pct: 0.8,
            compact_pct: 0.2,
            max_compacted_size_mb: 512,
            check_interval_ms: 250,
            num_threads: 4,
        }
    }
}
fn mutable_fraction() -> f64 {
    0.9
}
fn megabytes(value: u64) -> Result<u64, ConfigError> {
    value
        .checked_mul(1 << 20)
        .ok_or_else(|| invalid("size in MiB overflows u64"))
}
fn log_config(memory_mb: u64, fraction: f64) -> Result<LogConfig, ConfigError> {
    if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
        return Err(invalid(
            "mutable_fraction must be finite and between zero and one",
        ));
    }
    let bytes = megabytes(memory_mb)?;
    if bytes < 128 << 20 || !bytes.is_power_of_two() {
        return Err(invalid(
            "in_mem_size_mb must be a power of two and at least 128",
        ));
    }
    if bytes >> 25 > super::address::Address::MAX_PAGE + 1 {
        return Err(invalid(
            "in_mem_size_mb exceeds the hybrid log address space",
        ));
    }
    Ok(LogConfig {
        page_bits: 25,
        memory_pages: bytes >> 25,
        mutable_fraction: fraction,
    })
}
impl StoreConfig {
    /// Parses either [faster.hlog]/[faster.index] or top-level [hlog]/[index].
    /// F2 and unknown fields are rejected. Memory sizes use MiB and 32 MiB pages.
    /// pre_allocate is retained as a hint; this implementation allocates all
    /// frames at construction for either value (no lazy frame allocator).
    pub fn from_toml_str(text: &str) -> Result<Self, ConfigError> {
        let mut value: toml::Value = toml::from_str(text).map_err(ConfigError::Toml)?;
        if value.get("f2").is_some() {
            return Err(invalid("F2 configuration is not supported"));
        }
        if value.get("faster").is_some() {
            let table = value
                .as_table_mut()
                .ok_or_else(|| invalid("configuration must be a table"))?;
            if table.len() != 1 {
                return Err(invalid("only the faster table is supported at the root"));
            }
            value = table.remove("faster").expect("checked faster table");
        }
        let parsed: ConfigFile = value.try_into().map_err(ConfigError::Toml)?;
        let buckets = parsed.index.table_size;
        if !buckets.is_power_of_two() || buckets > i32::MAX as u64 {
            return Err(invalid(
                "index.table_size must be a power of two no greater than i32::MAX",
            ));
        }
        let log = log_config(parsed.hlog.in_mem_size_mb, parsed.hlog.mutable_fraction)?;
        let read_cache = match parsed.read_cache {
            Some(cache) if cache.enabled => {
                let log = log_config(cache.in_mem_size_mb, cache.mutable_fraction)?;
                Some(ReadCacheConfig {
                    log,
                    pre_allocate: cache.pre_allocate,
                })
            }
            _ => None,
        };
        if parsed
            .hlog
            .compaction
            .as_ref()
            .is_some_and(|c| c.hlog_size_budget_mb.is_none())
        {
            return Err(invalid("hlog.compaction requires hlog_size_budget_mb"));
        }
        let auto_compaction = match parsed.hlog.compaction {
            Some(compaction) if compaction.enabled => {
                let budget = megabytes(
                    compaction
                        .hlog_size_budget_mb
                        .expect("required budget checked"),
                )?;
                let max_bytes = megabytes(compaction.max_compacted_size_mb)?;
                if budget < log.memory_pages * (1 << log.page_bits)
                    || max_bytes == 0
                    || compaction.check_interval_ms == 0
                    || compaction.num_threads == 0
                    || compaction.num_threads > 64
                    || !compaction.trigger_pct.is_finite()
                    || !(0.0..=1.0).contains(&compaction.trigger_pct)
                    || compaction.trigger_pct == 0.0
                    || !compaction.compact_pct.is_finite()
                    || !(0.0..=1.0).contains(&compaction.compact_pct)
                    || compaction.compact_pct == 0.0
                {
                    return Err(invalid(
                        "enabled compaction requires a budget at least as large as the memory window, positive max size and interval, 1..64 threads and fractions in (0, 1]",
                    ));
                }
                Some(AutoCompactionConfig {
                    log_size_budget: budget,
                    check_interval: Duration::from_millis(compaction.check_interval_ms),
                    trigger_fraction: compaction.trigger_pct,
                    compact_fraction: compaction.compact_pct,
                    max_compacted_bytes: max_bytes,
                    threads: compaction.num_threads,
                })
            }
            _ => None,
        };
        Ok(Self {
            index_buckets: buckets,
            log,
            log_size_budget: auto_compaction.map_or(0, |config| config.log_size_budget),
            filepath: (!parsed.filepath.is_empty()).then(|| PathBuf::from(parsed.filepath)),
            pre_allocate: parsed.hlog.pre_allocate,
            checkpoint_dir: parsed.checkpoint_dir,
            read_cache,
            auto_compaction,
        })
    }
    pub fn from_toml_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::from_toml_str(&std::fs::read_to_string(path).map_err(ConfigError::Io)?)
    }
}
