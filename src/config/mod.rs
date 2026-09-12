//! Ownership configuration and strictness TOML mapping;Parsing and verifying does not open the storage device,Default values do not constitute a performance promise.

use crate::types::Error;
#[cfg(feature = "config-toml")]
mod document;
#[cfg(all(test, feature = "config-toml"))]
mod tests;

#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    pub storage: StorageConfig,
    pub index: IndexConfig,
    pub log: LogConfig,
    pub cache: CacheConfig,
    pub maintenance: MaintenanceConfig,
    pub session: SessionConfig,
    pub recovery: RecoveryConfig,
    pub scan: ScanConfig,
    pub statistics: StatisticsConfig,
}
/// Turning off collection does not affect real-time resource diagnosis;Sampled requests continue to be recorded until the end.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StatisticsConfig {
    pub enabled: bool,
}
/// Budget for scan registration and sync calls;Scanning in transit that is closed will also occupy the quota..
#[derive(Clone, Debug, PartialEq)]
pub struct ScanConfig {
    pub max_scanners: usize,
    pub timeout: std::time::Duration,
}
/// Recovery temporary metadata and index input budget,Do not change log resident page budget.
#[derive(Clone, Debug, PartialEq)]
pub struct RecoveryConfig {
    pub max_records: usize,
    pub max_index_bytes: usize,
    pub timeout: std::time::Duration,
}
#[derive(Clone, Debug, PartialEq)]
pub struct StorageConfig {
    /// File device requires actual root directory,Non-file devices are ignored.
    pub root: std::path::PathBuf,
    pub segment_bytes: u64,
    pub pre_allocate_log: bool,
}
#[derive(Clone, Debug, PartialEq)]
pub struct IndexConfig {
    pub buckets: usize,
}
#[derive(Clone, Debug, PartialEq)]
pub struct LogConfig {
    pub page_bytes: usize,
    pub memory_pages: usize,
    pub mutable_fraction: f64,
}
#[derive(Clone, Debug, PartialEq)]
pub struct CacheConfig {
    pub enabled: bool,
    pub capacity_bytes: usize,
    /// The proportion of the most recently inserted byte window;Older hits refresh elimination order,Record values are always immutable.
    pub mutable_fraction: f64,
    pub pre_allocate: bool,
}
impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            capacity_bytes: 0,
            mutable_fraction: 0.5,
            pre_allocate: false,
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct MaintenanceConfig {
    /// Checkpoint release checks dependencies by disk directory;Contains defunct directories,Overall rejection beyond limits.
    pub max_checkpoint_tokens: usize,
    /// Single directory name and commit/manifest Cumulative byte budget read.
    pub max_checkpoint_catalog_bytes: usize,
    /// ScanDedup Maximum number of different keys and total number of key bytes saved;Lookup Not accumulating candidates.
    pub max_compaction_keys: usize,
    /// The upper limit of compression threads that can be created by a single task,At the same time, reserve the corresponding completion route.
    pub max_compaction_workers: usize,
    pub max_compaction_key_bytes: usize,
    pub auto_compaction: bool,
    pub auto_compaction_policy: AutoCompactionPolicy,
    pub workers: usize,
}
/// Automatic compression is triggered by log span;Budget is not the upper limit for rejection,It does not represent the effective amount of data.
#[derive(Clone, Debug, PartialEq)]
pub struct AutoCompactionPolicy {
    pub check_interval: std::time::Duration,
    pub trigger_fraction: f64,
    pub compact_fraction: f64,
    pub max_compacted_bytes: u64,
    /// Default zero;Must be set explicitly when enabling automatic maintenance.
    pub log_size_budget: u64,
}
impl Default for AutoCompactionPolicy {
    fn default() -> Self {
        Self {
            check_interval: std::time::Duration::from_millis(250),
            trigger_fraction: 0.8,
            compact_fraction: 0.2,
            max_compacted_bytes: 512 * 1024 * 1024,
            log_size_budget: 0,
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct SessionConfig {
    pub max_sessions: usize,
    pub max_pending: usize,
    pub max_results: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig {
                root: std::path::PathBuf::new(),
                segment_bytes: 1 << 30,
                pre_allocate_log: false,
            },
            index: IndexConfig { buckets: 1024 },
            log: LogConfig {
                page_bytes: 32 * 1024 * 1024,
                memory_pages: 4,
                mutable_fraction: 0.9,
            },
            cache: CacheConfig::default(),
            maintenance: MaintenanceConfig {
                max_checkpoint_tokens: 4096,
                max_checkpoint_catalog_bytes: 64 * 1024 * 1024,
                max_compaction_keys: 1_000_000,
                max_compaction_workers: 64,
                max_compaction_key_bytes: 64 * 1024 * 1024,
                auto_compaction: false,
                auto_compaction_policy: AutoCompactionPolicy::default(),
                workers: 1,
            },
            statistics: StatisticsConfig::default(),
            scan: ScanConfig {
                max_scanners: 16,
                timeout: std::time::Duration::from_secs(30),
            },
            recovery: RecoveryConfig {
                max_records: 1_000_000,
                max_index_bytes: 128 * 1024 * 1024,
                timeout: std::time::Duration::from_secs(300),
            },
            session: SessionConfig {
                max_sessions: 96,
                max_pending: 1024,
                max_results: 1024,
            },
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        let invalid = |field, reason| Error::InvalidConfig { field, reason };
        if self.scan.max_scanners == 0
            || self.scan.timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(self.scan.timeout)
                .is_none()
        {
            return Err(invalid("scan", "Scan quota and timeout must be non-zero"));
        }
        self.io_capacity()?;
        if self.recovery.max_records == 0
            || self.recovery.max_index_bytes < 36
            || self.recovery.timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(self.recovery.timeout)
                .is_none()
        {
            return Err(invalid(
                "recovery",
                "Recovery record budget and timeout must be non-zero,Index byte budget is at least 36",
            ));
        }
        if !self.index.buckets.is_power_of_two() {
            return Err(invalid("index.buckets", "Must be a non-zero power of two"));
        }
        if !self.log.page_bytes.is_power_of_two() || self.log.page_bytes < 4096 {
            return Err(invalid(
                "log.page_bytes",
                "Must be no less than 4096 power of two",
            ));
        }
        if self.storage.segment_bytes == 0
            || !self
                .storage
                .segment_bytes
                .is_multiple_of(self.log.page_bytes as u64)
        {
            return Err(invalid(
                "storage.segment_bytes",
                "Segment size must be a non-zero integer number of pages",
            ));
        }
        if self.log.memory_pages < 2
            || self
                .log
                .page_bytes
                .checked_mul(self.log.memory_pages)
                .is_none()
        {
            return Err(invalid(
                "log.memory_pages",
                "At least two pages and the total size cannot exceed",
            ));
        }
        if !self.log.mutable_fraction.is_finite()
            || !(0.0..1.0).contains(&self.log.mutable_fraction)
        {
            return Err(invalid(
                "log.mutable_fraction",
                "Must be limited and located in [0, 1)",
            ));
        }
        if !self.cache.mutable_fraction.is_finite()
            || !(0.0..1.0).contains(&self.cache.mutable_fraction)
        {
            return Err(invalid(
                "cache.mutable_fraction",
                "Must be limited and located in [0, 1)",
            ));
        }
        if self.cache.enabled && self.cache.capacity_bytes == 0 {
            return Err(invalid(
                "cache.capacity_bytes",
                "Must be non-zero when caching is enabled",
            ));
        }
        let auto = &self.maintenance.auto_compaction_policy;
        if auto.check_interval.is_zero()
            || std::time::Instant::now()
                .checked_add(auto.check_interval)
                .is_none()
            || !auto.trigger_fraction.is_finite()
            || auto.trigger_fraction <= 0.0
            || auto.trigger_fraction > 1.0
            || !auto.compact_fraction.is_finite()
            || auto.compact_fraction <= 0.0
            || auto.compact_fraction > 1.0
            || auto.max_compacted_bytes < self.log.page_bytes as u64
            || (self.maintenance.auto_compaction && auto.log_size_budget == 0)
        {
            return Err(invalid(
                "maintenance.auto_compaction_policy",
                "The interval must be valid and non-zero,The proportion must be within (0, 1],The maximum limit for a single visit is at least one page,Budget must be non-zero when enabled",
            ));
        }
        if self.maintenance.workers > self.maintenance.max_compaction_workers {
            return Err(invalid(
                "maintenance.workers",
                "Cannot exceed compression thread budget",
            ));
        }
        if self.maintenance.max_checkpoint_tokens == 0
            || self.maintenance.max_checkpoint_catalog_bytes == 0
            || self.maintenance.max_compaction_keys == 0
            || self.maintenance.max_compaction_key_bytes == 0
            || self.maintenance.max_compaction_workers == 0
            || self.maintenance.workers == 0
            || self.session.max_sessions == 0
            || self.session.max_pending == 0
            || self.session.max_results == 0
        {
            return Err(invalid(
                "capacity",
                "threads,Session and request budgets must be non-zero",
            ));
        }
        Ok(())
    }

    /// user request,Each worker and one to-be-delivered copy,Background page refresh/Maintain scans and expose scanner routes.
    pub(crate) fn io_capacity(&self) -> Result<usize, Error> {
        self.session
            .max_sessions
            .checked_mul(self.session.max_pending)
            .and_then(|n| n.checked_add(self.maintenance.max_compaction_workers))
            .and_then(|n| n.checked_add(3))
            .and_then(|n| {
                self.scan
                    .max_scanners
                    .checked_mul(3)
                    .and_then(|scans| n.checked_add(scans))
            })
            .ok_or(Error::CapacityExceeded)
    }

    /// Parse document root table;unknown field,Type errors and over-limit input are not silently ignored.
    #[cfg(feature = "config-toml")]
    pub fn from_toml_str(input: &str) -> Result<Self, Error> {
        document::parse(input, &[])
    }
    /// The subtable path is represented by the key name of each segment,Dots within paragraphs are no longer split.
    ///
    /// ```
    /// use raster::config::Config;
    /// let config = Config::from_toml_str_at(
    ///     "[app.raster.log]\npage_bytes=4096\n[app.raster.statistics]\nenabled=true",
    ///     &["app", "raster"],
    /// )?;
    /// assert_eq!(config.log.page_bytes, 4096);
    /// assert!(config.statistics.enabled);
    /// # Ok::<(), raster::types::Error>(())
    /// ```
    #[cfg(feature = "config-toml")]
    pub fn from_toml_str_at(input: &str, table_path: &[&str]) -> Result<Self, Error> {
        document::parse(input, table_path)
    }
    #[cfg(feature = "config-toml")]
    pub fn from_toml_file(path: &std::path::Path) -> Result<Self, Error> {
        document::file(path, &[])
    }
    #[cfg(feature = "config-toml")]
    pub fn from_toml_file_at(path: &std::path::Path, table_path: &[&str]) -> Result<Self, Error> {
        document::file(path, table_path)
    }
}
