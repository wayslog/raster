//! 只校验配置，不打开设备；默认值是骨架起点，不是性能承诺。

use crate::types::Error;

#[derive(Clone, Debug)]
pub struct Config {
    pub storage: StorageConfig,
    pub index: IndexConfig,
    pub log: LogConfig,
    pub cache: CacheConfig,
    pub maintenance: MaintenanceConfig,
    pub session: SessionConfig,
    pub recovery: RecoveryConfig,
}
/// 恢复的临时元数据与索引输入预算，不改变日志驻留页预算。
#[derive(Clone, Debug)]
pub struct RecoveryConfig {
    pub max_records: usize,
    pub max_index_bytes: usize,
    pub timeout: std::time::Duration,
}
#[derive(Clone, Debug)]
pub struct StorageConfig {
    /// 文件设备要求实际根目录，非文件设备可忽略。
    pub root: std::path::PathBuf,
    pub segment_bytes: u64,
    pub pre_allocate_log: bool,
}
#[derive(Clone, Debug)]
pub struct IndexConfig {
    pub buckets: usize,
}
#[derive(Clone, Debug)]
pub struct LogConfig {
    pub page_bytes: usize,
    pub memory_pages: usize,
    pub mutable_fraction: f64,
}
#[derive(Clone, Debug, Default)]
pub struct CacheConfig {
    pub enabled: bool,
    pub capacity_bytes: usize,
}
#[derive(Clone, Debug)]
pub struct MaintenanceConfig {
    pub auto_compaction: bool,
    pub workers: usize,
}
#[derive(Clone, Debug)]
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
                auto_compaction: false,
                workers: 1,
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
        if self.recovery.max_records == 0
            || self.recovery.max_index_bytes < 36
            || self.recovery.timeout.is_zero()
        {
            return Err(invalid(
                "recovery",
                "恢复记录预算与超时须非零，索引字节预算至少 36",
            ));
        }
        if !self.index.buckets.is_power_of_two() {
            return Err(invalid("index.buckets", "必须是非零二次幂"));
        }
        if !self.log.page_bytes.is_power_of_two() || self.log.page_bytes < 4096 {
            return Err(invalid("log.page_bytes", "必须是不小于 4096 的二次幂"));
        }
        if self.storage.segment_bytes == 0
            || !self
                .storage
                .segment_bytes
                .is_multiple_of(self.log.page_bytes as u64)
        {
            return Err(invalid("storage.segment_bytes", "段尺寸必须是非零整数页"));
        }
        if self.log.memory_pages < 2
            || self
                .log
                .page_bytes
                .checked_mul(self.log.memory_pages)
                .is_none()
        {
            return Err(invalid("log.memory_pages", "至少两页且总尺寸不能溢出"));
        }
        if !self.log.mutable_fraction.is_finite()
            || !(0.0..1.0).contains(&self.log.mutable_fraction)
        {
            return Err(invalid("log.mutable_fraction", "必须有限且位于 [0, 1)"));
        }
        if self.cache.enabled && self.cache.capacity_bytes == 0 {
            return Err(invalid("cache.capacity_bytes", "启用缓存时必须非零"));
        }
        if self.maintenance.workers == 0
            || self.session.max_sessions == 0
            || self.session.max_pending == 0
            || self.session.max_results == 0
        {
            return Err(invalid("capacity", "线程、会话和请求预算必须非零"));
        }
        Ok(())
    }

    #[cfg(feature = "config-toml")]
    pub fn from_toml_str(_input: &str) -> Result<Self, Error> {
        Err(Error::unimplemented("config::toml"))
    }
    #[cfg(feature = "config-toml")]
    pub fn from_toml_file(_path: &std::path::Path) -> Result<Self, Error> {
        Err(Error::unimplemented("config::toml"))
    }
}
