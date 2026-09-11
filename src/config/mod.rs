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
    pub scan: ScanConfig,
}
/// 扫描注册和同步调用的预算；关闭中的在途扫描也占用名额。
#[derive(Clone, Debug)]
pub struct ScanConfig {
    pub max_scanners: usize,
    pub timeout: std::time::Duration,
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
    /// 检查点释放按磁盘目录核对依赖；包含已失效目录，超限整体拒绝。
    pub max_checkpoint_tokens: usize,
    /// 单次目录名称和 commit/manifest 读取的累计字节预算。
    pub max_checkpoint_catalog_bytes: usize,
    /// ScanDedup 最多保存的不同键数和键字节总量；Lookup 不累积候选。
    pub max_compaction_keys: usize,
    /// 单任务可创建的压缩线程上限，同时预留对应完成路由。
    pub max_compaction_workers: usize,
    pub max_compaction_key_bytes: usize,
    pub auto_compaction: bool,
    pub auto_compaction_policy: AutoCompactionPolicy,
    pub workers: usize,
}
/// 自动压缩按日志跨度触发；预算不是拒写上限，也不代表有效数据量。
#[derive(Clone, Debug)]
pub struct AutoCompactionPolicy {
    pub check_interval: std::time::Duration,
    pub trigger_fraction: f64,
    pub compact_fraction: f64,
    pub max_compacted_bytes: u64,
    /// 默认零；启用自动维护时必须显式设置。
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
                max_checkpoint_tokens: 4096,
                max_checkpoint_catalog_bytes: 64 * 1024 * 1024,
                max_compaction_keys: 1_000_000,
                max_compaction_workers: 64,
                max_compaction_key_bytes: 64 * 1024 * 1024,
                auto_compaction: false,
                auto_compaction_policy: AutoCompactionPolicy::default(),
                workers: 1,
            },
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
        if self.scan.max_scanners == 0 || self.scan.timeout.is_zero() {
            return Err(invalid("scan", "扫描名额和超时必须非零"));
        }
        self.io_capacity()?;
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
                "间隔须有效且非零，比例须在 (0, 1]，单次上限至少一页，启用时预算须非零",
            ));
        }
        if self.maintenance.workers > self.maintenance.max_compaction_workers {
            return Err(invalid("maintenance.workers", "不能超过压缩线程预算"));
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
            return Err(invalid("capacity", "线程、会话和请求预算必须非零"));
        }
        Ok(())
    }

    /// 用户请求、每个工作者及一个待投递复制、后台刷页/维护扫描和公开扫描器路由。
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

    #[cfg(feature = "config-toml")]
    pub fn from_toml_str(_input: &str) -> Result<Self, Error> {
        Err(Error::unimplemented("config::toml"))
    }
    #[cfg(feature = "config-toml")]
    pub fn from_toml_file(_path: &std::path::Path) -> Result<Self, Error> {
        Err(Error::unimplemented("config::toml"))
    }
}
