//! 结构化诊断与统计；计数不代表持久化，日志跨度和索引占用都不等于有效键数。
use crate::{
    api::maintenance::AutoCompactionStatus,
    types::{Generation, LogAddress},
};

#[derive(Clone, Debug)]
pub struct IndexTableDiagnostics {
    pub generation: Generation,
    /// 物理条目占用，包含同 tag 链头；不是键数量。扩容两张表可能同时保留条目。
    pub bucket_distribution: Vec<u64>,
}
#[derive(Clone, Debug)]
pub struct Diagnostics {
    pub log_span_bytes: u64,
    pub begin: LogAddress,
    pub tail: LogAddress,
    pub active_sessions: usize,
    /// 已接受且尚未终结，包含同步执行中的回调。
    pub active_requests: usize,
    /// 已返回 Pending 且尚未终结；不包含已完成但未收取的结果槽。
    pub pending_requests: usize,
    /// 活跃及被读者保留的记录计费；含记录头，不含索引树和分配器元数据。
    pub cached_bytes: usize,
    /// 预分配字节区的负载容量；未启用预分配时为零，不是 RSS。
    pub cache_reserved_bytes: usize,
    pub log_allocated_bytes: usize,
    pub log_resident_pages: usize,
    pub auto_compaction_scheduled: bool,
    pub auto_compaction: AutoCompactionStatus,
    pub table_generation: Generation,
    pub bucket_distribution: Vec<u64>,
    pub growing_index: Option<IndexTableDiagnostics>,
    pub migrated_buckets: usize,
    pub retired_index_retained: bool,
    pub failed: bool,
    pub shutdown_requested: bool,
}
/// 分桶表示每请求 I/O 完成数：0 为零次，1 为一次，随后为 `[2,3]`、`[4,7]` 等。
/// 无法完整读取路由计数的请求不进入直方图，Statistics::measurements_complete 为 false。
#[derive(Clone, Debug)]
pub struct OperationStatistics {
    pub accepted: u64,
    pub completed: u64,
    pub synchronous: u64,
    pub success: u64,
    pub not_found: u64,
    pub aborted: u64,
    pub failed: u64,
    pub failed_after_applied: u64,
    pub failed_with_unknown_effect: u64,
    pub io_completions: u64,
    pub record_invalidations: u64,
    pub pending_nanoseconds: u64,
    pub io_per_request: [u64; 65],
}
impl Default for OperationStatistics {
    fn default() -> Self {
        Self {
            accepted: 0,
            completed: 0,
            synchronous: 0,
            success: 0,
            not_found: 0,
            aborted: 0,
            failed: 0,
            failed_after_applied: 0,
            failed_with_unknown_effect: 0,
            io_completions: 0,
            record_invalidations: 0,
            pending_nanoseconds: 0,
            io_per_request: [0; 65],
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct CacheStatistics {
    pub lookups: u64,
    pub hits: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub promotions: u64,
}
#[derive(Clone, Debug, Default)]
pub struct IndexStatistics {
    pub lookups: u64,
    pub publication_attempts: u64,
    pub publication_conflicts: u64,
}
#[derive(Clone, Debug)]
pub struct Statistics {
    /// 开关只决定新请求是否采样；已采样请求仍记录至终结。
    pub enabled: bool,
    pub reads: OperationStatistics,
    pub upserts: OperationStatistics,
    pub rmw: OperationStatistics,
    pub deletes: OperationStatistics,
    /// success 表示实际复制，not_found 表示源已过时；不计入业务请求数量。
    pub conditional_copies: OperationStatistics,
    pub cache: CacheStatistics,
    pub index: IndexStatistics,
    pub refresh_nanoseconds: u64,
    pub maintenance_nanoseconds: u64,
    pub saturated: bool,
    /// 失败关闭时无法读取完整完成路由计数则为 false。
    pub measurements_complete: bool,
}
impl std::fmt::Display for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "统计采集：{}；计数饱和：{}；测量完整：{}",
            self.enabled, self.saturated, self.measurements_complete
        )?;
        for (name, s) in [
            ("读取", &self.reads),
            ("写入", &self.upserts),
            ("修改", &self.rmw),
            ("删除", &self.deletes),
            ("条件复制", &self.conditional_copies),
        ] {
            writeln!(
                f,
                "{name}：接受 {}，完成 {}，同步 {}，成功 {}，缺失/过时 {}，中止 {}，失败 {}（已生效 {}，影响未知 {}），I/O 完成 {}，记录失效 {}，挂起纳秒 {}",
                s.accepted,
                s.completed,
                s.synchronous,
                s.success,
                s.not_found,
                s.aborted,
                s.failed,
                s.failed_after_applied,
                s.failed_with_unknown_effect,
                s.io_completions,
                s.record_invalidations,
                s.pending_nanoseconds
            )?;
            for (bin, &count) in s
                .io_per_request
                .iter()
                .enumerate()
                .filter(|(_, count)| **count != 0)
            {
                let low = if bin == 0 { 0 } else { 1u64 << (bin - 1) };
                let high = if bin == 0 {
                    0
                } else if bin == 64 {
                    u64::MAX
                } else {
                    (1u64 << bin) - 1
                };
                writeln!(f, "  每请求 I/O 完成 [{low},{high}]：{count}")?;
            }
        }
        writeln!(
            f,
            "缓存：查询 {}，命中 {}，插入 {}，淘汰 {}，刷新 {}",
            self.cache.lookups,
            self.cache.hits,
            self.cache.insertions,
            self.cache.evictions,
            self.cache.promotions
        )?;
        writeln!(
            f,
            "索引：查询 {}，发布尝试 {}，发布冲突 {}",
            self.index.lookups, self.index.publication_attempts, self.index.publication_conflicts
        )?;
        write!(
            f,
            "会话刷新纳秒 {}，维护推进纳秒 {}",
            self.refresh_nanoseconds, self.maintenance_nanoseconds
        )
    }
}
