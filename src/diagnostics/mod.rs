//! 结构化诊断快照；日志跨度不等于有效键数。
use crate::types::{Generation, LogAddress};

#[derive(Clone, Debug)]
pub struct Diagnostics {
    pub log_span_bytes: u64,
    pub begin: LogAddress,
    pub tail: LogAddress,
    pub active_sessions: usize,
    pub pending_requests: usize,
    pub cached_bytes: usize,
    pub auto_compaction_scheduled: bool,
    pub table_generation: Generation,
    pub bucket_distribution: Vec<u64>,
}
