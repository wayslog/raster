//! Structured Diagnostics and Statistics;Counting does not mean persistence,Neither the log span nor the index occupancy is equal to the effective number of keys.
use crate::{
    api::maintenance::AutoCompactionStatus,
    types::{Generation, LogAddress, SessionId},
};

#[derive(Clone, Debug)]
pub struct IndexTableDiagnostics {
    pub generation: Generation,
    /// Physical entry occupancy,Contains the same tag Chain head;Not the number of keys.Expanding two tables may retain entries at the same time.
    pub bucket_distribution: Vec<u64>,
}
#[derive(Clone, Debug)]
pub struct Diagnostics {
    pub log_span_bytes: u64,
    pub begin: LogAddress,
    pub tail: LogAddress,
    pub active_sessions: usize,
    /// and active_sessions From the same registry read,Easily locate sessions blocked from closing.
    pub active_session_ids: Vec<SessionId>,
    /// Accepted and not yet finalized,Contains callbacks for synchronous execution.
    pub active_requests: usize,
    /// returned Pending And it's not over yet;Does not include result slots that were completed but not collected.
    pub pending_requests: usize,
    /// Billing for active and reader-retained records;With record header,Does not contain index tree and allocator metadata.
    pub cached_bytes: usize,
    /// The load capacity of the preallocated byte area;Zero when preallocation is not enabled,No RSS.
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
/// Bucketing means per request I/O Number of completions:0 is zero times,1 for once,followed by `[2,3]`,`[4,7]` Wait.
/// Requests that cannot fully read the route count do not enter the histogram,Statistics::measurements_complete for false.
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
    /// The switch only determines whether new requests are sampled or not;Sampled requests are still recorded until the end.
    pub enabled: bool,
    pub reads: OperationStatistics,
    pub upserts: OperationStatistics,
    pub rmw: OperationStatistics,
    pub deletes: OperationStatistics,
    /// success Indicates actual copy,not_found Indicates that the source is out of date;Not included in the number of business requests.
    pub conditional_copies: OperationStatistics,
    pub cache: CacheStatistics,
    pub index: IndexStatistics,
    pub refresh_nanoseconds: u64,
    pub maintenance_nanoseconds: u64,
    pub saturated: bool,
    /// Unable to read full completion route count on failed shutdown is false.
    pub measurements_complete: bool,
}
impl std::fmt::Display for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Statistics collection:{};count saturation:{};Measurement complete:{}",
            self.enabled, self.saturated, self.measurements_complete
        )?;
        for (name, s) in [
            ("read", &self.reads),
            ("write", &self.upserts),
            ("Modify", &self.rmw),
            ("delete", &self.deletes),
            ("conditional copy", &self.conditional_copies),
        ] {
            writeln!(
                f,
                "{name}:accept {},completed {},sync {},success {},missing/Outdated {},abort {},failure {}(Already effective {},Impact unknown {}),I/O completed {},Record invalid {},pending nanoseconds {}",
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
                writeln!(f, "  per request I/O completed [{low},{high}]:{count}")?;
            }
        }
        writeln!(
            f,
            "cache:Query {},hit {},Insert {},Eliminate {},Refresh {}",
            self.cache.lookups,
            self.cache.hits,
            self.cache.insertions,
            self.cache.evictions,
            self.cache.promotions
        )?;
        writeln!(
            f,
            "Index:Query {},publish attempt {},publishing conflict {}",
            self.index.lookups, self.index.publication_attempts, self.index.publication_conflicts
        )?;
        write!(
            f,
            "Session refresh nanoseconds {},Maintenance advances nanoseconds {}",
            self.refresh_nanoseconds, self.maintenance_nanoseconds
        )
    }
}
