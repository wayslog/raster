//! 维护接受与完成分离；仅索引检查点不声明会话持久化成功。
use crate::{engine::Engine, schema::Schema, types::*};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
pub enum CheckpointKind {
    Full,
    Index,
    Log,
}
#[derive(Clone, Debug)]
pub struct RecoverySet {
    pub store: StoreId,
    pub index: CheckpointToken,
    pub log: CheckpointToken,
}
#[derive(Clone, Debug)]
pub struct DurableProgress {
    pub session: SessionId,
    pub serial: Serial,
    pub version: CheckpointVersion,
}
#[derive(Clone, Debug)]
pub struct CheckpointReport {
    pub kind: CheckpointKind,
    pub token: CheckpointToken,
    pub version: CheckpointVersion,
    pub begin: LogAddress,
    pub end: LogAddress,
    pub sessions: Vec<DurableProgress>,
}
#[derive(Clone, Debug)]
pub struct RecoveryReport {
    pub set: RecoverySet,
    pub version: CheckpointVersion,
    pub sessions: Vec<DurableProgress>,
}
#[derive(Clone, Copy, Debug)]
pub enum CompactionAlgorithm {
    ScanDedup,
    Lookup,
}
#[derive(Clone, Debug)]
pub struct CompactionOptions {
    pub algorithm: CompactionAlgorithm,
    pub until: LogAddress,
    pub workers: usize,
    pub shift_begin: bool,
    pub checkpoint: bool,
}
#[derive(Clone, Debug)]
pub enum PhysicalReclamation {
    Completed,
    /// 旧页租约或在途读取仍需要旧范围；本次动作已终结，可稍后按相同 begin 重试。
    DeferredByRuntime {
        begin: LogAddress,
        end: LogAddress,
    },
    DeferredByRecoverySet {
        blockers: Vec<RecoverySet>,
        begin: LogAddress,
        end: LogAddress,
    },
}
#[derive(Clone, Debug)]
pub struct GcReport {
    pub begin: LogAddress,
    pub index_cleaned: bool,
    /// 本次经目录同步确认完成的段删除数，包含接续前次失败的删除。
    pub deleted_segments: u64,
    pub physical: PhysicalReclamation,
}
#[derive(Clone, Debug)]
pub struct CheckpointReleaseReport {
    pub token: CheckpointToken,
    pub retirement: CheckpointRetirement,
    /// 本次经目录同步确认不存在的材料数（含重试前已缺失项），不是新增删除数。
    pub confirmed_absent_materials: u64,
    pub physical: PhysicalReclamation,
}
#[derive(Clone, Debug)]
pub struct CompactionReport {
    pub until: LogAddress,
    pub copied: u64,
    pub gc: Option<GcReport>,
    pub checkpoint: Option<CheckpointReport>,
}
#[derive(Clone, Debug)]
pub struct IndexGrowthReport {
    pub old_buckets: usize,
    pub new_buckets: usize,
    pub generation: Generation,
}

pub type SharedReport<R> = Arc<Result<R, Error>>;
enum ReportState<R> {
    Pending,
    Ready(SharedReport<R>),
    Taken,
}
pub struct MaintenanceTicket<R> {
    pub(crate) store: StoreId,
    pub(crate) id: MaintenanceId,
    result: Arc<Mutex<ReportState<R>>>,
}
impl<R> MaintenanceTicket<R> {
    pub(crate) fn pair(store: StoreId, id: MaintenanceId) -> (Self, MaintenanceCompleter<R>) {
        let result = Arc::new(Mutex::new(ReportState::Pending));
        (
            Self {
                store,
                id,
                result: result.clone(),
            },
            MaintenanceCompleter { result },
        )
    }

    pub fn id(&self) -> MaintenanceId {
        self.id
    }
    pub fn try_report(&self) -> Result<Option<SharedReport<R>>, Error> {
        let slot = self
            .result
            .lock()
            .map_err(|_| Error::InvalidState("维护报告锁已中毒"))?;
        match &*slot {
            ReportState::Pending => Ok(None),
            ReportState::Ready(report) => Ok(Some(report.clone())),
            ReportState::Taken => Err(Error::InvalidState("内部维护结果已经取走")),
        }
    }
    /// 仅由不向外发布的子任务票据使用；移动原始错误，保留 OS 原因及部分效果。
    pub(crate) fn take_owned_report(&mut self) -> Result<Option<Result<R, Error>>, Error> {
        let mut slot = self
            .result
            .lock()
            .map_err(|_| Error::InvalidState("维护报告锁已中毒"))?;
        match std::mem::replace(&mut *slot, ReportState::Taken) {
            ReportState::Pending => {
                *slot = ReportState::Pending;
                Ok(None)
            }
            ReportState::Ready(report) => match Arc::try_unwrap(report) {
                Ok(report) => Ok(Some(report)),
                Err(report) => {
                    // 完成端刚发布结果时可能暂持一个副本，稍后推进，不阻塞等待。
                    *slot = ReportState::Ready(report);
                    Ok(None)
                }
            },
            ReportState::Taken => Err(Error::InvalidState("内部维护结果重复取走")),
        }
    }
}
/// 动作持有唯一完成端，报告一旦设置即不可替换。
pub(crate) struct MaintenanceCompleter<R> {
    result: Arc<Mutex<ReportState<R>>>,
}
impl<R> MaintenanceCompleter<R> {
    pub fn finish(&self, report: Result<R, Error>) -> Result<SharedReport<R>, Error> {
        let mut slot = self
            .result
            .lock()
            .map_err(|_| Error::InvalidState("维护报告锁已中毒"))?;
        if !matches!(*slot, ReportState::Pending) {
            return Err(Error::InvalidState("维护报告已经终结"));
        }
        let report = Arc::new(report);
        *slot = ReportState::Ready(report.clone());
        Ok(report)
    }
}
impl<R> Drop for MaintenanceCompleter<R> {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.result.lock()
            && matches!(*slot, ReportState::Pending)
        {
            *slot =
                ReportState::Ready(Arc::new(Err(Error::InvalidState("维护任务未完成即被放弃"))));
        }
    }
}
pub struct Maintenance<S: Schema> {
    pub(crate) inner: Arc<Engine<S>>,
}
impl<S: Schema> Maintenance<S> {
    /// 显式放弃一个检查点 token；若仍被有效 Log 引用则延后且释放动作占用。
    pub fn release_checkpoint(
        &self,
        token: CheckpointToken,
    ) -> Result<MaintenanceTicket<CheckpointReleaseReport>, Error> {
        self.inner.start_checkpoint_release(token)
    }
    pub fn checkpoint(
        &self,
        kind: CheckpointKind,
    ) -> Result<MaintenanceTicket<CheckpointReport>, Error> {
        self.inner.start_checkpoint(kind)
    }
    pub fn compact(
        &self,
        options: CompactionOptions,
    ) -> Result<MaintenanceTicket<CompactionReport>, Error> {
        self.inner.start_compaction(options)
    }
    pub fn shift_begin(&self, address: LogAddress) -> Result<MaintenanceTicket<GcReport>, Error> {
        self.inner.start_gc(address)
    }
    pub fn grow_index(&self) -> Result<MaintenanceTicket<IndexGrowthReport>, Error> {
        self.inner.start_growth()
    }
    pub fn poll(&self, budget: PollBudget) -> Result<Progress, Error> {
        self.inner.poll_maintenance(budget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 内部子任务移交原始错误且已取走状态禁止再次终结() {
        let (mut ticket, complete) =
            MaintenanceTicket::<()>::pair(StoreId([1; 16]), MaintenanceId(1));
        assert!(ticket.take_owned_report().unwrap().is_none());
        let shared = complete
            .finish(Err(Error::Io(std::io::Error::from_raw_os_error(13))))
            .unwrap();
        assert!(
            ticket.take_owned_report().unwrap().is_none(),
            "共享观察尚未结束"
        );
        drop(shared);
        let result = ticket.take_owned_report().unwrap().unwrap();
        assert!(matches!(result, Err(Error::Io(error)) if error.raw_os_error()==Some(13)));
        assert!(complete.finish(Ok(())).is_err());
        drop(complete);
        assert!(
            ticket.try_report().is_err(),
            "完成端析构不能把已取走结果改成另一终结"
        );
    }
}
