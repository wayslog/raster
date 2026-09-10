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
pub struct MaintenanceTicket<R> {
    pub(crate) store: StoreId,
    pub(crate) id: MaintenanceId,
    pub(crate) result: Arc<Mutex<Option<SharedReport<R>>>>,
}
impl<R> MaintenanceTicket<R> {
    #[allow(
        dead_code,
        reason = "P5.2 的实际材料任务将创建维护票据，当前验证报告协议"
    )]
    pub(crate) fn pair(store: StoreId, id: MaintenanceId) -> (Self, MaintenanceCompleter<R>) {
        let result = Arc::new(Mutex::new(None));
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
        Ok(self
            .result
            .lock()
            .map_err(|_| Error::InvalidState("维护报告锁已中毒"))?
            .clone())
    }
}
/// 动作持有唯一完成端，报告一旦设置即不可替换。
#[allow(dead_code, reason = "P5.2 的实际材料任务将持有完成端")]
pub(crate) struct MaintenanceCompleter<R> {
    result: Arc<Mutex<Option<SharedReport<R>>>>,
}
impl<R> MaintenanceCompleter<R> {
    #[allow(dead_code, reason = "P5.2 的实际材料任务将发布最终报告")]
    pub fn finish(&self, report: Result<R, Error>) -> Result<SharedReport<R>, Error> {
        let mut slot = self
            .result
            .lock()
            .map_err(|_| Error::InvalidState("维护报告锁已中毒"))?;
        if slot.is_some() {
            return Err(Error::InvalidState("维护报告已经终结"));
        }
        let report = Arc::new(report);
        *slot = Some(report.clone());
        Ok(report)
    }
}
impl<R> Drop for MaintenanceCompleter<R> {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.result.lock()
            && slot.is_none()
        {
            *slot = Some(Arc::new(Err(Error::InvalidState("维护任务未完成即被放弃"))));
        }
    }
}
pub struct Maintenance<S: Schema> {
    pub(crate) inner: Arc<Engine<S>>,
}
impl<S: Schema> Maintenance<S> {
    pub fn checkpoint(
        &self,
        _kind: CheckpointKind,
    ) -> Result<MaintenanceTicket<CheckpointReport>, Error> {
        self.inner.not_ready("checkpoint::start")
    }
    pub fn compact(
        &self,
        _options: CompactionOptions,
    ) -> Result<MaintenanceTicket<CompactionReport>, Error> {
        self.inner.not_ready("maintenance::compact")
    }
    pub fn shift_begin(&self, _address: LogAddress) -> Result<MaintenanceTicket<GcReport>, Error> {
        self.inner.not_ready("maintenance::gc")
    }
    pub fn grow_index(&self) -> Result<MaintenanceTicket<IndexGrowthReport>, Error> {
        self.inner.not_ready("index::grow")
    }
    pub fn poll(&self, budget: PollBudget) -> Result<Progress, Error> {
        self.inner.poll_maintenance(budget)
    }
}
