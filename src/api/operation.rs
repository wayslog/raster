//! 四种安全用户操作协议；计算允许重试，生效修改不能重复执行。
use crate::schema::{KeyOf, OwnedValueOf, Schema, ValueRead, ValueUpdate};
use crate::types::Error;

pub trait Keyed<S: Schema>: 'static {
    fn key(&self) -> &KeyOf<S>;
}
pub trait ReadOperation<S: Schema>: Keyed<S> {
    type Output: 'static;
    fn read(&mut self, value: ValueRead<'_, S>) -> Result<Self::Output, Error>;
}
pub enum UpdateDecision<T> {
    Updated(T),
    Append,
}
pub trait UpsertOperation<S: Schema>: Keyed<S> {
    type Output: 'static;
    fn replacement(&mut self) -> Result<(OwnedValueOf<S>, Self::Output), Error>;
    /// Append 和正常 Err 必须未修改旧值；部分修改失败需要失败关闭。
    fn update_in_place(
        &mut self,
        value: ValueUpdate<'_, S>,
    ) -> Result<UpdateDecision<Self::Output>, Error>;
}
pub trait RmwOperation<S: Schema>: Keyed<S> {
    type Output: 'static;
    fn initial(&mut self) -> Result<(OwnedValueOf<S>, Self::Output), Error>;
    fn copy_update(
        &mut self,
        value: ValueRead<'_, S>,
    ) -> Result<(OwnedValueOf<S>, Self::Output), Error>;
    fn update_in_place(
        &mut self,
        value: ValueUpdate<'_, S>,
    ) -> Result<UpdateDecision<Self::Output>, Error>;
}
pub trait DeleteOperation<S: Schema>: Keyed<S> {
    type Output: 'static;
    fn complete(self, outcome: DeleteOutcome) -> Self::Output;
}
#[derive(Clone, Copy, Debug)]
pub enum DeleteOutcome {
    /// 已写入或原地标记可达墓碑；不证明删除前存在活跃值。
    TombstoneWritten,
    /// 可变链头已标记墓碑并安全移除索引入口；日志字节仍遵循正常回收协议。
    IndexRemoved,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadOptions {
    pub abort_if_tombstone: bool,
}
#[derive(Clone, Copy, Debug)]
pub struct RmwOptions {
    pub create_if_missing: bool,
}
impl Default for RmwOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct DeleteOptions {
    /// 保留可查询墓碑，禁止索引移除；不延长显式截断或后续覆盖后的记录寿命。
    pub force_tombstone: bool,
}
