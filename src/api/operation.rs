//! Four Safe User Operation Protocols;Calculation allows retries,Effective modifications cannot be repeated.
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
    /// Append and normal Err The old value must be unmodified;Some modifications failed and need to be closed on failure.
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
    /// Reachable tombstone written or marked in situ;Does not prove that there was an active value before deletion.
    TombstoneWritten,
    /// Variable link heads are tombstoned and index entries safely removed;Log bytes still follow normal recycling protocol.
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
    /// Reserved tombstones can be queried,Disable index removal;Does not extend record life after explicit truncation or subsequent overwriting.
    pub force_tombstone: bool,
}
