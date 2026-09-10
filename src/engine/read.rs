//! Read 编排入口；待实现前拒绝并归还请求，不执行用户函数。
use super::{Engine, SessionRuntime};
use crate::{
    api::{
        Submission,
        operation::{ReadOperation, ReadOptions},
    },
    schema::Schema,
    types::*,
};
impl<S: Schema> Engine<S> {
    pub(crate) fn read<O: ReadOperation<S>>(
        &self,
        _session: &mut SessionRuntime,
        _serial: Serial,
        request: O,
        _options: ReadOptions,
    ) -> Result<Submission<O::Output>, Rejected<O>> {
        Err(Rejected {
            request,
            reason: Error::unimplemented("engine::read"),
        })
    }
}
