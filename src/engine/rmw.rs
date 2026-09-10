//! Rmw 编排入口；待实现前拒绝并归还请求，不执行用户函数。
use super::{Engine, SessionRuntime};
use crate::{
    api::{
        Submission,
        operation::{RmwOperation, RmwOptions},
    },
    schema::Schema,
    types::*,
};
impl<S: Schema> Engine<S> {
    pub(crate) fn rmw<O: RmwOperation<S>>(
        &self,
        _session: &mut SessionRuntime,
        _serial: Serial,
        request: O,
        _options: RmwOptions,
    ) -> Result<Submission<O::Output>, Rejected<O>> {
        Err(Rejected {
            request,
            reason: Error::unimplemented("engine::rmw"),
        })
    }
}
