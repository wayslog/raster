//! Delete 编排入口；待实现前拒绝并归还请求，不执行用户函数。
use super::{Engine, SessionRuntime};
use crate::{
    api::{
        Submission,
        operation::{DeleteOperation, DeleteOptions},
    },
    schema::Schema,
    types::*,
};
impl<S: Schema> Engine<S> {
    pub(crate) fn delete<O: DeleteOperation<S>>(
        &self,
        _session: &mut SessionRuntime,
        _serial: Serial,
        request: O,
        _options: DeleteOptions,
    ) -> Result<Submission<O::Output>, Rejected<O>> {
        Err(Rejected {
            request,
            reason: Error::unimplemented("engine::delete"),
        })
    }
}
