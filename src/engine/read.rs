//! 内存读取：接受前检查状态，结果与持久化进度分离。
use super::{Engine, SessionRuntime};
use crate::{
    api::{
        Submission,
        completion::{AbortReason, Outcome},
        operation::{ReadOperation, ReadOptions},
    },
    schema::{KeyCodec, Schema, ValueRead},
    types::*,
};
impl<S: Schema> Engine<S> {
    pub(crate) fn read<O: ReadOperation<S>>(
        &self,
        session: &mut SessionRuntime,
        serial: Serial,
        mut request: O,
        options: ReadOptions,
    ) -> Result<Submission<O::Output>, Rejected<O>> {
        let hash = self.schema.key_codec().hash(request.key());
        let _gate = match self.operations[hash.0 as usize % self.operations.len()].try_lock() {
            Ok(g) => g,
            Err(_) => {
                return Err(Rejected {
                    request,
                    reason: Error::Busy,
                });
            }
        };
        if let Err(reason) = self.admit(session, serial) {
            return Err(Rejected { request, reason });
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let entry = self.index.prepare(hash)?;
            match self
                .log
                .find(self.schema.key_codec(), request.key(), Self::head(entry)?)?
            {
                None => Ok(Outcome::NotFound),
                Some(lease) if lease.is_tombstone() => Ok(if options.abort_if_tombstone {
                    Outcome::Aborted(AbortReason::Tombstone)
                } else {
                    Outcome::NotFound
                }),
                Some(lease) => lease
                    .read(|view| request.read(ValueRead { view }))?
                    .map(Outcome::Success),
            }
        }));
        let result = match result {
            Ok(result) => result.map_err(|cause| OperationError {
                cause,
                effect: Effect::NotApplied,
            }),
            Err(_) => {
                self.failed.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(OperationError {
                    cause: Error::InvalidState("读取回调恐慌"),
                    effect: Effect::NotApplied,
                })
            }
        };
        Ok(Submission::Ready(result))
    }
}
