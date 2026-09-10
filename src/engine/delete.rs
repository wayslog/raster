//! 内存删除发布拥有键的墓碑；不存在的键按选项决定是否写入。
use super::{Engine, SessionRuntime};
use crate::{
    api::{
        Submission,
        completion::Outcome,
        operation::{DeleteOperation, DeleteOptions, DeleteOutcome},
    },
    index::{IndexHead, PublishResult},
    schema::Schema,
    types::*,
};
impl<S: Schema> Engine<S> {
    pub(crate) fn delete<O: DeleteOperation<S>>(
        &self,
        session: &mut SessionRuntime,
        serial: Serial,
        request: O,
        options: DeleteOptions,
    ) -> Result<Submission<O::Output>, Rejected<O>> {
        let (hash, key) = match self.prepare(session, serial, &request) {
            Ok(prepared) => prepared,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        let codec = self.schema.key_codec();
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
        let mut effect = Effect::NotApplied;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let entry = self.index.prepare(hash)?;
            let head = Self::head(entry)?;
            let exists = self
                .log
                .find(codec, request.key(), head)?
                .is_some_and(|lease| !lease.is_tombstone());
            if !exists && !options.force_tombstone {
                return Ok(Outcome::NotFound);
            }
            let address = self
                .log
                .finish_initialization(self.log.reserve_tombstone(&key, head)?)?;
            match self.index.compare_publish(entry, IndexHead::Log(address))? {
                PublishResult::Published => {
                    effect = Effect::Applied;
                    Ok(Outcome::Success(
                        request.complete(DeleteOutcome::TombstoneWritten),
                    ))
                }
                PublishResult::Conflict(_) => {
                    self.log.retire(address)?;
                    Err(Error::Busy)
                }
            }
        }));
        let result = match result {
            Ok(result) => result.map_err(|cause| OperationError { cause, effect }),
            Err(_) => {
                self.failed.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(OperationError {
                    cause: Error::InvalidState("写入回调恐慌"),
                    effect,
                })
            }
        };
        if result.is_err() && effect == Effect::Unknown {
            self.failed.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(Submission::Ready(result))
    }
}
