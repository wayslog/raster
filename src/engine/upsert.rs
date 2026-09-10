//! 内存 Upsert：相同哈希仲裁覆盖原地更新、替换与索引发布整个过程。
use super::{Engine, SessionRuntime};
use crate::{
    api::{
        Submission,
        completion::Outcome,
        operation::{UpdateDecision, UpsertOperation},
    },
    index::{IndexHead, PublishResult},
    schema::{Schema, ValueUpdate},
    types::*,
};
impl<S: Schema> Engine<S> {
    pub(crate) fn upsert<O: UpsertOperation<S>>(
        &self,
        session: &mut SessionRuntime,
        serial: Serial,
        mut request: O,
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
            if let Some(lease) = self
                .log
                .find(codec, request.key(), head)?
                .filter(|lease| !lease.is_tombstone())
            {
                effect = Effect::Unknown;
                match lease.update(|view| request.update_in_place(ValueUpdate { view }))? {
                    UpdateDecision::Updated(output) => {
                        effect = Effect::Applied;
                        return Ok(Outcome::Success(output));
                    }
                    UpdateDecision::Append => effect = Effect::NotApplied,
                }
            }
            let (value, output) = request.replacement()?;
            let address = self
                .log
                .finish_initialization(self.log.reserve_record(&key, head, value)?)?;
            match self.index.compare_publish(entry, IndexHead::Log(address))? {
                PublishResult::Published => {
                    effect = Effect::Applied;
                    Ok(Outcome::Success(output))
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
        Ok(Submission::Ready(
            self.finish_request(request, result, effect),
        ))
    }
}
