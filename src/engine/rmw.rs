//! 内存 RMW：在共同仲裁内完成条件创建、原地更新或复制追加。
use super::{Engine, SessionRuntime};
use crate::{
    api::{
        Submission,
        completion::Outcome,
        operation::{RmwOperation, RmwOptions, UpdateDecision},
    },
    index::{IndexHead, PublishResult},
    schema::{KeyCodec, Schema, ValueRead, ValueUpdate},
    types::*,
};
impl<S: Schema> Engine<S> {
    pub(crate) fn rmw<O: RmwOperation<S>>(
        &self,
        session: &mut SessionRuntime,
        serial: Serial,
        mut request: O,
        options: RmwOptions,
    ) -> Result<Submission<O::Output>, Rejected<O>> {
        let codec = self.schema.key_codec();
        let hash = codec.hash(request.key());
        let _gate = match self.operations[hash.0 as usize % self.operations.len()].try_lock() {
            Ok(g) => g,
            Err(_) => {
                return Err(Rejected {
                    request,
                    reason: Error::Busy,
                });
            }
        };
        let key = (|| {
            let len = codec.encoded_len(request.key())? as usize;
            let mut key = Vec::new();
            key.try_reserve_exact(len).map_err(|_| Error::OutOfMemory)?;
            key.resize(len, 0);
            codec.encode(request.key(), &mut key)?;
            Ok::<_, Error>(key)
        })();
        let key = match key {
            Ok(key) => key,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        if let Err(reason) = self.admit(session, serial) {
            return Err(Rejected { request, reason });
        }
        let mut effect = Effect::NotApplied;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let entry = self.index.prepare(hash)?;
            let head = Self::head(entry)?;
            let (value, output) = if let Some(lease) = self
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
                lease.read(|view| request.copy_update(ValueRead { view }))??
            } else {
                if !options.create_if_missing {
                    return Ok(Outcome::NotFound);
                }
                request.initial()?
            };
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
        Ok(Submission::Ready(result))
    }
}
