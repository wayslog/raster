//! Real engine trajectory adaptation;Rely only on public entrance,Results and models are calculated separately.
use crate::support::{
    model::{ResultValue, Submission as ModelSubmission},
    trace::{Operation, Step, Value},
};
use raster::{
    Submission,
    api::{
        completion::{AbortReason, Outcome},
        operation::*,
    },
    schema::{
        ValueRead, ValueUpdate,
        builtin::{ByteKey, SchemaPair, SerializedValue},
        value::ValueCodec,
    },
    types::*,
};
pub struct Codec;
impl ValueCodec for Codec {
    type Value = Value;
    fn format_id(&self) -> FormatId {
        FormatId([77; 16])
    }
    fn encode(&self, v: &Value) -> Result<Vec<u8>, Error> {
        Ok(match v {
            Value::Number(n) => {
                let mut b = vec![0];
                b.extend(n.to_le_bytes());
                b
            }
            Value::Bytes(b) => {
                let mut out = vec![1];
                out.extend(b);
                out
            }
        })
    }
    fn decode(&self, b: &[u8]) -> Result<Value, Error> {
        match b.first() {
            Some(0) if b.len() == 9 => Ok(Value::Number(u64::from_le_bytes(
                b[1..].try_into().unwrap(),
            ))),
            Some(1) => Ok(Value::Bytes(b[1..].to_vec())),
            _ => Err(Error::Codec("Corrupted trajectory values")),
        }
    }
}
pub type Schema = SchemaPair<ByteKey, SerializedValue<Codec>>;
struct Request {
    key: Vec<u8>,
    value: Value,
    append_bytes: bool,
}
impl Keyed<Schema> for Request {
    fn key(&self) -> &[u8] {
        &self.key
    }
}
impl ReadOperation<Schema> for Request {
    type Output = ResultValue;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<ResultValue, Error> {
        Ok(ResultValue::Value(v.view().clone()))
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = ResultValue;
    fn replacement(&mut self) -> Result<(Value, ResultValue), Error> {
        Ok((self.value.clone(), ResultValue::Written))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<ResultValue>, Error> {
        if self.append_bytes && matches!(self.value, Value::Bytes(_)) {
            return Ok(UpdateDecision::Append);
        }
        match v.view_mut().replace(&self.value) {
            Ok(()) => Ok(UpdateDecision::Updated(ResultValue::Written)),
            Err(Error::Codec(_)) => Ok(UpdateDecision::Append),
            Err(e) => Err(e),
        }
    }
}
fn add(old: &Value, operand: &Value) -> Result<Value, Error> {
    match (old, operand) {
        (Value::Number(a), Value::Number(b)) => Ok(Value::Number(a.wrapping_add(*b))),
        (Value::Bytes(a), Value::Bytes(b)) => {
            let mut v = a.clone();
            v.extend(b);
            Ok(Value::Bytes(v))
        }
        _ => Err(Error::Codec("type mismatch")),
    }
}
impl RmwOperation<Schema> for Request {
    type Output = ResultValue;
    fn initial(&mut self) -> Result<(Value, ResultValue), Error> {
        Ok((self.value.clone(), ResultValue::Value(self.value.clone())))
    }
    fn copy_update(&mut self, v: ValueRead<'_, Schema>) -> Result<(Value, ResultValue), Error> {
        let value = add(v.view(), &self.value)?;
        Ok((value.clone(), ResultValue::Value(value)))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<ResultValue>, Error> {
        let old = v.view_mut().read_owned()?;
        if self.append_bytes && matches!(old, Value::Bytes(_)) {
            return Ok(UpdateDecision::Append);
        }
        let Ok(value) = add(&old, &self.value) else {
            return Ok(UpdateDecision::Append);
        };
        match v.view_mut().replace(&value) {
            Ok(()) => Ok(UpdateDecision::Updated(ResultValue::Value(value))),
            Err(Error::Codec(_)) => Ok(UpdateDecision::Append),
            Err(e) => Err(e),
        }
    }
}
impl DeleteOperation<Schema> for Request {
    type Output = ResultValue;
    fn complete(self, _: DeleteOutcome) -> ResultValue {
        ResultValue::Deleted
    }
}
pub fn actual(
    session: &mut raster::Session<Schema>,
    step: &Step,
    pending: &mut [usize; 4],
    append_bytes: bool,
) -> ModelSubmission {
    let value = match &step.operation {
        Operation::Upsert(v) => v.clone(),
        Operation::Rmw { operand, .. } => operand.clone(),
        _ => Value::Number(0),
    };
    let request = Request {
        key: step.key.clone(),
        value,
        append_bytes,
    };
    let serial = Serial(step.serial);
    let result = match step.operation {
        Operation::Read { abort_if_tombstone } => {
            session.read(serial, request, ReadOptions { abort_if_tombstone })
        }
        Operation::Upsert(_) => session.upsert(serial, request),
        Operation::Rmw {
            create_if_missing, ..
        } => session.rmw(serial, request, RmwOptions { create_if_missing }),
        Operation::Delete { force_tombstone } => {
            session.delete(serial, request, DeleteOptions { force_tombstone })
        }
    };
    let result = match result {
        Err(rejected) => {
            assert!(
                matches!(rejected.reason, Error::InvalidState(_)),
                "unexpected rejection:{}",
                rejected.reason
            );
            assert_eq!(rejected.request.key, step.key);
            return ModelSubmission::Rejected(step.clone());
        }
        Ok(Submission::Ready(result)) => result,
        Ok(Submission::Pending(mut ticket)) => {
            let operation = match step.operation {
                Operation::Read { .. } => 0,
                Operation::Upsert(_) => 1,
                Operation::Rmw { .. } => 2,
                Operation::Delete { .. } => 3,
            };
            pending[operation] += 1;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "Request wait timeout:{step:?}"
                );
                session.poll(PollBudget::default()).unwrap();
                if let raster::api::completion::TicketState::Ready(result) =
                    ticket.try_take().unwrap()
                {
                    break result;
                }
                std::thread::yield_now();
            }
        }
    };
    let value = match result {
        Ok(Outcome::Success(v)) => v,
        Ok(Outcome::NotFound) => ResultValue::NotFound,
        Ok(Outcome::Aborted(AbortReason::Tombstone)) => ResultValue::Tombstone,
        Err(OperationError {
            cause: Error::Codec("type mismatch"),
            effect: Effect::NotApplied,
        }) => ResultValue::TypeMismatch,
        result => panic!("Unexpected engine results:{step:?} => {result:?}"),
    };
    ModelSubmission::Accepted(value)
}
