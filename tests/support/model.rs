//! 独立顺序模型，只比较逻辑结果，不模拟页、索引或持久化。
use super::trace::{Operation, Step, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultValue {
    Written,
    Value(Value),
    Deleted,
    NotFound,
    Tombstone,
    TypeMismatch,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Submission {
    Accepted(ResultValue),
    Rejected(Step),
}
#[derive(Default)]
pub struct Model {
    records: BTreeMap<Vec<u8>, Option<Value>>,
    serials: BTreeMap<u64, u64>,
    pub executions: usize,
}
impl Model {
    pub fn last_accepted(&self, session: u64) -> Option<u64> {
        self.serials.get(&session).copied()
    }
    pub fn submit(&mut self, step: Step) -> Submission {
        if self
            .last_accepted(step.session)
            .is_some_and(|last| step.serial <= last)
        {
            return Submission::Rejected(step);
        }
        self.serials.insert(step.session, step.serial);
        self.executions += 1;
        let old = self.records.get(&step.key);
        let outcome = match step.operation {
            Operation::Read { abort_if_tombstone } => match old {
                Some(Some(v)) => ResultValue::Value(v.clone()),
                Some(None) if abort_if_tombstone => ResultValue::Tombstone,
                _ => ResultValue::NotFound,
            },
            Operation::Upsert(v) => {
                self.records.insert(step.key, Some(v));
                ResultValue::Written
            }
            Operation::Delete { force_tombstone } => {
                if old.is_some_and(Option::is_some) || force_tombstone {
                    self.records.insert(step.key, None);
                    ResultValue::Deleted
                } else {
                    ResultValue::NotFound
                }
            }
            Operation::Rmw {
                operand,
                create_if_missing,
            } => {
                let updated = match (old.and_then(Option::as_ref), operand) {
                    (None, v) if create_if_missing => v,
                    (None, _) => return Submission::Accepted(ResultValue::NotFound),
                    (Some(Value::Number(a)), Value::Number(b)) => Value::Number(a.wrapping_add(b)),
                    (Some(Value::Bytes(a)), Value::Bytes(b)) => {
                        let mut v = a.clone();
                        v.extend(b);
                        Value::Bytes(v)
                    }
                    _ => return Submission::Accepted(ResultValue::TypeMismatch),
                };
                self.records.insert(step.key, Some(updated.clone()));
                ResultValue::Value(updated)
            }
        };
        Submission::Accepted(outcome)
    }
}
