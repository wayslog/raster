//! independent sequential model,Compare only logical results,Do not simulate pages,Index or persistence.
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

/// Non-deterministic observation model of blind deletion contract.Only relax the two results declared to be related to the representation method,
/// mandatory tombstone,active value,Sequence number rejection and logical side effects of four operations are still accurately checked.
#[derive(Default)]
pub struct ContractModel {
    logical: Model,
    tombstones: BTreeMap<Vec<u8>, bool>,
    reserved: std::collections::BTreeSet<Vec<u8>>,
}
impl ContractModel {
    pub fn last_accepted(&self, session: u64) -> Option<u64> {
        self.logical.last_accepted(session)
    }
    pub fn verify(&mut self, step: Step, observed: &Submission) -> Result<(), String> {
        if self
            .last_accepted(step.session)
            .is_some_and(|last| step.serial <= last)
        {
            return if *observed == Submission::Rejected(step) {
                Ok(())
            } else {
                Err("Illegal serial numbers must be rejected without side effects".into())
            };
        }
        let live = self
            .logical
            .records
            .get(&step.key)
            .is_some_and(Option::is_some);
        let required = self.tombstones.get(&step.key) == Some(&true);
        let has_history = !self.logical.records.is_empty() || !self.reserved.is_empty();
        let visibility = self.tombstones.get(&step.key).copied();
        let expected = self.logical.submit(step.clone());
        let accepted = |value| *observed == Submission::Accepted(value);
        let valid = match &step.operation {
            Operation::Delete {
                force_tombstone: true,
            } => accepted(ResultValue::Deleted),
            Operation::Delete {
                force_tombstone: false,
            } => {
                if live || required {
                    accepted(ResultValue::Deleted)
                } else {
                    accepted(ResultValue::NotFound) || has_history && accepted(ResultValue::Deleted)
                }
            }
            Operation::Read {
                abort_if_tombstone: true,
            } if !live => match visibility {
                Some(true) => accepted(ResultValue::Tombstone),
                Some(false) => accepted(ResultValue::Tombstone) || accepted(ResultValue::NotFound),
                None => accepted(ResultValue::NotFound),
            },
            _ => *observed == expected,
        };
        if !valid {
            return Err(format!(
                "Contract result does not match:{step:?},observe {observed:?},logical result {expected:?},Tombstone accessibility {visibility:?}"
            ));
        }
        if matches!(step.operation, Operation::Rmw { .. }) {
            // Does not predict whether physical slots will be cleared by maintenance,Only record business sources where blind deletion may find slots.
            self.reserved.insert(step.key.clone());
        }
        match step.operation {
            Operation::Delete { force_tombstone } => {
                if accepted(ResultValue::Deleted) {
                    self.logical.records.insert(step.key.clone(), None);
                    self.tombstones.insert(step.key, force_tombstone);
                } else {
                    self.logical.records.remove(&step.key);
                    self.tombstones.remove(&step.key);
                }
            }
            Operation::Upsert(_) | Operation::Rmw { .. }
                if self
                    .logical
                    .records
                    .get(&step.key)
                    .is_some_and(Option::is_some) =>
            {
                self.tombstones.remove(&step.key);
            }
            _ => {}
        }
        Ok(())
    }
}
