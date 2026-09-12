//! Short trajectories with fixed parameters;The oracle runs before timing,Hotspots are partitioned by threads and missing deletions are explicitly retained.
use crate::{
    model::{Model, ResultValue, Submission},
    trace::{Operation, Step, Trace, Value},
};
pub const KEYS: usize = 2048;
pub const OPERATIONS: usize = 16_384;
pub const PAGE_BYTES: usize = 16 * 1024;
pub const SEED: u64 = 0x7261737465720001;
#[derive(Clone, Copy, Debug)]
pub struct Case {
    pub variable: bool,
    pub hot: bool,
    pub threads: usize,
    pub disk: bool,
    pub round: usize,
}
impl Case {
    pub fn id(self) -> String {
        format!(
            "{}-{}-t{}-{}-r{}",
            if self.variable { "bytes" } else { "u64" },
            if self.hot { "hot" } else { "uniform" },
            self.threads,
            if self.disk { "disk" } else { "memory" },
            self.round
        )
    }
    pub fn input_id(self) -> String {
        format!(
            "{}-{}-t{}",
            if self.variable { "bytes" } else { "u64" },
            if self.hot { "hot" } else { "uniform" },
            self.threads
        )
    }
}
pub struct Prepared {
    pub fill: Vec<(Step, ResultValue)>,
    pub work: Vec<(Step, ResultValue)>,
    pub verify: Vec<(Step, ResultValue)>,
    pub application_bytes: u64,
}
pub fn bytes(value: &Value) -> u64 {
    match value {
        Value::Number(_) => 8,
        Value::Bytes(v) => v.len() as u64,
    }
}
fn value(variable: bool, ordinal: usize) -> Value {
    if variable {
        Value::Bytes(vec![ordinal as u8; (ordinal % 16 + 1) * 32])
    } else {
        Value::Number(SEED.wrapping_add(ordinal as u64))
    }
}
/// This fixed matrix only generates ordinary deletions:Duplicate deletion allows blind deletion to succeed,Are ordinary tombstones accessible after restoration?.
/// active value,Deletion successful for the first time,Conditional errors are still compared accurately according to the determined model.
pub fn matches_result(step: &Step, actual: &ResultValue, expected: &ResultValue) -> bool {
    actual == expected
        || matches!(
            (actual, expected, &step.operation),
            (
                ResultValue::Deleted,
                ResultValue::NotFound,
                Operation::Delete {
                    force_tombstone: false
                }
            ) | (
                ResultValue::NotFound,
                ResultValue::Tombstone,
                Operation::Read {
                    abort_if_tombstone: true
                }
            )
        )
}
fn expected(model: &mut Model, step: &Step) -> ResultValue {
    match model.submit(step.clone()) {
        Submission::Accepted(value) => value,
        _ => panic!("The generator serial number must be legal"),
    }
}
pub fn prepare(case: Case, worker: usize) -> Prepared {
    assert!([1, 4].contains(&case.threads) && worker < case.threads);
    let count = KEYS / case.threads;
    let mut model = Model::default();
    let mut serial = 0;
    let mut make = |local: usize, operation| {
        let step = Step {
            session: worker as u64,
            serial,
            key: ((worker * count + local) as u64).to_le_bytes().to_vec(),
            operation,
        };
        serial += 1;
        step
    };
    let mut fill = Vec::with_capacity(count);
    let mut application_bytes = 0;
    for local in 0..count {
        let value = value(case.variable, worker * count + local);
        application_bytes += bytes(&value);
        let step = make(local, Operation::Upsert(value));
        let result = expected(&mut model, &step);
        fill.push((step, result));
    }
    let mut work = Vec::with_capacity(OPERATIONS / case.threads);
    for kind in 0..4 {
        for i in 0..OPERATIONS / case.threads / 4 {
            let key = if case.hot {
                0
            } else if kind == 3 {
                ((i * 17) % (count / 2)) * 2
            } else {
                (i * 17) % count
            };
            let operation = match kind {
                0 => Operation::Read {
                    abort_if_tombstone: true,
                },
                1 => Operation::Upsert(value(case.variable, worker * count + i + 7)),
                2 => Operation::Rmw {
                    operand: if case.variable {
                        Value::Bytes(vec![0xa5])
                    } else {
                        Value::Number(1)
                    },
                    create_if_missing: true,
                },
                _ => Operation::Delete {
                    force_tombstone: false,
                },
            };
            let step = make(key, operation);
            let result = expected(&mut model, &step);
            application_bytes += match &step.operation {
                Operation::Upsert(v) | Operation::Rmw { operand: v, .. } => bytes(v),
                Operation::Delete { .. } if result == ResultValue::Deleted => step.key.len() as u64,
                _ => 0,
            };
            work.push((step, result));
        }
    }
    let verify = (0..count)
        .map(|key| {
            let step = make(
                key,
                Operation::Read {
                    abort_if_tombstone: true,
                },
            );
            let result = expected(&mut model, &step);
            (step, result)
        })
        .collect();
    Prepared {
        fill,
        work,
        verify,
        application_bytes,
    }
}
pub fn trace(prepared: &[Prepared]) -> Trace {
    // The key fields of each thread do not intersect;Files arranged by thread,Replay with the same step-by-step expectations,Runtime is still executed concurrently by real threads.
    Trace {
        seed: SEED,
        steps: prepared
            .iter()
            .flat_map(|p| p.fill.iter().chain(p.work.iter()).map(|(s, _)| s.clone()))
            .collect(),
    }
}
