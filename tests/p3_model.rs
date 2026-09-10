//! 用 P0 的独立模型逐操作核验公开引擎；适配器不模拟索引和日志。
#[allow(dead_code)]
mod support;
use raster::{
    RasterKV, Submission,
    api::{
        completion::{AbortReason, Outcome},
        operation::*,
        session::SessionOptions,
    },
    schema::{
        ValueRead, ValueUpdate,
        builtin::{ByteKey, SchemaPair, SerializedValue},
        value::ValueCodec,
    },
    types::*,
};
use support::{
    model::{Model, ResultValue, Submission as ModelSubmission},
    trace::{Operation, Step, Trace, Value},
};
struct Codec;
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
            _ => Err(Error::Codec("轨迹值损坏")),
        }
    }
}
type Schema = SchemaPair<ByteKey, SerializedValue<Codec>>;
struct Request {
    key: Vec<u8>,
    value: Value,
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
        _ => Err(Error::Codec("类型不匹配")),
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
fn actual(
    session: &mut raster::Session<Schema>,
    step: &Step,
    pending: &mut [usize; 4],
) -> ModelSubmission {
    let value = match &step.operation {
        Operation::Upsert(v) => v.clone(),
        Operation::Rmw { operand, .. } => operand.clone(),
        _ => Value::Number(0),
    };
    let request = Request {
        key: step.key.clone(),
        value,
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
                "未预期的拒绝：{}",
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
                    "请求等待超时：{step:?}"
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
            cause: Error::Codec("类型不匹配"),
            effect: Effect::NotApplied,
        }) => ResultValue::TypeMismatch,
        result => panic!("未预期的引擎结果：{step:?} => {result:?}"),
    };
    ModelSubmission::Accepted(value)
}
fn replay(trace: Trace) {
    let store = RasterKV::builder(SchemaPair::new(ByteKey, SerializedValue::new(Codec)))
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    assert_eq!(replay_store(trace, store), [0; 4]);
}
fn replay_store(trace: Trace, store: RasterKV<Schema>) -> [usize; 4] {
    let mut pending = [0; 4];
    let mut sessions = std::collections::BTreeMap::new();
    let mut model = Model::default();
    for (i, step) in trace.steps.iter().enumerate() {
        let session = sessions
            .entry(step.session)
            .or_insert_with(|| store.start_session(SessionOptions::default()).unwrap());
        let expected = model.submit(step.clone());
        assert_eq!(
            actual(session, step, &mut pending),
            expected,
            "种子 {} 步骤 {i}",
            trace.seed
        );
        assert_eq!(
            session.last_accepted().map(|s| s.0),
            model.last_accepted(step.session)
        );
    }
    pending
}
#[test]
fn 固定轨迹通过真实引擎逐操作对照() {
    replay(Trace::decode(include_str!("fixtures/p0.trace")).unwrap());
}
#[test]
fn 确定种子混合值多会话四操作对照() {
    for seed in [0, 1, 42, 0xabcdef, u64::MAX] {
        replay(Trace::generate(seed, 1500));
    }
}
#[test]
fn 变长值增长收缩类型错误与序号拒绝对照() {
    let key = vec![255, 0];
    let mut steps = vec![];
    for (serial, operation) in [
        (1, Operation::Upsert(Value::Bytes(vec![]))),
        (
            3,
            Operation::Rmw {
                operand: Value::Bytes(vec![8; 600]),
                create_if_missing: true,
            },
        ),
        (
            4,
            Operation::Read {
                abort_if_tombstone: false,
            },
        ),
        (7, Operation::Upsert(Value::Bytes(vec![9]))),
        (
            8,
            Operation::Rmw {
                operand: Value::Number(1),
                create_if_missing: true,
            },
        ),
        (8, Operation::Upsert(Value::Number(9))),
        (
            9,
            Operation::Read {
                abort_if_tombstone: false,
            },
        ),
    ] {
        steps.push(Step {
            session: 0,
            serial,
            key: key.clone(),
            operation,
        });
    }
    replay(Trace { seed: 77, steps });
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn 原生文件两页内存变长值四操作混合轨迹() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-model-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = raster::config::Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    config.storage.segment_bytes = 65536;
    config.storage.root = root.0.clone();
    let store = RasterKV::builder(SchemaPair::new(ByteKey, SerializedValue::new(Codec)))
        .config(config)
        .device(Box::new(
            raster::device::thread_pool::ThreadPoolDeviceFactory {
                workers: 3,
                queue_capacity: 64,
            },
        ))
        .create()
        .unwrap();
    let key = |i: u64| {
        let mut key = b"warm".to_vec();
        key.extend(i.to_le_bytes());
        key
    };
    let mut steps = vec![];
    for i in 0..200 {
        steps.push(Step {
            session: 0,
            serial: i + 1,
            key: key(i),
            operation: Operation::Upsert(Value::Bytes(vec![i as u8; 64 + i as usize % 12 * 64])),
        });
    }
    let mut random = Trace::generate(42, 1500);
    for (i, step) in random.steps.iter_mut().enumerate() {
        step.serial += 10000;
        if !step.key.is_empty() {
            step.key.extend((((i / 8) % 64) as u64).to_le_bytes());
        }
        if let Operation::Upsert(Value::Bytes(bytes)) = &mut step.operation {
            bytes.resize(64 + i % 24 * 48, i as u8);
        }
    }
    steps.extend(random.steps);
    for i in 0..200 {
        steps.push(Step {
            session: 0,
            serial: 1_000_000 + i,
            key: key(i),
            operation: if i % 2 == 0 {
                Operation::Rmw {
                    operand: Value::Bytes(vec![1, 2]),
                    create_if_missing: false,
                }
            } else {
                Operation::Delete {
                    force_tombstone: false,
                }
            },
        });
    }
    for i in 0..200 {
        steps.push(Step {
            session: 0,
            serial: 2_000_000 + i,
            key: key(i),
            operation: Operation::Read {
                abort_if_tombstone: i % 3 == 0,
            },
        });
    }
    let pending = replay_store(Trace { seed: 42, steps }, store);
    assert!(
        pending.iter().all(|count| *count > 0),
        "四操作均应实际经历 Pending：{pending:?}"
    );
    let files: Vec<_> = std::fs::read_dir(root.0.join("segments"))
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .collect();
    assert!(files.len() > 1);
    assert!(files.iter().sum::<u64>() > 4096 * 16);
    eprintln!(
        "原生混合轨迹：Read/Upsert/RMW/Delete Pending={pending:?}，段数={}，文件字节={}",
        files.len(),
        files.iter().sum::<u64>()
    );
}
