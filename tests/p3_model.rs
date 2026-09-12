//! use P0 independent model operation-by-operation verification public engine;Adapter does not simulate indexes and logs.
#[path = "support/actor.rs"]
mod actor;
#[path = "support/replay.rs"]
mod replay;
#[allow(dead_code)]
mod support;
use raster::{
    RasterKV,
    api::session::SessionOptions,
    schema::builtin::{ByteKey, SchemaPair, SerializedValue},
    types::*,
};
use replay::{Codec, Schema, actual};
use support::{
    model::ContractModel,
    trace::{Operation, Step, Trace, Value},
};
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
    let mut model = ContractModel::default();
    for (i, step) in trace.steps.iter().enumerate() {
        let session = sessions.entry(step.session).or_insert_with(|| {
            let store = store.clone();
            actor::Actor::new(move || store.start_session(SessionOptions::default()).unwrap())
        });
        let input = step.clone();
        let (observed, step_pending, last) = session.call(move |session| {
            let mut pending = [0; 4];
            let observed = actual(session, &input, &mut pending, false);
            (observed, pending, session.last_accepted())
        });
        model
            .verify(step.clone(), &observed)
            .unwrap_or_else(|error| panic!("seeds {} step {i}:{error}", trace.seed));
        assert_eq!(last.map(|s| s.0), model.last_accepted(step.session));
        for (sum, count) in pending.iter_mut().zip(step_pending) {
            *sum += count;
        }
    }
    for session in sessions.values() {
        session.call(|session| {
            session
                .close(Deadline(
                    std::time::Instant::now() + std::time::Duration::from_secs(15),
                ))
                .unwrap()
        });
    }
    drop(sessions);
    store
        .shutdown(Deadline(
            std::time::Instant::now() + std::time::Duration::from_secs(15),
        ))
        .unwrap();
    pending
}

#[test]
fn fixed_trajectories_are_compared_operation_by_operation_through_the_real_engine() {
    replay(Trace::decode(include_str!("fixtures/p0.trace")).unwrap());
}
#[test]
fn byte_key_storage_boundaries_preserve_full_keys_across_four_operations() {
    let keys: Vec<_> = [0, 1, 15, 16, 17, 31, 64, 1024]
        .into_iter()
        .map(|length| vec![0xfa; length])
        .chain([vec![0; 17], {
            let mut key = vec![0; 17];
            key[16] = 1;
            key
        }])
        .collect();
    let mut steps = Vec::new();
    for stage in 0..9 {
        for (index, key) in keys.iter().enumerate() {
            let operation = match stage {
                0 => Operation::Upsert(Value::Number(index as u64)),
                1 => Operation::Rmw {
                    operand: Value::Number(9),
                    create_if_missing: false,
                },
                2 | 7 => Operation::Read {
                    abort_if_tombstone: false,
                },
                3 => Operation::Delete {
                    force_tombstone: true,
                },
                4 => Operation::Read {
                    abort_if_tombstone: true,
                },
                5 => Operation::Rmw {
                    operand: Value::Number(1),
                    create_if_missing: false,
                },
                6 => Operation::Upsert(Value::Bytes(vec![index as u8; 80])),
                8 => Operation::Delete {
                    force_tombstone: false,
                },
                _ => unreachable!(),
            };
            steps.push(Step {
                session: index as u64 % 2,
                serial: steps.len() as u64 + 1,
                key: key.clone(),
                operation,
            });
        }
    }
    replay(Trace { seed: 0, steps });
}
#[test]
fn determining_seed_mix_values_for_multi_session_four_operation_control() {
    for seed in [0, 1, 42, 0xabcdef, u64::MAX] {
        replay(Trace::generate(seed, 1500));
    }
}
#[test]
fn variable_length_value_growth_and_shrinkage_type_error_and_serial_number_rejection_comparison() {
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
fn native_file_two_page_memory_variable_length_value_four_operation_mixed_trajectory() {
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
        "All four operations should be done through actual experience Pending:{pending:?}"
    );
    let files: Vec<_> = std::fs::read_dir(root.0.join("segments"))
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .collect();
    assert!(files.len() > 1);
    assert!(files.iter().sum::<u64>() > 4096 * 16);
    eprintln!(
        "Native hybrid track:Read/Upsert/RMW/Delete Pending={pending:?},Number of segments={},file bytes={}",
        files.len(),
        files.iter().sum::<u64>()
    );
}
