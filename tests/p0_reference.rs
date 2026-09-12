//! P0 Support tool acceptance;No execution here RasterKV engine.
mod support;
use raster::types::Effect;
use support::{
    fault::*,
    model::{Model, ResultValue as R, Submission},
    trace::*,
};
fn step(serial: u64, operation: Operation) -> Step {
    Step {
        session: 0,
        serial,
        key: vec![],
        operation,
    }
}
fn read(serial: u64) -> Step {
    step(
        serial,
        Operation::Read {
            abort_if_tombstone: false,
        },
    )
}
#[test]
fn blind_deletion_observation_model_rejects_wrong_values_to_force_tombstone_loss_and_forgery_success()
 {
    use support::model::ContractModel;
    let accepted = |value| Submission::Accepted(value);
    let mut empty = ContractModel::default();
    assert!(
        empty
            .verify(
                step(
                    0,
                    Operation::Delete {
                        force_tombstone: false
                    }
                ),
                &accepted(R::Deleted)
            )
            .is_err()
    );
    let mut live = ContractModel::default();
    live.verify(
        step(0, Operation::Upsert(Value::Number(7))),
        &accepted(R::Written),
    )
    .unwrap();
    assert!(
        live.verify(read(1), &accepted(R::Value(Value::Number(8))))
            .is_err()
    );
    for wrong in [R::NotFound, R::Value(Value::Number(7)), R::TypeMismatch] {
        let mut forced = ContractModel::default();
        forced
            .verify(
                step(
                    0,
                    Operation::Delete {
                        force_tombstone: true,
                    },
                ),
                &accepted(R::Deleted),
            )
            .unwrap();
        assert!(
            forced
                .verify(
                    step(
                        1,
                        Operation::Read {
                            abort_if_tombstone: true
                        }
                    ),
                    &accepted(wrong)
                )
                .is_err()
        );
    }
    let mut live = ContractModel::default();
    live.verify(
        step(0, Operation::Upsert(Value::Number(7))),
        &accepted(R::Written),
    )
    .unwrap();
    assert!(
        live.verify(
            step(
                1,
                Operation::Delete {
                    force_tombstone: false
                }
            ),
            &accepted(R::NotFound)
        )
        .is_err()
    );
}
#[test]
fn the_blind_deletion_observation_model_limits_the_accessibility_of_ordinary_tombstones_and_rejects_illegal_serial_numbers_without_any_side_effects()
 {
    use support::model::ContractModel;
    for visible in [R::NotFound, R::Tombstone] {
        let mut model = ContractModel::default();
        for (request, result) in [
            (step(0, Operation::Upsert(Value::Number(7))), R::Written),
            (
                step(
                    2,
                    Operation::Delete {
                        force_tombstone: false,
                    },
                ),
                R::Deleted,
            ),
            (
                step(
                    3,
                    Operation::Read {
                        abort_if_tombstone: true,
                    },
                ),
                visible,
            ),
            (
                step(
                    5,
                    Operation::Rmw {
                        create_if_missing: false,
                        operand: Value::Number(1),
                    },
                ),
                R::NotFound,
            ),
            (
                step(
                    7,
                    Operation::Delete {
                        force_tombstone: true,
                    },
                ),
                R::Deleted,
            ),
            (
                step(
                    9,
                    Operation::Read {
                        abort_if_tombstone: true,
                    },
                ),
                R::Tombstone,
            ),
        ] {
            model
                .verify(request, &Submission::Accepted(result))
                .unwrap();
        }
        let rejected = step(9, Operation::Upsert(Value::Number(9)));
        model
            .verify(rejected.clone(), &Submission::Rejected(rejected))
            .unwrap();
        assert_eq!(model.last_accepted(0), Some(9));
        model
            .verify(
                step(
                    10,
                    Operation::Read {
                        abort_if_tombstone: true,
                    },
                ),
                &Submission::Accepted(R::Tombstone),
            )
            .unwrap();
    }
}
#[test]
fn the_fixed_trajectory_is_gradually_consistent_with_independent_expectations() {
    let trace = Trace::decode(include_str!("fixtures/p0.trace")).unwrap();
    let expected = [
        R::NotFound,
        R::Written,
        R::Value(Value::Number(0)),
        R::Deleted,
        R::Tombstone,
        R::NotFound,
        R::NotFound,
        R::Deleted,
    ];
    assert_eq!(trace.steps.len(), expected.len());
    let mut model = Model::default();
    for (step, expected) in trace.steps.into_iter().zip(expected) {
        assert_eq!(model.submit(step), Submission::Accepted(expected));
    }
    assert_eq!(model.executions, 8);
}
#[test]
fn generator_fixed_seed_and_format_round_trip_replayable() {
    let first = Trace::generate(0, 1);
    assert_eq!(first.steps[0].session, 1);
    assert_eq!(first.steps[0].key, vec![4; 4]);
    assert_eq!(first.encode(), "raster-trace 1 0\n1 2 04040404 delete 1\n");
    for seed in [0, 1, 42, u64::MAX] {
        let trace = Trace::generate(seed, 256);
        assert_eq!(trace, Trace::generate(seed, 256));
        assert_eq!(Trace::decode(&trace.encode()).unwrap(), trace);
        let mut a = Model::default();
        let mut b = Model::default();
        for step in &trace.steps {
            assert_eq!(
                a.submit(step.clone()),
                b.submit(step.clone()),
                "seeds {seed}\n{}",
                trace.encode()
            );
        }
    }
    assert_ne!(Trace::generate(1, 32), Trace::generate(2, 32));
}
#[test]
fn damaged_trajectory_rejection() {
    for bad in [
        "",
        "raster-trace 2 0",
        "raster-trace 1 -1",
        "raster-trace 1 18446744073709551616",
        "raster-trace 1 0\n0 1 - read 2",
        "raster-trace 1 0\n0 1 f read 0",
        "raster-trace 1 0\n0 1 zz read 0",
        "raster-trace 1 0\n0 1 - upsert u",
        "raster-trace 1 0\n0 1 - read 0 extra",
        "raster-trace 1 0\n0 1 - upsert z 1",
    ] {
        assert!(Trace::decode(bad).is_err(), "{bad}");
    }
}
#[test]
fn number_hopping_rejection_and_multi_session_do_not_consume_sequence_numbers_from_each_other() {
    let mut model = Model::default();
    assert_eq!(model.submit(read(0)), Submission::Accepted(R::NotFound));
    assert_eq!(model.submit(read(9)), Submission::Accepted(R::NotFound));
    for serial in [9, 8] {
        let request = read(serial);
        assert_eq!(model.submit(request.clone()), Submission::Rejected(request));
    }
    assert_eq!(model.executions, 2);
    assert_eq!(model.last_accepted(0), Some(9));
    let mut other = read(0);
    other.session = 1;
    assert_eq!(model.submit(other), Submission::Accepted(R::NotFound));
    assert_eq!(model.last_accepted(1), Some(0));
}
#[test]
fn variable_length_append_and_type_error_do_not_modify_the_old_value() {
    let mut model = Model::default();
    assert_eq!(
        model.submit(step(
            0,
            Operation::Rmw {
                operand: Value::Bytes(vec![]),
                create_if_missing: true
            }
        )),
        Submission::Accepted(R::Value(Value::Bytes(vec![])))
    );
    assert_eq!(
        model.submit(step(
            1,
            Operation::Rmw {
                operand: Value::Bytes(vec![0, 255]),
                create_if_missing: true
            }
        )),
        Submission::Accepted(R::Value(Value::Bytes(vec![0, 255])))
    );
    assert_eq!(
        model.submit(step(
            2,
            Operation::Rmw {
                operand: Value::Number(1),
                create_if_missing: true
            }
        )),
        Submission::Accepted(R::TypeMismatch)
    );
    assert_eq!(
        model.submit(read(3)),
        Submission::Accepted(R::Value(Value::Bytes(vec![0, 255])))
    );
}
#[test]
fn faults_are_triggered_single_time_based_on_event_and_number_of_occurrences() {
    let mut names = std::collections::BTreeSet::new();
    for event in Event::ALL {
        assert!(names.insert(event.name()));
        assert!(FaultScript::new(event, 0).is_err());
        let mut script = FaultScript::new(event, 2).unwrap();
        for other in Event::ALL {
            if other != event {
                assert!(!script.hit(other));
            }
        }
        assert!(!script.hit(event));
        assert!(script.hit(event));
        assert!(!script.hit(event));
    }
}
#[test]
fn terminate_once_and_the_error_cannot_be_retried_after_modification() {
    for effect in [Effect::NotApplied, Effect::Applied, Effect::Unknown] {
        let mut life = Lifecycle::default();
        assert!(life.finish(effect, true).is_err());
        assert!(life.retry_computation().is_err());
        life.accept().unwrap();
        assert!(life.accept().is_err());
        life.retry_computation().unwrap();
        life.finish(effect, true).unwrap();
        assert!(life.retry_computation().is_err());
        assert!(life.finish(effect, true).is_err());
        assert_eq!(life.failed, effect != Effect::NotApplied);
        assert_eq!(life.attempts, 2);
    }
}

#[test]
fn panics_in_the_possible_effective_range_will_not_be_modified_repeatedly() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let mut life = Lifecycle::default();
    let mut value = 0;
    life.accept().unwrap();
    life.begin_mutation().unwrap();
    let failure = catch_unwind(AssertUnwindSafe(|| {
        value += 1;
        panic!("Panic after simulation modification");
    }));
    assert!(failure.is_err());
    assert!(life.retry_computation().is_err());
    life.finish(Effect::Unknown, true).unwrap();
    assert!(life.failed);
    assert_eq!(value, 1);
    assert_eq!(life.attempts, 1);
}

#[test]
fn notification_panic_does_not_change_the_finalized_result() {
    let mut life = Lifecycle::default();
    life.accept().unwrap();
    life.finish(Effect::Applied, false).unwrap();
    assert!(std::panic::catch_unwind(|| panic!("Simulation result observer panics")).is_err());
    assert!(life.finish(Effect::Applied, false).is_err());
    assert!(life.retry_computation().is_err());
    assert!(!life.failed);
}
