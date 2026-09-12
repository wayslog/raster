//! Benchmarks can't get pretty results by counting or entering caliber errors;Use true short transmission and independent fixed expectation check.
#[path = "../examples/benchmark/meter.rs"]
mod meter;
#[path = "support/model.rs"]
#[allow(
    dead_code,
    reason = "Fixed benchmarks only use deterministic value models;The blind deletion observation model is given by P0/P3/P9 Trajectory acceptance"
)]
mod model;
#[allow(
    dead_code,
    reason = "Reuse benchmark generator,The test does not execute the entire timing matrix"
)]
#[path = "../examples/benchmark/scenario.rs"]
mod scenario;
#[allow(
    dead_code,
    reason = "Reuse trajectory data types,There is also a complete test of the text protocol"
)]
#[path = "support/trace.rs"]
mod trace;
use model::ResultValue;
use raster::{device::*, types::*};
use std::sync::Arc;
use trace::Value;

#[test]
fn the_benchmark_only_accepts_limited_results_of_ordinary_deletion_and_retains_the_logical_load_caliber()
 {
    use trace::{Operation, Step};
    let step = |operation| Step {
        session: 0,
        serial: 0,
        key: vec![],
        operation,
    };
    assert!(scenario::matches_result(
        &step(Operation::Delete {
            force_tombstone: false
        }),
        &ResultValue::Deleted,
        &ResultValue::NotFound
    ));
    assert!(!scenario::matches_result(
        &step(Operation::Delete {
            force_tombstone: true
        }),
        &ResultValue::NotFound,
        &ResultValue::Deleted
    ));
    assert!(!scenario::matches_result(
        &step(Operation::Delete {
            force_tombstone: false
        }),
        &ResultValue::NotFound,
        &ResultValue::Deleted
    ));
    assert!(!scenario::matches_result(
        &step(Operation::Read {
            abort_if_tombstone: true
        }),
        &ResultValue::Value(Value::Number(7)),
        &ResultValue::Tombstone
    ));
    assert!(!scenario::matches_result(
        &step(Operation::Read {
            abort_if_tombstone: false
        }),
        &ResultValue::NotFound,
        &ResultValue::Value(Value::Number(7))
    ));
}

#[test]
fn metering_only_accumulates_actual_completed_bytes_and_rejects_errors_and_old_output_without_double_counting()
 {
    let inner = Arc::new(memory::MemoryDevice::new(1, 1024).unwrap());
    let meter = meter::Meter::default();
    let device = meter.wrap(inner.clone());
    let request = |operation| IoRequest {
        route: CompletionRoute(1),
        operation,
    };
    device
        .submit(request(IoOperation::Open {
            path: "data".into(),
            create_new: true,
        }))
        .unwrap();
    let mut output = Vec::new();
    device.poll(PollBudget::default(), &mut output).unwrap();
    let IoOutcome::Opened(file) = output[0].result.as_ref().unwrap() else {
        panic!("The test file should be opened")
    };
    let file = *file;
    inner.inject_next(memory::MemoryFault::Short(3)).unwrap();
    device
        .submit(request(IoOperation::Write {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(8, 8).unwrap(),
        }))
        .unwrap();
    let rejected = device
        .submit(request(IoOperation::Read {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(8, 8).unwrap(),
        }))
        .unwrap_err();
    assert!(matches!(rejected.reason, Error::Busy));
    assert_eq!((meter.snapshot().read, meter.snapshot().written), (0, 0));
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert_eq!((meter.snapshot().read, meter.snapshot().written), (0, 3));
    device.submit(rejected.request).unwrap();
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert_eq!((meter.snapshot().read, meter.snapshot().written), (3, 3));
    inner
        .inject_next(memory::MemoryFault::Fail(std::io::ErrorKind::Other))
        .unwrap();
    device
        .submit(request(IoOperation::Read {
            file,
            offset: 0,
            buffer: AlignedBuffer::new_zeroed(8, 8).unwrap(),
        }))
        .unwrap();
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert!(output.last().unwrap().result.is_err());
    device.poll(PollBudget::default(), &mut output).unwrap();
    assert_eq!((meter.snapshot().read, meter.snapshot().written), (3, 3));
}

#[test]
fn fixed_trajectory_blend_ratio_deletion_missing_and_amplified_denominator_have_independent_expectations()
 {
    let fixed = scenario::prepare(
        scenario::Case {
            variable: false,
            hot: false,
            threads: 1,
            disk: true,
            round: 1,
        },
        0,
    );
    assert_eq!(
        (fixed.fill.len(), fixed.work.len(), fixed.verify.len()),
        (2048, 16384, 2048)
    );
    assert_eq!(fixed.application_bytes, 90_112);
    assert_eq!(
        fixed
            .work
            .iter()
            .filter(|(_, r)| *r == ResultValue::Deleted)
            .count(),
        1024
    );
    assert_eq!(
        fixed
            .work
            .iter()
            .filter(|(_, r)| *r == ResultValue::NotFound)
            .count(),
        3072
    );
    assert_eq!(fixed.verify[0].1, ResultValue::Tombstone);
    assert_eq!(
        fixed.verify[1].1,
        ResultValue::Value(Value::Number(scenario::SEED + 2298))
    );
    let hot = scenario::prepare(
        scenario::Case {
            variable: true,
            hot: true,
            threads: 4,
            disk: false,
            round: 3,
        },
        1,
    );
    assert_eq!(
        (hot.fill.len(), hot.work.len(), hot.verify.len()),
        (512, 4096, 512)
    );
    assert_eq!(hot.application_bytes, 418_824);
    assert_eq!(
        hot.work
            .iter()
            .filter(|(_, r)| *r == ResultValue::Deleted)
            .count(),
        1
    );
    assert_eq!(
        hot.work
            .iter()
            .filter(|(_, r)| *r == ResultValue::NotFound)
            .count(),
        1023
    );
    let mut expected = vec![6; 224];
    expected.extend(vec![0xa5; 1024]);
    assert_eq!(hot.work[3071].1, ResultValue::Value(Value::Bytes(expected)));
    assert_eq!(hot.verify[0].1, ResultValue::Tombstone);
    assert_eq!(
        hot.verify[1].1,
        ResultValue::Value(Value::Bytes(vec![1; 64]))
    );
}
