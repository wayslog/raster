//! 基准不能靠计数或输入口径错误得到漂亮结果；使用真实短传输及独立固定预期校验。
#[path = "../examples/benchmark/meter.rs"]
mod meter;
#[path = "support/model.rs"]
mod model;
#[allow(dead_code, reason = "复用基准生成器，测试不执行整套计时矩阵")]
#[path = "../examples/benchmark/scenario.rs"]
mod scenario;
#[allow(dead_code, reason = "复用轨迹数据类型，文本协议另有完整测试")]
#[path = "support/trace.rs"]
mod trace;
use model::ResultValue;
use raster::{device::*, types::*};
use std::sync::Arc;
use trace::Value;

#[test]
fn 计量只累计实际完成字节且拒绝错误和旧输出不重复计数() {
    let inner = Arc::new(memory::MemoryDevice::new(1, 1024).unwrap());
    let meter = meter::Meter::default();
    let device = meter.wrap(inner.clone());
    let request = |operation| IoRequest {
        route: CompletionRoute(1),
        operation,
    };
    device
        .submit(request(IoOperation::Open {
            path: "数据".into(),
            create_new: true,
        }))
        .unwrap();
    let mut output = Vec::new();
    device.poll(PollBudget::default(), &mut output).unwrap();
    let IoOutcome::Opened(file) = output[0].result.as_ref().unwrap() else {
        panic!("应打开测试文件")
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
fn 固定轨迹的混合比例删除缺失及放大分母有独立预期() {
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
