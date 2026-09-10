//! 骨架已实现的基础契约；这些检查不表示存储引擎功能通过。
use raster::{config::Config, device::AlignedBuffer, schema::value::ValuePlan, types::Error};

#[test]
fn 默认配置合法且拒绝非有限比例与溢出() {
    let mut config = Config::default();
    assert!(config.validate().is_ok());
    for value in [f64::NAN, f64::INFINITY, -0.1, 1.0] {
        config.log.mutable_fraction = value;
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfig {
                field: "log.mutable_fraction",
                ..
            })
        ));
    }
    config.log.mutable_fraction = 0.9;
    config.log.memory_pages = usize::MAX;
    assert!(config.validate().is_err());
}

#[test]
fn 容量要求同时覆盖活跃表示和磁盘表示() {
    let valid = ValuePlan {
        live_bytes: 16,
        encoded_bytes: 8,
        capacity: 16,
        alignment: 8,
    };
    assert!(valid.validate().is_ok());
    assert!(
        ValuePlan {
            live_bytes: 17,
            ..valid
        }
        .validate()
        .is_err()
    );
    assert!(
        ValuePlan {
            encoded_bytes: 17,
            ..valid
        }
        .validate()
        .is_err()
    );
    assert!(
        ValuePlan {
            alignment: 3,
            ..valid
        }
        .validate()
        .is_err()
    );
}

#[test]
fn 缓冲移动后仍对齐并保留数据() {
    for alignment in [1, 8, 64, 512, 4096] {
        for length in [1, 17, 4096] {
            let mut buffer = AlignedBuffer::new_zeroed(length, alignment).unwrap();
            assert!(buffer.as_slice().iter().all(|value| *value == 0));
            buffer.as_mut_slice().fill(0x5a);
            let moved = Box::new(buffer);
            assert_eq!(moved.as_slice().as_ptr() as usize % alignment, 0);
            assert_eq!(moved.as_slice(), vec![0x5a; length]);
        }
    }
}

#[test]
fn 错误的缓冲参数不会触发分配() {
    assert!(AlignedBuffer::new_zeroed(1, 0).is_err());
    assert!(AlignedBuffer::new_zeroed(1, 3).is_err());
    assert!(AlignedBuffer::new_zeroed(0, 8).is_err());
    assert!(matches!(
        AlignedBuffer::new_zeroed(usize::MAX, 8),
        Err(Error::CapacityExceeded)
    ));
}
