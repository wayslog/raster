//! Skeleton implemented base contract;These checks do not indicate that the storage engine functionality passes.
use raster::{config::Config, device::AlignedBuffer, schema::value::ValuePlan, types::Error};

#[test]
fn the_default_configuration_is_legal_and_rejects_non_finite_ratios_and_overflows() {
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
fn capacity_requirements_cover_both_active_and_disk_representations() {
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
fn buffers_remain_aligned_and_retain_data_after_being_moved() {
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
fn bad_buffer_parameters_dont_trigger_allocation() {
    assert!(AlignedBuffer::new_zeroed(1, 0).is_err());
    assert!(AlignedBuffer::new_zeroed(1, 3).is_err());
    assert!(AlignedBuffer::new_zeroed(0, 8).is_err());
    assert!(matches!(
        AlignedBuffer::new_zeroed(usize::MAX, 8),
        Err(Error::CapacityExceeded)
    ));
}
