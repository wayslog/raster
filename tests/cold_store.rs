mod common;

use common::varlen::{TestDirectory, VarKey, VarRequest, VarValue};
use raster::core::cold_store::ColdStore;
use raster::core::hlog::LogConfig;
use raster::core::{ReadContext, Status};

#[test]
fn evicted_variable_length_keys_with_one_slot_follow_the_data_chain() {
    let directory = TestDirectory::new("cold-store-variable-collisions");
    let log = LogConfig {
        page_bits: 12,
        memory_pages: 4,
        mutable_fraction: 0.5,
    };
    let store = ColdStore::<VarKey<true>, VarValue>::open(&directory.0, 1, log, log).unwrap();
    let mut session = store.try_start_session().unwrap();
    for key in 0..64 {
        let mut op = VarRequest::<true>::new(key, 173, 391);
        op.data.fill(key as u8);
        let status = session.upsert(&mut op, key + 1);
        assert!(matches!(status, Status::Ok | Status::Pending));
        assert!(session.complete_pending(true));
    }

    let mut op = VarRequest::<true>::new(0, 173, 0);
    assert_eq!(session.read(&mut op, 65), Status::Pending);
    assert!(session.complete_pending(true));
    assert_eq!(op.out.borrow_mut().take(), Some((Status::Ok, vec![0; 391])));

    let mut op = VarRequest::<true>::new(63, 173, 0);
    assert_eq!(session.read(&mut op, 66), Status::Ok);
    ReadContext::on_complete(&mut op, Status::Ok);
    assert_eq!(
        op.out.borrow_mut().take(),
        Some((Status::Ok, vec![63; 391]))
    );
}
