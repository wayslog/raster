//! 原生设备页读取接入扫描游标；尚不替代公开扫描器的缓冲与生命周期验收。
use super::*;
use crate::{
    api::completion::TicketState, engine::io_hub::CompletionHub, log::read_page::PageRead,
};

#[test]
fn 原生磁盘页扫描输出独立且共享轮询不执行另一会话回调() {
    let (_root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    let checkpoint = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    wait(&mut session, &checkpoint);
    let frontiers = store.inner.log.frontiers().unwrap();
    assert!(frontiers.head > frontiers.begin);
    assert_eq!(frontiers.tail.0 % 4096, 0);
    let Submission::Pending(mut user_read) = session
        .read(Serial(400), Read(0), Default::default())
        .unwrap()
    else {
        panic!("冷页用户读取必须挂起")
    };
    let id = store
        .inner
        .io
        .reserve(SessionId::generate().unwrap())
        .unwrap();
    let mut outputs = Vec::new();
    for number in frontiers.begin.0 / 4096..frontiers.tail.0 / 4096 {
        let page = PageId(number);
        let mut read = PageRead::new(page, 4096, CompletionHub::route(id)).unwrap();
        let stop = Instant::now() + Duration::from_secs(10);
        let page = loop {
            assert!(Instant::now() < stop, "磁盘扫描读取超时");
            match read.submit_next(&store.inner.storage) {
                Ok(_) | Err(Error::Busy) => {}
                Err(error) => panic!("提交扫描读取失败：{error:?}"),
            }
            store
                .inner
                .io
                .poll(&*store.inner.storage.device, PollBudget::default())
                .unwrap();
            if let Some(completion) = store.inner.io.take(id).unwrap() {
                read.accept(&store.inner.storage, completion).unwrap();
            }
            if let Some(page) = read.finish(&store.inner.storage).unwrap() {
                break page;
            }
            std::thread::yield_now();
        };
        assert!(!read.has_inflight());
        let mut cursor = page
            .into_scan(LogAddress(number * 4096), LogAddress((number + 1) * 4096))
            .unwrap();
        while let Some(record) = cursor.next_record(&*store.inner.schema).unwrap() {
            assert!(!record.invalid && !record.tombstone);
            outputs.push(record);
        }
    }
    store.inner.io.release(id).unwrap();
    // 其他路由已经被轮询，但用户上下文必须由自己的会话推进。
    assert!(matches!(
        user_read.try_take().unwrap(),
        TicketState::Pending
    ));
    assert!(matches!(
        session.wait(&mut user_read, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(0)
    ));
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    assert_eq!(outputs.len(), 400);
    for (key, record) in outputs.iter().enumerate() {
        assert_eq!(record.key, key as u64);
        assert_eq!(record.value, Some(key as u64));
        if key > 0 {
            assert!(record.address > outputs[key - 1].address);
        }
    }
}
