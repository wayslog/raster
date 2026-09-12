use super::*;
use std::sync::Arc;

#[test]
fn local_routes_and_global_work_respect_one_reserved_capacity() {
    let hub = Arc::new(CompletionHub::new(StoreId([1; 16]), 3).unwrap());
    assert!(matches!(
        hub.session_routes(SessionId([2; 16]), 0),
        Err(Error::CapacityExceeded)
    ));
    assert!(matches!(
        hub.session_routes(SessionId([2; 16]), 4),
        Err(Error::Busy)
    ));
    assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    let routes = hub.session_routes(SessionId([2; 16]), 2).unwrap();
    let mut first = routes.reserve().unwrap();
    let mut second = routes.reserve().unwrap();
    assert!(matches!(routes.reserve(), Err(Error::Busy)));
    let global = hub.reserve(SessionId([3; 16])).unwrap();
    assert!(matches!(hub.reserve(global.session), Err(Error::Busy)));
    assert_eq!(hub.occupied.load(Ordering::Acquire), 3);
    first.activate(&hub).unwrap();
    first.activate(&hub).unwrap();
    second.activate(&hub).unwrap();
    assert_eq!(hub.state.lock().unwrap().mailboxes.len(), 3);
    assert_eq!(hub.next.load(Ordering::Acquire), 3);
    first.release(&hub).unwrap();
    assert!(first.release(&hub).is_err());
    assert!(first.activate(&hub).is_err());
    // The released local owner still retains its credit until it is dropped.
    assert!(matches!(routes.reserve(), Err(Error::Busy)));
    drop(first);
    let third = routes.reserve().unwrap();
    assert!(third.id().slot > global.slot);
    hub.release(global).unwrap();
    assert_eq!(hub.occupied.load(Ordering::Acquire), 2);
    drop(routes);
    assert_eq!(hub.occupied.load(Ordering::Acquire), 2);
    // Abandonment releases an activated mailbox as well as a transient route.
    drop((second, third));
    assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    assert_eq!(hub.state.lock().unwrap().mailboxes.len(), 0);
}

#[test]
fn foreign_hubs_and_exhausted_ids_leave_the_quota_and_route_intact() {
    let hub = Arc::new(CompletionHub::new(StoreId([1; 16]), 1).unwrap());
    let other = CompletionHub::new(StoreId([1; 16]), 1).unwrap();
    let routes = hub.session_routes(SessionId([2; 16]), 1).unwrap();
    let mut route = routes.reserve().unwrap();
    assert!(route.activate(&other).is_err());
    assert!(route.release(&other).is_err());
    assert!(route.registered_id().is_none());
    route.activate(&hub).unwrap();
    route.release(&hub).unwrap();
    drop(route);
    hub.next.store(u64::MAX, Ordering::Release);
    assert!(matches!(routes.reserve(), Err(Error::CapacityExceeded)));
    assert_eq!(hub.next.load(Ordering::Acquire), u64::MAX);
    assert_eq!(hub.occupied.load(Ordering::Acquire), 1);
    drop(routes);
    assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    assert_eq!(other.occupied.load(Ordering::Acquire), 0);
}

#[test]
fn concurrently_booked_session_quotas_cannot_overcommit_the_hub() {
    let hub = Arc::new(CompletionHub::new(StoreId([1; 16]), 5).unwrap());
    for _ in 0..8 {
        let (send, receive) = std::sync::mpsc::channel();
        let release = std::sync::Barrier::new(9);
        let (accepted, booked, peak, global_release) = std::thread::scope(|scope| {
            for id in 1..=8 {
                let hub = hub.clone();
                let send = send.clone();
                let release = &release;
                scope.spawn(move || {
                    let routes = hub.session_routes(SessionId([id; 16]), 2);
                    send.send(match &routes {
                        Ok(_) => Ok(true),
                        Err(Error::Busy) => Ok(false),
                        Err(error) => Err(format!("unexpected quota error: {error:?}")),
                    })
                    .unwrap();
                    release.wait();
                    drop(routes);
                });
            }
            let accepted: Vec<_> = (0..8).map(|_| receive.recv().unwrap()).collect();
            let booked = hub.occupied.load(Ordering::Acquire);
            let global = hub.reserve(SessionId([9; 16]));
            let peak = hub.occupied.load(Ordering::Acquire);
            let global_release = global.and_then(|id| hub.release(id));
            // Release every worker before making assertions so a regression
            // reports a failure instead of leaving scoped threads blocked.
            release.wait();
            (accepted, booked, peak, global_release)
        });
        assert!(accepted.iter().all(Result::is_ok));
        assert_eq!(accepted.into_iter().filter(|v| *v == Ok(true)).count(), 2);
        assert_eq!(booked, 4);
        assert_eq!(peak, 5);
        assert!(global_release.is_ok());
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    }
}

#[test]
fn poisoned_mailboxes_retain_their_quota_without_a_hub_ownership_cycle() {
    let hub = Arc::new(CompletionHub::new(StoreId([1; 16]), 1).unwrap());
    let lifetime = Arc::downgrade(&hub);
    let routes = hub.session_routes(SessionId([2; 16]), 1).unwrap();
    let mut route = routes.reserve().unwrap();
    route.activate(&hub).unwrap();
    let poison = hub.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.state.lock().unwrap();
            panic!("poison the mailbox registry");
        })
        .join()
        .is_err()
    );
    assert!(routes.reserve().is_err());
    drop((routes, route));
    assert_eq!(hub.occupied.load(Ordering::Acquire), 1);
    drop(hub);
    assert!(lifetime.upgrade().is_none());
}

#[test]
fn retained_route_identity_cannot_match_hubs_created_after_its_owner_dies() {
    let hub = Arc::new(CompletionHub::new(StoreId([1; 16]), 1).unwrap());
    let lifetime = Arc::downgrade(&hub);
    let routes = hub.session_routes(SessionId([2; 16]), 1).unwrap();
    let mut route = routes.reserve().unwrap();
    drop((routes, hub));
    assert!(lifetime.upgrade().is_none());
    drop(lifetime);
    for _ in 0..128 {
        let replacement = Arc::new(CompletionHub::new(StoreId([1; 16]), 1).unwrap());
        assert!(route.activate(&replacement).is_err());
        assert!(route.release(&replacement).is_err());
        assert_eq!(replacement.occupied.load(Ordering::Acquire), 0);
    }
}
