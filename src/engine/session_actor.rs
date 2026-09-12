//! Engine component testing controls interleaving via real thread sessions.
#[path = "../../tests/support/actor.rs"]
mod actor;
pub(crate) use actor::Actor;
pub(crate) fn session<S: crate::schema::Schema>(
    store: &crate::RasterKV<S>,
) -> Actor<crate::Session<S>> {
    let store = store.clone();
    Actor::new(move || store.start_session(Default::default()).unwrap())
}
