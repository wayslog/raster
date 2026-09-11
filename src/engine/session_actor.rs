//! 引擎组件测试通过真实线程会话控制交错。
#[path = "../../tests/support/actor.rs"]
mod actor;
pub(crate) use actor::Actor;
pub(crate) fn session<S: crate::schema::Schema>(
    store: &crate::RasterKV<S>,
) -> Actor<crate::Session<S>> {
    let store = store.clone();
    Actor::new(move || store.start_session(Default::default()).unwrap())
}
