//! 键语义与值布局为共享策略；用户每次操作的上下文属于会话。
pub mod builtin;
pub mod key;
pub mod value;

pub use key::KeyCodec;
pub use value::{ValueLayout, ValueRead, ValueUpdate};

pub trait Schema: Send + Sync + 'static {
    type Key: KeyCodec;
    type Value: ValueLayout;
    fn key_codec(&self) -> &Self::Key;
    fn value_layout(&self) -> &Self::Value;
}
pub type KeyOf<S> = <<S as Schema>::Key as KeyCodec>::Key;
pub type OwnedKeyOf<S> = <<S as Schema>::Key as KeyCodec>::OwnedKey;
pub type OwnedValueOf<S> = <<S as Schema>::Value as ValueLayout>::Owned;

/// 引擎内共享 schema 的值布局适配，避免要求用户布局实现 Clone。
pub(crate) struct SharedValue<S: Schema>(pub std::sync::Arc<S>);
// SAFETY: 所有许可原样交给同一个 schema 的专家布局，不改变视图寿命或并发承诺。
unsafe impl<S: Schema> ValueLayout for SharedValue<S> {
    type Owned = OwnedValueOf<S>;
    type Read<'a> = <S::Value as ValueLayout>::Read<'a>;
    type Update<'a> = <S::Value as ValueLayout>::Update<'a>;
    fn concurrent_updates(&self) -> bool {
        self.0.value_layout().concurrent_updates()
    }
    fn format_id(&self) -> crate::types::FormatId {
        self.0.value_layout().format_id()
    }
    fn plan(&self, v: &Self::Owned) -> Result<value::ValuePlan, crate::types::Error> {
        self.0.value_layout().plan(v)
    }
    fn initialize(
        &self,
        p: value::InitPermit<'_>,
        v: Self::Owned,
    ) -> Result<(), crate::types::Error> {
        self.0.value_layout().initialize(p, v)
    }
    fn read<'a>(&'a self, p: value::ReadPermit<'a>) -> Result<Self::Read<'a>, crate::types::Error> {
        self.0.value_layout().read(p)
    }
    fn update<'a>(
        &'a self,
        p: value::UpdatePermit<'a>,
    ) -> Result<Self::Update<'a>, crate::types::Error> {
        self.0.value_layout().update(p)
    }
    fn stable_encoded_len(&self, p: value::StablePermit<'_>) -> Result<usize, crate::types::Error> {
        self.0.value_layout().stable_encoded_len(p)
    }
    fn encode_stable(
        &self,
        p: value::StablePermit<'_>,
        out: &mut [u8],
    ) -> Result<(), crate::types::Error> {
        self.0.value_layout().encode_stable(p, out)
    }
    fn decode_initialize(
        &self,
        b: &[u8],
        p: value::InitPermit<'_>,
    ) -> Result<(), crate::types::Error> {
        self.0.value_layout().decode_initialize(b, p)
    }
    fn drop_value(&self, p: value::DropPermit<'_>) -> Result<(), crate::types::Error> {
        self.0.value_layout().drop_value(p)
    }
}
