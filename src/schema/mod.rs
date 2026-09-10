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
