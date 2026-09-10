//! 内建策略的组合入口；哈希和活跃布局尚未选定，不伪造可用 Schema。
use super::{KeyCodec, Schema, ValueLayout};
use std::marker::PhantomData;

pub struct SchemaPair<K: KeyCodec, V: ValueLayout> {
    key: K,
    value: V,
}
impl<K: KeyCodec, V: ValueLayout> SchemaPair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}
impl<K: KeyCodec, V: ValueLayout> Schema for SchemaPair<K, V> {
    type Key = K;
    type Value = V;
    fn key_codec(&self) -> &K {
        &self.key
    }
    fn value_layout(&self) -> &V {
        &self.value
    }
}

/// 待实现字节键策略；必须选择带版本的稳定哈希后才能实现 KeyCodec。
pub struct ByteKey;
/// 待实现整数键策略；需要明确字节序和哈希。
pub struct U64Key;
/// 待实现通用字节槽；必须接入记录独占许可后才能实现 ValueLayout。
pub struct SerializedValue<C> {
    _codec: PhantomData<C>,
}
/// 待实现原子值布局；不能用普通字节转换代替原子初始化和稳定读取。
pub struct AtomicU64Value;
