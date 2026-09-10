//! 内部并发原语入口；epoch 单锁转换通过实际接口穷举线性化交错。
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};
pub(crate) use std::sync::{Mutex, MutexGuard};

/// 首版发布按顺序一致性设计；后续弱化必须经过模型验证。
pub(crate) const PUBLISH_ORDER: Ordering = Ordering::SeqCst;
