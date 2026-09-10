//! 内部并发原语入口；未来只在模型测试配置下替换为 Loom，而非混用两套原语。
pub(crate) use std::sync::Mutex;
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};

/// 首版发布按顺序一致性设计；后续弱化必须经过模型验证。
pub(crate) const PUBLISH_ORDER: Ordering = Ordering::SeqCst;
