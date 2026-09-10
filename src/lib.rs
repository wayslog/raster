//! RasterKV 嵌入式键值存储：会话、混合日志、检查点和恢复。
//!
//! 尚未接入的维护能力显式返回错误。实现进度与交付证据见项目文档。

pub mod api;
pub mod config;
pub mod device;
pub mod diagnostics;
pub mod schema;
pub mod types;

// 内部协议尚未接入运行路径；每个模块实现时移除对应的临时抑制。
#[allow(dead_code)]
mod cache;
mod checkpoint;
#[allow(dead_code)]
mod coordination;
#[allow(dead_code)]
mod engine;
#[allow(dead_code)]
mod epoch;
#[allow(dead_code)]
mod format;
#[allow(dead_code)]
mod index;
#[allow(dead_code)]
mod log;
#[allow(dead_code)]
mod maintenance;
#[allow(dead_code)]
mod scan;
#[allow(dead_code)]
mod storage;
#[allow(dead_code)]
mod sync;

pub use api::{Builder, RasterKV, Session, Submission, Ticket};
