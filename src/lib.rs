//! RasterKV 模块骨架：公开类型可组合，存储算法尚未实现。
//!
//! 创建、恢复和业务执行不会返回伪造成功。详见模块骨架说明。

pub mod api;
pub mod config;
pub mod device;
pub mod diagnostics;
pub mod schema;
pub mod types;

// 内部协议尚未接入运行路径；每个模块实现时移除对应的临时抑制。
#[allow(dead_code)]
mod cache;
#[allow(dead_code)]
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
