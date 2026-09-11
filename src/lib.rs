//! RasterKV 嵌入式键值存储：会话、混合日志、检查点和恢复。
//!
//! 安全操作通过本线程 Session 提交，存储实例可以跨线程共享。Ready/Pending
//! 表示结果交付方式，成功的请求完成不等于检查点持久化完成。Linux/macOS
//! 提供基础文件后端；Windows、io_uring、F2 和跨存储压缩属于后续范围。
//!
//! 下例完成原子计数器的创建、RMW、结果收取和关闭。Null 设备适用于此受限内存示例，
//! 不提供磁盘溢出或重启恢复；持久化流程使用 ThreadPoolDeviceFactory。
//!
//! ```
//! use raster::{RasterKV, Submission, config::Config, api::{Outcome, operation::*},
//!     schema::{ValueRead, ValueUpdate, builtin::{SchemaPair, U64Key, AtomicU64Value}},
//!     types::{Deadline, Error, Serial}};
//! use std::{sync::atomic::Ordering, time::{Duration, Instant}};
//! type Schema = SchemaPair<U64Key, AtomicU64Value>;
//! struct Increment;
//! impl Keyed<Schema> for Increment { fn key(&self) -> &u64 { &1 } }
//! impl RmwOperation<Schema> for Increment {
//!     type Output = u64;
//!     fn initial(&mut self) -> Result<(u64, u64), Error> { Ok((1, 1)) }
//!     fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
//!         let next = value.view().wrapping_add(1);
//!         Ok((next, next))
//!     }
//!     fn update_in_place(&mut self, mut value: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<u64>, Error> {
//!         let next = value.view_mut().fetch_add(1, Ordering::SeqCst).wrapping_add(1);
//!         Ok(UpdateDecision::Updated(next))
//!     }
//! }
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut config = Config::default();
//! config.log.page_bytes = 4096;
//! let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
//!     .config(config).device(Box::new(raster::device::null::NullDeviceFactory)).create()?;
//! let mut session = store.start_session(Default::default())?;
//! let end = Deadline(Instant::now() + Duration::from_secs(10));
//! let submitted = session.rmw(Serial(1), Increment, RmwOptions::default()).map_err(|rejected| rejected.reason)?;
//! let result = match submitted {
//!     Submission::Ready(result) => result?,
//!     Submission::Pending(mut ticket) => session.wait(&mut ticket, end)??,
//! };
//! assert!(matches!(result, Outcome::Success(1)));
//! session.close(end)?;
//! store.shutdown(end)?;
//! # Ok(())
//! # }
//! ```
//!
//! 超时不取消已接受的请求，继续等待同一票据；操作失败保留
//! [`types::OperationError::effect`]，不能自动重放可能已经生效的修改。
//! 完整的磁盘与异构票据流程见仓库的 `disk_lifecycle` 和 `interface` 示例；
//! 示例会实际运行并验证结果，当前第一期总验收进度以项目计划地图为准。

pub mod api;
pub mod config;
pub mod device;
pub mod diagnostics;
pub mod schema;
pub mod types;

// 内部协议不作为公开 API；遗留 dead_code 抑制在 P9.2 的入口审查中统一核对。
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
mod index;
#[allow(dead_code)]
mod log;
#[allow(dead_code)]
mod maintenance;
mod scan;
#[allow(dead_code)]
mod storage;
#[allow(dead_code)]
mod sync;

pub use api::{Builder, RasterKV, Session, Submission, Ticket};
