//! RasterKV Embedded key-value store:session,mixed log,Checkpoint and recovery.
//!
//! Safe operation through this thread Session commit,Storage instances can be shared across threads.Ready/Pending
//! Indicates how results are delivered,Successful request completion does not equal checkpoint persistence completion.Linux/macOS
//! Provides a basic file backend;Windows,io_uring,F2 and cross-storage compression fall within the subsequent scope.
//!
//! The following example completes the creation of an atomic counter,RMW,Result collection and closure.Null The device is suitable for this restricted memory example,
//! No disk spill or reboot recovery provided;Persistence process usage ThreadPoolDeviceFactory.
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
//! Accepted requests not canceled after timeout,Continue waiting for the same ticket;Keep if operation fails
//! [`types::OperationError::effect`],Modifications that may have already taken effect cannot be automatically replayed.
//! The complete disk and heterogeneous bill process can be found in the warehouse `disk_lifecycle` and `interface` Example;
//! The example will actually run and verify the results,The current overall acceptance progress of the first phase is subject to the project plan map.

pub mod api;
pub mod config;
pub mod device;
pub mod diagnostics;
pub mod schema;
pub mod types;

// Internal agreements not to be made public API;The test-specific entrance limits the compilation scope at the definition.
mod cache;
mod checkpoint;
mod coordination;
mod engine;
mod epoch;
mod format;
mod index;
mod log;
mod maintenance;
mod scan;
mod storage;
mod sync;

pub use api::{Builder, RasterKV, Session, Submission, Ticket};
