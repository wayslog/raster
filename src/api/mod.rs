//! 面向应用的接口，隐藏索引、日志页和全局阶段的私有状态。
pub mod completion;
pub mod maintenance;
pub mod operation;
pub mod scan;
pub mod session;
pub mod store;

pub use completion::{OperationResult, Outcome, Submission, Ticket, TicketState};
pub use session::Session;
pub use store::{Builder, RasterKV};

pub(crate) mod recover;
