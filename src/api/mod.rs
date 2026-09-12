//! application-oriented interface,Hidden index,Private state for log pages and global stages.
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
