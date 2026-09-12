//! Keep error classification,Causes and effects of writing;Express rejection and failure after acceptance separately.

use std::fmt;

/// Checkpoint failure and material deletion are confirmed separately;Unknown results cannot be assumed to be recoverable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointRetirement {
    /// No invalidation operation was issued this time;Not proven to be unknown or previously invalid token Recoverable.
    NotAttempted,
    PossiblyRetired,
    Retired,
}

#[derive(Debug)]
pub enum Error {
    /// Do not retain or echo TOML Original text and file path;offset Yes UTF-8 Byte position.
    ConfigDocument {
        field: &'static str,
        offset: Option<usize>,
        reason: &'static str,
    },
    CheckpointReleaseFailed {
        token: super::CheckpointToken,
        retirement: CheckpointRetirement,
        confirmed_absent_materials: u64,
        cause: Box<Error>,
    },
    /// Recycling failure retains the logical boundaries that have taken effect and the number of confirmed physical deletions.
    GcFailed {
        begin: super::LogAddress,
        index_cleaned: bool,
        deleted_segments: u64,
        cause: Box<Error>,
    },
    /// Compaction failure retains number of published migrations;until is the end address of the request,Does not indicate that the scan range has been completed.
    CompactionFailed {
        until: super::LogAddress,
        copied: u64,
        /// Next steps completed;GC Some of the effects of the failure are still represented by cause in GcFailed given.
        checkpoint: Option<Box<crate::api::maintenance::CheckpointReport>>,
        gc: Option<Box<crate::api::maintenance::GcReport>>,
        cause: Box<Error>,
    },
    NotImplemented {
        module: &'static str,
    },
    InvalidConfig {
        field: &'static str,
        reason: &'static str,
    },
    InvalidState(&'static str),
    InvalidFormat(&'static str),
    Codec(&'static str),
    CapacityExceeded,
    OutOfMemory,
    DeadlineExceeded,
    Busy,
    UnsupportedDurability,
    RangeTruncated,
    SessionAbandoned,
    Io(std::io::Error),
}

impl Error {
    pub const fn unimplemented(module: &'static str) -> Self {
        Self::NotImplemented { module }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigDocument {
                field,
                offset,
                reason,
            } => write!(
                f,
                "Configuration document {field} Error(Byte position {offset:?}):{reason}"
            ),
            Self::CheckpointReleaseFailed {
                token,
                retirement,
                confirmed_absent_materials,
                cause,
            } => write!(
                f,
                "Checkpoint release failed:token {:x?},Failure state {retirement:?},Confirmed not to exist {confirmed_absent_materials} materials,Reason:{cause}",
                token.0
            ),
            Self::GcFailed {
                begin,
                index_cleaned,
                deleted_segments,
                cause,
            } => write!(
                f,
                "Recycling failed:begin {},Index cleaning {index_cleaned},Deletion confirmed {deleted_segments} segment,Reason:{cause}",
                begin.0
            ),
            Self::CompactionFailed {
                until,
                copied,
                checkpoint,
                gc,
                cause,
            } => write!(
                f,
                "Compression failed:destination address {},Migrated {copied} Article,Checkpoint completed {},Completed GC {},Reason:{cause}",
                until.0,
                checkpoint.is_some(),
                gc.is_some()
            ),
            Self::NotImplemented { module } => write!(f, "Module not implemented yet:{module}"),
            Self::InvalidConfig { field, reason } => {
                write!(f, "Invalid configuration:{field},{reason}")
            }
            Self::InvalidState(reason) => write!(f, "Invalid status:{reason}"),
            Self::InvalidFormat(reason) => write!(f, "Invalid format:{reason}"),
            Self::Codec(reason) => write!(f, "Encoding failed:{reason}"),
            Self::CapacityExceeded => f.write_str("Insufficient capacity budget"),
            Self::OutOfMemory => f.write_str("Memory allocation failed"),
            Self::DeadlineExceeded => f.write_str("Waiting time exceeded"),
            Self::Busy => {
                f.write_str("There are mutually exclusive actions that have not yet been completed")
            }
            Self::UnsupportedDurability => {
                f.write_str("The device does not support the required persistence semantics")
            }
            Self::RangeTruncated => f.write_str("Scan range has been truncated"),
            Self::SessionAbandoned => {
                f.write_str("Session was not closed gracefully,Write impact may be unknown")
            }
            Self::Io(source) => write!(f, "Device error:{source}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            Self::CompactionFailed { cause, .. }
            | Self::GcFailed { cause, .. }
            | Self::CheckpointReleaseFailed { cause, .. } => Some(&**cause),
            _ => None,
        }
    }
}
impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    NotApplied,
    Applied,
    Unknown,
}

/// Operation failed after acceptance,Both the error chain and formatted output retain the cause and effective range..
///
/// ```
/// use raster::types::{Effect, Error, OperationError};
/// let failure = OperationError { cause: Error::Codec("Output encoding failed"), effect: Effect::Applied };
/// assert!(failure.to_string().contains("Already effective"));
/// assert_eq!(std::error::Error::source(&failure).unwrap().to_string(), failure.cause.to_string());
/// // Modifications that have taken effect or have unknown impact cannot be automatically replayed due to failed output..
/// ```
#[derive(Debug)]
pub struct OperationError {
    pub cause: Error,
    pub effect: Effect,
}
impl fmt::Display for OperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let effect = match self.effect {
            Effect::NotApplied => "Not effective",
            Effect::Applied => "Already effective",
            Effect::Unknown => "Impact unknown",
        };
        write!(f, "Operation failed({effect}):{}", self.cause)
    }
}
impl std::error::Error for OperationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

#[derive(Debug)]
pub struct Rejected<R> {
    pub request: R,
    pub reason: Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TicketError {
    WrongSession,
    AlreadyTaken,
    AlreadyCompleted,
    BorrowConflict,
}
