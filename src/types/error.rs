//! 保留错误分类、原因与写入影响；拒绝和接受后的失败分开表达。

use std::fmt;

#[derive(Debug)]
pub enum Error {
    /// 压缩失败保留已发布的迁移数；until 是请求的截止地址，不表示已完成扫描范围。
    CompactionFailed {
        until: super::LogAddress,
        copied: u64,
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
            Self::CompactionFailed {
                until,
                copied,
                cause,
            } => write!(
                f,
                "压缩失败：目标地址 {}，已迁移 {copied} 条，原因：{cause}",
                until.0
            ),
            Self::NotImplemented { module } => write!(f, "模块尚未实现：{module}"),
            Self::InvalidConfig { field, reason } => write!(f, "配置无效：{field}，{reason}"),
            Self::InvalidState(reason) => write!(f, "状态无效：{reason}"),
            Self::InvalidFormat(reason) => write!(f, "格式无效：{reason}"),
            Self::Codec(reason) => write!(f, "编码失败：{reason}"),
            Self::CapacityExceeded => f.write_str("容量预算不足"),
            Self::OutOfMemory => f.write_str("内存分配失败"),
            Self::DeadlineExceeded => f.write_str("等待已超过截止时间"),
            Self::Busy => f.write_str("存在尚未完成的互斥动作"),
            Self::UnsupportedDurability => f.write_str("设备不支持要求的持久化语义"),
            Self::RangeTruncated => f.write_str("扫描范围已被截断"),
            Self::SessionAbandoned => f.write_str("会话未正常关闭，写入影响可能未知"),
            Self::Io(source) => write!(f, "设备错误：{source}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            Self::CompactionFailed { cause, .. } => Some(&**cause),
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

#[derive(Debug)]
pub struct OperationError {
    pub cause: Error,
    pub effect: Effect,
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
