//! 跨模块共享的身份、地址、时间预算与错误；不依赖引擎。

mod error;
mod id;
mod progress;

pub use error::*;
pub use id::*;
pub use progress::*;
