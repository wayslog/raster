//! 不同地址和身份使用不同类型；逻辑有效性不授予内存访问许可。
use super::Error;

macro_rules! identifier {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub [u8; 16]);
    };
}
identifier!(StoreId, "存储身份。");
identifier!(SessionId, "可恢复会话身份。");
identifier!(CheckpointToken, "检查点身份；有 token 不代表已经持久化。");
identifier!(FormatId, "格式或布局语义标识。");

macro_rules! number {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u64);
    };
}
number!(LogAddress, "逻辑日志地址，不能作为内存指针使用。");
number!(CacheAddress, "读缓存地址，不得持久化到主索引。");
number!(PageId, "逻辑页号。");
number!(Generation, "页、请求槽或表的复用代次。");
number!(Serial, "应用提供的会话操作序号。");
number!(CheckpointVersion, "检查点版本，与回收 epoch 不同。");
number!(EpochVersion, "访问安全回收版本。");
number!(KeyHash, "稳定键哈希值。");
number!(IoId, "设备范围内的在途请求标识。");
number!(MaintenanceId, "存储范围内的维护任务标识。");

/// 票据身份包含会话、存储和槽代次，防止跨会话或迟到响应误投。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestId {
    pub store: StoreId,
    pub session: SessionId,
    pub slot: u64,
    pub generation: Generation,
}

/// 哈希算法及其种子与键编码版本分开记录。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashDescriptor {
    pub algorithm: FormatId,
    pub seed: Vec<u8>,
}

macro_rules! generated_identifier {
    ($name:ident) => {
        impl $name {
            /// 从系统随机源生成 128 位身份；失败直接返回，不回退到时间或进程号。
            /// 唯一性是概率保证，恢复与注册仍须检测重复身份。
            pub fn generate() -> Result<Self, Error> {
                let value = Self(random_identity()?);
                value.validate()?;
                Ok(value)
            }

            /// 全零身份保留为无效值；已有持久身份应复用原始字节。
            pub fn validate(self) -> Result<(), Error> {
                if self.0 == [0; 16] {
                    Err(Error::InvalidFormat("身份不能全零"))
                } else {
                    Ok(())
                }
            }
        }
    };
}
generated_identifier!(StoreId);
generated_identifier!(SessionId);
generated_identifier!(CheckpointToken);

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn random_identity() -> Result<[u8; 16], Error> {
    read_identity(std::fs::File::open("/dev/urandom")?)
}

#[cfg(any(target_os = "linux", target_os = "macos", test))]
fn read_identity(mut source: impl std::io::Read) -> Result<[u8; 16], Error> {
    let mut bytes = [0; 16];
    source.read_exact(&mut bytes)?;
    Ok(bytes)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn random_identity() -> Result<[u8; 16], Error> {
    Err(Error::InvalidState("身份生成仅支持 Linux 与 macOS"))
}

macro_rules! address {
    ($name:ident) => {
        impl $name {
            /// 无效哨兵；零是有效的逻辑偏移。记录存在性由日志层检查。
            pub const INVALID: Self = Self(u64::MAX);

            pub fn validate(self) -> Result<(), Error> {
                if self == Self::INVALID {
                    Err(Error::InvalidFormat("无效逻辑地址"))
                } else {
                    Ok(())
                }
            }

            /// 地址推进不允许回绕或进入无效哨兵。
            pub fn checked_add(self, bytes: u64) -> Result<Self, Error> {
                self.validate()?;
                let next = Self(self.0.checked_add(bytes).ok_or(Error::CapacityExceeded)?);
                if next == Self::INVALID {
                    return Err(Error::CapacityExceeded);
                }
                Ok(next)
            }

            /// 页尺寸必须是非零二次幂；不隐含当前页已分配或仍被保护。
            pub fn page_offset(self, page_bytes: u64) -> Result<(PageId, u64), Error> {
                self.validate()?;
                validate_page_bytes(page_bytes)?;
                Ok((PageId(self.0 / page_bytes), self.0 % page_bytes))
            }

            pub fn from_page_offset(
                page: PageId,
                offset: u64,
                page_bytes: u64,
            ) -> Result<Self, Error> {
                validate_page_bytes(page_bytes)?;
                if offset >= page_bytes {
                    return Err(Error::InvalidFormat("页内偏移越界"));
                }
                let base = page
                    .0
                    .checked_mul(page_bytes)
                    .ok_or(Error::CapacityExceeded)?;
                Self(base).checked_add(offset)
            }
        }
    };
}
address!(LogAddress);
address!(CacheAddress);

fn validate_page_bytes(bytes: u64) -> Result<(), Error> {
    if !bytes.is_power_of_two() {
        return Err(Error::InvalidConfig {
            field: "page_bytes",
            reason: "页尺寸必须是非零二次幂",
        });
    }
    Ok(())
}

impl KeyHash {
    /// 高 16 位仅用于候选筛选；即使完整哈希相等也必须比较编码键。
    pub const fn tag(self) -> u16 {
        (self.0 >> 48) as u16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 随机源读取不完整或报错时不返回身份() {
        assert!(matches!(read_identity(&[1_u8; 15][..]), Err(Error::Io(_))));
        struct Failed;
        impl std::io::Read for Failed {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("测试随机源失败"))
            }
        }
        assert!(matches!(read_identity(Failed), Err(Error::Io(_))));
        assert_eq!(read_identity(&[42_u8; 16][..]).unwrap(), [42; 16]);
    }
}
