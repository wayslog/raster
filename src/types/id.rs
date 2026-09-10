//! 不同地址和身份使用不同类型；此处不生成随机 ID 或冻结磁盘编码。

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
