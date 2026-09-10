//! 恢复集合的语义与逐材料校验；全部验证之前不能发布恢复后的实例。
use crate::{
    api::maintenance::RecoverySet,
    config::Config,
    format::{IndexSnapshot, Kind, Manifest, Material, PageFrame, match_recovery},
    schema::{KeyCodec, Schema, ValueLayout},
    types::*,
};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) enum ValidatedMaterial<'a> {
    Index(IndexSnapshot),
    Log(PageFrame<'a>),
}
pub(crate) struct RecoveryPlan {
    index: Manifest,
    log: Manifest,
    buckets: usize,
    page_bytes: usize,
    materials: BTreeMap<(CheckpointToken, u64), Material>,
    verified: BTreeSet<(CheckpointToken, u64)>,
}
impl RecoveryPlan {
    /// 清单应先通过提交读取器；此处不将合法描述等同于磁盘材料已验证。
    pub fn new<S: Schema>(
        set: &RecoverySet,
        index: Manifest,
        log: Manifest,
        schema: &S,
        config: &Config,
    ) -> Result<Self, Error> {
        config.validate()?;
        set.store.validate()?;
        set.index.validate()?;
        set.log.validate()?;
        if index.store != set.store
            || log.store != set.store
            || index.token != set.index
            || log.token != set.log
        {
            return Err(Error::InvalidFormat("恢复集合身份与清单不匹配"));
        }
        match_recovery(&index, &log)?;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            schema
                .key_codec()
                .validate_identity(index.key_format, &index.hash)?;
            if schema.value_layout().format_id() != index.value_format {
                return Err(Error::InvalidFormat("恢复值布局不匹配"));
            }
            Ok(())
        }))
        .map_err(|_| Error::InvalidState("恢复语义校验恐慌"))??;
        let maximum_index = config
            .index
            .buckets
            .checked_mul(65_536)
            .and_then(|n| n.checked_mul(24))
            .and_then(|n| n.checked_add(36))
            .ok_or(Error::CapacityExceeded)?;
        let mut materials = BTreeMap::new();
        for manifest in [&index, &log] {
            let count = manifest
                .materials
                .iter()
                .filter(|m| m.kind == Kind::Index)
                .count();
            if (manifest.kind == Kind::Log && count != 0)
                || (manifest.kind != Kind::Log && count != 1)
            {
                return Err(Error::InvalidFormat("当前索引格式要求恰好一份完整索引材料"));
            }
            for material in &manifest.materials {
                match material.kind {
                    Kind::Index => {
                        if material.bytes < 36
                            || material.bytes > maximum_index as u64
                            || !(material.bytes - 36).is_multiple_of(24)
                        {
                            return Err(Error::InvalidFormat("索引材料长度不符合配置容量"));
                        }
                    }
                    Kind::Log => {
                        let page = material.begin.page_offset(config.log.page_bytes as u64)?.0;
                        let start =
                            LogAddress::from_page_offset(page, 0, config.log.page_bytes as u64)?;
                        if material.begin != start.max(manifest.begin)
                            || material.end != start.checked_add(config.log.page_bytes as u64)?
                            || material.bytes
                                != PageFrame::encoded_size(config.log.page_bytes)? as u64
                        {
                            return Err(Error::InvalidFormat("日志材料页布局与恢复配置不匹配"));
                        }
                    }
                    Kind::Full => return Err(Error::InvalidFormat("材料不能使用完整检查点类型")),
                }
                let key = (manifest.token, material.id);
                if let Some(previous) = materials.insert(key, material.clone())
                    && previous != *material
                {
                    return Err(Error::InvalidFormat("同一材料存在冲突描述"));
                }
            }
        }
        Ok(Self {
            index,
            log,
            buckets: config.index.buckets,
            page_bytes: config.log.page_bytes,
            materials,
            verified: BTreeSet::new(),
        })
    }
    pub fn requests(&self) -> impl Iterator<Item = (CheckpointToken, &Material)> {
        self.materials
            .iter()
            .map(|((token, _), material)| (*token, material))
    }
    pub fn remaining(&self) -> usize {
        self.materials.len() - self.verified.len()
    }
    pub fn index(&self) -> &Manifest {
        &self.index
    }
    pub fn log(&self) -> &Manifest {
        &self.log
    }
    /// 错误和重复材料不推进完成计数；地址有效性仍不代替完整链及重放校验。
    pub fn verify_material<'a, S: Schema>(
        &mut self,
        token: CheckpointToken,
        id: u64,
        bytes: &'a [u8],
        schema: &S,
    ) -> Result<ValidatedMaterial<'a>, Error> {
        let key = (token, id);
        if self.verified.contains(&key) {
            return Err(Error::InvalidState("恢复材料已验证"));
        }
        let material = self
            .materials
            .get(&key)
            .ok_or(Error::InvalidFormat("材料不属于恢复集合"))?;
        material.verify(bytes)?;
        let parsed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            schema
                .key_codec()
                .validate_identity(self.index.key_format, &self.index.hash)?;
            if schema.value_layout().format_id() != self.index.value_format {
                return Err(Error::InvalidFormat("恢复值布局发生变化"));
            }
            Ok(match material.kind {
                Kind::Index => {
                    let image = IndexSnapshot::decode(bytes)?;
                    if image.buckets != self.buckets as u64
                        || image.entries.iter().any(|entry| {
                            entry.address < material.begin || entry.address >= material.end
                        })
                    {
                        return Err(Error::InvalidFormat("恢复索引桶数或链头范围不匹配"));
                    }
                    ValidatedMaterial::Index(image)
                }
                Kind::Log => {
                    let frame = PageFrame::decode(
                        bytes,
                        material.begin.page_offset(self.page_bytes as u64)?.0,
                        self.page_bytes,
                    )?;
                    for (_, record) in frame.records()? {
                        if !record.header.invalid {
                            crate::schema::key::decode_canonical(schema.key_codec(), record.key)?;
                        }
                    }
                    ValidatedMaterial::Log(frame)
                }
                Kind::Full => return Err(Error::InvalidFormat("非法恢复材料类型")),
            })
        }))
        .map_err(|_| Error::InvalidState("恢复材料语义校验恐慌"))??;
        self.verified.insert(key);
        Ok(parsed)
    }
}
