//! 有界检查点描述与提交证据；验证字节不等于已经执行持久化同步。
use super::wire::{Reader, checksum, invalid};
use crate::types::*;
use std::collections::BTreeSet;

pub(crate) const MAX_ITEMS: usize = 65_536;
const MAX_SEED: usize = 1024;
const MAX_MANIFEST: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Full,
    Index,
    Log,
}
impl Kind {
    fn code(self) -> u16 {
        match self {
            Self::Full => 1,
            Self::Index => 2,
            Self::Log => 3,
        }
    }
    fn decode(code: u16) -> Result<Self, Error> {
        match code {
            1 => Ok(Self::Full),
            2 => Ok(Self::Index),
            3 => Ok(Self::Log),
            _ => Err(invalid()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Material {
    /// 数字身份映射至根目录内的固定文件名，不接受磁盘提供的路径。
    pub id: u64,
    pub generation: Generation,
    pub kind: Kind,
    pub begin: LogAddress,
    pub end: LogAddress,
    pub bytes: u64,
    pub checksum: u32,
}
impl Material {
    pub fn verify(&self, bytes: &[u8]) -> Result<(), Error> {
        if self.bytes != bytes.len() as u64 || self.checksum != checksum(bytes) {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub store: StoreId,
    pub token: CheckpointToken,
    pub kind: Kind,
    /// 日志检查点显式绑定所选索引；完整和索引检查点绑定自己。
    pub base_index: CheckpointToken,
    pub key_format: FormatId,
    pub value_format: FormatId,
    pub hash: HashDescriptor,
    pub version: CheckpointVersion,
    pub begin: LogAddress,
    pub end: LogAddress,
    /// 索引模糊区间重放起点。日志检查点保留其绑定索引的起点。
    pub replay_from: LogAddress,
    pub session_progress: Vec<(SessionId, Serial)>,
    pub materials: Vec<Material>,
}
impl Manifest {
    pub fn validate(&self) -> Result<(), Error> {
        self.store.validate()?;
        self.token.validate()?;
        self.base_index.validate()?;
        self.begin.validate()?;
        self.end.validate()?;
        self.replay_from.validate()?;
        if self.begin > self.replay_from
            || self.replay_from > self.end
            || self.hash.seed.len() > MAX_SEED
            || self.session_progress.len() > MAX_ITEMS
            || self.materials.len() > MAX_ITEMS
        {
            return Err(invalid());
        }
        if self.kind != Kind::Log && self.base_index != self.token {
            return Err(invalid());
        }
        if self.kind == Kind::Index && !self.session_progress.is_empty() {
            return Err(invalid());
        }
        let mut sessions = BTreeSet::new();
        for (session, _) in &self.session_progress {
            session.validate()?;
            if !sessions.insert(*session) {
                return Err(invalid());
            }
        }
        let mut ids = BTreeSet::new();
        let mut log_end = self.begin;
        let mut index_count = 0;
        for material in &self.materials {
            material.begin.validate()?;
            material.end.validate()?;
            if !ids.insert(material.id) || material.begin > material.end || material.bytes == 0 {
                return Err(invalid());
            }
            match material.kind {
                Kind::Full => return Err(invalid()),
                Kind::Index => {
                    index_count += 1;
                    if self.kind == Kind::Log
                        || material.begin != self.begin
                        || material.end != self.end
                    {
                        return Err(invalid());
                    }
                }
                Kind::Log => {
                    if self.kind == Kind::Index
                        || material.begin != log_end
                        || material.end <= material.begin
                    {
                        return Err(invalid());
                    }
                    log_end = material.end;
                }
            }
        }
        if (self.kind != Kind::Log && index_count == 0)
            || (self.kind != Kind::Index && log_end != self.end)
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut b = Vec::new();
        b.try_reserve_exact(
            184 + self.hash.seed.len()
                + self.session_progress.len() * 24
                + self.materials.len() * 48,
        )
        .map_err(|_| Error::OutOfMemory)?;
        b.extend_from_slice(b"RMAN");
        put16(&mut b, 1);
        put16(&mut b, self.kind.code());
        for id in [
            self.store.0,
            self.token.0,
            self.base_index.0,
            self.key_format.0,
            self.value_format.0,
            self.hash.algorithm.0,
        ] {
            b.extend_from_slice(&id);
        }
        for n in [self.version.0, self.begin.0, self.end.0, self.replay_from.0] {
            put64(&mut b, n);
        }
        put32(&mut b, self.hash.seed.len() as u32);
        put32(&mut b, self.session_progress.len() as u32);
        put32(&mut b, self.materials.len() as u32);
        b.extend_from_slice(&self.hash.seed);
        for (session, serial) in &self.session_progress {
            b.extend_from_slice(&session.0);
            put64(&mut b, serial.0);
        }
        for m in &self.materials {
            put64(&mut b, m.id);
            put64(&mut b, m.generation.0);
            put16(&mut b, m.kind.code());
            put16(&mut b, 0);
            for n in [m.begin.0, m.end.0, m.bytes] {
                put64(&mut b, n);
            }
            put32(&mut b, m.checksum);
        }
        let crc = checksum(&b);
        put32(&mut b, crc);
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let payload = checked_payload(bytes, MAX_MANIFEST)?;
        let mut r = Reader::new(payload);
        if r.take(4)? != b"RMAN" || r.u16()? != 1 {
            return Err(invalid());
        }
        let kind = Kind::decode(r.u16()?)?;
        let store = StoreId(id(&mut r)?);
        let token = CheckpointToken(id(&mut r)?);
        let base_index = CheckpointToken(id(&mut r)?);
        let key_format = FormatId(id(&mut r)?);
        let value_format = FormatId(id(&mut r)?);
        let algorithm = FormatId(id(&mut r)?);
        let version = CheckpointVersion(r.u64()?);
        let begin = LogAddress(r.u64()?);
        let end = LogAddress(r.u64()?);
        let replay_from = LogAddress(r.u64()?);
        let seed_len = r.u32()? as usize;
        let sessions = r.u32()? as usize;
        let materials = r.u32()? as usize;
        if seed_len > MAX_SEED || sessions > MAX_ITEMS || materials > MAX_ITEMS {
            return Err(invalid());
        }
        // 在按声明分配前验证完整剩余长度，防止短输入导致大分配。
        let expected = 148usize
            .checked_add(seed_len)
            .and_then(|n| n.checked_add(sessions * 24))
            .and_then(|n| n.checked_add(materials * 48))
            .ok_or_else(invalid)?;
        if payload.len() != expected {
            return Err(invalid());
        }
        let hash = HashDescriptor {
            algorithm,
            seed: r.take(seed_len)?.to_vec(),
        };
        let mut session_progress = Vec::new();
        session_progress
            .try_reserve_exact(sessions)
            .map_err(|_| Error::OutOfMemory)?;
        for _ in 0..sessions {
            session_progress.push((SessionId(id(&mut r)?), Serial(r.u64()?)));
        }
        let mut list = Vec::new();
        list.try_reserve_exact(materials)
            .map_err(|_| Error::OutOfMemory)?;
        for _ in 0..materials {
            let id = r.u64()?;
            let generation = Generation(r.u64()?);
            let kind = Kind::decode(r.u16()?)?;
            if r.u16()? != 0 {
                return Err(invalid());
            }
            list.push(Material {
                id,
                generation,
                kind,
                begin: LogAddress(r.u64()?),
                end: LogAddress(r.u64()?),
                bytes: r.u64()?,
                checksum: r.u32()?,
            });
        }
        r.finish()?;
        let result = Self {
            store,
            token,
            base_index,
            kind,
            key_format,
            value_format,
            hash,
            version,
            begin,
            end,
            replay_from,
            session_progress,
            materials: list,
        };
        result.validate()?;
        Ok(result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Commit {
    pub store: StoreId,
    pub token: CheckpointToken,
    pub manifest_bytes: u64,
    pub manifest_checksum: u32,
}
impl Commit {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.store.validate()?;
        self.token.validate()?;
        if self.manifest_bytes < 152 || self.manifest_bytes > MAX_MANIFEST as u64 {
            return Err(invalid());
        }
        let mut b = Vec::with_capacity(56);
        b.extend_from_slice(b"RCMT");
        put16(&mut b, 1);
        put16(&mut b, 0);
        b.extend_from_slice(&self.store.0);
        b.extend_from_slice(&self.token.0);
        put64(&mut b, self.manifest_bytes);
        put32(&mut b, self.manifest_checksum);
        let crc = checksum(&b);
        put32(&mut b, crc);
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != 56 {
            return Err(invalid());
        }
        let mut r = Reader::new(checked_payload(bytes, 56)?);
        if r.take(4)? != b"RCMT" || r.u16()? != 1 || r.u16()? != 0 {
            return Err(invalid());
        }
        let result = Self {
            store: StoreId(id(&mut r)?),
            token: CheckpointToken(id(&mut r)?),
            manifest_bytes: r.u64()?,
            manifest_checksum: r.u32()?,
        };
        r.finish()?;
        result.encode()?;
        Ok(result)
    }
    pub fn verify(&self, bytes: &[u8]) -> Result<Manifest, Error> {
        if bytes.len() as u64 != self.manifest_bytes || checksum(bytes) != self.manifest_checksum {
            return Err(invalid());
        }
        let manifest = Manifest::decode(bytes)?;
        if manifest.store != self.store || manifest.token != self.token {
            return Err(invalid());
        }
        Ok(manifest)
    }
}

/// 两份描述必须已分别通过提交标识及全部材料校验；本函数只负责集合语义。
pub(crate) fn match_recovery(index: &Manifest, log: &Manifest) -> Result<(), Error> {
    index.validate()?;
    log.validate()?;
    if index.kind == Kind::Log
        || log.kind == Kind::Index
        || log.base_index != index.token
        || index.store != log.store
        || index.key_format != log.key_format
        || index.value_format != log.value_format
        || index.hash != log.hash
        || index.version > log.version
        || log.begin > index.begin
        || index.end > log.end
        || log.replay_from != index.replay_from
    {
        return Err(invalid());
    }
    if index.token == log.token && index != log {
        return Err(invalid());
    }
    Ok(())
}
fn id(r: &mut Reader<'_>) -> Result<[u8; 16], Error> {
    r.take(16)?.try_into().map_err(|_| invalid())
}
fn put16(b: &mut Vec<u8>, n: u16) {
    b.extend_from_slice(&n.to_le_bytes());
}
fn put32(b: &mut Vec<u8>, n: u32) {
    b.extend_from_slice(&n.to_le_bytes());
}
fn put64(b: &mut Vec<u8>, n: u64) {
    b.extend_from_slice(&n.to_le_bytes());
}
fn checked_payload(bytes: &[u8], max: usize) -> Result<&[u8], Error> {
    if bytes.len() < 4 || bytes.len() > max {
        return Err(invalid());
    }
    let (data, tail) = bytes.split_at(bytes.len() - 4);
    if Reader::new(tail).u32()? != checksum(data) {
        return Err(invalid());
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bytes(text: &str) -> Vec<u8> {
        let text = text.trim();
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }
    fn fixture() -> Vec<u8> {
        bytes(include_str!("../../tests/fixtures/p1-manifest.hex"))
    }
    fn manifest() -> Manifest {
        Manifest::decode(&fixture()).unwrap()
    }
    fn repair(b: &mut [u8]) {
        let n = b.len() - 4;
        let crc = checksum(&b[..n]);
        b[n..].copy_from_slice(&crc.to_le_bytes());
    }
    #[test]
    fn 固定清单与提交样例逐字节往返() {
        let b = fixture();
        let m = manifest();
        assert_eq!(m.store, StoreId([1; 16]));
        assert_eq!(m.token, CheckpointToken([2; 16]));
        assert_eq!(m.session_progress, vec![(SessionId([6; 16]), Serial(99))]);
        assert_eq!(m.hash.seed, vec![9, 8]);
        assert_eq!(m.encode().unwrap(), b);
        m.materials[0].verify(b"index").unwrap();
        m.materials[1].verify(b"data").unwrap();
        assert!(m.materials[0].verify(b"other").is_err());
        let c = bytes(include_str!("../../tests/fixtures/p1-commit.hex"));
        let commit = Commit::decode(&c).unwrap();
        assert_eq!(commit.encode().unwrap(), c);
        assert_eq!(commit.verify(&b).unwrap(), m);
        match_recovery(&m, &m).unwrap();
    }
    #[test]
    fn 清单和提交拒绝所有截断及逐字节损坏() {
        for b in [
            fixture(),
            bytes(include_str!("../../tests/fixtures/p1-commit.hex")),
        ] {
            let decode = |data: &[u8]| {
                if b.len() == 56 {
                    Commit::decode(data).map(|_| ())
                } else {
                    Manifest::decode(data).map(|_| ())
                }
            };
            for len in 0..b.len() {
                assert!(decode(&b[..len]).is_err());
            }
            for i in 0..b.len() {
                let mut bad = b.clone();
                bad[i] ^= 1;
                assert!(decode(&bad).is_err());
            }
            let mut bad = b.clone();
            bad.push(0);
            assert!(decode(&bad).is_err());
        }
    }
    #[test]
    fn 重算校验后未知版本计数保留位和身份仍拒绝() {
        for (offset, data) in [
            (4, vec![2, 0]),
            (6, vec![4, 0]),
            (8, vec![0; 16]),
            (136, u32::MAX.to_le_bytes().to_vec()),
            (140, u32::MAX.to_le_bytes().to_vec()),
            (144, u32::MAX.to_le_bytes().to_vec()),
            (192, vec![1, 0]),
        ] {
            let mut b = fixture();
            b[offset..offset + data.len()].copy_from_slice(&data);
            repair(&mut b);
            assert!(Manifest::decode(&b).is_err(), "偏移 {offset}");
        }
        let mut c = bytes(include_str!("../../tests/fixtures/p1-commit.hex"));
        c[6] = 1;
        repair(&mut c);
        assert!(Commit::decode(&c).is_err());
    }
    #[test]
    fn 重复材料会话范围缺口与索引进度均拒绝() {
        let m = manifest();
        let mut bad = m.clone();
        bad.session_progress.push(bad.session_progress[0]);
        assert!(bad.encode().is_err());
        let mut bad = m.clone();
        bad.materials[1].id = bad.materials[0].id;
        assert!(bad.encode().is_err());
        let mut bad = m.clone();
        bad.materials[1].begin = LogAddress(1);
        assert!(bad.encode().is_err());
        let mut bad = m.clone();
        bad.materials.pop();
        assert!(bad.encode().is_err());
        let mut bad = m.clone();
        bad.materials[0].kind = Kind::Full;
        assert!(bad.encode().is_err());
        let mut bad = m.clone();
        bad.kind = Kind::Index;
        bad.materials.pop();
        assert!(bad.encode().is_err());
        bad.session_progress.clear();
        assert!(bad.encode().is_ok());
        bad.end = LogAddress::INVALID;
        assert!(bad.encode().is_err());
    }
    #[test]
    fn 分离检查点匹配及错配拒绝() {
        let mut index = manifest();
        index.kind = Kind::Index;
        index.session_progress.clear();
        index.materials.pop();
        let mut log = manifest();
        log.kind = Kind::Log;
        log.token = CheckpointToken([8; 16]);
        log.version = CheckpointVersion(8);
        log.materials.remove(0);
        match_recovery(&index, &log).unwrap();
        for n in 0..8 {
            let mut bad = log.clone();
            match n {
                0 => bad.store = StoreId([9; 16]),
                1 => bad.base_index = CheckpointToken([9; 16]),
                2 => bad.key_format = FormatId([9; 16]),
                3 => bad.value_format = FormatId([9; 16]),
                4 => bad.hash.seed.push(0),
                5 => bad.version = CheckpointVersion(6),
                6 => bad.replay_from = LogAddress(1),
                _ => bad.hash.algorithm = FormatId([9; 16]),
            }
            assert!(match_recovery(&index, &bad).is_err(), "错配项 {n}");
        }
        assert!(match_recovery(&log, &index).is_err());
        let mut commit =
            Commit::decode(&bytes(include_str!("../../tests/fixtures/p1-commit.hex"))).unwrap();
        commit.token = CheckpointToken([9; 16]);
        assert!(commit.verify(&fixture()).is_err());
    }
}
