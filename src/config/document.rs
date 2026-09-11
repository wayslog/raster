//! 使用完整 TOML 解析器，字段映射严格且拥有输出；错误只保留位置和静态原因。
use super::*;
use std::{io::Read, path::Path, time::Duration};
use toml::{
    Spanned,
    de::{DeTable, DeValue},
};

pub(super) const MAX_BYTES: usize = 1024 * 1024;
fn error(field: &'static str, offset: Option<usize>, reason: &'static str) -> Error {
    Error::ConfigDocument {
        field,
        offset,
        reason,
    }
}
struct Table<'a, 'i> {
    name: &'static str,
    value: &'a DeTable<'i>,
}
impl<'a, 'i> Table<'a, 'i> {
    fn check(&self, allowed: &[&str]) -> Result<(), Error> {
        for key in self.value.keys() {
            if !allowed.contains(&key.get_ref().as_ref()) {
                return Err(error(self.name, Some(key.span().start), "存在未知字段"));
            }
        }
        Ok(())
    }
    fn value(&self, key: &str) -> Option<&Spanned<DeValue<'i>>> {
        self.value.get(key)
    }
    fn invalid(&self, v: &Spanned<DeValue<'i>>, reason: &'static str) -> Error {
        error(self.name, Some(v.span().start), reason)
    }
    fn unsigned(&self, v: &Spanned<DeValue<'i>>) -> Result<u64, Error> {
        let number = v
            .get_ref()
            .as_integer()
            .ok_or_else(|| self.invalid(v, "需要无符号整数"))?;
        u64::from_str_radix(number.as_str(), number.radix())
            .map_err(|_| self.invalid(v, "整数为负数或超出 u64 范围"))
    }
    fn u64(&self, key: &str, out: &mut u64) -> Result<(), Error> {
        if let Some(v) = self.value(key) {
            *out = self.unsigned(v)?;
        }
        Ok(())
    }
    fn usize(&self, key: &str, out: &mut usize) -> Result<(), Error> {
        if let Some(v) = self.value(key) {
            *out = usize::try_from(self.unsigned(v)?)
                .map_err(|_| self.invalid(v, "整数超出本平台 usize 范围"))?;
        }
        Ok(())
    }
    fn boolean(&self, key: &str, out: &mut bool) -> Result<(), Error> {
        if let Some(v) = self.value(key) {
            *out = v
                .get_ref()
                .as_bool()
                .ok_or_else(|| self.invalid(v, "需要布尔值"))?;
        }
        Ok(())
    }
    fn fraction(&self, key: &str, out: &mut f64) -> Result<(), Error> {
        if let Some(v) = self.value(key) {
            *out = if let Some(value) = v.get_ref().as_float() {
                value
                    .as_str()
                    .parse()
                    .map_err(|_| self.invalid(v, "无效浮点数"))?
            } else if let Some(value) = v.get_ref().as_integer() {
                i128::from_str_radix(value.as_str(), value.radix())
                    .map_err(|_| self.invalid(v, "整数比例超出范围"))? as f64
            } else {
                return Err(self.invalid(v, "需要整数或浮点比例"));
            };
        }
        Ok(())
    }
    fn duration(&self, key: &str, milliseconds: &str, out: &mut Duration) -> Result<(), Error> {
        if let Some(ms) = self.value(milliseconds) {
            if self.value(key).is_some() {
                return Err(self.invalid(ms, "不能同时指定时长结构与毫秒字段"));
            }
            *out = Duration::from_millis(self.unsigned(ms)?);
        } else if let Some(value) = self.value(key) {
            let table = Table {
                name: self.name,
                value: value
                    .get_ref()
                    .as_table()
                    .ok_or_else(|| self.invalid(value, "时长需要 seconds/nanoseconds 子表"))?,
            };
            table.check(&["seconds", "nanoseconds"])?;
            let mut seconds = 0;
            let mut nanoseconds = 0;
            table.u64("seconds", &mut seconds)?;
            table.u64("nanoseconds", &mut nanoseconds)?;
            if nanoseconds >= 1_000_000_000 {
                return Err(self.invalid(value, "nanoseconds 必须小于十亿"));
            }
            *out = Duration::new(seconds, nanoseconds as u32);
        }
        Ok(())
    }
    fn child(
        &self,
        key: &str,
        name: &'static str,
        apply: impl FnOnce(Table<'_, 'i>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        if let Some(value) = self.value(key) {
            apply(Table {
                name,
                value: value
                    .get_ref()
                    .as_table()
                    .ok_or_else(|| self.invalid(value, "配置分组必须是子表"))?,
            })?;
        }
        Ok(())
    }
}
pub(super) fn parse(input: &str, path: &[&str]) -> Result<Config, Error> {
    if input.len() > MAX_BYTES {
        return Err(error("config-toml", None, "文档超过 1 MiB"));
    }
    let document = DeTable::parse(input)
        .map_err(|e| error("config-toml", e.span().map(|s| s.start), "TOML 语法无效"))?;
    let mut selected = document.get_ref();
    for &key in path {
        selected = selected
            .get(key)
            .ok_or_else(|| error("table_path", None, "选定子表不存在"))?
            .get_ref()
            .as_table()
            .ok_or_else(|| error("table_path", None, "选定路径不是子表"))?;
    }
    let table = Table {
        name: "config-toml",
        value: selected,
    };
    table.check(&[
        "storage",
        "index",
        "log",
        "cache",
        "maintenance",
        "session",
        "recovery",
        "scan",
        "statistics",
    ])?;
    let mut c = Config::default();
    table.child("storage", "storage", |t| {
        t.check(&["root", "segment_bytes", "pre_allocate_log"])?;
        if let Some(value) = t.value("root") {
            c.storage.root = value
                .get_ref()
                .as_str()
                .ok_or_else(|| t.invalid(value, "根目录需要字符串"))?
                .into();
        }
        t.u64("segment_bytes", &mut c.storage.segment_bytes)?;
        t.boolean("pre_allocate_log", &mut c.storage.pre_allocate_log)
    })?;
    table.child("index", "index", |t| {
        t.check(&["buckets"])?;
        t.usize("buckets", &mut c.index.buckets)
    })?;
    table.child("log", "log", |t| {
        t.check(&["page_bytes", "memory_pages", "mutable_fraction"])?;
        t.usize("page_bytes", &mut c.log.page_bytes)?;
        t.usize("memory_pages", &mut c.log.memory_pages)?;
        t.fraction("mutable_fraction", &mut c.log.mutable_fraction)
    })?;
    table.child("cache", "cache", |t| {
        t.check(&[
            "enabled",
            "capacity_bytes",
            "mutable_fraction",
            "pre_allocate",
        ])?;
        t.boolean("enabled", &mut c.cache.enabled)?;
        t.usize("capacity_bytes", &mut c.cache.capacity_bytes)?;
        t.fraction("mutable_fraction", &mut c.cache.mutable_fraction)?;
        t.boolean("pre_allocate", &mut c.cache.pre_allocate)
    })?;
    table.child("maintenance", "maintenance", |t| {
        t.check(&[
            "max_checkpoint_tokens",
            "max_checkpoint_catalog_bytes",
            "max_compaction_keys",
            "max_compaction_workers",
            "max_compaction_key_bytes",
            "auto_compaction",
            "workers",
            "auto_compaction_policy",
        ])?;
        t.usize(
            "max_checkpoint_tokens",
            &mut c.maintenance.max_checkpoint_tokens,
        )?;
        t.usize(
            "max_checkpoint_catalog_bytes",
            &mut c.maintenance.max_checkpoint_catalog_bytes,
        )?;
        t.usize(
            "max_compaction_keys",
            &mut c.maintenance.max_compaction_keys,
        )?;
        t.usize(
            "max_compaction_workers",
            &mut c.maintenance.max_compaction_workers,
        )?;
        t.usize(
            "max_compaction_key_bytes",
            &mut c.maintenance.max_compaction_key_bytes,
        )?;
        t.boolean("auto_compaction", &mut c.maintenance.auto_compaction)?;
        t.usize("workers", &mut c.maintenance.workers)?;
        t.child(
            "auto_compaction_policy",
            "maintenance.auto_compaction_policy",
            |t| {
                let p = &mut c.maintenance.auto_compaction_policy;
                t.check(&[
                    "check_interval",
                    "check_interval_ms",
                    "trigger_fraction",
                    "compact_fraction",
                    "max_compacted_bytes",
                    "log_size_budget",
                ])?;
                t.duration("check_interval", "check_interval_ms", &mut p.check_interval)?;
                t.fraction("trigger_fraction", &mut p.trigger_fraction)?;
                t.fraction("compact_fraction", &mut p.compact_fraction)?;
                t.u64("max_compacted_bytes", &mut p.max_compacted_bytes)?;
                t.u64("log_size_budget", &mut p.log_size_budget)
            },
        )
    })?;
    table.child("session", "session", |t| {
        t.check(&["max_sessions", "max_pending", "max_results"])?;
        t.usize("max_sessions", &mut c.session.max_sessions)?;
        t.usize("max_pending", &mut c.session.max_pending)?;
        t.usize("max_results", &mut c.session.max_results)
    })?;
    table.child("recovery", "recovery", |t| {
        t.check(&["max_records", "max_index_bytes", "timeout", "timeout_ms"])?;
        t.usize("max_records", &mut c.recovery.max_records)?;
        t.usize("max_index_bytes", &mut c.recovery.max_index_bytes)?;
        t.duration("timeout", "timeout_ms", &mut c.recovery.timeout)
    })?;
    table.child("scan", "scan", |t| {
        t.check(&["max_scanners", "timeout", "timeout_ms"])?;
        t.usize("max_scanners", &mut c.scan.max_scanners)?;
        t.duration("timeout", "timeout_ms", &mut c.scan.timeout)
    })?;
    table.child("statistics", "statistics", |t| {
        t.check(&["enabled"])?;
        t.boolean("enabled", &mut c.statistics.enabled)
    })?;
    c.validate()?;
    Ok(c)
}
pub(super) fn file(path: &Path, table: &[&str]) -> Result<Config, Error> {
    let input = std::fs::File::open(path).map_err(Error::Io)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(MAX_BYTES + 1)
        .map_err(|_| Error::OutOfMemory)?;
    input
        .take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(Error::Io)?;
    if bytes.len() > MAX_BYTES {
        return Err(error("config-toml", None, "文档超过 1 MiB"));
    }
    let input = std::str::from_utf8(&bytes)
        .map_err(|e| error("config-toml", Some(e.valid_up_to()), "文档不是 UTF-8"))?;
    parse(input, table)
}
