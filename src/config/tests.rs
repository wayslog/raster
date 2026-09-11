//! 配置解析不执行设备操作；完整映射、默认补全和错误边界使用独立输入验证。
use super::*;
use std::time::Duration;

const FULL: &str = r#"
[storage]
root = '相对目录/数据'
segment_bytes = 0x10_000
pre_allocate_log = true
[index]
buckets = 32
[log]
page_bytes = 4096
memory_pages = 8
mutable_fraction = 0.75
[cache]
enabled = true
capacity_bytes = 8192
mutable_fraction = 0.25
pre_allocate = true
[maintenance]
max_checkpoint_tokens = 12
max_checkpoint_catalog_bytes = 65536
max_compaction_keys = 300
max_compaction_workers = 3
max_compaction_key_bytes = 65536
auto_compaction = true
workers = 2
[maintenance.auto_compaction_policy]
check_interval = { seconds = 1, nanoseconds = 123 }
trigger_fraction = 0.9
compact_fraction = 0.3
max_compacted_bytes = 8192
log_size_budget = 65536
[session]
max_sessions = 4
max_pending = 5
max_results = 6
[recovery]
max_records = 500
max_index_bytes = 8192
timeout_ms = 4321
[scan]
max_scanners = 2
timeout = { seconds = 3, nanoseconds = 456 }
[statistics]
enabled = true
"#;

#[test]
fn 空文档采用默认值且全部字段拥有映射结果() {
    assert_eq!(Config::from_toml_str("").unwrap(), Config::default());
    assert_eq!(
        Config::from_toml_str(include_str!("../../docs/examples/raster.toml")).unwrap(),
        Config::default()
    );
    let mut expected = Config {
        storage: StorageConfig {
            root: "相对目录/数据".into(),
            segment_bytes: 65536,
            pre_allocate_log: true,
        },
        ..Default::default()
    };
    expected.index.buckets = 32;
    expected.log = LogConfig {
        page_bytes: 4096,
        memory_pages: 8,
        mutable_fraction: 0.75,
    };
    expected.cache = CacheConfig {
        enabled: true,
        capacity_bytes: 8192,
        mutable_fraction: 0.25,
        pre_allocate: true,
    };
    expected.maintenance = MaintenanceConfig {
        max_checkpoint_tokens: 12,
        max_checkpoint_catalog_bytes: 65536,
        max_compaction_keys: 300,
        max_compaction_workers: 3,
        max_compaction_key_bytes: 65536,
        auto_compaction: true,
        workers: 2,
        auto_compaction_policy: AutoCompactionPolicy {
            check_interval: Duration::new(1, 123),
            trigger_fraction: 0.9,
            compact_fraction: 0.3,
            max_compacted_bytes: 8192,
            log_size_budget: 65536,
        },
    };
    expected.session = SessionConfig {
        max_sessions: 4,
        max_pending: 5,
        max_results: 6,
    };
    expected.recovery = RecoveryConfig {
        max_records: 500,
        max_index_bytes: 8192,
        timeout: Duration::from_millis(4321),
    };
    expected.scan = ScanConfig {
        max_scanners: 2,
        timeout: Duration::new(3, 456),
    };
    expected.statistics.enabled = true;
    let source = FULL.to_string();
    let actual = Config::from_toml_str(&source).unwrap();
    drop(source);
    assert_eq!(actual, expected);
}

#[test]
fn 完整语法支持选定子表与带点键但不忽略组内未知项() {
    let source = "\"旁支\" = [1, 2, 3]\n[\"服务.甲\".raster]\nlog.page_bytes = 4096\nlog.mutable_fraction = +0\nstorage.root = \"多行\\n数据\\u002f目录\"\n";
    let c = Config::from_toml_str_at(source, &["服务.甲", "raster"]).unwrap();
    assert_eq!(c.log.page_bytes, 4096);
    assert_eq!(c.log.mutable_fraction, 0.0);
    assert_eq!(c.storage.root, std::path::PathBuf::from("多行\n数据/目录"));
    assert!(Config::from_toml_str(source).is_err());
    assert!(Config::from_toml_str_at(source, &["服务", "甲"]).is_err());
    assert!(Config::from_toml_str_at(source, &["旁支"]).is_err());
    assert!(Config::from_toml_str_at("[a]\nunknown = 1", &["a"]).is_err());
    assert!(Config::from_toml_str("[scan]\ntimeout_ms = +123").is_ok());
}

#[test]
fn 语法类型范围和组合错误明确拒绝且不回显输入() {
    for input in [
        "[log",
        "[log]\npage_bytes=4096\npage_bytes=8192",
        "storage = 3",
        "[storage]\nroot = 42",
        "[storage]\npre_allocate_log = 1",
        "[index]\nbuckets = -1",
        "[index]\nbuckets = 3",
        "[index]\nbuckets = 1.0",
        "[index]\nbuckets = 18446744073709551616",
        "[log]\nmemory_pages = 1",
        "[log]\nmutable_fraction = nan",
        "[cache]\nmutable_fraction = inf",
        "[cache]\nmutable_fraction = 1",
        "[cache]\nenabled = true",
        "[maintenance]\nauto_compaction = true",
        "[maintenance]\nworkers = 65",
        "[session]\nmax_pending = 0",
        "[scan]\ntimeout_ms = 0",
        "[scan]\ntimeout = {seconds=1, nanoseconds=1000000000}",
        "[scan]\ntimeout = {seconds=1, invalid=1}",
        "[scan]\ntimeout = 3",
        "[scan]\ntimeout = {seconds=1}\ntimeout_ms = 3",
        "[recovery]\ntimeout = {seconds=18446744073709551615}",
        "[recovery]\nmax_index_bytes = 35",
        "[statistics]\nenabled = 'true'",
    ] {
        assert!(Config::from_toml_str(input).is_err(), "意外接受：{input}");
    }
    for input in [
        "[storage]\nroot = \"不应回显的私人值",
        "[storage]\n\"不应回显的私人键\"=1",
        "[statistics]\nenabled = '不应回显的私人值'",
    ] {
        let error = Config::from_toml_str(input).unwrap_err();
        assert!(!format!("{error:?} {error}").contains("不应回显"));
        assert!(matches!(
            error,
            Error::ConfigDocument {
                offset: Some(_),
                ..
            }
        ));
    }
    assert!(Config::from_toml_str(&" ".repeat(document::MAX_BYTES + 1)).is_err());
}

#[test]
fn 文件入口选择子表并有界拒绝非文本与超限文件() {
    struct File(std::path::PathBuf);
    impl Drop for File {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let file = File(std::env::temp_dir().join(format!(
        "raster-config-{:x?}.toml",
        crate::types::StoreId::generate().unwrap().0
    )));
    std::fs::write(&file.0, FULL).unwrap();
    assert_eq!(
        Config::from_toml_file(&file.0).unwrap(),
        Config::from_toml_str(FULL).unwrap()
    );
    std::fs::write(&file.0, "[raster.statistics]\nenabled=true").unwrap();
    assert!(
        Config::from_toml_file_at(&file.0, &["raster"])
            .unwrap()
            .statistics
            .enabled
    );
    std::fs::write(&file.0, [0xff, 0xfe]).unwrap();
    assert!(matches!(
        Config::from_toml_file(&file.0),
        Err(Error::ConfigDocument { .. })
    ));
    std::fs::write(&file.0, vec![b' '; document::MAX_BYTES + 1]).unwrap();
    assert!(Config::from_toml_file(&file.0).is_err());
    std::fs::remove_file(&file.0).unwrap();
    assert!(matches!(Config::from_toml_file(&file.0), Err(Error::Io(_))));
}
