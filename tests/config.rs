use raster::core::StoreConfig;

const CONFIG: &str = r#"
[faster]
filepath = "store-data"
checkpoint_dir = "checkpoints"
[faster.hlog]
in_mem_size_mb = 1024
mutable_fraction = 0.85
pre_allocate = false
[faster.hlog.compaction]
enabled = true
hlog_size_budget_mb = 2048
trigger_pct = 0.8
compact_pct = 0.2
max_compacted_size_mb = 512
check_interval_ms = 250
num_threads = 4
[faster.index]
table_size = 128
[faster.read_cache]
enabled = true
in_mem_size_mb = 512
mutable_fraction = 0.81
pre_allocate = false
"#;

#[test]
fn faster_from_toml_string() {
    let config = StoreConfig::from_toml_str(CONFIG).unwrap();
    assert_eq!(config.index_buckets, 128);
    assert_eq!(config.log.memory_pages, 32);
    assert_eq!(config.log.mutable_fraction, 0.85);
    assert!(!config.pre_allocate);
    assert_eq!(config.filepath.unwrap().to_str(), Some("store-data"));
    assert_eq!(config.log_size_budget, 2048 << 20);
    let cache = config.read_cache.unwrap();
    assert_eq!(cache.log.memory_pages, 16);
    assert_eq!(cache.log.mutable_fraction, 0.81);
    assert!(!cache.pre_allocate);
    let compaction = config.auto_compaction.unwrap();
    assert_eq!(compaction.check_interval.as_millis(), 250);
    assert_eq!(compaction.max_compacted_bytes, 512 << 20);
    assert_eq!(compaction.threads, 4);
}

#[test]
fn faster_from_toml_file() {
    let file = std::env::temp_dir().join(format!("raster-config-{}.toml", std::process::id()));
    std::fs::write(&file, CONFIG).unwrap();
    assert_eq!(
        StoreConfig::from_toml_file(&file).unwrap().index_buckets,
        128
    );
    std::fs::remove_file(file).unwrap();
}

#[test]
fn f2_from_toml_string_is_explicitly_unsupported() {
    let error = StoreConfig::from_toml_str("[f2]\nfilepath = 'f2-store'").unwrap_err();
    assert!(error.to_string().contains("F2"));
}

#[test]
fn f2_from_toml_file_is_explicitly_unsupported() {
    let file = std::env::temp_dir().join(format!("raster-config-f2-{}.toml", std::process::id()));
    std::fs::write(&file, "[f2]\nfilepath = 'f2-store'").unwrap();
    let error = StoreConfig::from_toml_file(&file).unwrap_err();
    std::fs::remove_file(file).unwrap();
    assert!(error.to_string().contains("F2"));
}

#[test]
fn defaults_and_integer_mutable_fraction_are_supported() {
    let config = StoreConfig::from_toml_str(
        "filepath = ''\n[hlog]\nin_mem_size_mb = 128\nmutable_fraction = 1\n[index]\ntable_size = 64",
    )
    .unwrap();
    assert_eq!(config.log.mutable_fraction, 1.0);
    assert!(config.read_cache.is_none());
    assert!(config.auto_compaction.is_none());
    assert_eq!(config.log_size_budget, 0);
    assert!(config.filepath.is_none());
}

#[test]
fn invalid_geometry_and_unknown_fields_are_rejected() {
    for (from, to) in [
        ("table_size = 128", "table_size = 3"),
        ("in_mem_size_mb = 1024", "in_mem_size_mb = 96"),
        ("mutable_fraction = 0.85", "mutable_fraction = nan"),
        ("num_threads = 4", "num_threads = 0"),
        ("hlog_size_budget_mb = 2048", "hlog_size_budget_mb = 0"),
        (
            "hlog_size_budget_mb = 2048",
            "hlog_size_budget_mb = 9223372036854775807",
        ),
        ("table_size = 128", "table_size = 128\nmisspelled = true"),
    ] {
        assert!(
            StoreConfig::from_toml_str(&CONFIG.replace(from, to)).is_err(),
            "accepted {to}"
        );
    }
}

#[test]
fn partial_configuration_requires_filepath_and_compaction_budget() {
    for text in [
        CONFIG.replace("filepath = \"store-data\"\n", ""),
        CONFIG.replace("hlog_size_budget_mb = 2048\n", ""),
        CONFIG
            .replace("hlog_size_budget_mb = 2048\n", "")
            .replace("enabled = true", "enabled = false"),
    ] {
        assert!(
            StoreConfig::from_toml_str(&text).is_err(),
            "required field silently defaulted"
        );
    }
}

#[test]
fn disabled_cache_ignores_unused_geometry() {
    let text = CONFIG
        .replace(
            "[faster.read_cache]\nenabled = true",
            "[faster.read_cache]\nenabled = false",
        )
        .replace("in_mem_size_mb = 512", "in_mem_size_mb = 3")
        .replace("mutable_fraction = 0.81", "mutable_fraction = nan");
    assert!(
        StoreConfig::from_toml_str(&text)
            .unwrap()
            .read_cache
            .is_none()
    );
}

#[test]
fn configuration_rejects_impossible_window_and_compaction_budget() {
    for (from, to) in [
        ("in_mem_size_mb = 1024", "in_mem_size_mb = 268435456"),
        ("hlog_size_budget_mb = 2048", "hlog_size_budget_mb = 512"),
    ] {
        assert!(
            StoreConfig::from_toml_str(&CONFIG.replace(from, to)).is_err(),
            "accepted {to}"
        );
    }
}

#[test]
fn toml_and_direct_configuration_share_preallocation_defaults() {
    let parsed = StoreConfig::from_toml_str("filepath = ''\n[hlog]\nin_mem_size_mb = 256\n[index]\ntable_size = 64\n[read_cache]\nenabled = true").unwrap();
    assert_eq!(parsed.pre_allocate, StoreConfig::default().pre_allocate);
    assert_eq!(
        parsed.read_cache.unwrap().pre_allocate,
        raster::core::ReadCacheConfig::default().pre_allocate
    );
}
