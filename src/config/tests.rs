//! Configuration parsing does not perform device operations;full mapping,Default completion and error bounds use independent input validation.
use super::*;
use std::time::Duration;

const FULL: &str = r#"
[storage]
root = 'relative directory/data'
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
fn an_empty_document_uses_default_values_and_all_fields_have_mapping_results() {
    assert_eq!(Config::from_toml_str("").unwrap(), Config::default());
    assert_eq!(
        Config::from_toml_str(include_str!("../../docs/examples/raster.toml")).unwrap(),
        Config::default()
    );
    let mut expected = Config {
        storage: StorageConfig {
            root: "relative directory/data".into(),
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
fn full_syntax_supports_selected_subtables_and_dotted_keys_but_does_not_ignore_unknown_items_within_the_group()
 {
    let source = "\"side branch\" = [1, 2, 3]\n[\"service.A\".raster]\nlog.page_bytes = 4096\nlog.mutable_fraction = +0\nstorage.root = \"multiple lines\\ndata\\u002fdirectory\"\n";
    let c = Config::from_toml_str_at(source, &["service.A", "raster"]).unwrap();
    assert_eq!(c.log.page_bytes, 4096);
    assert_eq!(c.log.mutable_fraction, 0.0);
    assert_eq!(
        c.storage.root,
        std::path::PathBuf::from("multiple lines\ndata/directory")
    );
    assert!(Config::from_toml_str(source).is_err());
    assert!(Config::from_toml_str_at(source, &["service", "A"]).is_err());
    assert!(Config::from_toml_str_at(source, &["side branch"]).is_err());
    assert!(Config::from_toml_str_at("[a]\nunknown = 1", &["a"]).is_err());
    assert!(Config::from_toml_str("[scan]\ntimeout_ms = +123").is_ok());
}

#[test]
fn syntax_type_range_and_combination_errors_are_explicitly_rejected_and_the_input_is_not_echoed() {
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
        assert!(
            Config::from_toml_str(input).is_err(),
            "unexpected acceptance:{input}"
        );
    }
    for input in [
        "[storage]\nroot = \"Private value that should not be echoed",
        "[storage]\n\"Private keys that should not be echoed\"=1",
        "[statistics]\nenabled = 'Private value that should not be echoed'",
    ] {
        let error = Config::from_toml_str(input).unwrap_err();
        assert!(!format!("{error:?} {error}").contains("should not be echoed"));
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
fn file_entry_selects_subtables_and_rejects_non_text_and_over_limit_files_in_a_bounded_manner() {
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
