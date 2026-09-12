//! P1.1 True type and key policy acceptance;It does not mean that the storage engine is ready to run.
use raster::{
    schema::{
        KeyCodec,
        builtin::{ByteKey, U64Key},
    },
    types::{CacheAddress, CheckpointToken, FormatId, LogAddress, PageId, SessionId, StoreId},
};
use std::{collections::BTreeSet, process::Command};

#[test]
fn byte_key_fixed_vector_and_owned_round_trip() {
    // independent fixation FNV-1a vector,Do not generate expectations from the implementation under test.
    for (key, expected) in [
        (&b""[..], 0xcbf29ce484222325),
        (&b"a"[..], 0xaf63dc4c8601ec8c),
        (&b"foobar"[..], 0x85944171f73967e8),
    ] {
        assert_eq!(ByteKey.hash(key).0, expected);
    }
    for key in [vec![], vec![0, 255, 128], vec![42; 65537]] {
        assert_eq!(ByteKey.encoded_len(&key).unwrap() as usize, key.len());
        let mut encoded = vec![0; key.len()];
        ByteKey.encode(&key, &mut encoded).unwrap();
        assert_eq!(encoded, key);
        assert!(ByteKey.equals_encoded(&key, &encoded).unwrap());
        assert_eq!(ByteKey.decode_owned(&encoded).unwrap(), key);
        encoded.push(0);
        assert!(!ByteKey.equals_encoded(&key, &encoded).unwrap());
    }
}

#[test]
fn integer_bounds_fixed_little_endian_encoding() {
    for (value, expected) in [
        (0, 0xa8c7f832281a39c5),
        (1, 0x89cd31291d2aefa4),
        (1 << 63, 0xa8c7783228196045),
        (u64::MAX, 0x8cf51a8bfca3883d),
    ] {
        assert_eq!(U64Key.hash(&value).0, expected);
    }
    for value in [0, 1, u64::MAX / 2, 1 << 63, u64::MAX] {
        let mut encoded = [0; 8];
        U64Key.encode(&value, &mut encoded).unwrap();
        assert_eq!(encoded, value.to_le_bytes());
        assert_eq!(U64Key.decode_owned(&encoded).unwrap(), value);
        assert!(U64Key.equals_encoded(&value, &encoded).unwrap());
        assert_eq!(U64Key.hash(&value), ByteKey.hash(&encoded));
    }
    let mut encoded = [0; 8];
    U64Key.encode(&0x0102030405060708, &mut encoded).unwrap();
    assert_eq!(encoded, [8, 7, 6, 5, 4, 3, 2, 1]);
    assert!(!U64Key.equals_encoded(&0, &encoded).unwrap());
}

#[test]
fn different_keys_for_the_same_tag_still_have_to_be_compared_for_encoding() {
    let first = 8969_u64;
    let second = 9239_u64;
    assert_eq!(U64Key.hash(&first).tag(), 31722);
    assert_eq!(U64Key.hash(&first).tag(), U64Key.hash(&second).tag());
    assert!(
        !U64Key
            .equals_encoded(&first, &second.to_le_bytes())
            .unwrap()
    );
    assert!(
        !ByteKey
            .equals_encoded(&first.to_le_bytes(), &second.to_le_bytes())
            .unwrap()
    );
}

#[test]
fn encoding_length_errors_do_not_modify_the_output_and_integer_corrupted_input_is_rejected() {
    for length in [0, 1, 7, 9, 16] {
        let mut output = vec![0xaa; length];
        assert!(U64Key.encode(&42, &mut output).is_err());
        assert_eq!(output, vec![0xaa; length]);
        assert!(U64Key.decode_owned(&output).is_err());
        assert!(U64Key.equals_encoded(&42, &output).is_err());
    }
    for length in [0, 2, 4] {
        let mut output = vec![0xaa; length];
        assert!(ByteKey.encode(b"abc", &mut output).is_err());
        assert_eq!(output, vec![0xaa; length]);
    }
}

#[test]
fn algorithm_seeds_and_encoded_versions_reject_changes_respectively() {
    let descriptor = ByteKey.hash_descriptor();
    assert_eq!(descriptor.algorithm, FormatId(*b"raster:fnv1a64:1"));
    assert_eq!(
        descriptor.seed,
        [0x25, 0x23, 0x22, 0x84, 0xe4, 0x9c, 0xf2, 0xcb]
    );
    ByteKey
        .validate_identity(ByteKey.format_id(), &descriptor)
        .unwrap();
    assert!(
        ByteKey
            .validate_identity(U64Key.format_id(), &descriptor)
            .is_err()
    );
    let mut format = ByteKey.format_id();
    format.0[14] ^= 1;
    assert!(ByteKey.validate_identity(format, &descriptor).is_err());
    let mut changed = descriptor.clone();
    changed.algorithm.0[15] ^= 1;
    assert!(
        ByteKey
            .validate_identity(ByteKey.format_id(), &changed)
            .is_err()
    );
    changed = descriptor.clone();
    changed.seed[0] ^= 1;
    assert!(
        ByteKey
            .validate_identity(ByteKey.format_id(), &changed)
            .is_err()
    );
    changed.seed.clear();
    assert!(
        ByteKey
            .validate_identity(ByteKey.format_id(), &changed)
            .is_err()
    );
    assert!(
        U64Key
            .validate_identity(ByteKey.format_id(), &descriptor)
            .is_err()
    );
}

#[test]
fn address_verification_paging_and_pushing_boundaries() {
    assert!(LogAddress(0).validate().is_ok());
    assert!(LogAddress::INVALID.validate().is_err());
    assert!(CacheAddress::INVALID.validate().is_err());
    for value in [0, 1, 4095, 4096, u64::MAX - 1] {
        let address = LogAddress(value);
        let (page, offset) = address.page_offset(4096).unwrap();
        assert_eq!(
            LogAddress::from_page_offset(page, offset, 4096).unwrap(),
            address
        );
        let (page, offset) = CacheAddress(value).page_offset(4096).unwrap();
        assert_eq!(
            CacheAddress::from_page_offset(page, offset, 4096).unwrap(),
            CacheAddress(value)
        );
    }
    assert_eq!(LogAddress(4095).checked_add(1).unwrap(), LogAddress(4096));
    assert!(LogAddress(u64::MAX - 1).checked_add(1).is_err());
    assert!(CacheAddress(u64::MAX - 1).checked_add(2).is_err());
    assert!(LogAddress::INVALID.checked_add(0).is_err());
    for size in [0, 3, u64::MAX] {
        assert!(LogAddress(0).page_offset(size).is_err());
        assert!(CacheAddress::from_page_offset(PageId(0), 0, size).is_err());
    }
    assert!(LogAddress::from_page_offset(PageId(u64::MAX), 0, 4096).is_err());
    assert!(LogAddress::from_page_offset(PageId(0), 4096, 4096).is_err());
}

#[test]
fn identity_rejects_null_values_and_concurrent_generation_does_not_duplicate() {
    assert!(StoreId([0; 16]).validate().is_err());
    assert!(SessionId([0; 16]).validate().is_err());
    assert!(CheckpointToken([0; 16]).validate().is_err());
    let workers: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                (0..32)
                    .flat_map(|_| {
                        [
                            StoreId::generate().unwrap().0,
                            SessionId::generate().unwrap().0,
                            CheckpointToken::generate().unwrap().0,
                        ]
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let values: Vec<_> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    assert!(values.iter().all(|value| *value != [0; 16]));
    assert_eq!(values.iter().collect::<BTreeSet<_>>().len(), values.len());
    assert!(StoreId([1; 16]).validate().is_ok());
}

#[test]
fn independent_process_key_fingerprint() {
    assert_eq!(U64Key.hash(&u64::MAX).0, 0x8cf51a8bfca3883d);
    if std::env::var_os("RASTER_P1_CHILD").is_none() {
        return;
    }
    let mut encoded = [0; 8];
    U64Key.encode(&0x0102030405060708, &mut encoded).unwrap();
    println!(
        "key fingerprint:{:02x?}/{:016x}/{:016x}/{:?}/{:?}",
        encoded,
        U64Key.hash(&u64::MAX).0,
        ByteKey.hash(b"foobar").0,
        ByteKey.format_id(),
        ByteKey.hash_descriptor()
    );
    println!("new identity:{:02x?}", StoreId::generate().unwrap().0);
}

#[test]
fn encoded_hash_consistent_with_semantic_description_after_restart() {
    let run = || {
        let result = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "independent_process_key_fingerprint",
                "--nocapture",
            ])
            .env("RASTER_P1_CHILD", "1")
            .output()
            .unwrap();
        assert!(result.status.success(), "Child process failed");
        String::from_utf8(result.stdout).unwrap()
    };
    let first = run();
    let second = run();
    let fingerprint = |text: &str| {
        text.lines()
            .find(|line| line.starts_with("key fingerprint:"))
            .unwrap()
            .to_owned()
    };
    assert_eq!(fingerprint(&first), fingerprint(&second));
    assert!(fingerprint(&first).starts_with("key fingerprint:[08, 07, 06, 05, 04, 03, 02, 01]/"));
    let identity = |text: &str| {
        text.lines()
            .find(|line| line.starts_with("new identity:"))
            .unwrap()
            .to_owned()
    };
    assert_ne!(identity(&first), identity(&second));
}
