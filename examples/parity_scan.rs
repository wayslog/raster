//! Resident append and physical-scan regression driver with independent checks.
use raster::{
    RasterKV, Submission,
    api::{
        Outcome,
        operation::{Keyed, UpdateDecision, UpsertOperation},
        scan::{Buffering, ScanOptions},
        session::SessionOptions,
    },
    schema::{
        ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::{Deadline, Error, Serial},
};
use std::time::{Duration, Instant};

type Schema = SchemaPair<U64Key, AtomicU64Value>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
struct Insert(u64);
impl Keyed<Schema> for Insert {
    fn key(&self) -> &u64 {
        &self.0
    }
}
fn value(key: u64) -> u64 {
    key ^ 0x7261_7374_6572_0001
}
impl UpsertOperation<Schema> for Insert {
    type Output = ();
    fn replacement(&mut self) -> std::result::Result<(u64, ()), Error> {
        Ok((value(self.0), ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> std::result::Result<UpdateDecision<()>, Error> {
        Err(Error::InvalidState(
            "append workload encountered an existing key",
        ))
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(60))
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: parity_scan RECORDS SCANS".into());
    }
    let count: u64 = args[0].parse()?;
    let scans: u64 = args[1].parse()?;
    if !(1..=131_072).contains(&count) || !(1..=64).contains(&scans) {
        return Err("invalid resident scan benchmark dimensions".into());
    }
    let config = raster::config::Config {
        index: raster::config::IndexConfig { buckets: 65_536 },
        log: raster::config::LogConfig {
            page_bytes: 65_536,
            memory_pages: 256,
            mutable_fraction: 0.9,
        },
        ..Default::default()
    };
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()?;
    let mut session = store.start_session(SessionOptions::default())?;
    let started = Instant::now();
    for key in 0..count {
        match session
            .upsert(Serial(key), Insert(key))
            .map_err(|e| e.reason)?
        {
            Submission::Ready(Ok(Outcome::Success(()))) => {}
            _ => return Err("resident append did not complete successfully".into()),
        }
        if key & 255 == 255 {
            session.refresh()?;
        }
    }
    let append_ns = started.elapsed().as_nanos();
    session.close(deadline())?;
    let boundaries = store.diagnostics()?;
    let mut samples = Vec::with_capacity((count * scans).div_ceil(64) as usize);
    let mut checksum = 0u64;
    let expected = (0..count).fold(0u64, |sum, key| sum.wrapping_add(value(key)));
    let started = Instant::now();
    for pass in 0..scans {
        let mut scanner = store.scan(ScanOptions {
            begin: boundaries.begin,
            end: boundaries.tail,
            buffering: Buffering::Unbuffered,
        })?;
        let mut records = 0;
        let mut previous = None;
        while records < count {
            let ordinal = pass * count + records;
            let clock = (ordinal & 63 == (ordinal / 64) & 63).then(Instant::now);
            let record = scanner.next_record()?.ok_or("scan ended early")?;
            if let Some(clock) = clock {
                samples.push(clock.elapsed().as_nanos());
            }
            if record.key != records
                || record.value != Some(value(records))
                || record.invalid
                || record.tombstone
                || previous.is_some_and(|address| address >= record.address)
            {
                return Err("physical scan result differs from the inserted sequence".into());
            }
            checksum = checksum.wrapping_add(record.value.unwrap());
            previous = Some(record.address);
            records += 1;
        }
        if scanner.next_record()?.is_some() || scanner.next_record()?.is_some() {
            return Err("scan returned extra records after completion".into());
        }
        scanner.close()?;
    }
    let scan_ns = started.elapsed().as_nanos();
    if checksum != expected.wrapping_mul(scans) {
        return Err("scan checksum differs from the independent expectation".into());
    }
    samples.sort_unstable();
    let p99_ns = samples[(samples.len() - 1) * 99 / 100];
    store.shutdown(deadline())?;
    println!(
        "{{\"records\":{count},\"scans\":{scans},\"append_ns\":{append_ns},\"scan_ns\":{scan_ns},\"p99_ns\":{p99_ns},\"checksum\":{checksum},\"samples\":{}}}",
        samples.len()
    );
    Ok(())
}
