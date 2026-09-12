//! Same process repeats full disk life cycle;Fixed resource growth threshold before running.
#[path = "resources/allocator.rs"]
mod allocation;
#[path = "resources/observation.rs"]
mod observation;
#[path = "disk_lifecycle/main.rs"]
mod workload;

use allocation::{Counters, TrackingAllocator};
use observation::{Observation, observe};
use std::{
    fs,
    io::{BufWriter, Write},
    path::Path,
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
static COUNTERS: Counters = Counters::new();
#[global_allocator]
static ALLOCATOR: TrackingAllocator<'static> = TrackingAllocator::new(&COUNTERS);
const WARMUP: usize = 8;
const CYCLES: usize = 64;

fn cycle(parent: &Path, ordinal: usize) -> Result<()> {
    let root = parent.join(format!("cycle-{ordinal:03}"));
    fs::create_dir(&root)?;
    if let Err(error) = workload::run(root.clone()) {
        eprintln!(
            "Resource cycle {ordinal} failed; synthetic materials remain in cycle-{ordinal:03}"
        );
        return Err(error);
    }
    fs::remove_dir_all(root)?;
    Ok(())
}
fn within_budget(baseline: Observation, current: Observation) -> bool {
    current.allocation.bytes <= baseline.allocation.bytes + 256 * 1024
        && current.allocation.allocations <= baseline.allocation.allocations + 16
        && current.files <= baseline.files + 2
        && current.threads <= baseline.threads + 1
        && current.rss <= baseline.rss + 16 * 1024 * 1024
}
fn row(output: &mut impl Write, cycle: usize, sample: Observation, elapsed: u128) -> Result<()> {
    writeln!(
        output,
        "{cycle},{},{},{},{},{},{},{elapsed}",
        sample.allocation.bytes,
        sample.allocation.allocations,
        sample.allocation.peak,
        sample.rss,
        sample.files,
        sample.threads
    )?;
    Ok(())
}
fn main() -> Result<()> {
    if std::env::args_os().len() != 1 {
        return Err("Resource acceptance does not accept location arguments; set the output path with RASTER_RESOURCE_OUTPUT".into());
    }
    let output = std::env::var_os("RASTER_RESOURCE_OUTPUT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| "target/p9-resources/resources.csv".into());
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)?;
    let mut csv = BufWriter::new(file);
    writeln!(
        csv,
        "round,Survival allocated bytes,number of live allocations,Allocate peak bytes,RSSbytes,file descriptor,threads,milliseconds taken"
    )?;
    for ordinal in 0..WARMUP {
        cycle(parent, ordinal)?;
        observe()?;
    }
    let baseline = observe()?;
    row(&mut csv, 0, baseline, 0)?;
    csv.flush()?;
    for ordinal in 1..=CYCLES {
        let started = Instant::now();
        cycle(parent, WARMUP + ordinal - 1)?;
        let sample = observe()?;
        row(&mut csv, ordinal, sample, started.elapsed().as_millis())?;
        csv.flush()?;
        if !within_budget(baseline, sample) {
            return Err(format!(
                "ordinal {ordinal} The round exceeds the preset resource growth threshold;baseline {baseline:?};current {sample:?}"
            )
            .into());
        }
    }
    println!(
        "Same-process resource cycle passed: warmup {WARMUP} rounds, measurement {CYCLES} rounds, and a full disk lifecycle with two recoveries per round."
    );
    Ok(())
}
