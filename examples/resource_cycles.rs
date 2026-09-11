//! 同一进程重复完整磁盘生命周期；在运行前固定资源增长门槛。
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
        eprintln!("资源循环 {ordinal} 失败，合成材料保留于 cycle-{ordinal:03}");
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
        return Err("资源验收不接收位置参数；输出位置通过 RASTER_RESOURCE_OUTPUT 设置".into());
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
        "轮次,存活分配字节,存活分配数,分配峰值字节,RSS字节,文件描述符,线程,耗时毫秒"
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
                "第 {ordinal} 轮超过预设资源增长门槛；基线 {baseline:?}；当前 {sample:?}"
            )
            .into());
        }
    }
    println!(
        "同进程资源循环通过：预热 {WARMUP} 轮，测量 {CYCLES} 轮，每轮完整磁盘生命周期与两次恢复。"
    );
    Ok(())
}
