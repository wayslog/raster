//! Native process observation;Read only this process,No RSS treated as still alive Rust Allocation amount.
use super::{COUNTERS, Result, allocation::Snapshot};
use std::{fs, process::Command};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Observation {
    pub allocation: Snapshot,
    pub rss: u64,
    pub files: usize,
    pub threads: usize,
}
fn process_output(arguments: &[&str]) -> Result<String> {
    let output = Command::new("ps").args(arguments).output()?;
    if !output.status.success() {
        return Err("Failed to read resources of this process".into());
    }
    Ok(String::from_utf8(output.stdout)?)
}
pub(crate) fn observe() -> Result<Observation> {
    let pid = std::process::id().to_string();
    let rss = process_output(&["-o", "rss=", "-p", &pid])?
        .trim()
        .parse::<u64>()?
        .checked_mul(1024)
        .ok_or("RSS Bytes overflow")?;
    #[cfg(target_os = "linux")]
    let (directory, threads) = {
        let status = fs::read_to_string("/proc/self/status")?;
        let threads = status
            .lines()
            .find_map(|line| line.strip_prefix("Threads:"))
            .ok_or("process status does not contain a thread count")?
            .trim()
            .parse::<usize>()?;
        ("/proc/self/fd", threads)
    };
    #[cfg(target_os = "macos")]
    let (directory, threads) = {
        let output = process_output(&["-M", "-p", &pid])?;
        let mut lines = output.lines();
        let end = lines
            .next()
            .ok_or("thread observation does not contain a header")?
            .find("PID")
            .ok_or("Thread watch missing process column")?
            + 3;
        let mut count = 0;
        for line in lines.filter(|line| !line.trim().is_empty()) {
            // macOS Only displayed on the first thread line USER/PID,Subsequent threads leave these two columns blank.
            match line
                .get(..end)
                .ok_or("Thread observes line being truncated")?
                .split_whitespace()
                .last()
            {
                Some(value) if value == pid => {}
                None if count > 0 => {}
                _ => return Err("Thread observation shows the presence of other processes or missing first line identity".into()),
            }
            count += 1;
        }
        ("/dev/fd", count)
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let (directory, threads): (&str, usize) =
        return Err("Resource acceptance only supports Linux/macOS".into());
    if threads == 0 {
        return Err("Thread observation is empty".into());
    }
    let files = fs::read_dir(directory)?.try_fold(0, |count, entry| entry.map(|_| count + 1))?;
    Ok(Observation {
        allocation: COUNTERS.snapshot(),
        rss,
        files,
        threads,
    })
}
