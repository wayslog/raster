//! 原生进程观察；只读取本进程，不把 RSS 当作仍存活的 Rust 分配量。
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
        return Err("读取本进程资源失败".into());
    }
    Ok(String::from_utf8(output.stdout)?)
}
pub(crate) fn observe() -> Result<Observation> {
    let pid = std::process::id().to_string();
    let rss = process_output(&["-o", "rss=", "-p", &pid])?
        .trim()
        .parse::<u64>()?
        .checked_mul(1024)
        .ok_or("RSS 字节数溢出")?;
    #[cfg(target_os = "linux")]
    let (directory, threads) = {
        let status = fs::read_to_string("/proc/self/status")?;
        let threads = status
            .lines()
            .find_map(|line| line.strip_prefix("Threads:"))
            .ok_or("进程状态缺少线程数")?
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
            .ok_or("线程观察缺少标题")?
            .find("PID")
            .ok_or("线程观察缺少进程列")?
            + 3;
        let mut count = 0;
        for line in lines.filter(|line| !line.trim().is_empty()) {
            // macOS 只在第一条线程行显示 USER/PID，后续线程将这两列留空。
            match line
                .get(..end)
                .ok_or("线程观察行被截断")?
                .split_whitespace()
                .last()
            {
                Some(value) if value == pid => {}
                None if count > 0 => {}
                _ => return Err("线程观察出现其他进程或缺少首行身份".into()),
            }
            count += 1;
        }
        ("/dev/fd", count)
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let (directory, threads): (&str, usize) = return Err("资源验收仅支持 Linux/macOS".into());
    if threads == 0 {
        return Err("线程观察为空".into());
    }
    let files = fs::read_dir(directory)?.try_fold(0, |count, entry| entry.map(|_| count + 1))?;
    Ok(Observation {
        allocation: COUNTERS.snapshot(),
        rss,
        files,
        threads,
    })
}
