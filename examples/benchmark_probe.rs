//! Repeat the unchanged memory trajectory to diagnose short-window regressions.
#[allow(
    dead_code,
    reason = "Reuse the original worker, operation callbacks, and oracle without running its recovery matrix."
)]
#[path = "benchmark/main.rs"]
mod benchmark;
use benchmark::{model, trace};
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    benchmark::probe::run()
}
