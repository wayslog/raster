//! Shared latency accounting for the paired device benchmark drivers.
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

#[derive(Default)]
pub struct Samples {
    pub values: Vec<u64>,
    pub pending: usize,
}

#[derive(Clone)]
pub struct Sample {
    started: Instant,
    samples: Rc<RefCell<Samples>>,
}

impl Sample {
    pub fn start(samples: &Rc<RefCell<Samples>>) -> Self {
        let samples = samples.clone();
        Self {
            started: Instant::now(),
            samples,
        }
    }

    pub fn finish(slot: &mut Option<Self>, pending: bool) {
        if let Some(sample) = slot.take() {
            let elapsed = sample.started.elapsed().as_nanos() as u64;
            let mut samples = sample.samples.borrow_mut();
            samples.values.push(elapsed);
            samples.pending += usize::from(pending);
        }
    }
}

pub fn io_threads() -> usize {
    let threads = std::env::var("RASTER_BENCH_IO_THREADS")
        .map_or(8, |value| value.parse().expect("I/O thread count"));
    assert!(
        (1..=64).contains(&threads),
        "I/O thread count must be in 1..64"
    );
    threads
}
