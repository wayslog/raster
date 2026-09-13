//! Repeated live-memory measurement; checkpoint and recovery acceptance remain separate.
use super::*;

pub(crate) fn run() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let output = PathBuf::from(
        args.next()
            .ok_or("Expected output directory and repeat count")?,
    );
    let repetitions: usize = args
        .next()
        .ok_or("Expected repeat count")?
        .to_str()
        .ok_or("Repeat count must be UTF-8")?
        .parse()?;
    if args.next().is_some() || !(2..=512).contains(&repetitions) {
        return Err(
            "Repeat count must be between 2 and 512; no extra arguments are accepted".into(),
        );
    }
    fs::create_dir(&output)?;
    let case = Case {
        variable: true,
        hot: true,
        threads: 1,
        disk: false,
        round: 1,
    };
    let input = scenario::trace(&[scenario::prepare(case, 0)]);
    let text = input.encode();
    if trace::Trace::decode(&text)? != input {
        return Err("Probe trace round trip failed".into());
    }
    fs::write(output.join("input.trace"), text)?;
    let mut csv = BufWriter::new(fs::File::create(output.join("repetitions.csv"))?);
    writeln!(
        csv,
        "repetition,operations,verified_keys,elapsed_ns,read_ns,upsert_ns,rmw_ns,delete_ns,pending,retries"
    )?;
    let mut elapsed_ns = 0;
    let mut engine_ns = [0u128; 4];
    let mut latencies: [Vec<u128>; 4] =
        std::array::from_fn(|_| Vec::with_capacity(repetitions * OPERATIONS / 4));
    let mut verified_keys = 0;
    for repetition in 0..repetitions {
        let data = output.join(format!("store-{repetition}"));
        fs::create_dir(&data)?;
        let mut config = Config::default();
        config.storage.root = data.clone();
        config.storage.segment_bytes = 1024 * 1024;
        config.index.buckets = 2048;
        config.log.page_bytes = PAGE_BYTES;
        config.log.memory_pages = 4096;
        config.log.mutable_fraction = 0.5;
        let budget = config.log.page_bytes * config.log.memory_pages;
        let meter = Meter::default();
        let store = build::<Variable>(config, &meter).create()?;
        let prepared = scenario::prepare(case, 0);
        let ready = Barrier::new(2);
        let go = Barrier::new(2);
        let (start, result) = std::thread::scope(|scope| -> Result<_> {
            let worker = scope.spawn(|| worker::<Variable>(&store, prepared, &ready, &go));
            ready.wait();
            let start = Instant::now();
            go.wait();
            Ok((
                start,
                worker.join().map_err(|_| "Repeated worker panicked")??,
            ))
        })?;
        let duration = result.finished.duration_since(start).as_nanos();
        let counts = meter.snapshot();
        if result.pending.iter().sum::<usize>() != 0
            || result.retries != 0
            || counts.read != 0
            || counts.written != 0
            || store.diagnostics()?.log_span_bytes >= budget as u64
        {
            return Err("Repeated memory workload left the validated resident path".into());
        }
        if result.latency.len() != OPERATIONS || result.verify.len() != KEYS {
            return Err("Repeated workload or verification is incomplete".into());
        }
        let mut totals = [0; 4];
        for (kind, values) in result.by_kind.iter().enumerate() {
            if values.len() != OPERATIONS / 4 {
                return Err("Operation mix changed".into());
            }
            totals[kind] = values.iter().sum();
            engine_ns[kind] += totals[kind];
            latencies[kind].extend_from_slice(values);
        }
        // Verification runs after timing with a fresh session on the same live store.
        // This deliberately does not claim checkpoint or recovery coverage.
        let mut session = store.start_session(Default::default())?;
        for (step, expected) in &result.verify {
            let (actual, pending, retries) =
                execute::<Variable>(&mut session, step, request::<Variable>(step)?, deadline())?;
            check(&actual, expected, step)?;
            if pending || retries != 0 {
                return Err("Verification unexpectedly suspended or retried".into());
            }
            verified_keys += 1;
        }
        session.close(deadline())?;
        drop(session);
        drop(store);
        fs::remove_dir_all(&data)?;
        elapsed_ns += duration;
        writeln!(
            csv,
            "{repetition},{OPERATIONS},{KEYS},{duration},{},{},{},{},0,0",
            totals[0], totals[1], totals[2], totals[3]
        )?;
        csv.flush()?;
    }
    let p99: [u128; 4] = std::array::from_fn(|kind| percentile(&mut latencies[kind], 99));
    let mut all_latencies: Vec<_> = latencies.iter().flatten().copied().collect();
    let overall_p99 = percentile(&mut all_latencies, 99);
    let summary = format!(
        "{{\"protocol\":1,\"repetitions\":{repetitions},\"operations\":{},\"verified_keys\":{verified_keys},\"elapsed_ns\":{elapsed_ns},\"engine_ns\":{engine_ns:?},\"p99_ns\":{p99:?},\"pending\":0,\"retries\":0,\"overall_p99_ns\":{overall_p99}}}",
        repetitions * OPERATIONS
    );
    fs::write(output.join("summary.json"), format!("{summary}\n"))?;
    println!("{summary}");
    Ok(())
}
