//! Diagnose synchronous Read cost while completed Pending tickets retain results.
//! All requests use public APIs; the blocking callback only prepares the workload.
use raster::{
    RasterKV, Submission,
    api::{Outcome, TicketState, operation::*},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{SchemaPair, SerializedValue, U64Key, U64ValueCodec},
    },
    types::*,
};
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

type Schema = SchemaPair<U64Key, SerializedValue<U64ValueCodec>>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Request;
impl Keyed<Schema> for Request {
    fn key(&self) -> &u64 {
        &0
    }
}
impl ReadOperation<Schema> for Request {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> std::result::Result<u64, Error> {
        Ok(*value.view())
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = u64;
    fn replacement(&mut self) -> std::result::Result<(u64, u64), Error> {
        Ok((42, 42))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> std::result::Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Append)
    }
}

struct BlockingRead {
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
}
impl Keyed<Schema> for BlockingRead {
    fn key(&self) -> &u64 {
        &0
    }
}
impl ReadOperation<Schema> for BlockingRead {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> std::result::Result<u64, Error> {
        self.entered
            .send(())
            .map_err(|_| Error::InvalidState("setup observer disappeared"))?;
        self.release
            .recv_timeout(Duration::from_secs(30))
            .map_err(|_| Error::DeadlineExceeded)?;
        Ok(*value.view())
    }
}

fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(30))
}

fn ready(submission: Submission<u64>) -> Result<u64> {
    match submission {
        Submission::Ready(Ok(Outcome::Success(value))) => Ok(value),
        _ => Err("expected synchronous success".into()),
    }
}

fn main() -> Result<()> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments.len() != 2 {
        return Err("usage: result_budget RETAINED_RESULTS COUNT".into());
    }
    let retained: usize = arguments[0].parse()?;
    let count: u64 = arguments[1].parse()?;
    if retained > 4096 || !(16_384..=100_000_000).contains(&count) || !count.is_multiple_of(64) {
        return Err("retained results must be at most 4096; count must be 16384..100000000 and divisible by 64".into());
    }
    let mut config = raster::config::Config::default();
    config.session.max_pending = retained.max(1);
    config.session.max_results = retained + 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, SerializedValue::new(U64ValueCodec)))
        .config(config)
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()?;
    let mut session = store.start_session(Default::default())?;
    assert_eq!(
        ready(session.upsert(Serial(0), Request).map_err(|r| r.reason)?)?,
        42
    );
    let mut tickets = Vec::with_capacity(retained);
    if retained != 0 {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        std::thread::scope(|scope| -> Result<()> {
            let store = &store;
            let holder = scope.spawn(move || -> Result<()> {
                let mut other = store.start_session(Default::default())?;
                assert_eq!(
                    ready(
                        other
                            .read(
                                Serial(0),
                                BlockingRead {
                                    entered: entered_tx,
                                    release: release_rx,
                                },
                                ReadOptions::default(),
                            )
                            .map_err(|r| r.reason)?,
                    )?,
                    42
                );
                other.close(deadline())?;
                Ok(())
            });
            entered_rx.recv_timeout(Duration::from_secs(30))?;
            let mut setup_error = None;
            for serial in 1..=retained as u64 {
                match session.read(Serial(serial), Request, ReadOptions::default()) {
                    Ok(Submission::Pending(ticket)) => tickets.push(ticket),
                    _ => {
                        setup_error = Some("blocking callback must produce accepted Pending reads");
                        break;
                    }
                }
            }
            // Always unblock and join the holder before reporting a setup error.
            release_tx.send(())?;
            holder.join().map_err(|_| "blocking reader panicked")??;
            if let Some(error) = setup_error {
                return Err(error.into());
            }
            Ok(())
        })?;
        assert!(matches!(
            session.complete_pending(WaitMode::Until(deadline()))?,
            DrainReport::Drained
        ));
    }
    let mut serial = retained as u64;
    for _ in 0..1024 {
        serial += 1;
        assert_eq!(
            ready(
                session
                    .read(Serial(serial), Request, ReadOptions::default())
                    .map_err(|r| r.reason)?,
            )?,
            42
        );
    }
    let mut checksum = 0u64;
    let mut latencies = Vec::with_capacity((count / 64) as usize);
    let started = Instant::now();
    for index in 0..count {
        serial += 1;
        let sampled = index % 64 == (index / 64) % 64;
        let sample = sampled.then(Instant::now);
        let value = ready(
            session
                .read(Serial(serial), Request, ReadOptions::default())
                .map_err(|r| r.reason)?,
        )?;
        checksum = checksum.wrapping_add(value);
        if let Some(sample) = sample {
            latencies.push(sample.elapsed().as_nanos() as u64);
        }
        if index % 256 == 255 {
            session.refresh()?;
        }
    }
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(checksum, count * 42);
    assert_eq!(latencies.len(), (count / 64) as usize);
    assert_eq!(session.last_accepted(), Some(Serial(serial)));
    let mut retained_checksum = 0u64;
    for ticket in &mut tickets {
        match ticket
            .try_take()
            .map_err(|_| "retained ticket could not be read")?
        {
            TicketState::Ready(Ok(Outcome::Success(value))) => retained_checksum += value,
            _ => return Err("retained read did not complete successfully".into()),
        }
        assert!(ticket.try_take().is_err());
    }
    assert_eq!(retained_checksum, retained as u64 * 42);
    latencies.sort_unstable();
    let p99 = latencies[(latencies.len() * 99 / 100).min(latencies.len() - 1)];
    session.close(deadline())?;
    store.shutdown(deadline())?;
    println!(
        "{{\"engine\":\"rust\",\"case\":\"retained-results/read\",\"retained\":{retained},\"count\":{count},\"elapsed_ns\":{elapsed},\"p99_ns\":{p99},\"samples\":{},\"checksum\":{checksum},\"retained_checksum\":{retained_checksum},\"last_accepted\":{serial}}}",
        latencies.len()
    );
    Ok(())
}
