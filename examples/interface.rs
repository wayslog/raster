//! Real cold page requests show heterogeneous tickets,Jump number,refuse to return,Result ownership after timeout and session closure.
use raster::{
    RasterKV, Session, Submission,
    api::{Outcome, TicketState, operation::*},
    config::Config,
    device::memory::MemoryDeviceFactory,
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(30))
}
#[derive(Debug)]
struct Put(u64);
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = ();
    fn replacement(&mut self) -> std::result::Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> std::result::Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[derive(Debug)]
struct Number {
    key: u64,
    callbacks: Rc<Cell<usize>>,
}
impl Keyed<Schema> for Number {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadOperation<Schema> for Number {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> std::result::Result<u64, Error> {
        self.callbacks.set(self.callbacks.get() + 1);
        Ok(*value.view())
    }
}
#[derive(Debug)]
struct Text(Number);
impl Keyed<Schema> for Text {
    fn key(&self) -> &u64 {
        &self.0.key
    }
}
impl ReadOperation<Schema> for Text {
    type Output = String;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> std::result::Result<String, Error> {
        self.0.callbacks.set(self.0.callbacks.get() + 1);
        Ok(format!("the_value_is{}", value.view()))
    }
}
fn take<T: 'static>(
    session: &mut Session<Schema>,
    submission: Submission<T>,
) -> Result<Outcome<T>> {
    Ok(match submission {
        Submission::Ready(result) => result?,
        Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline())??,
    })
}
fn demo(store: &RasterKV<Schema>) -> Result<()> {
    let mut session = store.start_session(Default::default())?;
    for n in 0..400 {
        let submitted = session
            .upsert(Serial(n), Put(n))
            .map_err(|rejected| rejected.reason)?;
        if !matches!(take(&mut session, submitted)?, Outcome::Success(())) {
            return Err("Initial write failed".into());
        }
    }
    let callbacks = Rc::new(Cell::new(0));
    let number = session
        .read(
            Serial(400),
            Number {
                key: 0,
                callbacks: callbacks.clone(),
            },
            ReadOptions::default(),
        )
        .map_err(|rejected| rejected.reason)?;
    let text = session
        .read(
            Serial(410),
            Text(Number {
                key: 1,
                callbacks: callbacks.clone(),
            }),
            ReadOptions::default(),
        )
        .map_err(|rejected| rejected.reason)?;
    let (Submission::Pending(mut number), Submission::Pending(mut text)) = (number, text) else {
        return Err(
            "Small memory configuration does not produce the expected two cold page hangs".into(),
        );
    };
    let rejected = match session.read(
        Serial(405),
        Number {
            key: 99,
            callbacks: callbacks.clone(),
        },
        ReadOptions::default(),
    ) {
        Err(rejected) => rejected,
        Ok(_) => return Err("Backward sequence number unexpectedly accepted".into()),
    };
    if rejected.request.key != 99
        || session.last_accepted() != Some(Serial(410))
        || callbacks.get() != 0
    {
        return Err(
            "Rejecting an incomplete return request or changing the acceptance schedule".into(),
        );
    }
    // Expired deadlines do not consume bills that are still in transit.,nor resubmit the read.
    if !matches!(
        session.wait(&mut number, Deadline(Instant::now())),
        Err(Error::DeadlineExceeded)
    ) {
        return Err("Expected bounded wait timeout".into());
    }
    if !matches!(number.try_take(), Ok(TicketState::Pending)) {
        return Err("Timeout changed ticket status".into());
    }
    let before = store.diagnostics()?;
    if before.pending_requests != 2 {
        return Err("Number of hangs in diagnostics does not match".into());
    }
    session.refresh()?;
    let _progress = session.complete_pending(WaitMode::Once)?;
    session.close(deadline())?;
    drop(session);
    let number = match number
        .try_take()
        .map_err(|e| format!("Digital ticket error:{e:?}"))?
    {
        TicketState::Ready(result) => result?,
        TicketState::Pending => return Err("Request not completed after session closed".into()),
    };
    let output = match text
        .try_take()
        .map_err(|e| format!("Text ticket error:{e:?}"))?
    {
        TicketState::Ready(result) => result?,
        TicketState::Pending => {
            return Err("Text remains unfinished after session is closed".into());
        }
    };
    if !matches!(number, Outcome::Success(0))
        || !matches!(output, Outcome::Success(ref value) if value == "the_value_is1")
        || callbacks.get() != 2
    {
        return Err("Heterogeneous results or callback number mismatch".into());
    }
    if !matches!(text.try_take(), Err(TicketError::AlreadyTaken)) {
        return Err("The same bill was collected repeatedly".into());
    }
    let after = store.diagnostics()?;
    if after.active_sessions != 0 || after.active_requests != 0 {
        return Err("There are still active sessions or requests after closing".into());
    }
    println!(
        "Public ticket life cycle passed:Twice isomerism Pending,Jump number accepted,Backward rejection,No cancellation after timeout,Receive owned results after closing,Once per callback."
    );
    Ok(())
}
fn main() -> Result<()> {
    fn shared<T: Send + Sync>() {}
    shared::<RasterKV<Schema>>();
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(MemoryDeviceFactory))
        .create()?;
    let result = demo(&store);
    let cleanup = store.shutdown(deadline());
    match (result, cleanup) {
        (Ok(()), Ok(_)) => Ok(()),
        (Err(error), Ok(_)) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Err(error), Err(cleanup)) => {
            Err(format!("Example fails:{error};Closing failed:{cleanup}").into())
        }
    }
}
