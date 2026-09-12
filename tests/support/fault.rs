//! Failure scripts and request lifecycle oracles,Not yet connected to the real engine.
use raster::types::Effect;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    BeforeAccept,
    AfterAccept,
    Compute,
    MayApply,
    Notify,
    IoSubmit,
    IoComplete,
    MaterialSync,
    CommitPublish,
}
impl Event {
    pub const ALL: [Self; 9] = [
        Self::BeforeAccept,
        Self::AfterAccept,
        Self::Compute,
        Self::MayApply,
        Self::Notify,
        Self::IoSubmit,
        Self::IoComplete,
        Self::MaterialSync,
        Self::CommitPublish,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::BeforeAccept => "request.before_accept",
            Self::AfterAccept => "request.after_accept",
            Self::Compute => "user.compute",
            Self::MayApply => "user.may_apply",
            Self::Notify => "result.notify",
            Self::IoSubmit => "io.submit",
            Self::IoComplete => "io.complete",
            Self::MaterialSync => "checkpoint.material_sync",
            Self::CommitPublish => "checkpoint.commit_publish",
        }
    }
}
pub struct FaultScript {
    event: Event,
    occurrence: usize,
    seen: usize,
    fired: bool,
}
impl FaultScript {
    pub fn new(event: Event, occurrence: usize) -> Result<Self, &'static str> {
        if occurrence == 0 {
            return Err("Number of failures from the beginning");
        }
        Ok(Self {
            event,
            occurrence,
            seen: 0,
            fired: false,
        })
    }
    pub fn hit(&mut self, event: Event) -> bool {
        if event != self.event || self.fired {
            return false;
        }
        self.seen = self.seen.saturating_add(1);
        self.fired = self.seen == self.occurrence;
        self.fired
    }
}
#[derive(Default)]
pub struct Lifecycle {
    accepted: bool,
    terminal: bool,
    may_apply: bool,
    pub failed: bool,
    pub attempts: usize,
}
impl Lifecycle {
    pub fn accept(&mut self) -> Result<(), &'static str> {
        if self.accepted {
            return Err("Requests cannot be accepted repeatedly");
        }
        self.accepted = true;
        self.attempts = 1;
        Ok(())
    }
    pub fn retry_computation(&mut self) -> Result<(), &'static str> {
        if !self.accepted || self.terminal || self.failed || self.may_apply {
            return Err("The request cannot be retried");
        }
        self.attempts += 1;
        Ok(())
    }
    pub fn finish(&mut self, effect: Effect, error: bool) -> Result<(), &'static str> {
        if !self.accepted || self.terminal {
            return Err("A request can only be terminated once after acceptance");
        }
        self.terminal = true;
        self.failed = error && effect != Effect::NotApplied;
        Ok(())
    }
    pub fn begin_mutation(&mut self) -> Result<(), &'static str> {
        if !self.accepted || self.terminal || self.may_apply {
            return Err("Can't start modification");
        }
        self.may_apply = true;
        Ok(())
    }
}
