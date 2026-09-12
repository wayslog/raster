//! bounded worker thread file device;Threads only handle device requests and owned buffers.
use super::*;
#[derive(Clone, Debug)]
pub struct ThreadPoolDeviceFactory {
    /// Number of worker threads executing file requests,Must be non-zero.
    pub workers: usize,
    /// Total limit of accepted uncharged requests,It also opens the file independently./Maximum number of lock handles.
    /// Work segment binding will occupy the handle,Until recycling or equipment shutdown;Also need to leave margin for checkpoint temporary files.
    pub queue_capacity: usize,
}
impl DeviceFactory for ThreadPoolDeviceFactory {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            Ok(Box::new(backend::ThreadPoolDevice::new(self, options)?))
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = options;
            Err(Error::unimplemented("File worker thread platform"))
        }
    }
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod backend {
    use super::*;
    use crate::device::local_files::LocalFiles;
    use std::{
        collections::VecDeque,
        sync::{Arc, Condvar, Mutex},
        thread::JoinHandle,
        time::Instant,
    };
    struct Queued {
        id: IoId,
        request: IoRequest,
        cancelled: bool,
    }
    struct State {
        pending: VecDeque<Queued>,
        ready: VecDeque<IoCompletion>,
        inflight: usize,
        alive: usize,
        closed: bool,
        next: u64,
    }
    struct Shared {
        state: Mutex<State>,
        changed: Condvar,
        files: LocalFiles,
        capacity: usize,
    }
    pub(super) struct ThreadPoolDevice {
        shared: Arc<Shared>,
        threads: Mutex<Vec<JoinHandle<()>>>,
    }
    impl ThreadPoolDevice {
        pub fn new(
            factory: &ThreadPoolDeviceFactory,
            options: DeviceOpenOptions,
        ) -> Result<Self, Error> {
            if factory.workers == 0 || factory.queue_capacity == 0 {
                return Err(Error::InvalidConfig {
                    field: "thread_pool",
                    reason: "Thread count and capacity must be non-zero",
                });
            }
            let mut pending = VecDeque::new();
            let mut ready = VecDeque::new();
            pending
                .try_reserve(factory.queue_capacity)
                .map_err(|_| Error::OutOfMemory)?;
            ready
                .try_reserve(factory.queue_capacity)
                .map_err(|_| Error::OutOfMemory)?;
            let files = LocalFiles::new(options, factory.queue_capacity)?;
            let shared = Arc::new(Shared {
                state: Mutex::new(State {
                    pending,
                    ready,
                    inflight: 0,
                    alive: 0,
                    closed: false,
                    next: 0,
                }),
                changed: Condvar::new(),
                files,
                capacity: factory.queue_capacity,
            });
            let mut threads = Vec::new();
            threads
                .try_reserve(factory.workers)
                .map_err(|_| Error::OutOfMemory)?;
            for i in 0..factory.workers {
                shared
                    .state
                    .lock()
                    .map_err(|_| Error::InvalidState("device_queue_lock_poisoned"))?
                    .alive += 1;
                let worker = shared.clone();
                match std::thread::Builder::new()
                    .name(format!("raster-file-{i}"))
                    .spawn(move || run(worker))
                {
                    Ok(thread) => threads.push(thread),
                    Err(error) => {
                        {
                            let mut state = shared
                                .state
                                .lock()
                                .map_err(|_| Error::InvalidState("device_queue_lock_poisoned"))?;
                            state.alive -= 1;
                            state.closed = true;
                            shared.changed.notify_all();
                        }
                        for thread in threads {
                            let _ = thread.join();
                        }
                        return Err(Error::Io(error));
                    }
                }
            }
            Ok(Self {
                shared,
                threads: Mutex::new(threads),
            })
        }
    }
    fn run(shared: Arc<Shared>) {
        loop {
            let queued = {
                let mut state = shared
                    .state
                    .lock()
                    .expect("Worker thread control lock is not poisoned");
                while state.pending.is_empty() && !state.closed {
                    state = shared
                        .changed
                        .wait(state)
                        .expect("Worker thread wait lock is not poisoned");
                }
                if state.pending.is_empty() {
                    state.alive -= 1;
                    shared.changed.notify_all();
                    return;
                }
                let queued = state
                    .pending
                    .pop_front()
                    .expect("The pending queue is not empty");
                state.inflight += 1;
                queued
            };
            let completion = if queued.cancelled {
                let buffer = match queued.request.operation {
                    IoOperation::Read { buffer, .. } | IoOperation::Write { buffer, .. } => {
                        Some(buffer)
                    }
                    _ => None,
                };
                IoCompletion {
                    id: queued.id,
                    route: queued.request.route,
                    result: Err(Error::Io(std::io::Error::from(
                        std::io::ErrorKind::Interrupted,
                    ))),
                    buffer,
                }
            } else if let IoOperation::Cancel(target) = queued.request.operation {
                let mut state = shared
                    .state
                    .lock()
                    .expect("Worker thread control lock is not poisoned");
                if let Some(target) = state.pending.iter_mut().find(|q| q.id == target) {
                    target.cancelled = true;
                }
                IoCompletion {
                    id: queued.id,
                    route: queued.request.route,
                    result: Ok(IoOutcome::Done),
                    buffer: None,
                }
            } else {
                shared.files.execute(queued.id, queued.request)
            };
            let mut state = shared
                .state
                .lock()
                .expect("Worker thread control lock is not poisoned");
            state.inflight -= 1;
            state.ready.push_back(completion);
            shared.changed.notify_all();
        }
    }
    impl Device for ThreadPoolDevice {
        fn capabilities(&self) -> DeviceCapabilities {
            DeviceCapabilities {
                supports_files: true,
                memory_alignment: 1,
                transfer_alignment: 1,
                supports_file_sync: true,
                supports_directory_sync: true,
                supports_atomic_publish: true,
                supports_directory_listing: true,
                supports_file_locks: true,
            }
        }
        fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
            let mut state = match self.shared.state.lock() {
                Ok(state) => state,
                Err(_) => {
                    return Err(RejectedIo {
                        request,
                        reason: Error::InvalidState("device_queue_lock_poisoned"),
                    });
                }
            };
            if state.closed
                || state.pending.len() + state.ready.len() + state.inflight >= self.shared.capacity
            {
                return Err(RejectedIo {
                    request,
                    reason: if state.closed {
                        Error::InvalidState("Device is turned off")
                    } else {
                        Error::Busy
                    },
                });
            }
            let Some(next) = state.next.checked_add(1) else {
                return Err(RejectedIo {
                    request,
                    reason: Error::CapacityExceeded,
                });
            };
            if let IoOperation::Cancel(target) = &request.operation
                && let Some(queued) = state.pending.iter_mut().find(|q| q.id == *target)
            {
                queued.cancelled = true;
            }
            let id = IoId(state.next);
            state.next = next;
            state.pending.push_back(Queued {
                id,
                request,
                cancelled: false,
            });
            self.shared.changed.notify_one();
            Ok(id)
        }
        fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
            let mut state = self
                .shared
                .state
                .lock()
                .map_err(|_| Error::InvalidState("device_queue_lock_poisoned"))?;
            let n = budget.0.get().min(state.ready.len());
            output.try_reserve(n).map_err(|_| Error::OutOfMemory)?;
            for _ in 0..n {
                output.push(
                    state
                        .ready
                        .pop_front()
                        .expect("Completion queue is not empty"),
                );
            }
            Ok(())
        }
        fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
            let mut state = self
                .shared
                .state
                .lock()
                .map_err(|_| Error::InvalidState("device_queue_lock_poisoned"))?;
            state.closed = true;
            self.shared.changed.notify_all();
            while state.alive != 0 {
                let Some(remaining) = deadline.0.checked_duration_since(Instant::now()) else {
                    return Err(Error::DeadlineExceeded);
                };
                state = self
                    .shared
                    .changed
                    .wait_timeout(state, remaining)
                    .map_err(|_| Error::InvalidState("Close wait lock poisoning"))?
                    .0;
            }
            drop(state);
            for thread in self
                .threads
                .lock()
                .map_err(|_| Error::InvalidState("Thread table lock poisoning"))?
                .drain(..)
            {
                thread
                    .join()
                    .map_err(|_| Error::InvalidState("File worker thread panics"))?;
            }
            self.shared.files.close_all()
        }
    }
    impl Drop for ThreadPoolDevice {
        fn drop(&mut self) {
            if let Ok(mut state) = self.shared.state.lock() {
                state.closed = true;
                self.shared.changed.notify_all();
            }
            // Not explicit shutdown There is no infinite waiting system I/O;The worker thread still owns Shared,The buffer will not be released early.
        }
    }
}
