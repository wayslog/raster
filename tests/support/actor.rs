//! Synchronous instruction thread for testing;true Session,Not Send Requests and tickets are always created and retained in the owning thread.
//! Not automatic between commands poll,Explicitly control interleaving with the main thread;Does not bypass any public registration checks.
use std::{sync::mpsc, thread::JoinHandle, time::Duration};
type Command<T> = Box<dyn FnOnce(&mut T) + Send>;
pub(crate) struct Actor<T: 'static> {
    sender: Option<mpsc::SyncSender<Command<T>>>,
    worker: Option<JoinHandle<()>>,
}
impl<T: 'static> Actor<T> {
    pub fn new(init: impl FnOnce() -> T + Send + 'static) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<Command<T>>(1);
        let (ready, initialized) = mpsc::sync_channel(0);
        let worker = std::thread::spawn(move || {
            let mut state = init();
            ready.send(()).unwrap();
            while let Ok(command) = receiver.recv() {
                command(&mut state);
            }
        });
        initialized
            .recv_timeout(Duration::from_secs(60))
            .expect("Session thread initialization timed out or failed");
        Self {
            sender: Some(sender),
            worker: Some(worker),
        }
    }
    pub fn call<R: Send + 'static>(&self, action: impl FnOnce(&mut T) -> R + Send + 'static) -> R {
        let (result, receiver) = mpsc::sync_channel(0);
        self.sender
            .as_ref()
            .unwrap()
            .send(Box::new(move |state| {
                let value = action(state);
                let _ = result.send(value);
            }))
            .expect("Session command thread has exited");
        receiver
            .recv_timeout(Duration::from_secs(60))
            .expect("Session command timeout or thread failure")
    }
}
impl<T> Drop for Actor<T> {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !std::thread::panicking() {
                result.expect("Session command thread failed");
            }
        }
    }
}
