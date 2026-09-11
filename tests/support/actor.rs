//! 测试用同步指令线程；真实 Session、非 Send 请求及票据始终创建和保留在所属线程。
//! 指令间不自动 poll，用主线程明确控制交错；不绕过任何公开注册检查。
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
            .expect("会话线程初始化超时或失败");
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
            .expect("会话指令线程已退出");
        receiver
            .recv_timeout(Duration::from_secs(60))
            .expect("会话指令超时或线程失败")
    }
}
impl<T> Drop for Actor<T> {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !std::thread::panicking() {
                result.expect("会话指令线程失败");
            }
        }
    }
}
