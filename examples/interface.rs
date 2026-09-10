//! 只验证接口组合，不创建伪造的运行中引擎。
#![allow(
    dead_code,
    reason = "这些泛型函数用于编译期接口组合验证，不在骨架阶段执行"
)]
use raster::{
    RasterKV, Session, Submission, Ticket,
    api::{
        TicketState,
        operation::{ReadOperation, ReadOptions},
    },
    schema::Schema,
    types::*,
};

fn assert_shared<S: Schema>() {
    fn require<T: Send + Sync>() {}
    require::<RasterKV<S>>();
}

fn submit_two<S, A, B>(session: &mut Session<S>, first: A, second: B)
where
    S: Schema,
    A: ReadOperation<S>,
    B: ReadOperation<S>,
{
    let one = session.read(Serial(1), first, ReadOptions::default());
    let two = session.read(Serial(2), second, ReadOptions::default());
    let _both = (one, two);
}

fn receive_after_close<T: 'static>(ticket: &mut Ticket<T>) -> Result<TicketState<T>, TicketError> {
    ticket.try_take()
}

fn inspect<S: Schema>(session: &mut Session<S>, result: Submission<u64>) {
    if let Submission::Pending(mut ticket) = result {
        let _result = session.try_take(&mut ticket);
    }
}

fn main() {
    // 泛型函数由编译器检查，但不在没有引擎实现时执行。
    println!("RasterKV 接口骨架已载入；存储执行与持久化仍待实现。");
}
