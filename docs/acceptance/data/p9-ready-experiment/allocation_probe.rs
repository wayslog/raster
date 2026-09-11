//! 诊断用累计分配事件，只统计预热后的同步热点写入；计数器成本不用于性能定时。
use raster::{RasterKV,Submission,api::{completion::Outcome,operation::*},schema::{ValueUpdate,builtin::{SchemaPair,U64Key,AtomicU64Value}},types::*};
use std::{alloc::{GlobalAlloc,Layout,System},sync::atomic::{AtomicBool,AtomicU64,Ordering::SeqCst},time::{Instant,Duration}};
static ENABLED:AtomicBool=AtomicBool::new(false);
static ALLOCATIONS:AtomicU64=AtomicU64::new(0);
static RELEASES:AtomicU64=AtomicU64::new(0);
static BYTES:AtomicU64=AtomicU64::new(0);
static SIZES:[AtomicU64;1025]=[const{AtomicU64::new(0)};1025];
struct Counter;
fn record(size:usize){if ENABLED.load(SeqCst){ALLOCATIONS.fetch_add(1,SeqCst);BYTES.fetch_add(size as u64,SeqCst);SIZES[size.min(1024)].fetch_add(1,SeqCst);}}
// SAFETY: 指针及布局原样交给 System；统计只调用无分配原子操作。
unsafe impl GlobalAlloc for Counter{
 unsafe fn alloc(&self,l:Layout)->*mut u8{
  // SAFETY: 由调用者保证布局满足分配契约。
  let p=unsafe{System.alloc(l)};if !p.is_null(){record(l.size());}p
 }
 unsafe fn alloc_zeroed(&self,l:Layout)->*mut u8{
  // SAFETY: 由调用者保证布局满足分配契约。
  let p=unsafe{System.alloc_zeroed(l)};if !p.is_null(){record(l.size());}p
 }
 unsafe fn dealloc(&self,p:*mut u8,l:Layout){
  // SAFETY: 原始存活指针和配对布局原样传递。
  unsafe{System.dealloc(p,l)};if ENABLED.load(SeqCst){RELEASES.fetch_add(1,SeqCst);}
 }
 unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8{
  // SAFETY: 原始指针、布局和合法新大小由调用者提供。
  let p=unsafe{System.realloc(p,l,n)};if !p.is_null(){record(n);}p
 }
}
#[global_allocator]static GLOBAL:Counter=Counter;
type S=SchemaPair<U64Key,AtomicU64Value>;
struct Request(u64);
impl Keyed<S> for Request{fn key(&self)->&u64{&1}}
impl UpsertOperation<S> for Request{
 type Output=u64;
 fn replacement(&mut self)->Result<(u64,u64),Error>{Ok((self.0,self.0))}
 fn update_in_place(&mut self,mut v:ValueUpdate<'_,S>)->Result<UpdateDecision<u64>,Error>{v.view_mut().store(self.0,SeqCst);Ok(UpdateDecision::Updated(self.0))}
}
fn deadline()->Deadline{Deadline(Instant::now()+Duration::from_secs(5))}
fn main(){
 let store=RasterKV::builder(SchemaPair::new(U64Key,AtomicU64Value)).device(Box::new(raster::device::null::NullDeviceFactory)).create().unwrap();
 let mut session=store.start_session(Default::default()).unwrap();
 for i in 0..128{assert!(matches!(session.upsert(Serial(i),Request(i)).map_err(|r|r.reason).unwrap(),Submission::Ready(Ok(Outcome::Success(n))) if n==i));}
 ENABLED.store(true,SeqCst);
 for i in 128..200128{assert!(matches!(session.upsert(Serial(i),Request(i)).map_err(|r|r.reason).unwrap(),Submission::Ready(Ok(Outcome::Success(n))) if n==i));}
 ENABLED.store(false,SeqCst);
 println!("操作数,分配事件,释放事件,分配字节\n200000,{},{},{}",ALLOCATIONS.load(SeqCst),RELEASES.load(SeqCst),BYTES.load(SeqCst));
 for (size,count) in SIZES.iter().enumerate(){let n=count.load(SeqCst);if n!=0{eprintln!("分配大小桶,{size},{n}");}}
 session.close(deadline()).unwrap();store.shutdown(deadline()).unwrap();
}
