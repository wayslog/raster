// 每个逻辑会话使用真实线程，空闲时参与上游阶段推进；上下文与结果不跨归属线程执行。
#pragma once
#include <array>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <optional>
#include <thread>
#include "context.h"
namespace comparison {
inline thread_local std::optional<uint64_t> current_session;
struct SessionState {
  Guid guid;
  uint64_t serial=0;
  std::array<uint64_t,4> pending{};
};
template<class Store>
class SessionWorker {
 public:
  SessionWorker(Store& store,uint64_t logical,std::optional<SessionState> resumed={}):store_(store) {
    std::promise<void> initialized;
    auto ready=initialized.get_future();
    worker_=std::thread([this,logical,resumed,initialized=std::move(initialized)]() mutable {
      current_session=logical;
      bool registered=false;
      try {
        if(resumed) {
          state_.guid=resumed->guid;
          state_.serial=store_.ContinueSession(state_.guid);
          registered=true;
          if(state_.serial!=resumed->serial) throw std::runtime_error("上游续会话序号与检查点不同");
        } else {state_.guid=store_.StartSession();registered=true;}
        initialized.set_value();
      } catch(...) {
        auto error=std::current_exception();
        if(registered) store_.StopSession();
        current_session.reset();initialized.set_exception(error);return;
      }
      for(;;) {
        std::function<void()> job;
        {
          std::unique_lock<std::mutex> lock(mutex_);
          changed_.wait_for(lock,std::chrono::milliseconds(1),[this]{return stopped_ || !jobs_.empty();});
          if(stopped_ && jobs_.empty()) break;
          if(!jobs_.empty()){job=std::move(jobs_.front());jobs_.pop_front();}
        }
        if(job) job();
        store_.Refresh();
        store_.CompletePending(false);
      }
      store_.StopSession();
      current_session.reset();
    });
    try {ready.get();}
    catch(...) {worker_.join();throw;}
  }
  SessionWorker(const SessionWorker&)=delete;
  ~SessionWorker() {
    {std::lock_guard<std::mutex> lock(mutex_);stopped_=true;}
    changed_.notify_one();worker_.join();
  }
  std::string execute(Step step) {return invoke([this,step]{return apply(step);});}
  SessionState snapshot() {return invoke([this]{return state_;});}
  bool quiescent() {return invoke([this]{return store_.CompletePending(false);});}
 private:
  template<class F>
  auto invoke(F action)->decltype(action()) {
    using Result=decltype(action());
    auto task=std::make_shared<std::packaged_task<Result()>>(std::move(action));
    auto ready=task->get_future();
    {std::lock_guard<std::mutex> lock(mutex_);jobs_.push_back([task]{(*task)();});}
    changed_.notify_one();
    // 整个执行器还有外层进程期限，覆盖上游内部同步循环和 StopSession。
    return ready.get();
  }
  std::string apply(const Step& step) {
    Context context(step);
    Status status;
    size_t operation;
    if(step.operation=="read") {operation=0;status=store_.Read(context,callback,step.serial,step.option);}
    else if(step.operation=="upsert") {operation=1;status=store_.Upsert(context,callback,step.serial);}
    else if(step.operation=="rmw") {operation=2;status=store_.Rmw(context,callback,step.serial,step.option);}
    else {operation=3;status=store_.Delete(context,callback,step.serial,step.option);}
    if(status==Status::Pending) {
      ++state_.pending[operation];
      auto end=std::chrono::steady_clock::now()+std::chrono::seconds(60);
      while(context.reply->completions==0) {
        if(std::chrono::steady_clock::now()>=end) throw std::runtime_error("上游已接受请求等待超时");
        store_.CompletePending(false);store_.Refresh();std::this_thread::yield();
      }
      if(context.reply->completions!=1) throw std::runtime_error("上游异步结果没有恰好终结一次");
    } else {
      if(context.reply->completions) throw std::runtime_error("上游同步返回同时通知异步回调");
      context.reply->status=status;
    }
    state_.serial=step.serial;
    return result(step,*context.reply);
  }
  Store& store_;
  SessionState state_;
  std::mutex mutex_;
  std::condition_variable changed_;
  std::deque<std::function<void()>> jobs_;
  bool stopped_=false;
  std::thread worker_;
};
}
