// Use real threads per logical session,Participate in upstream stage advancement during free time;Context and results are not executed across owning threads.
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
          if(state_.serial!=resumed->serial) throw std::runtime_error("The upstream continued session sequence number is different from the checkpoint");
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
    // The entire executor also has outer process deadlines,Override the upstream inner sync loop and StopSession.
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
        if(std::chrono::steady_clock::now()>=end) throw std::runtime_error("The upstream accepted the request and waited for timeout");
        store_.CompletePending(false);store_.Refresh();std::this_thread::yield();
      }
      if(context.reply->completions!=1) throw std::runtime_error("The upstream asynchronous result did not end exactly once");
    } else {
      if(context.reply->completions) throw std::runtime_error("The upstream returns synchronously and notifies the asynchronous callback.");
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
