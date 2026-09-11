// 上游行为执行端：逐操作使用真实 FasterKv，输出可比较的业务结果。
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <thread>
#include "device/null_disk.h"
#include "context.h"
namespace comparison {
using NullStore=FasterKv<Key,Value,FASTER::device::NullDisk>;
using Disk=FASTER::device::FileSystemDisk<FASTER::environment::QueueIoHandler,33554432L>;
using DiskStore=FasterKv<Key,Value,Disk>;
template<class Store>
class SessionWorker {
 public:
  explicit SessionWorker(Store& store):store_(store) {
    std::promise<void> initialized;
    auto ready=initialized.get_future();
    worker_=std::thread([this,initialized=std::move(initialized)]() mutable {
      try { store_.StartSession(); initialized.set_value(); }
      catch(...) { initialized.set_exception(std::current_exception()); return; }
      for(;;) {
        std::function<void()> job;
        {
          std::unique_lock<std::mutex> lock(mutex_);
          changed_.wait_for(lock,std::chrono::milliseconds(1),[this]{return stopped_ || !jobs_.empty();});
          if(stopped_ && jobs_.empty()) break;
          if(!jobs_.empty()){ job=std::move(jobs_.front()); jobs_.pop_front(); }
        }
        if(job) job();
        store_.Refresh();
        store_.CompletePending(false);
      }
      store_.StopSession();
    });
    try { ready.get(); }
    catch(...) { worker_.join(); throw; }
  }
  SessionWorker(const SessionWorker&)=delete;
  ~SessionWorker() {
    {std::lock_guard<std::mutex> lock(mutex_);stopped_=true;}
    changed_.notify_one();
    worker_.join();
  }
  std::string execute(Step step) {
    auto task=std::make_shared<std::packaged_task<std::string()>>([this,step]{return apply(step);});
    auto ready=task->get_future();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      jobs_.push_back([task]{(*task)();});
    }
    changed_.notify_one();
    return ready.get();
  }
  uint64_t pending=0;
 private:
  std::string apply(const Step& step) {
    Context context(step);
    Status status;
    if(step.operation=="read") status=store_.Read(context,callback,step.serial,step.option);
    else if(step.operation=="upsert") status=store_.Upsert(context,callback,step.serial);
    else if(step.operation=="rmw") status=store_.Rmw(context,callback,step.serial,step.option);
    else status=store_.Delete(context,callback,step.serial,step.option);
    if(status==Status::Pending) {
      ++pending;
      auto end=std::chrono::steady_clock::now()+std::chrono::seconds(60);
      while(context.reply->completions==0) {
        if(std::chrono::steady_clock::now()>=end) throw std::runtime_error("上游已接受请求等待超时");
        store_.CompletePending(false); store_.Refresh(); std::this_thread::yield();
      }
      if(context.reply->completions!=1) throw std::runtime_error("上游异步结果没有恰好终结一次");
    } else {
      if(context.reply->completions) throw std::runtime_error("上游同步返回同时通知异步回调");
      context.reply->status=status;
    }
    return result(step,*context.reply);
  }
  Store& store_;
  std::mutex mutex_;
  std::condition_variable changed_;
  std::deque<std::function<void()>> jobs_;
  bool stopped_=false;
  std::thread worker_;
};
template<class Store>
void replay(Store& store,const Trace& trace,const std::string& result_path) {
    std::map<uint64_t,std::unique_ptr<SessionWorker<Store>>> sessions;
    std::ofstream output(result_path);
    if(!output) throw std::runtime_error("无法创建结果文件");
    output<<"raster-results 1 "<<trace.seed<<'\n';
    for(size_t index=0;index<trace.steps.size();++index) {
      const auto& step=trace.steps[index];
      auto& session=sessions[step.session];
      if(!session) session=std::make_unique<SessionWorker<Store>>(store);
      try { output<<step.session<<' '<<step.serial<<' '<<session->execute(step)<<'\n'; }
      catch(const std::exception& e){throw std::runtime_error("步骤 "+std::to_string(index)+"："+e.what());}
    }
    output.close();
    if(!output) throw std::runtime_error("结果文件写入失败");
    uint64_t pending=0;
    for(const auto& entry:sessions) pending+=entry.second->pending;
    sessions.clear();
    std::cout<<"上游轨迹执行完成：种子 "<<trace.seed<<"，"<<trace.steps.size()<<" 次操作，Pending "<<pending<<"\n";
}
}
int main(int argc,char** argv) {
  using namespace comparison;
  if(argc!=3 && !(argc==5 && std::string(argv[3])=="--disk")) {
    std::cerr<<"用法：上游执行器 轨迹文件 结果文件 [--disk 全新目录]\n";return 2;
  }
  try {
    auto trace=decode(argv[1]);
    if(argc==5) {
      if(!std::filesystem::create_directory(argv[4])) throw std::runtime_error("磁盘对照目录已经存在");
      DiskStore store{128,256_MiB,argv[4],0.4};
      replay(store,trace,argv[2]);
    } else {
      NullStore store{128,1_GiB,"",0.9};
      replay(store,trace,argv[2]);
    }
  } catch(const std::exception& e){std::cerr<<"上游对照失败："<<e.what()<<'\n';return 1;}
}
