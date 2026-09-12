// Upstream behavior execution end:true FasterKv data path,Three types of checkpoints,close,Recovery and session resumption.
#include <algorithm>
#include <iostream>
#include <numeric>
#include "device/null_disk.h"
#include "checkpoint.h"
namespace comparison {
using NullStore=FasterKv<Key,Value,FASTER::device::NullDisk>;
using Disk=FASTER::device::FileSystemDisk<FASTER::environment::QueueIoHandler,33554432L>;
using DiskStore=FasterKv<Key,Value,Disk>;
struct Options {
  std::string root,checkpoint="full";
  size_t split=0;
  bool split_set=false,kind_set=false;
};
template<class Store>
class Replay {
 public:
  explicit Replay(std::function<std::unique_ptr<Store>()> factory):factory_(std::move(factory)),store_(factory_()) {}
  void run(const Trace& trace,const std::string& path,const Options& options) {
    std::ofstream output(path);
    if(!output) throw std::runtime_error("Unable to create results file");
    output<<"raster-results 1 "<<trace.seed<<'\n';
    for(size_t index=0;index<trace.steps.size();++index) {
      const auto& step=trace.steps[index];
      auto& session=sessions_[step.session];
      if(!session) session=std::make_unique<SessionWorker<Store>>(*store_,step.session);
      try {output<<step.session<<' '<<step.serial<<' '<<session->execute(step)<<'\n';}
      catch(const std::exception& e){throw std::runtime_error("step "+std::to_string(index)+":"+e.what());}
      if(options.split==index+1) {
        if constexpr(std::is_same_v<Store,DiskStore>) recover_boundary(options.checkpoint);
        else throw std::runtime_error("Null Backend does not support checkpoint recovery");
      }
    }
    output.close();
    if(!output) throw std::runtime_error("Result file writing failed");
    accumulate();
    const auto begin=store_->hlog.begin_address.load().control();
    const auto tail=store_->hlog.GetTailAddress().control();
    sessions_.clear();
    std::cout<<"Upstream trace execution completed:seeds "<<trace.seed<<","<<trace.steps.size()<<" operations,Pending "
      <<std::accumulate(pending_.begin(),pending_.end(),uint64_t{0})<<'\n';
    std::cout<<"Four upstream operations Pending:["<<pending_[0]<<','<<pending_[1]<<','<<pending_[2]<<','<<pending_[3]<<"]\n";
    std::cout<<"upstream log boundary:["<<begin<<','<<tail<<"),span "<<tail-begin<<" bytes\n";
  }
 private:
  void accumulate() {
    for(auto& entry:sessions_) {
      const auto counts=entry.second->snapshot().pending;
      for(size_t i=0;i<4;++i) pending_[i]+=counts[i];
    }
  }
  void recover_boundary(const std::string& kind) {
    Guid index,log;
    if(kind=="pair") {
      checkpoint(*store_,sessions_,"index",index);
      checkpoint(*store_,sessions_,"log",log);
    } else {
      checkpoint(*store_,sessions_,"full",index);log=index;
    }
    std::map<uint64_t,SessionState> identities;
    for(auto& entry:sessions_) identities.emplace(entry.first,entry.second->snapshot());
    accumulate();sessions_.clear();store_.reset();
    store_=factory_();
    uint32_t version=0;
    std::vector<Guid> recovered;
    const auto status=store_->Recover(index,log,version,recovered);
    if(status!=Status::Ok || !version || recovered.size()!=identities.size())
      throw std::runtime_error("Upstream recovery status,Version or number of sessions does not match");
    for(const auto& entry:identities) {
      if(std::find(recovered.begin(),recovered.end(),entry.second.guid)==recovered.end())
        throw std::runtime_error("Upstream recovery missing original session identity");
      sessions_.emplace(entry.first,std::make_unique<SessionWorker<Store>>(*store_,entry.first,entry.second));
    }
    std::cout<<"Upstream trajectory boundary recovery passes:mode "<<(kind=="pair"?"Index+Log":"Full")
      <<","<<identities.size()<<" sessions_resumed,version "<<version<<'\n';
  }
  std::function<std::unique_ptr<Store>()> factory_;
  std::unique_ptr<Store> store_;
  SessionMap<Store> sessions_;
  std::array<uint64_t,4> pending_{};
};
}
int main(int argc,char** argv) {
  using namespace comparison;
  if(argc<3){std::cerr<<"Usage:upstream executor trajectory result [--disk Brand new catalog] [--split number of steps] [--checkpoint full|pair]\n";return 2;}
  try {
    Options options;
    for(int i=3;i<argc;i+=2) {
      if(i+1>=argc) throw std::runtime_error("Option missing value");
      const std::string key=argv[i];
      if(key=="--disk") options.root=argv[i+1];
      else if(key=="--split") {options.split=number(argv[i+1]);options.split_set=true;}
      else if(key=="--checkpoint") {options.checkpoint=argv[i+1];options.kind_set=true;}
      else throw std::runtime_error("Unknown executor option");
    }
    auto trace=decode(argv[1]);
    if(options.checkpoint!="full" && options.checkpoint!="pair") throw std::runtime_error("Unknown checkpoint mode");
    if((options.split_set && (!options.split || options.root.empty() || options.split>=trace.steps.size())) || (options.kind_set && !options.split_set))
      throw std::runtime_error("The recovery boundary must be inside the disk trace");
    if(!options.root.empty()) {
      if(!std::filesystem::create_directory(options.root)) throw std::runtime_error("The disk comparison directory already exists");
      // Checkpoints split the index into 256 block,Each block must be pressed Linux of 512 Byte sector alignment.
      // 2048 a 64 Byte bucket meets this public implementation prerequisite;Pure trajectory retains original observation configuration.
      const uint64_t buckets=options.split ? 2048 : 128;
      Replay<DiskStore> runner([&options,buckets]{return std::make_unique<DiskStore>(buckets,256_MiB,options.root,0.4);});
      runner.run(trace,argv[2],options);
    } else {
      Replay<NullStore> runner([]{return std::make_unique<NullStore>(128,1_GiB,"",0.9);});
      runner.run(trace,argv[2],options);
    }
  } catch(const std::exception& e){std::cerr<<"Upstream control failed:"<<e.what()<<'\n';return 1;}
}
