// 上游行为执行端：真实 FasterKv 数据路径、三种检查点、关闭、恢复及会话续接。
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
    if(!output) throw std::runtime_error("无法创建结果文件");
    output<<"raster-results 1 "<<trace.seed<<'\n';
    for(size_t index=0;index<trace.steps.size();++index) {
      const auto& step=trace.steps[index];
      auto& session=sessions_[step.session];
      if(!session) session=std::make_unique<SessionWorker<Store>>(*store_,step.session);
      try {output<<step.session<<' '<<step.serial<<' '<<session->execute(step)<<'\n';}
      catch(const std::exception& e){throw std::runtime_error("步骤 "+std::to_string(index)+"："+e.what());}
      if(options.split==index+1) {
        if constexpr(std::is_same_v<Store,DiskStore>) recover_boundary(options.checkpoint);
        else throw std::runtime_error("Null 后端不支持检查点恢复");
      }
    }
    output.close();
    if(!output) throw std::runtime_error("结果文件写入失败");
    accumulate();
    const auto begin=store_->hlog.begin_address.load().control();
    const auto tail=store_->hlog.GetTailAddress().control();
    sessions_.clear();
    std::cout<<"上游轨迹执行完成：种子 "<<trace.seed<<"，"<<trace.steps.size()<<" 次操作，Pending "
      <<std::accumulate(pending_.begin(),pending_.end(),uint64_t{0})<<'\n';
    std::cout<<"上游四操作 Pending：["<<pending_[0]<<','<<pending_[1]<<','<<pending_[2]<<','<<pending_[3]<<"]\n";
    std::cout<<"上游日志边界：["<<begin<<','<<tail<<")，跨度 "<<tail-begin<<" 字节\n";
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
      throw std::runtime_error("上游恢复状态、版本或会话数量不符");
    for(const auto& entry:identities) {
      if(std::find(recovered.begin(),recovered.end(),entry.second.guid)==recovered.end())
        throw std::runtime_error("上游恢复缺少原会话身份");
      sessions_.emplace(entry.first,std::make_unique<SessionWorker<Store>>(*store_,entry.first,entry.second));
    }
    std::cout<<"上游轨迹边界恢复通过：模式 "<<(kind=="pair"?"Index+Log":"Full")
      <<"，"<<identities.size()<<" 个会话续接，版本 "<<version<<'\n';
  }
  std::function<std::unique_ptr<Store>()> factory_;
  std::unique_ptr<Store> store_;
  SessionMap<Store> sessions_;
  std::array<uint64_t,4> pending_{};
};
}
int main(int argc,char** argv) {
  using namespace comparison;
  if(argc<3){std::cerr<<"用法：上游执行器 轨迹 结果 [--disk 全新目录] [--split 步数] [--checkpoint full|pair]\n";return 2;}
  try {
    Options options;
    for(int i=3;i<argc;i+=2) {
      if(i+1>=argc) throw std::runtime_error("选项缺少值");
      const std::string key=argv[i];
      if(key=="--disk") options.root=argv[i+1];
      else if(key=="--split") {options.split=number(argv[i+1]);options.split_set=true;}
      else if(key=="--checkpoint") {options.checkpoint=argv[i+1];options.kind_set=true;}
      else throw std::runtime_error("未知执行器选项");
    }
    auto trace=decode(argv[1]);
    if(options.checkpoint!="full" && options.checkpoint!="pair") throw std::runtime_error("未知检查点模式");
    if((options.split_set && (!options.split || options.root.empty() || options.split>=trace.steps.size())) || (options.kind_set && !options.split_set))
      throw std::runtime_error("恢复边界必须位于磁盘轨迹内部");
    if(!options.root.empty()) {
      if(!std::filesystem::create_directory(options.root)) throw std::runtime_error("磁盘对照目录已经存在");
      // 检查点把索引拆成 256 块，每块必须按 Linux 的 512 字节扇区对齐。
      // 2048 个 64 字节桶满足该公开实现前提；纯轨迹保留原观察配置。
      const uint64_t buckets=options.split ? 2048 : 128;
      Replay<DiskStore> runner([&options,buckets]{return std::make_unique<DiskStore>(buckets,256_MiB,options.root,0.4);});
      runner.run(trace,argv[2],options);
    } else {
      Replay<NullStore> runner([]{return std::make_unique<NullStore>(128,1_GiB,"",0.9);});
      runner.run(trace,argv[2],options);
    }
  } catch(const std::exception& e){std::cerr<<"上游对照失败："<<e.what()<<'\n';return 1;}
}
