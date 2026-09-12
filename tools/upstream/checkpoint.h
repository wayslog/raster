// The upstream public persistence callback does not carry the user pointer;The test process only allows one checkpoint capture to exist at the same time.
#pragma once
#include <atomic>
#include <memory>
#include "session.h"
namespace comparison {
struct CheckpointCapture {
  std::mutex mutex;
  bool want_index,want_log,index_done=false;
  std::map<uint64_t,uint64_t> serials;
  std::string error;
  CheckpointCapture(bool index,bool log):want_index(index),want_log(log) {}
};
// Use atoms shared_ptr operation,Repeated or late callbacks will also not access destroyed capture objects.
inline std::shared_ptr<CheckpointCapture> active_checkpoint;
inline std::shared_ptr<CheckpointCapture> capture() {
  auto state=std::atomic_load(&active_checkpoint);
  if(!state) throw std::runtime_error("Received persistence callback without active checkpoint");
  return state;
}
inline void index_persisted(Status status) {
  auto state=capture();std::lock_guard<std::mutex> lock(state->mutex);
  if(!state->want_index || state->index_done || status!=Status::Ok)
    state->error="Upstream index persistence callback error or duplication";
  state->index_done=true;
}
inline void log_persisted(Status status,uint64_t serial) {
  auto state=capture();std::lock_guard<std::mutex> lock(state->mutex);
  if(!state->want_log || !current_session || status!=Status::Ok ||
     !state->serials.emplace(current_session.value_or(0),serial).second)
    state->error="Upstream log persistence callback error,Missing or duplicate identity";
}
template<class Store>
using SessionMap=std::map<uint64_t,std::unique_ptr<SessionWorker<Store>>>;
template<class Store>
std::shared_ptr<CheckpointCapture> checkpoint(Store& store,SessionMap<Store>& sessions,
                                            const std::string& kind,Guid& token) {
  auto state=std::make_shared<CheckpointCapture>(kind!="log",kind!="index");
  if(std::atomic_load(&active_checkpoint)) throw std::runtime_error("Checkpoint capture is not over yet");
  std::atomic_store(&active_checkpoint,state);
  bool accepted=kind=="full" ? store.Checkpoint(index_persisted,log_persisted,token)
    : kind=="index" ? store.CheckpointIndex(index_persisted,token)
    : store.CheckpointHybridLog(log_persisted,token);
  if(!accepted) throw std::runtime_error("Checkpoint not accepted by upstream");
  const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(120);
  for(;;) {
    bool callbacks;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if(!state->error.empty()) throw std::runtime_error(state->error);
      callbacks=(!state->want_index || state->index_done) &&
        (!state->want_log || state->serials.size()==sessions.size());
    }
    // After the callback arrives, it is still confirmed that each session is observed REST,Avoid treating callback arrival as an acceptable next action.
    bool quiet=callbacks;
    for(auto& entry:sessions) quiet=entry.second->quiescent() && quiet;
    if(quiet) break;
    if(std::chrono::steady_clock::now()>=end) throw std::runtime_error("Upstream checkpoint push timeout");
    std::this_thread::yield();
  }
  std::map<uint64_t,uint64_t> accepted_serials;
  if(state->want_log) for(auto& entry:sessions)
    accepted_serials.emplace(entry.first,entry.second->snapshot().serial);
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if(!state->error.empty()) throw std::runtime_error(state->error);
    if(state->want_log) for(const auto& entry:accepted_serials) {
      auto observed=state->serials.find(entry.first);
      if(observed==state->serials.end() || observed->second!=entry.second)
        throw std::runtime_error("The upstream persistence sequence number is different from the front-boundary acceptance sequence number.");
    }
  }
  std::atomic_store(&active_checkpoint,std::shared_ptr<CheckpointCapture>{});
  return state;
}
}
