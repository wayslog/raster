// Buffered F2 benchmark paired with examples/f2_bench.rs.
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include "core/f2.h"
#include "device/file_system_disk.h"

using namespace FASTER::core;
using Clock = std::chrono::steady_clock;
using base_disk_t = FASTER::device::FileSystemDisk<FASTER::environment::QueueIoHandler, 1_GiB>;
class Disk : public base_disk_t {
 public:
  Disk(const std::string& path, LightEpoch& epoch, const std::string& config)
      : base_disk_t(path, epoch, config, false, false) {}
};

constexpr uint64_t Buckets = 1ull << 18;

struct Key {
  uint64_t key;
  uint32_t size() const { return sizeof(Key); }
  KeyHash GetHash() const { return KeyHash{(key & (Buckets - 1)) | ((key >> 18) << 48)}; }
  bool operator==(const Key& other) const { return key == other.key; }
  bool operator!=(const Key& other) const { return !(*this == other); }
};

template <size_t N>
struct Payload {
  union { uint64_t word; std::atomic<uint64_t> atomic; };
  uint64_t pad[N];
  static constexpr uint32_t size() { return sizeof(Payload); }
  void Set(uint64_t value) { word = value; std::fill(pad, pad + N, 0); }
};

struct Completion { Status status = Status::Pending; uint64_t result = 0; };

template <size_t N>
struct Request : IAsyncContext {
  using key_t = Key;
  using value_t = Payload<N>;
  Key key_;
  uint64_t result = 0;
  Completion* completion;
  Request(uint64_t key, Completion& done) : key_{key}, completion(&done) {}
  const Key& key() const { return key_; }
  static constexpr uint32_t value_size() { return sizeof(value_t); }
  static constexpr uint32_t value_size(const value_t&) { return sizeof(value_t); }
  void Put(value_t& value) { value.Set(key_.key + 1); }
  bool PutAtomic(value_t&) { return false; }
  void Get(const value_t& value) { result = value.word; }
  void GetAtomic(const value_t& value) { result = value.atomic.load(std::memory_order_relaxed); }
  void RmwInitial(value_t& value) { result = 1; value.Set(result); }
  void RmwCopy(const value_t& old, value_t& value) { result = old.word + 1; value.Set(result); }
  bool RmwAtomic(value_t& value) { result = value.atomic.fetch_add(1) + 1; return true; }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& copy) override { return IAsyncContext::DeepCopy_Internal(*this, copy); }
};

void require(bool condition) { if (!condition) throw std::runtime_error("F2 probe invariant"); }

template <size_t N>
std::unique_ptr<F2Kv<Key, Payload<N>, Disk>> open_store(const std::string& root) {
  using Store = F2Kv<Key, Payload<N>, Disk>;
  std::filesystem::create_directories(root + "/hot");
  std::filesystem::create_directories(root + "/cold");
  auto policy = DEFAULT_F2_COMPACTION_CONFIG;
  policy.hot_store.enabled = policy.cold_store.enabled = false;
  typename Store::ColdIndexConfig index{Buckets, 256_MiB, 0.5};
  return std::make_unique<Store>(typename Store::HotIndexConfig{Buckets}, 256_MiB,
      root + "/hot/", index, 256_MiB, root + "/cold/", 0.5, 0.5,
      DEFAULT_READ_CACHE_CONFIG, policy);
}

template <size_t N>
void run(const std::string& root, uint64_t records, uint64_t requests, bool rmw) {
  auto owner = open_store<N>(root);
  auto& store = *owner;
  auto callback = [](IAsyncContext* raw, Status status) {
    CallbackContext<Request<N>> request{raw};
    *request->completion = {status, request->result};
  };
  store.StartSession();
  for (uint64_t key = 0; key < records; ++key) {
    Completion done;
    Request<N> request{key, done};
    auto status = store.cold_store.Upsert(request, callback, key + 1);
    if (status == Status::Pending) {
      store.CompletePending(true);
      status = done.status;
    }
    require(status == Status::Ok);
  }
  store.StopSession();

  store.StartSession();
  uint64_t pending = 0, checksum = 0;
  std::vector<uint64_t> samples;
  auto started = Clock::now();
  for (uint64_t i = 0; i < requests; ++i) {
    uint64_t key = (i * 17) & (rmw ? 8191 : 4095);
    Completion done;
    Request<N> request{key, done};
    bool sampled = (i & 15) == ((i >> 4) & 15);
    Clock::time_point clock;
    if (sampled) clock = Clock::now();
    auto status = rmw ? store.Rmw(request, callback, i + 1) : store.Read(request, callback, i + 1);
    uint64_t result = request.result;
    if (status == Status::Pending) {
      ++pending;
      store.CompletePending(true);
      status = done.status;
      result = done.result;
    }
    if (sampled) samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - clock).count());
    require(status == Status::Ok && result == key + 1 + rmw);
    checksum += result;
    if ((i & 255) == 255) store.Refresh();
  }
  auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count();
  store.StopSession();
  std::sort(samples.begin(), samples.end());
  std::cout << "{\"elapsed_ns\":" << elapsed << ",\"requests\":" << requests << ",\"p99_ns\":" << samples[samples.size() * 99 / 100]
            << ",\"pending\":" << pending << ",\"checksum\":" << checksum << "}" << std::endl;
}

std::atomic<Status> checkpoint_status{Status::Pending};

void migrate(const std::string& root) {
  constexpr size_t N = 511;
  auto owner = open_store<N>(root);
  auto& store = *owner;
  auto callback = [](IAsyncContext* raw, Status status) {
    CallbackContext<Request<N>> request{raw};
    *request->completion = {status, request->result};
  };
  store.StartSession();
  for (uint64_t key = 0; key < 81920; ++key) {
    Completion done;
    Request<N> request{key, done};
    require(store.Upsert(request, callback, key + 1) == Status::Ok);
  }
  const Address cutoff{2, 0};
  require(store.hot_store.hlog.safe_read_only_address.load() >= cutoff);
  auto data_start = store.cold_store.hlog.GetTailAddress();
  auto index_start = store.cold_store.hash_index_.store_->hlog.GetTailAddress();
  auto started = Clock::now();
  require(store.CompactHotLog(cutoff.control(), true, 1));
  auto migration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count();
  auto data_bytes = store.cold_store.hlog.GetTailAddress().control() - data_start.control();
  auto index_bytes = store.cold_store.hash_index_.store_->hlog.GetTailAddress().control() - index_start.control();
  Guid token;
  auto checkpoint_started = Clock::now();
  require(store.Checkpoint([](Status status, uint64_t) { checkpoint_status.store(status); }, token, false));
  while (checkpoint_status.load() == Status::Pending) {
    store.CompletePending(false);
    std::this_thread::yield();
  }
  require(checkpoint_status.load() == Status::Ok);
  auto checkpoint_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - checkpoint_started).count();
  store.StopSession();
  owner.reset();

  auto recovered = open_store<N>(root);
  uint32_t version = 0;
  std::vector<Guid> sessions;
  auto recover_started = Clock::now();
  require(recovered->Recover(token, version, sessions) == Status::Ok && sessions.size() == 1);
  auto recover_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - recover_started).count();
  recovered->StartSession();
  uint64_t checksum = 0;
  for (uint64_t key = 0; key < 81920; key += 7) {
    Completion done;
    Request<N> request{key, done};
    auto status = recovered->Read(request, callback, key + 1);
    uint64_t result = request.result;
    if (status == Status::Pending) {
      recovered->CompletePending(true);
      status = done.status;
      result = done.result;
    }
    require(status == Status::Ok && result == key + 1);
    checksum += result;
  }
  recovered->StopSession();
  std::cout << "{\"migration_ns\":" << migration_ns << ",\"checkpoint_ns\":" << checkpoint_ns
            << ",\"recover_ns\":" << recover_ns << ",\"data_bytes\":" << data_bytes
            << ",\"index_bytes\":" << index_bytes << ",\"checksum\":" << checksum << "}" << std::endl;
}

int main(int argc, char* argv[]) {
  require(argc == 4);
  std::string workload = argv[1], root = argv[2];
  uint64_t requests = std::stoull(argv[3]);
  require(requests > 0 && (workload != "rmw" || requests <= 8192));
  if (workload == "migrate") migrate(root);
  else if (workload == "resident") run<0>(root, 8192, requests, false);
  else if (workload == "index") run<0>(root, 1200000, requests, false);
  else if (workload == "both") run<63>(root, 1200000, requests, false);
  else if (workload == "rmw") run<63>(root, 1200000, requests, true);
  else throw std::runtime_error("unknown workload");
}
