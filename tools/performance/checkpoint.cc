// Checkpoint and recovery benchmark, paired with examples/checkpoint.rs.
//   checkpoint run THREADS KEYS OPS DIR       -> takes a full fold-over checkpoint
//   checkpoint recover THREADS KEYS TOKEN DIR -> recovers it and verifies values
// Values start at key and every RMW adds 1, so after recovery the sum of all
// values must equal sum(keys) + sum(persisted serial numbers).
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>
#include "core/faster.h"
#include "device/file_system_disk.h"
using namespace FASTER::core;
using Clock = std::chrono::steady_clock;
using handler_t = FASTER::environment::QueueIoHandler;
using base_disk_t = FASTER::device::FileSystemDisk<handler_t, 1073741824ull>;
class disk_t : public base_disk_t {
 public:
  disk_t(const std::string& path, LightEpoch& epoch, const std::string& config)
    : base_disk_t(path, epoch, config, false, /*unbuffered=*/false) {}
};

struct Key {
  uint64_t value;
  uint32_t size() const { return sizeof(Key); }
  KeyHash GetHash() const {
    uint64_t hash = 0xcbf29ce484222325ULL;
    for (unsigned i = 0; i < 8; ++i) hash = (hash ^ ((value >> (8 * i)) & 255)) * 0x100000001b3ULL;
    return KeyHash{hash};
  }
  bool operator==(const Key& other) const { return value == other.value; }
  bool operator!=(const Key& other) const { return !(*this == other); }
};
struct Value {
  union { uint64_t value; std::atomic<uint64_t> atomic_value; };
  static constexpr uint32_t size() { return sizeof(Value); }
};
class Request : public IAsyncContext {
 public:
  using key_t = Key;
  using value_t = Value;
  Key key_;
  uint64_t input, output = 0;
  uint64_t* sum;
  Request(uint64_t key, uint64_t value, uint64_t* sum_) : key_{key}, input(value), sum(sum_) {}
  Request(const Request& other) = default;
  const Key& key() const { return key_; }
  static constexpr uint32_t value_size() { return sizeof(Value); }
  static constexpr uint32_t value_size(const Value&) { return sizeof(Value); }
  void Put(Value& value) { value.value = input; }
  bool PutAtomic(Value& value) { value.atomic_value.store(input); return true; }
  void Get(const Value& value) { output = value.value; }
  void GetAtomic(const Value& value) { output = value.atomic_value.load(); }
  void RmwInitial(Value& value) { value.value = input; }
  void RmwCopy(const Value& old_value, Value& value) { value.value = old_value.value + input; }
  bool RmwAtomic(Value& value) { value.atomic_value.fetch_add(input); return true; }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& copy) override { return IAsyncContext::DeepCopy_Internal(*this, copy); }
};
using Store = FasterKv<Key, Value, disk_t>;

void require(bool condition) { if (!condition) throw std::runtime_error("benchmark contract failed"); }
void read_done(IAsyncContext* ctxt, Status status) {
  CallbackContext<Request> context{ctxt};
  require(status == Status::Ok);
  *context->sum += context->output;
}
void rmw_done(IAsyncContext*, Status status) { require(status == Status::Ok); }
uint64_t next(uint64_t& state) { state ^= state << 13; state ^= state >> 7; state ^= state << 17; return state; }
uint64_t ns(Clock::duration d) { return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count(); }

std::atomic<size_t> log_callbacks{0};
std::atomic<uint64_t> persisted{0};
void index_persisted(Status status) { require(status == Status::Ok); }
void log_persisted(Status status, uint64_t serial) {
  require(status == Status::Ok);
  persisted += serial;
  ++log_callbacks;
}

int main(int argc, char** argv) {
  require(argc == 6);
  std::string mode = argv[1];
  size_t threads = std::stoull(argv[2]), keys = std::stoull(argv[3]);
  std::string dir = argv[5];
  Store store{1ULL << 22, 512ULL << 20, dir, 0.5};
  if (mode == "run") {
    size_t ops = std::stoull(argv[4]);
    uint64_t ignored = 0;
    store.StartSession();
    for (uint64_t key = 0; key < keys; ++key) {
      Request context{key, key, &ignored};
      require(store.Upsert(context, rmw_done, key) == Status::Ok);
      if ((key & 255) == 255) store.Refresh();
    }
    store.CompletePending(true);
    store.StopSession();
    std::atomic<size_t> done{0}, enrolled{0};
    std::vector<std::thread> workers;
    auto started = Clock::now();
    for (size_t t = 0; t < threads; ++t) {
      workers.emplace_back([&, t]() {
        uint64_t thread_sum = 0;
        store.StartSession();
        uint64_t state = (t + 1) * 0x9E3779B97F4A7C15ULL;
        enrolled.fetch_add(1);
        while (enrolled.load() != threads) std::this_thread::yield();
        for (size_t i = 1; i <= ops / threads; ++i) {
          Request context{next(state) % keys, 1, &thread_sum};
          Status status = store.Rmw(context, rmw_done, i);
          require(status == Status::Ok || status == Status::Pending);
          if ((i & 255) == 0) { store.CompletePending(false); done.fetch_add(256); }
        }
        // Keep driving the checkpoint until every session has persisted.
        while (log_callbacks.load() < threads) store.CompletePending(false);
        store.CompletePending(true);
        store.StopSession();
      });
    }
    while (done.load() < ops / 4) std::this_thread::yield();
    Guid token;
    auto checkpoint_started = Clock::now();
    size_t ops_at_start = done.load();
    require(store.Checkpoint(index_persisted, log_persisted, token));
    while (log_callbacks.load() < threads) std::this_thread::yield();
    auto checkpoint_done = Clock::now();
    size_t ops_at_done = done.load();
    for (auto& w : workers) w.join();
    auto elapsed = ns(Clock::now() - started);
    auto window = ns(checkpoint_done - checkpoint_started);
    std::cout << "{\"engine\":\"cpp\",\"mode\":\"run\",\"threads\":" << threads << ",\"keys\":" << keys
      << ",\"ops\":" << ops << ",\"elapsed_ns\":" << elapsed << ",\"checkpoint_ns\":" << window
      << ",\"ops_during_checkpoint\":" << (ops_at_done - ops_at_start)
      << ",\"persisted_serials\":" << persisted.load() << ",\"token\":\"" << token.ToString() << "\"}\n";
  } else {
    Guid token = Guid::Parse(argv[4]);
    uint32_t version;
    std::vector<Guid> sessions;
    auto started = Clock::now();
    require(store.Recover(token, token, version, sessions) == Status::Ok);
    auto recovered = ns(Clock::now() - started);
    uint64_t serials = 0, sum = 0;
    for (auto& guid : sessions) {
      serials += store.ContinueSession(guid);
      store.StopSession();
    }
    store.StartSession();
    for (uint64_t key = 0; key < keys; ++key) {
      Request context{key, 0, &sum};
      Status status = store.Read(context, read_done, key);
      if (status == Status::Ok) sum += context.output;
      else require(status == Status::Pending);
      if ((key & 255) == 255) store.CompletePending(false);
    }
    store.CompletePending(true);
    store.StopSession();
    uint64_t expected = keys * (keys - 1) / 2 + serials;
    std::cout << "{\"engine\":\"cpp\",\"mode\":\"recover\",\"recover_ns\":" << recovered
      << ",\"sessions\":" << sessions.size() << ",\"persisted_serials\":" << serials
      << ",\"sum\":" << sum << ",\"expected\":" << expected << "}\n";
    require(sum == expected);
  }
}
