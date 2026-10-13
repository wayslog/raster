// Larger-than-memory benchmark: uniform random Read or RMW over KEYS keys with a
// 256 MiB in-memory log, so most records live on a buffered file device.
// Paired with examples/disk.rs; both print identical checksums and digests.
#include <algorithm>
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
// Buffered files so the log can live on tmpfs; only the driver chooses this.
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
  uint64_t* sum;  // Per-thread accumulator for pending completions.
  Request(uint64_t key, uint64_t value, uint64_t* sum_) : key_{key}, input(value), sum(sum_) {}
  Request(const Request& other) = default;
  const Key& key() const { return key_; }
  static constexpr uint32_t value_size() { return sizeof(Value); }
  static constexpr uint32_t value_size(const Value&) { return sizeof(Value); }
  void Put(Value& value) { value.value = input; }
  bool PutAtomic(Value& value) { value.atomic_value.store(input); return true; }
  void Get(const Value& value) { output = value.value; }
  void GetAtomic(const Value& value) { output = value.atomic_value.load(); }
  void RmwInitial(Value& value) { value.value = input; output = input; }
  void RmwCopy(const Value& old_value, Value& value) { value.value = old_value.value + input; output = value.value; }
  bool RmwAtomic(Value& value) { output = value.atomic_value.fetch_add(input) + input; return true; }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& copy) override { return IAsyncContext::DeepCopy_Internal(*this, copy); }
};
using Store = FasterKv<Key, Value, disk_t>;
void completed(IAsyncContext* ctxt, Status status) {
  CallbackContext<Request> context{ctxt};
  if (status != Status::Ok) std::abort();
  *context->sum += context->output;
}
void require(bool condition) { if (!condition) throw std::runtime_error("benchmark contract failed"); }
uint64_t next(uint64_t& state) { state ^= state << 13; state ^= state >> 7; state ^= state << 17; return state; }
// hotread sends 9 of 10 reads to the first keys / 32, which were loaded first
// and so live on the device.
uint64_t pick(uint64_t& state, uint64_t keys, bool hot) {
  uint64_t r = next(state);
  if (hot && r % 10 != 0) return next(state) % (keys / 32);
  return r % keys;
}
uint64_t nanoseconds(Clock::duration duration) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
}

int main(int argc, char** argv) {
  require(argc == 7 || argc == 8);
  std::string operation = argv[1];
  uint64_t read_cache_mb = argc == 8 ? std::stoull(argv[7]) : 0;
  bool hot = operation == "hotread";
  size_t threads = std::stoull(argv[2]), keys = std::stoull(argv[3]), count = std::stoull(argv[4]);
  size_t stride = std::stoull(argv[5]);
  std::string dir = argv[6];
  require((operation == "read" || operation == "hotread" || operation == "rmw") && count % threads == 0 &&
          !(stride & (stride - 1)));
  ReadCacheConfig read_cache{read_cache_mb << 20, 0.9, false, read_cache_mb > 0};
  Store store{1ULL << 22, 256ULL << 20, dir, 0.5, read_cache};
  uint64_t ignored = 0;
  store.StartSession();
  for (uint64_t key = 0; key < keys; ++key) {
    Request context{key, key, &ignored};
    require(store.Upsert(context, completed, key) == Status::Ok);
    if ((key & 255) == 255) store.Refresh();
  }
  store.CompletePending(true);
  store.StopSession();

  struct alignas(64) Result { uint64_t elapsed = 0, sum = 0, pending = 0; std::vector<uint64_t> samples; };
  std::vector<Result> results(threads);
  std::vector<std::thread> workers;
  std::atomic<size_t> enrolled{0};
  for (size_t worker = 0; worker < threads; ++worker) {
    workers.emplace_back([&, worker]() {
      store.StartSession();
      auto& result = results[worker];
      result.samples.reserve(count / threads / stride + 1);
      uint64_t state = (worker + 1) * 0x9E3779B97F4A7C15ULL;
      enrolled.fetch_add(1);
      while (enrolled.load() != threads) std::this_thread::yield();
      auto started = Clock::now();
      for (size_t i = 0; i < count / threads; ++i) {
        Request context{pick(state, keys, hot), 1, &result.sum};
        bool sampled = (i & (stride - 1)) == ((i / stride) & (stride - 1));
        auto clock = sampled ? Clock::now() : Clock::time_point{};
        Status status = operation != "rmw" ? store.Read(context, completed, i) : store.Rmw(context, completed, i);
        if (status == Status::Ok) {
          result.sum += context.output;
          if (sampled) result.samples.push_back(nanoseconds(Clock::now() - clock));
        } else {
          require(status == Status::Pending);
          ++result.pending;
        }
        if ((i & 255) == 255) store.CompletePending(false);
      }
      store.CompletePending(true);
      result.elapsed = nanoseconds(Clock::now() - started);
      store.StopSession();
    });
  }
  for (auto& worker : workers) worker.join();

  uint64_t elapsed = 0, sum = 0, pending = 0;
  std::vector<uint64_t> samples;
  for (auto& result : results) {
    elapsed = std::max(elapsed, result.elapsed);
    sum += result.sum;
    pending += result.pending;
    samples.insert(samples.end(), result.samples.begin(), result.samples.end());
  }
  std::sort(samples.begin(), samples.end());
  // Digest: total of all final values, read back through the store.
  uint64_t digest = 0;
  store.StartSession();
  for (uint64_t key = 0; key < keys; ++key) {
    Request context{key, 0, &digest};
    Status status = store.Read(context, completed, key);
    if (status == Status::Ok) digest += context.output;
    else require(status == Status::Pending);
    if ((key & 255) == 255) store.CompletePending(false);
  }
  store.CompletePending(true);
  store.StopSession();
  std::cout << "{\"engine\":\"cpp\",\"operation\":\"" << operation << "\",\"threads\":" << threads
    << ",\"keys\":" << keys << ",\"count\":" << count << ",\"elapsed_ns\":" << elapsed
    << ",\"p99_ns\":" << (samples.empty() ? 0 : samples[samples.size() * 99 / 100])
    << ",\"samples\":" << samples.size() << ",\"pending\":" << pending
    << ",\"checksum\":" << sum << ",\"digest\":" << digest << "}\n";
}
