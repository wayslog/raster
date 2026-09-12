// Direct-engine memory benchmark; no per-operation queues or formatted output.
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
#include "device/null_disk.h"
using namespace FASTER::core;
using Clock = std::chrono::steady_clock;
struct Key {
  uint64_t value;
  uint32_t size() const { return sizeof(Key); }
  KeyHash GetHash() const {
    // Exactly the same FNV-1a of eight little-endian bytes as RasterKV U64Key.
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
  Request(uint64_t key, uint64_t value) : key_{key}, input(value) {}
  Request(const Request& other) : key_(other.key_), input(other.input), output(other.output) {}
  const Key& key() const { return key_; }
  static constexpr uint32_t value_size() { return sizeof(Value); }
  static constexpr uint32_t value_size(const Value&) { return sizeof(Value); }
  void Put(Value& value) { value.value = input; output = input; }
  bool PutAtomic(Value& value) { value.atomic_value.store(input); output = input; return true; }
  void Get(const Value& value) { output = value.value; }
  void GetAtomic(const Value& value) { output = value.atomic_value.load(); }
  void RmwInitial(Value& value) { value.value = 1; output = 1; }
  void RmwCopy(const Value& old_value, Value& value) { value.value = old_value.value + 1; output = value.value; }
  bool RmwAtomic(Value& value) { output = value.atomic_value.fetch_add(1) + 1; return true; }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& copy) override { return IAsyncContext::DeepCopy_Internal(*this, copy); }
};
using Store = FasterKv<Key, Value, FASTER::device::NullDisk>;
void callback(IAsyncContext*, Status) { std::abort(); }
void require(bool condition) { if (!condition) throw std::runtime_error("benchmark contract failed"); }
uint64_t key_for(const std::string& distribution, size_t worker, size_t i) {
  if (distribution == "uniform") return worker * 256 + (i * 17) % 256;
  if (distribution == "worker-hot") return worker * 256;
  return 0;
}
uint64_t nanoseconds(Clock::duration duration) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
}
int main(int argc, char** argv) {
  require(argc == 6);
  std::string operation = argv[1], distribution = argv[2];
  size_t threads = std::stoull(argv[3]), count = std::stoull(argv[4]), stride = std::stoull(argv[5]);
  require(operation == "read" || operation == "upsert" || operation == "rmw");
  require(distribution == "uniform" || distribution == "worker-hot" || distribution == "shared-hot");
  require((threads == 1 || threads == 4) && count && count % threads == 0 && stride && !(stride & (stride - 1)));
  Store store{1024, 128ULL << 20, "", 0.9};
  store.StartSession();
  for (size_t key = 0; key < threads * 256; ++key) {
    Request context{key, 7};
    require(store.Upsert(context, callback, key) == Status::Ok);
  }
  store.StopSession();
  struct alignas(64) Result { uint64_t elapsed = 0, checksum = 0; std::vector<uint64_t> samples; };
  std::vector<Result> results(threads);
  std::vector<std::thread> workers;
  std::atomic<size_t> enrolled{0};
  for (size_t worker = 0; worker < threads; ++worker) {
    workers.emplace_back([&, worker]() {
      store.StartSession();
      auto& result = results[worker];
      uint64_t checksum = 0;
      result.samples.reserve(count / threads / stride + 1);
      enrolled.fetch_add(1);
      while (enrolled.load() != threads) std::this_thread::yield();
      auto started = Clock::now();
      for (size_t i = 0; i < count / threads; ++i) {
        Request context{key_for(distribution, worker, i), 42};
        bool sampled = (i & (stride - 1)) == ((i / stride) & (stride - 1));
        auto clock = sampled ? Clock::now() : Clock::time_point{};
        Status status;
        if (operation == "upsert") status = store.Upsert(context, callback, i);
        else if (operation == "read") status = store.Read(context, callback, i);
        else status = store.Rmw(context, callback, i);
        require(status == Status::Ok);
        if (sampled) result.samples.push_back(nanoseconds(Clock::now() - clock));
        checksum += context.output;
        if ((i & 255) == 255) store.Refresh();
      }
      result.elapsed = nanoseconds(Clock::now() - started);
      result.checksum = checksum;
      store.StopSession();
    });
  }
  for (auto& worker : workers) worker.join();
  uint64_t elapsed = 0, checksum = 0;
  std::vector<uint64_t> samples, expected(threads * 256, 7);
  for (size_t worker = 0; worker < threads; ++worker) {
    auto& result = results[worker];
    elapsed = std::max(elapsed, result.elapsed);
    checksum += result.checksum;
    samples.insert(samples.end(), result.samples.begin(), result.samples.end());
    for (size_t i = 0; i < count / threads; ++i) {
      auto& value = expected[key_for(distribution, worker, i)];
      if (operation == "upsert") value = 42;
      if (operation == "rmw") ++value;
    }
  }
  std::sort(samples.begin(), samples.end());
  uint64_t digest = 0xcbf29ce484222325ULL;
  store.StartSession();
  for (size_t key = 0; key < expected.size(); ++key) {
    Request context{key, 0};
    require(store.Read(context, callback, key) == Status::Ok && context.output == expected[key]);
    digest = (digest ^ context.output) * 0x100000001b3ULL;
  }
  store.StopSession();
  std::cout << "{\"engine\":\"cpp\",\"operation\":\"" << operation << "\",\"distribution\":\"" << distribution
    << "\",\"threads\":" << threads << ",\"count\":" << count << ",\"elapsed_ns\":" << elapsed
    << ",\"p99_ns\":" << samples[samples.size() * 99 / 100] << ",\"samples\":" << samples.size()
    << ",\"checksum\":" << checksum << ",\"digest\":" << digest << ",\"verified_keys\":" << expected.size() << "}\n";
}
