// Variable-value benchmark paired with examples/varlen.rs.
// Usage: varlen OP THREADS KEYS COUNT SAMPLE_STRIDE DIR MIN_VALUE MAX_VALUE
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
constexpr uint64_t LOCKED = 1ULL << 62;
constexpr uint64_t REPLACED = 1ULL << 63;
constexpr uint64_t FILL = 0x5858585858585858ULL;
struct Value {
  std::atomic<uint64_t> generation;
  uint32_t capacity;
  std::atomic<uint32_t> length;
  uint32_t size() const { return 16 + capacity; }
  std::atomic<uint64_t>& word(size_t i) const {
    return reinterpret_cast<std::atomic<uint64_t>*>(const_cast<Value*>(this) + 1)[i];
  }
  void initialize(uint32_t allocated, uint64_t counter, uint32_t len) {
    new (&generation) std::atomic<uint64_t>{0};
    capacity = allocated;
    new (&length) std::atomic<uint32_t>{len};
    for (size_t i = 0; i < capacity / 8; ++i) new (&word(i)) std::atomic<uint64_t>{i == 0 ? counter : FILL};
  }
  bool lock() {
    for (;;) {
      uint64_t current = generation.load();
      if (current & REPLACED) return false;
      if (!(current & LOCKED) && generation.compare_exchange_strong(current, current | LOCKED)) return true;
      _mm_pause();
    }
  }
  void unlock(bool replaced) {
    generation.store(((generation.load() & ~LOCKED) + 1) | (replaced ? REPLACED : 0));
  }
};
static_assert(sizeof(Value) == 16);
class Request : public IAsyncContext {
 public:
  using key_t = Key;
  using value_t = Value;
  Key key_;
  uint64_t input, output = 0;
  uint32_t length, maximum;
  bool verify;
  uint64_t* sum;
  Request(uint64_t key, uint64_t value, uint32_t len, uint32_t max, bool verify_, uint64_t* sum_)
    : key_{key}, input(value), length(len), maximum(max), verify(verify_), sum(sum_) {}
  Request(const Request& other) = default;
  const Key& key() const { return key_; }
  uint32_t value_size() const { return 16 + length; }
  uint32_t value_size(const Value& old) const { return 16 + std::min(old.capacity + 8, maximum); }
  void Put(Value& value) { value.initialize(length, input, length); output = input + (uint64_t(length) << 32); }
  bool PutAtomic(Value& value) {
    if (!value.lock()) return false;
    if (length > value.capacity) { value.unlock(true); return false; }
    value.word(0).store(input);
    for (size_t i = 1; i < length / 8; ++i) value.word(i).store(FILL);
    value.length.store(length);
    value.unlock(false);
    output = input + (uint64_t(length) << 32);
    return true;
  }
  void Get(const Value& value) {
    for (;;) {
      uint64_t before = value.generation.load();
      if (before & LOCKED) { _mm_pause(); continue; }
      uint32_t len = value.length.load();
      uint64_t counter = value.word(0).load();
      if (verify) {
        for (size_t i = 1; i < len / 8; ++i) if (value.word(i).load() != FILL) std::abort();
      }
      if (before == value.generation.load()) { output = counter + (uint64_t(len) << 32); return; }
    }
  }
  void GetAtomic(const Value& value) { Get(value); }
  void RmwInitial(Value& value) { value.initialize(length, input, length); output = input; }
  void RmwCopy(const Value& old, Value& value) {
    uint32_t len = std::min(old.capacity + 8, maximum);
    output = old.word(0).load() + input;
    value.initialize(len, output, len);
  }
  bool RmwAtomic(Value& value) {
    if (!value.lock()) return false;
    if (value.capacity < maximum) { value.unlock(true); return false; }
    output = value.word(0).fetch_add(input) + input;
    value.unlock(false);
    return true;
  }
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
uint32_t value_length(uint64_t key, uint32_t minimum, uint32_t maximum) {
  return (minimum + (Key{key}.GetHash().control_ % (maximum - minimum + 1))) & ~7U;
}
uint64_t nanoseconds(Clock::duration duration) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
}

int main(int argc, char** argv) {
  require(argc == 9);
  std::string operation = argv[1];
  uint32_t minimum = std::stoul(argv[7]), maximum = std::stoul(argv[8]);
  size_t threads = std::stoull(argv[2]), keys = std::stoull(argv[3]), count = std::stoull(argv[4]);
  size_t stride = std::stoull(argv[5]);
  std::string dir = argv[6];
  require((operation == "read" || operation == "upsert" || operation == "rmw") && threads > 0 && keys > 0 && count > 0 && keys % threads == 0 && count % threads == 0 &&
          stride > 0 && !(stride & (stride - 1)) && minimum >= 8 && minimum <= maximum && maximum < (1U << 25) - 32 && minimum % 8 == 0 && maximum % 8 == 0);
  Store store{1ULL << 22, 256ULL << 20, dir, 0.5};
  uint64_t ignored = 0;
  store.StartSession();
  for (uint64_t key = 0; key < keys; ++key) {
    Request context{key, key, value_length(key, minimum, maximum), maximum, false, &ignored};
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
        uint64_t random = next(state);
        uint64_t key = operation == "upsert" ? worker + random % (keys / threads) * threads : random % keys;
        uint64_t round = i / (keys / threads) + 1;
        Request context{key, operation == "upsert" ? key : 1, value_length(key + round, minimum, maximum), maximum, false, &result.sum};
        bool sampled = (i & (stride - 1)) == ((i / stride) & (stride - 1));
        auto clock = sampled ? Clock::now() : Clock::time_point{};
        Status status = operation == "read" ? store.Read(context, completed, i) :
                        operation == "upsert" ? store.Upsert(context, completed, i) : store.Rmw(context, completed, i);
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
    Request context{key, 0, minimum, maximum, true, &digest};
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
