// Compaction benchmark, paired with examples/compact.rs:
//   compact THREADS KEYS DIR
// Loads KEYS keys (value = key), overwrites the even ones (value + 1), then
// compacts and truncates the log up to the safe read-only address with
// THREADS threads, and verifies the sum of all values afterwards.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
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
void upsert_done(IAsyncContext*, Status) { std::abort(); }

int main(int argc, char** argv) {
  require(argc == 4);
  int threads = std::stoi(argv[1]);
  uint64_t keys = std::stoull(argv[2]);
  Store store{1ULL << 22, 256ULL << 20, argv[3], 0.5};
  store.StartSession();
  uint64_t ignored = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    Request context{key, key, &ignored};
    require(store.Upsert(context, upsert_done, key) == Status::Ok);
    if ((key & 255) == 255) store.Refresh();
  }
  for (uint64_t key = 0; key < keys; key += 2) {
    Request context{key, key + 1, &ignored};
    require(store.Upsert(context, upsert_done, keys + key) == Status::Ok);
    if ((key & 511) == 510) store.Refresh();
  }
  store.CompletePending(true);
  uint64_t begin = store.hlog.begin_address.load().control();
  uint64_t until = store.hlog.safe_read_only_address.load().control();
  uint64_t size_before = store.hlog.GetTailAddress().control() - begin;
  auto started = Clock::now();
  require(store.CompactWithLookup(until, true, threads));
  auto compact_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count();
  uint64_t size_after = store.hlog.GetTailAddress().control() - store.hlog.begin_address.load().control();
  uint64_t sum = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    Request context{key, 0, &sum};
    Status status = store.Read(context, read_done, 2 * keys + key);
    if (status == Status::Ok) sum += context.output;
    else require(status == Status::Pending);
    if ((key & 255) == 255) store.CompletePending(false);
  }
  store.CompletePending(true);
  store.StopSession();
  uint64_t expected = keys * (keys - 1) / 2 + (keys + 1) / 2;
  std::cout << "{\"engine\":\"cpp\",\"threads\":" << threads << ",\"keys\":" << keys
    << ",\"compact_ns\":" << compact_ns << ",\"compacted_bytes\":" << (until - begin)
    << ",\"size_before\":" << size_before << ",\"size_after\":" << size_after
    << ",\"sum\":" << sum << ",\"expected\":" << expected << "}\n";
  require(sum == expected);
}
