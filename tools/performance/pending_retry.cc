// Deterministic reference-engine regression: CompletePending(false) must return
// when a fuzzy-region RMW cannot yet complete. Run with an external timeout.
#include <atomic>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include "core/faster.h"
#include "device/null_disk.h"
using namespace FASTER::core;
struct Key {
  uint64_t value;
  uint32_t size() const { return sizeof(Key); }
  KeyHash GetHash() const { return KeyHash{value}; }
  bool operator==(const Key& other) const { return value == other.value; }
  bool operator!=(const Key& other) const { return !(*this == other); }
};
struct Value {
  std::atomic<uint64_t> value;
  static constexpr uint32_t size() { return sizeof(Value); }
};
class Request : public IAsyncContext {
 public:
  using key_t = Key;
  using value_t = Value;
  Key key_{1};
  uint64_t output = 0;
  const Key& key() const { return key_; }
  static constexpr uint32_t value_size() { return sizeof(Value); }
  static constexpr uint32_t value_size(const Value&) { return sizeof(Value); }
  void Put(Value& value) { new (&value.value) std::atomic<uint64_t>{10}; }
  bool PutAtomic(Value& value) { value.value.store(10); return true; }
  void RmwInitial(Value& value) { new (&value.value) std::atomic<uint64_t>{1}; output = 1; }
  void RmwCopy(const Value& old, Value& value) {
    output = old.value.load() + 1;
    new (&value.value) std::atomic<uint64_t>{output};
  }
  bool RmwAtomic(Value& value) { output = value.value.fetch_add(1) + 1; return true; }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& copy) override { return IAsyncContext::DeepCopy_Internal(*this, copy); }
};
unsigned callbacks = 0;
void require(bool value) { if (!value) throw std::runtime_error("pending retry contract failed"); }
void completed(IAsyncContext* raw, Status status) {
  CallbackContext<Request> request{raw};
  require(status == Status::Ok && request->output == 11);
  ++callbacks;
}
int main() {
  FasterKv<Key, Value, FASTER::device::NullDisk> store{1024, 256ULL << 20, "", 0.5};
  store.StartSession();
  Request request;
  require(store.Upsert(request, completed, 1) == Status::Ok);
  Address original = store.hlog.read_only_address.load();
  // Hold the existing record in the fuzzy region without scheduling an epoch
  // callback, so only the test can make this pending request runnable again.
  store.hlog.read_only_address.store(store.hlog.GetTailAddress());
  require(store.Rmw(request, completed, 2) == Status::Pending);
  std::cout << "fuzzy request pending; checking nonblocking completion" << std::endl;
  require(!store.CompletePending(false));
  require(callbacks == 0);
  store.hlog.read_only_address.store(original);
  require(store.CompletePending(true));
  require(callbacks == 1);
  store.StopSession();
  std::cout << "pending retry returned and completed exactly once" << std::endl;
}
