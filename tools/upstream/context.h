// Real context protocol using fixed upstream;Have input and asynchronous results,Do not count independent models.
#pragma once
#include <atomic>
#include <cstring>
#include <memory>
#include "core/faster.h"
#include "trace.h"
namespace comparison {
using namespace FASTER::core;
// The key in the log only contains the length and the bytes that follow it,Do not write vector or pointer.
struct Key {
  uint32_t length;
  uint32_t size() const { return sizeof(Key)+length; }
  const uint8_t* data() const { return reinterpret_cast<const uint8_t*>(this+1); }
  KeyHash GetHash() const { return KeyHash{FasterHashHelper<uint8_t>::compute(data(),length)}; }
  bool operator==(const Key& other) const {
    return length==other.length && (!length || std::memcmp(data(),other.data(),length)==0);
  }
  bool operator!=(const Key& other) const { return !(*this==other); }
};
// Upstream supports different shallow key interfaces;A deep copy of the context still owns the entire byte input.
struct OwnedKey {
  Bytes bytes;
  uint32_t size() const { return sizeof(Key)+bytes.size(); }
  KeyHash GetHash() const { return KeyHash{FasterHashHelper<uint8_t>::compute(bytes.data(),bytes.size())}; }
  void write_deep_key_at(Key* destination) const {
    new(destination) Key{static_cast<uint32_t>(bytes.size())};
    if(!bytes.empty()) std::memcpy(reinterpret_cast<uint8_t*>(destination+1),bytes.data(),bytes.size());
  }
  bool operator==(const Key& other) const {
    return bytes.size()==other.length && (bytes.empty() || std::memcmp(bytes.data(),other.data(),bytes.size())==0);
  }
  bool operator!=(const Key& other) const { return !(*this==other); }
};
// Digital in-place updates access only atomic;Ordinary byte records remain immutable,grow and replace append.
struct Value {
  uint32_t numeric=0,length=0;
  std::atomic<uint64_t> number{0};
  uint32_t size() const { return sizeof(Value)+length; }
  const uint8_t* data() const { return reinterpret_cast<const uint8_t*>(this+1); }
  uint8_t* data() { return reinterpret_cast<uint8_t*>(this+1); }
};
struct Reply {
  std::string value;
  Status status=Status::Pending;
  unsigned completions=0;
};
class Context: public IAsyncContext {
 public:
  using key_t=Key;
  using value_t=Value;
  Step input;
  OwnedKey key_;
  std::shared_ptr<Reply> reply=std::make_shared<Reply>();
  explicit Context(Step step):input(std::move(step)),key_{input.key} {}
  Context(const Context& other):input(other.input),key_(other.key_),reply(other.reply) {}
  const OwnedKey& key() const { return key_; }
  uint32_t value_size() const { return sizeof(Value)+input.operand.bytes.size(); }
  uint32_t value_size(const Value& old) const {
    if(old.numeric!=input.operand.numeric) throw std::runtime_error("Control trace has undefined cross-type RMW");
    return sizeof(Value)+old.length+input.operand.bytes.size();
  }
  void Get(const Value& value) { GetAtomic(value); }
  void GetAtomic(const Value& value) {
    reply->value=value.numeric ? "u "+std::to_string(value.number.load())
      : "b "+hex(Bytes(value.data(),value.data()+value.length));
  }
  void Put(Value& value) {
    initialize(value,input.operand);
    reply->value="written";
  }
  bool PutAtomic(Value& value) {
    if(!value.numeric || !input.operand.numeric) return false;
    value.number.store(input.operand.number);
    reply->value="written";
    return true;
  }
  void RmwInitial(Value& value) { initialize(value,input.operand); GetAtomic(value); }
  void RmwCopy(const Value& old, Value& value) {
    if(old.numeric!=input.operand.numeric) throw std::runtime_error("Control trace has undefined cross-type RMW");
    Operand updated=input.operand;
    if(updated.numeric) updated.number+=old.number.load();
    else { updated.bytes.assign(old.data(),old.data()+old.length); updated.bytes.insert(updated.bytes.end(),input.operand.bytes.begin(),input.operand.bytes.end()); }
    initialize(value,updated); GetAtomic(value);
  }
  bool RmwAtomic(Value& value) {
    if(!value.numeric || !input.operand.numeric) return false;
    reply->value="u "+std::to_string(value.number.fetch_add(input.operand.number)+input.operand.number);
    return true;
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& copy) override {
    return IAsyncContext::DeepCopy_Internal(*this,copy);
  }
 private:
  static void initialize(Value& value,const Operand& operand) {
    new(&value) Value{};
    value.numeric=operand.numeric; value.length=operand.bytes.size(); value.number.store(operand.number);
    if(!operand.bytes.empty()) std::memcpy(value.data(),operand.bytes.data(),operand.bytes.size());
  }
};
inline void callback(IAsyncContext* raw,Status status) {
  CallbackContext<Context> context(raw);
  context->reply->status=status;
  ++context->reply->completions;
}
inline std::string result(const Step& step,const Reply& reply) {
  if(reply.status==Status::NotFound) return "missing";
  if(reply.status==Status::Aborted && step.operation=="read" && step.option) return "tombstone";
  if(reply.status!=Status::Ok) throw std::runtime_error(std::string("Upstream business failure:")+StatusStr(reply.status));
  if(step.operation=="delete") return "deleted";
  if(reply.value.empty()) throw std::runtime_error("Success result has no business output");
  return reply.value;
}
}
