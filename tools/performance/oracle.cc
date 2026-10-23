// Storage-independent business oracle for disk.cc and varlen.cc.
// Usage: oracle fixed|varlen OP THREADS KEYS COUNT [MIN_VALUE MAX_VALUE]
// RMW checksum is order independent: each key returns key+1 through key+n.
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

void require(bool value) { if (!value) throw std::runtime_error("invalid oracle workload"); }
uint64_t next(uint64_t& state) { state ^= state << 13; state ^= state >> 7; state ^= state << 17; return state; }
uint64_t hash(uint64_t key) {
  uint64_t value = 0xcbf29ce484222325ULL;
  for (unsigned i = 0; i < 8; ++i) value = (value ^ ((key >> (8 * i)) & 255)) * 0x100000001b3ULL;
  return value;
}
uint32_t length(uint64_t key, uint32_t minimum, uint32_t maximum) {
  return (minimum + hash(key) % (maximum - minimum + 1)) & ~7U;
}
int main(int argc, char** argv) {
  require(argc == 6 || argc == 8);
  std::string mode = argv[1], operation = argv[2];
  bool variable = mode == "varlen";
  uint64_t threads = std::stoull(argv[3]), keys = std::stoull(argv[4]), count = std::stoull(argv[5]);
  require((mode == "fixed" || variable) && threads && keys && count && count % threads == 0 && keys % threads == 0);
  require(operation == "read" || operation == "rmw" || (variable ? operation == "upsert" : operation == "hotread"));
  require(variable ? argc == 8 : argc == 6);
  uint32_t minimum = variable ? std::stoul(argv[6]) : 8;
  uint32_t maximum = variable ? std::stoul(argv[7]) : 8;
  require(minimum >= 8 && minimum <= maximum && minimum % 8 == 0 && maximum % 8 == 0);
  require(operation != "hotread" || keys >= 32);
  std::vector<uint32_t> visits(operation == "rmw" ? keys : 0);
  std::vector<uint32_t> final_lengths(variable && operation == "upsert" ? keys : 0);
  for (uint64_t key = 0; key < final_lengths.size(); ++key) final_lengths[key] = length(key, minimum, maximum);
  uint64_t checksum = 0;
  for (uint64_t worker = 0; worker < threads; ++worker) {
    uint64_t state = (worker + 1) * 0x9E3779B97F4A7C15ULL;
    for (uint64_t i = 0; i < count / threads; ++i) {
      uint64_t random = next(state);
      uint64_t key = random % keys;
      if (operation == "hotread" && random % 10 != 0) key = next(state) % (keys / 32);
      if (operation == "upsert") {
        key = worker + random % (keys / threads) * threads;
        uint64_t round = i / (keys / threads) + 1;
        final_lengths[key] = length(key + round, minimum, maximum);
        checksum += key + (uint64_t(final_lengths[key]) << 32);
      } else if (operation == "rmw") {
        checksum += key + ++visits[key];
      } else {
        checksum += key + (variable ? uint64_t(length(key, minimum, maximum)) << 32 : 0);
      }
    }
  }
  uint64_t digest = 0;
  for (uint64_t key = 0; key < keys; ++key) {
    uint64_t updates = operation == "rmw" ? visits[key] : 0;
    uint64_t bytes = 0;
    if (variable) {
      bytes = operation == "upsert" ? final_lengths[key] :
              std::min<uint64_t>(length(key, minimum, maximum) + 8 * updates, maximum);
    }
    digest += key + updates + (bytes << 32);
  }
  std::cout << "{\"mode\":\"" << mode << "\",\"operation\":\"" << operation << "\",\"threads\":" << threads
            << ",\"keys\":" << keys << ",\"count\":" << count << ",\"checksum\":" << checksum << ",\"digest\":" << digest << "}\n";
}
