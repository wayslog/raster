#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

struct LatencySamples {
  std::vector<uint64_t> values;
  size_t pending = 0;
};

// A copied pending context retains the submitting thread's sample destination.
// FASTER invokes completion callbacks from that thread's CompletePending call.
class LatencySample {
 public:
  void Start(LatencySamples& samples) {
    samples_ = &samples;
    started_ = Clock::now();
  }
  void Finish(bool pending) {
    if (!samples_) return;
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started_).count();
    samples_->values.push_back(static_cast<uint64_t>(elapsed));
    samples_->pending += pending;
    samples_ = nullptr;
  }
 private:
  using Clock = std::chrono::steady_clock;
  Clock::time_point started_{};
  LatencySamples* samples_ = nullptr;
};
