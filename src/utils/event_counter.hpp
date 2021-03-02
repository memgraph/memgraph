#pragma once
#include <atomic>
#include <cstdlib>
#include <memory>

namespace utils {
using Event = uint64_t;
using Count = uint64_t;
using Counter = std::atomic<Count>;

class EventCounters {
 public:
  explicit EventCounters(Counter *allocated_counters) noexcept : counters_(allocated_counters) {}

  auto &operator[](const Event event) { return counters_[event]; }

  const auto &operator[](const Event event) const { return counters_[event]; }

  void Increment(Event event, Count amount = 1);

  static const Event num_counters;

 private:
  Counter *counters_;
};

extern EventCounters global_counters;

void IncrementEventCounter(Event event, Count amount = 1);

const char *GetEventName(Event event);
const char *GetEventDocumentation(Event event);

Event EventCount();
}  // namespace utils
