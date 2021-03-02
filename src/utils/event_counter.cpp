#include "utils/event_counter.hpp"

#define APPLY_FOR_EVENTS(M)                                 \
  M(ReadQueries, "Number of read-only queries executed.")   \
  M(WriteQueries, "Number of write-only queries executed.") \
  M(ReadWriteQueries, "Number of read-write queries executed.")

namespace utils {

// define every Event as an index in the array of counters
#define M(NAME, DOCUMENTATION) extern const Event NAME = __COUNTER__;
APPLY_FOR_EVENTS(M)
#undef M

constexpr Event END = __COUNTER__;

// Initialize array for the global counter with all values set to 0
Counter global_counters_array[END]{};
// Initialize global counters
EventCounters global_counters(global_counters_array);

const Event EventCounters::num_counters = END;

void EventCounters::Increment(const Event event, Count amount) {
  counters_[event].fetch_add(amount, std::memory_order_relaxed);
}

void IncrementEventCounter(const Event event, Count amount) { global_counters.Increment(event, amount); }

const char *GetEventName(const Event event) {
  static const char *strings[] = {
#define M(NAME, DOCUMENTATION) #NAME,
      APPLY_FOR_EVENTS(M)
#undef M
  };

  return strings[event];
}

const char *GetEventDocumentation(const Event event) {
  static const char *strings[] = {
#define M(NAME, DOCUMENTATION) DOCUMENTATION,
      APPLY_FOR_EVENTS(M)
#undef M
  };

  return strings[event];
}

Event EventCount() { return END; }

}  // namespace utils
