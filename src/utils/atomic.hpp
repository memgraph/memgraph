#pragma once

#include <atomic>

namespace utils {

/**
 * Ensures that the given atomic is greater or equal to the given value. If it
 * already satisfies that predicate, it's not modified.
 *
 * @param atomic - The atomic variable to ensure on.
 * @param value - The minimal value the atomic must have after this call.
 * @tparam TValue - Type of value.
 */
template <typename TValue>
void EnsureAtomicGe(std::atomic<TValue> &atomic, TValue value) {
  while (true) {
    auto current = atomic.load();
    if (current >= value) break;
    if (atomic.compare_exchange_strong(current, value)) break;
  }
}
}  // namespace utils
