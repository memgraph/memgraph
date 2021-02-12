#include "utils/memory_tracker.hpp"

#include <atomic>
#include <exception>
#include <stdexcept>

#include "utils/likely.hpp"
#include "utils/logging.hpp"

namespace {

// Prevent memory tracker for throwing during the stack unwinding
bool MemoryTrackerCanThrow() { return !std::uncaught_exceptions(); }

std::string GetReadableSize(double size) {
  // TODO (antonio2368): Add support for base 1000 (KB, GB, TB...)
  constexpr std::array units = {"B", "KiB", "GiB", "TiB"};
  constexpr double delimiter = 1024;

  size_t i = 0;
  for (; i + 1 < units.size() && size >= delimiter; ++i) {
    size /= delimiter;
  }

  // bytes don't need decimals
  if (i == 0) {
    return fmt::format("{:.0f}{}", size, units[i]);
  }

  return fmt::format("{:.2f}{}", size, units[i]);
}

}  // namespace

namespace utils {

thread_local uint64_t MemoryTracker::BlockerInThread::counter_ = 0;

MemoryTracker::BlockerInThread::BlockerInThread() { ++counter_; }

MemoryTracker::BlockerInThread::~BlockerInThread() { --counter_; }

bool MemoryTracker::BlockerInThread::IsBlocked() { return counter_ > 0; }

MemoryTracker total_memory_tracker;

MemoryTracker::~MemoryTracker() {
  try {
    BlockerInThread log_blocker;
    // LogPeakMemoryUsage();
  } catch (...) {
    // Catch Exception in Logger
  }
}

void MemoryTracker::LogPeakMemoryUsage() const {
  // TODO (antonio2368): Make the size more readable
  spdlog::info("Peak memory usage: {} bytes", peak_);
}

void MemoryTracker::LogMemoryUsage(const int64_t current) {
  // TODO (antonio2368): Make the size more readable
  spdlog::info("Current memory usage: {} bytes", current);
}

void MemoryTracker::UpdatePeak(const int64_t will_be) {
  auto peak_old = peak_.load(std::memory_order_relaxed);
  if (will_be > peak_old) {
    peak_.store(will_be, std::memory_order_relaxed);

    BlockerInThread log_blocker;
    // LogPeakMemoryUsage();
  }
}

void MemoryTracker::SetHardLimit(const int64_t limit) { hard_limit_.store(limit, std::memory_order_relaxed); }

void MemoryTracker::SetOrRaiseHardLimit(const int64_t limit) {
  int64_t old_limit = hard_limit_.load(std::memory_order_relaxed);
  while (old_limit < limit && !hard_limit_.compare_exchange_weak(old_limit, limit))
    ;
}

void MemoryTracker::Alloc(const int64_t size) {
  MG_ASSERT(size > 0, "Negative size passed to the MemoryTracker");

  if (BlockerInThread::IsBlocked()) {
    return;
  }

  const int64_t will_be = size + amount_.fetch_add(size, std::memory_order_relaxed);

  const auto current_hard_limit = hard_limit_.load(std::memory_order_relaxed);

  if (UNLIKELY(current_hard_limit && will_be > current_hard_limit && MemoryTrackerCanThrow())) {
    // Prevent recursion. Exception::ctor -> std::string -> new[] -> MemoryTracker::alloc
    BlockerInThread untrack_lock;

    throw OutOfMemoryException(
        fmt::format("Memory limit exceeded! Atempting to allocate a chunk of {} which would put the current "
                    "use to {}, while the maximum allowed size for allocation is set to {}.",
                    GetReadableSize(size), GetReadableSize(will_be), GetReadableSize(current_hard_limit)));
  }

  UpdatePeak(will_be);
}

void MemoryTracker::Free(const int64_t size) {
  if (BlockerInThread::IsBlocked()) {
    return;
  }

  amount_.fetch_sub(size, std::memory_order_relaxed);
}

}  // namespace utils
