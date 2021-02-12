#pragma once

#include <atomic>

#include "utils/exceptions.hpp"

namespace utils {

class OutOfMemoryException : public utils::BasicException {
 public:
  explicit OutOfMemoryException(const std::string &msg) : utils::BasicException(msg) {}
};

class MemoryTracker final {
 private:
  std::atomic<int64_t> amount_{0};
  std::atomic<int64_t> peak_{0};
  std::atomic<int64_t> hard_limit_{0};

  void UpdatePeak(int64_t will_be);

  static void LogMemoryUsage(int64_t current);
  void LogPeakMemoryUsage() const;

 public:
  MemoryTracker() = default;
  ~MemoryTracker();

  MemoryTracker(const MemoryTracker &) = delete;
  MemoryTracker &operator=(const MemoryTracker &) = delete;
  MemoryTracker(MemoryTracker &&) = delete;
  MemoryTracker &operator=(MemoryTracker &&) = delete;

  void Alloc(int64_t size);
  void Free(int64_t size);

  auto Amount() const { return amount_.load(std::memory_order_relaxed); }

  auto Peak() const { return peak_.load(std::memory_order_relaxed); }

  void SetHardLimit(int64_t limit);
  void SetOrRaiseHardLimit(int64_t limit);

  class BlockerInThread final {
   public:
    BlockerInThread(const BlockerInThread &) = delete;
    BlockerInThread &operator=(const BlockerInThread &) = delete;
    BlockerInThread(BlockerInThread &&) = delete;
    BlockerInThread &operator=(BlockerInThread &&) = delete;

    BlockerInThread();
    ~BlockerInThread();

    static bool IsBlocked();

   private:
    static thread_local uint64_t counter_;
  };
};

extern MemoryTracker total_memory_tracker;
}  // namespace utils
