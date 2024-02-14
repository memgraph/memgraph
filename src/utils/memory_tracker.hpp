// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <atomic>
#include <optional>
#include <string>
#include <type_traits>

#include "utils/exceptions.hpp"

namespace memgraph::utils {

struct MemoryTrackerStatus {
  struct data {
    int64_t size;
    int64_t will_be;
    int64_t hard_limit;
  };

  // DEVNOTE: Do not call from within allocator, will cause another allocation
  auto msg() -> std::optional<std::string>;

  void set(data d) { data_ = d; }

 private:
  std::optional<data> data_;
};

auto MemoryErrorStatus() -> MemoryTrackerStatus &;

class OutOfMemoryException : public utils::BasicException {
 public:
  explicit OutOfMemoryException(const std::string &msg) : utils::BasicException(msg) {}
  SPECIALIZE_GET_EXCEPTION_NAME(OutOfMemoryException)
};

class MemoryTracker final {
 public:
  void LogPeakMemoryUsage() const;

  MemoryTracker() = default;
  ~MemoryTracker() = default;

  MemoryTracker(MemoryTracker &&other) noexcept
      : amount_(other.amount_.load(std::memory_order_acquire)),
        peak_(other.peak_.load(std::memory_order_acquire)),
        hard_limit_(other.hard_limit_.load(std::memory_order_acquire)),
        maximum_hard_limit_(other.maximum_hard_limit_) {
    other.maximum_hard_limit_ = 0;
    other.amount_.store(0, std::memory_order_acquire);
    other.peak_.store(0, std::memory_order_acquire);
    other.hard_limit_.store(0, std::memory_order_acquire);
  }

  MemoryTracker(const MemoryTracker &) = delete;
  MemoryTracker &operator=(const MemoryTracker &) = delete;

  MemoryTracker &operator=(MemoryTracker &&) = delete;

  bool Alloc(int64_t size);
  void Free(int64_t size);
  void DoCheck();

  auto Amount() const { return amount_.load(std::memory_order_relaxed); }

  auto Peak() const { return peak_.load(std::memory_order_relaxed); }

  auto HardLimit() const { return hard_limit_.load(std::memory_order_relaxed); }

  void SetHardLimit(int64_t limit);
  void TryRaiseHardLimit(int64_t limit);
  void SetMaximumHardLimit(int64_t limit);

  void ResetTrackings();

  bool IsProcedureTracked();

  void SetProcTrackingLimit(size_t limit);

  void StartProcTracking();
  void StopProcTracking();

  // By creating an object of this class, every allocation in its scope that goes over
  // the set hard limit produces an OutOfMemoryException.
  class OutOfMemoryExceptionEnabler final {
   public:
    OutOfMemoryExceptionEnabler(const OutOfMemoryExceptionEnabler &) = delete;
    OutOfMemoryExceptionEnabler &operator=(const OutOfMemoryExceptionEnabler &) = delete;
    OutOfMemoryExceptionEnabler(OutOfMemoryExceptionEnabler &&) = delete;
    OutOfMemoryExceptionEnabler &operator=(OutOfMemoryExceptionEnabler &&) = delete;

    OutOfMemoryExceptionEnabler();
    ~OutOfMemoryExceptionEnabler();

    static bool CanThrow();

   private:
    static thread_local uint64_t counter_;
  };

  // By creating an object of this class, we negate the effect of every OutOfMemoryExceptionEnabler
  // object. We need this object so we can guard only the smaller parts of code from exceptions while
  // allowing the exception in the other parts if the OutOfMemoryExceptionEnabler is defined.
  class OutOfMemoryExceptionBlocker final {
   public:
    OutOfMemoryExceptionBlocker(const OutOfMemoryExceptionBlocker &) = delete;
    OutOfMemoryExceptionBlocker &operator=(const OutOfMemoryExceptionBlocker &) = delete;
    OutOfMemoryExceptionBlocker(OutOfMemoryExceptionBlocker &&) = delete;
    OutOfMemoryExceptionBlocker &operator=(OutOfMemoryExceptionBlocker &&) = delete;

    OutOfMemoryExceptionBlocker();
    ~OutOfMemoryExceptionBlocker();

    static bool IsBlocked();

   private:
    static thread_local uint64_t counter_;
  };

 private:
  std::atomic<int64_t> amount_{0};
  std::atomic<int64_t> peak_{0};
  std::atomic<int64_t> hard_limit_{0};
  // Maximum possible value of a hard limit. If it's set to 0, no upper bound on the hard limit is set.
  int64_t maximum_hard_limit_{0};

  void UpdatePeak(int64_t will_be);

  static void LogMemoryUsage(int64_t current);
};

// Global memory tracker which tracks every allocation in the application.
extern MemoryTracker total_memory_tracker;
}  // namespace memgraph::utils
