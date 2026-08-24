// Copyright 2026 Memgraph Ltd.
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
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <utility>

#include "utils/exceptions.hpp"

namespace memgraph::utils {

struct MemoryTrackerStatus {
  enum Type { kQuery, kGlobal, kUser };

  struct data {
    int64_t size{0};
    int64_t will_be{0};
    int64_t hard_limit{0};
    Type type{kQuery};
  };

  // DEVNOTE: Do not call from within allocator, will cause another allocation
  auto msg() -> std::optional<std::string>;

  void set(data d) {
    data_ = d;
    has_data_ = true;
  }

 private:
  data data_{};
  bool has_data_{false};
};

auto MemoryErrorStatus() -> MemoryTrackerStatus &;

class OutOfMemoryException : public utils::BasicException {
 public:
  explicit OutOfMemoryException(std::string msg) : utils::BasicException(std::move(msg)) {}
  SPECIALIZE_GET_EXCEPTION_NAME(OutOfMemoryException)
};

class MemoryTracker final {
 public:
  void LogPeakMemoryUsage() const;

  // 0-parent: global leaf trackers (total_memory_tracker, db_total_memory_tracker_)
  constexpr MemoryTracker() = default;

  // 1-parent: domain aggregators (graph_memory_tracker, vector_index_memory_tracker)
  constexpr explicit MemoryTracker(MemoryTracker *parent1) : parent1_(parent1) {}

  // 2-parent: per-DB domain trackers that roll up to both the domain global AND the
  // per-DB total enforcement tracker.
  constexpr MemoryTracker(MemoryTracker *parent1, MemoryTracker *parent2) : parent1_(parent1), parent2_(parent2) {}

  ~MemoryTracker() = default;

  MemoryTracker(MemoryTracker &&other) noexcept
      : amount_(other.amount_.exchange(0, std::memory_order_acq_rel)),
        peak_(other.peak_.exchange(0, std::memory_order_acq_rel)),
        hard_limit_(other.hard_limit_.exchange(0, std::memory_order_acq_rel)),
        maximum_hard_limit_(std::exchange(other.maximum_hard_limit_, 0)),
        parent1_(std::exchange(other.parent1_, nullptr)),
        parent2_(std::exchange(other.parent2_, nullptr)) {}

  MemoryTracker(const MemoryTracker &) = delete;
  MemoryTracker &operator=(const MemoryTracker &) = delete;

  MemoryTracker &operator=(MemoryTracker &&) = delete;

  bool Alloc(int64_t size);
  void Free(int64_t size);
  void DoCheck();

  auto Amount() const { return amount_.load(std::memory_order_relaxed); }

  auto Peak() const { return peak_.load(std::memory_order_relaxed); }

  auto HardLimit() const { return hard_limit_.load(std::memory_order_relaxed); }

  auto MaximumHardLimit() const { return maximum_hard_limit_; }

  void SetHardLimit(int64_t limit);
  void TryRaiseHardLimit(int64_t limit);
  void SetMaximumHardLimit(int64_t limit);

  void ResetTrackings();

  void ResetLimit();

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

    OutOfMemoryExceptionEnabler() { ++counter_; }

    ~OutOfMemoryExceptionEnabler() { --counter_; }

    static bool CanThrow() { return counter_ > 0; };

   private:
    static thread_local uint64_t counter_ [[gnu::tls_model("initial-exec")]];
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

    OutOfMemoryExceptionBlocker() { ++counter_; }

    ~OutOfMemoryExceptionBlocker() { --counter_; }

    static bool IsBlocked() { return counter_ > 0; };

   private:
    static thread_local uint64_t counter_ [[gnu::tls_model("initial-exec")]];
  };

  // Marks a scope whose caller turns a refused allocation into an exception. Refusing is only
  // meaningful to a caller that can act on it; the dynamic loader, and C libraries generally, have
  // no path back from a null return, and for the loader a null return is fatal. Outside such a
  // scope an over-limit allocation is therefore tracked and allowed, and the limit is enforced at
  // the next allocation that does come through one.
  class RefusalHandledScope final {
   public:
    RefusalHandledScope(const RefusalHandledScope &) = delete;
    RefusalHandledScope &operator=(const RefusalHandledScope &) = delete;
    RefusalHandledScope(RefusalHandledScope &&) = delete;
    RefusalHandledScope &operator=(RefusalHandledScope &&) = delete;

    // Saved and restored rather than cleared, because these scopes nest: an allocation made while
    // handling one is not itself covered by it.
    RefusalHandledScope() : previous_{handled_} { handled_ = true; }

    ~RefusalHandledScope() { handled_ = previous_; }

    static bool IsRefusalHandled() { return handled_; };

   private:
    bool previous_;
    static thread_local bool handled_ [[gnu::tls_model("initial-exec")]];
  };

 private:
  std::atomic<int64_t> amount_{0};
  std::atomic<int64_t> peak_{0};
  std::atomic<int64_t> hard_limit_{0};
  // Maximum possible value of a hard limit. If it's set to 0, no upper bound on the hard limit is set.
  int64_t maximum_hard_limit_{0};
  MemoryTracker *parent1_{nullptr};
  MemoryTracker *parent2_{nullptr};

  void UpdatePeak(int64_t will_be);

  static void LogMemoryUsage(int64_t current);
};

// Global memory tracker which tracks every allocation in the application.
extern constinit MemoryTracker total_memory_tracker;
// Global domain trackers: aggregate graph-storage and embedding memory across all DBs.
// Required for AI_PLATFORM license enforcement (SetHardLimit on a domain subset).
// Per-DB domain trackers parent to these; these parent to total_memory_tracker.
extern constinit MemoryTracker graph_memory_tracker;
extern constinit MemoryTracker vector_index_memory_tracker;

namespace detail {

// Guards the one part of MemoryTrackerCanThrow() that can allocate. Reading libstdc++'s
// exception-handling thread-local goes through the dynamic loader, which on a thread whose
// thread-local storage generation is stale satisfies the read by reallocating that thread's
// storage vector. That allocation is tracked, so it asks whether it may throw, and at the limit
// the answer never changes. Reporting the re-entry lets the nested question be answered without
// asking libstdc++ again, which terminates the descent.
class ThrowCheckReentrancyGuard final {
 public:
  ThrowCheckReentrancyGuard(const ThrowCheckReentrancyGuard &) = delete;
  ThrowCheckReentrancyGuard &operator=(const ThrowCheckReentrancyGuard &) = delete;
  ThrowCheckReentrancyGuard(ThrowCheckReentrancyGuard &&) = delete;
  ThrowCheckReentrancyGuard &operator=(ThrowCheckReentrancyGuard &&) = delete;

  ThrowCheckReentrancyGuard() { evaluating_ = true; }

  ~ThrowCheckReentrancyGuard() { evaluating_ = false; }

  static bool IsEvaluating() { return evaluating_; };

 private:
  static thread_local bool evaluating_ [[gnu::tls_model("initial-exec")]];
};

}  // namespace detail

// Whether an allocation that exceeds the hard limit may be refused by throwing.
// The order is the layering, not a micro-optimisation. Throwing is opt-in per thread, so a thread
// that never opted in cannot be refused whatever the remaining checks say; asking them anyway
// would put the allocating check below on every thread in the process, including third-party
// threads we do not control. Each check is therefore cheaper and more owned than the one after it,
// and std::uncaught_exceptions() comes last because it is purely defensive: it asks whether
// throwing is safe at this instant, having already established that throwing is wanted.
inline bool MemoryTrackerCanThrow() {
  if (!MemoryTracker::OutOfMemoryExceptionEnabler::CanThrow()) return false;
  if (MemoryTracker::OutOfMemoryExceptionBlocker::IsBlocked()) return false;
  if (detail::ThrowCheckReentrancyGuard::IsEvaluating()) return false;

  const detail::ThrowCheckReentrancyGuard guard;
  return !std::uncaught_exceptions();
}

// Whether an allocation that exceeds the hard limit may be refused. Refusing is only meaningful
// where the caller turns it into an exception, and only safe where throwing is. Every refusal
// site answers this one question, so they agree on which allocations are exempt.
inline bool MayRefuseAllocation() {
  return MemoryTracker::RefusalHandledScope::IsRefusalHandled() && MemoryTrackerCanThrow();
}

}  // namespace memgraph::utils
