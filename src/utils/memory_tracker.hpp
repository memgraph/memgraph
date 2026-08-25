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
  // meaningful to a caller that can act on it: the dynamic loader and C libraries have no path
  // back from a null return, and for the loader it is fatal. An over-limit allocation outside any
  // such scope is tracked and allowed, and the limit is enforced at the next one inside it.
  //
  // Calling into a query module is such a scope, at every C API entry point: procedures, their
  // initializers and cleanups, user-defined functions, and stream transformations. A module
  // reports a refusal through its own return value or error message. Its allocations reach the
  // tracker through the same C allocation functions the C library uses, which cannot themselves
  // be marked, so the module's side of that boundary is where refusal becomes available again.
  class RefusalHandledScope final {
   public:
    RefusalHandledScope(const RefusalHandledScope &) = delete;
    RefusalHandledScope &operator=(const RefusalHandledScope &) = delete;
    RefusalHandledScope(RefusalHandledScope &&) = delete;
    RefusalHandledScope &operator=(RefusalHandledScope &&) = delete;

    // These scopes nest, so the previous value is restored: an allocation made while handling a
    // refusal is not itself covered by the scope handling it.
    RefusalHandledScope() : previous_{refusal_handled_} { refusal_handled_ = true; }

    ~RefusalHandledScope() { refusal_handled_ = previous_; }

   private:
    bool previous_;
  };

  // Marks a scope that may allocate on behalf of the runtime rather than its caller, so nothing
  // allocated within it is refusable. Deciding whether a refusal may throw reads a libstdc++
  // thread-local, which the dynamic loader can satisfy by reallocating the thread's storage
  // vector; that allocation belongs to the loader, which cannot survive a refusal.
  class RefusalSuspendedScope final {
   public:
    RefusalSuspendedScope(const RefusalSuspendedScope &) = delete;
    RefusalSuspendedScope &operator=(const RefusalSuspendedScope &) = delete;
    RefusalSuspendedScope(RefusalSuspendedScope &&) = delete;
    RefusalSuspendedScope &operator=(RefusalSuspendedScope &&) = delete;

    RefusalSuspendedScope() : previous_{refusal_handled_} { refusal_handled_ = false; }

    ~RefusalSuspendedScope() { refusal_handled_ = previous_; }

   private:
    bool previous_;
  };

  static bool IsRefusalHandled() { return refusal_handled_; }

 private:
  static thread_local bool refusal_handled_ [[gnu::tls_model("initial-exec")]];

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

// Whether a refusal may be delivered by throwing. Throwing is opt-in per thread, so the opt-in is
// tested first and the libstdc++ read below is reached only on threads that asked to be limited.
// That read resolves through the dynamic loader and can allocate, so it runs under a suspension:
// what it allocates belongs to the loader rather than to this caller.
inline bool MemoryTrackerCanThrow() {
  if (!MemoryTracker::OutOfMemoryExceptionEnabler::CanThrow()) return false;
  if (MemoryTracker::OutOfMemoryExceptionBlocker::IsBlocked()) return false;

  const MemoryTracker::RefusalSuspendedScope refusal_suspended;
  return !std::uncaught_exceptions();
}

// Whether an allocation that exceeds the hard limit may be refused. Every site that can refuse an
// allocation asks this, so they agree on which allocations are exempt.
inline bool MayRefuseAllocation() { return MemoryTracker::IsRefusalHandled() && MemoryTrackerCanThrow(); }

}  // namespace memgraph::utils
