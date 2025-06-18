// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query_memory_control.hpp"
#include "utils/query_memory_tracker.hpp"

#include <cstdint>

#include "utils/logging.hpp"

namespace memgraph::memory {

#if USE_JEMALLOC

namespace {

inline auto &GetQueryTracker() {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  static thread_local utils::QueryMemoryTracker *query_memory_tracker_ = nullptr;
  return query_memory_tracker_;
}

struct ThreadTrackingBlocker {
  ThreadTrackingBlocker() : prev_state_{GetQueryTracker()} {
    // Disable thread tracking
    GetQueryTracker() = nullptr;
  }

  ~ThreadTrackingBlocker() {
    // Reset thread tracking to previous state
    GetQueryTracker() = prev_state_;
  }

  ThreadTrackingBlocker(ThreadTrackingBlocker &) = delete;
  ThreadTrackingBlocker &operator=(ThreadTrackingBlocker &) = delete;
  ThreadTrackingBlocker(ThreadTrackingBlocker &&) = delete;
  ThreadTrackingBlocker &operator=(ThreadTrackingBlocker &&) = delete;

 private:
  utils::QueryMemoryTracker *prev_state_;
};

}  // namespace

bool TrackAllocOnCurrentThread(size_t size) {
  // Read and check tracker before blocker as it wil temporarily reset the tracker
  auto *const tracker = GetQueryTracker();
  if (!tracker) return true;

  const ThreadTrackingBlocker
      blocker{};  // makes sure we cannot recursively track allocations
                  // if allocations could happen here we would try to track that, which calls alloc
  return tracker->TrackAlloc(size);
}

void TrackFreeOnCurrentThread(size_t size) {
  // Read and check tracker before blocker as it wil temporarily reset the tracker
  auto *const tracker = GetQueryTracker();
  if (!tracker) return;

  const ThreadTrackingBlocker
      blocker{};  // makes sure we cannot recursively track allocations
                  // if allocations could happen here we would try to track that, which calls alloc
  tracker->TrackFree(size);
}

bool IsThreadTracked() { return GetQueryTracker() != nullptr; }

#endif

void StartTrackingCurrentThread(utils::QueryMemoryTracker *tracker) {
#if USE_JEMALLOC
  GetQueryTracker() = tracker;
#endif
}

void StopTrackingCurrentThread() {
#if USE_JEMALLOC
  GetQueryTracker() = nullptr;
#endif
}

bool IsQueryTracked() {
#if USE_JEMALLOC
  return IsThreadTracked();
#else
  return false;
#endif
}

void CreateOrContinueProcedureTracking(int64_t procedure_id, size_t limit) {
#if USE_JEMALLOC
  DMG_ASSERT(GetQueryTracker(), "Query memory tracker was not set");
  GetQueryTracker()->TryCreateProcTracker(procedure_id, limit);
  GetQueryTracker()->SetActiveProc(procedure_id);
#endif
}

void PauseProcedureTracking() {
#if USE_JEMALLOC
  DMG_ASSERT(GetQueryTracker(), "Query memory tracker was not set");
  GetQueryTracker()->StopProcTracking();
#endif
}

}  // namespace memgraph::memory
