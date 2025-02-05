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

#include <cstdint>

#include "utils/logging.hpp"

namespace memgraph::memory {

#if USE_JEMALLOC

namespace {

inline int &GetThreadTracked() {
  static thread_local int is_thread_tracked{0};
  return is_thread_tracked;
}

inline auto &GetQueryTracker() {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  static thread_local utils::QueryMemoryTracker *query_memory_tracker_ = nullptr;
  return query_memory_tracker_;
}

}  // namespace

bool TrackAllocOnCurrentThread(size_t size) {
  const ThreadTrackingBlocker
      blocker{};  // makes sure we cannot recursevly track allocations
                  // if allocations could happen here we would try to track that, which calls alloc
  return !GetQueryTracker() || GetQueryTracker()->TrackAlloc(size);
}

void TrackFreeOnCurrentThread(size_t size) {
  const ThreadTrackingBlocker
      blocker{};  // makes sure we cannot recursevly track allocations
                  // if allocations could happen here we would try to track that, which calls alloc
  if (GetQueryTracker()) GetQueryTracker()->TrackFree(size);
}

bool IsThreadTracked() { return GetThreadTracked() == 1; }

ThreadTrackingBlocker::ThreadTrackingBlocker() : prev_state_{GetThreadTracked()} {
  // Disable thread tracking
  GetThreadTracked() = 0;
}

ThreadTrackingBlocker::~ThreadTrackingBlocker() {
  // Reset thread tracking to previous state
  GetThreadTracked() = prev_state_;
}

#endif

void StartTrackingCurrentThread(utils::QueryMemoryTracker *tracker) {
#if USE_JEMALLOC
  GetQueryTracker() = tracker;
  GetThreadTracked() = 1;
#endif
}

void StopTrackingCurrentThread() {
#if USE_JEMALLOC
  GetQueryTracker() = nullptr;
  GetThreadTracked() = 0;
#endif
}

#if USE_JEMALLOC
bool IsQueryTracked() { return IsThreadTracked(); }
#else
bool IsQueryTracked() { return false; }
#endif

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
