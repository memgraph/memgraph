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

#include <cstdint>

#include "query_memory_control.hpp"

#include "utils/logging.hpp"
#include "utils/query_memory_tracker.hpp"
#include "utils/resource_monitoring.hpp"

namespace memgraph::memory {

#if USE_JEMALLOC

namespace {

inline auto &GetQueryTracker() {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  static thread_local utils::QueryMemoryTracker *query_memory_tracker_ = nullptr;
  return query_memory_tracker_;
}

inline auto &GetUserTracker() {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  static thread_local utils::UserResources *user_resource_ = nullptr;
  return user_resource_;
}

struct ThreadTrackingBlocker {
  ThreadTrackingBlocker() : prev_state_{GetQueryTracker()}, prev_user_state_(GetUserTracker()) {
    // Disable thread tracking
    GetQueryTracker() = nullptr;
    GetUserTracker() = nullptr;
  }

  ~ThreadTrackingBlocker() {
    // Reset thread tracking to previous state
    GetQueryTracker() = prev_state_;
    GetUserTracker() = prev_user_state_;
  }

  ThreadTrackingBlocker(ThreadTrackingBlocker &) = delete;
  ThreadTrackingBlocker &operator=(ThreadTrackingBlocker &) = delete;
  ThreadTrackingBlocker(ThreadTrackingBlocker &&) = delete;
  ThreadTrackingBlocker &operator=(ThreadTrackingBlocker &&) = delete;

 private:
  utils::QueryMemoryTracker *prev_state_;
  utils::UserResources *prev_user_state_;
};

}  // namespace

bool TrackAllocOnCurrentThread(size_t size) {
  // Read and check tracker before blocker as it wil temporarily reset the tracker
  auto *const tracker = GetQueryTracker();
  auto *const user_resource = GetUserTracker();
  if (!tracker && !user_resource) return true;

  const ThreadTrackingBlocker
      blocker{};  // makes sure we cannot recursively track allocations
                  // if allocations could happen here we would try to track that, which calls alloc

  if (user_resource && !user_resource->IncrementTransactionsMemory(size)) {
    // register our error data, we will pick this up on the other side of jemalloc
    utils::MemoryErrorStatus().set({1, 2, 3});
    return false;
  }
  if (tracker && !tracker->TrackAlloc(size)) {
    if (user_resource) user_resource->DecrementTransactionsMemory(size);
    return false;
  }
  return true;
}

void TrackFreeOnCurrentThread(size_t size) {
  // Read and check tracker before blocker as it wil temporarily reset the tracker
  auto *const tracker = GetQueryTracker();
  auto *const user_resource = GetUserTracker();
  if (!tracker && !user_resource) return;

  const ThreadTrackingBlocker
      blocker{};  // makes sure we cannot recursively track allocations
                  // if allocations could happen here we would try to track that, which calls alloc
  if (user_resource) user_resource->DecrementTransactionsMemory(size);
  if (tracker) tracker->TrackFree(size);
}

bool IsThreadTracked() { return GetQueryTracker() != nullptr || GetUserTracker() != nullptr; }

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

void StartTrackingUserResource(utils::UserResources *resource) {
#if USE_JEMALLOC
  GetUserTracker() = resource;
#endif
}

void StopTrackingUserResource() {
#if USE_JEMALLOC
  GetUserTracker() = nullptr;
#endif
}

bool IsQueryTracked() {
#if USE_JEMALLOC
  return GetQueryTracker() != nullptr;
#else
  return false;
#endif
}

// TODO
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
