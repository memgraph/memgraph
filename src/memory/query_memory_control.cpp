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
#include "utils/skip_list.hpp"

namespace memgraph::memory {

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

}  // namespace

bool TrackAllocOnCurrentThread(size_t size) {
  const ThreadTrackingBlocker blocker{};  // makes sure we cannot recursively track allocations
  // if allocations could happen here we would try to track that, which calls alloc

  // Read and check tracker before blocker as it wil temporarily reset the tracker
  auto *const tracker = blocker.GetPrevMemoryTracker();
  if (!tracker) return true;

  if (!tracker->TrackAlloc(size)) {
    return false;
  }
  auto *const user_resource = blocker.GetPrevUserTracker();
  if (user_resource && !user_resource->IncrementTransactionsMemory(size)) {
    tracker->TrackFree(size);
    return false;
  }

  return true;
}

void TrackFreeOnCurrentThread(size_t size) {
  const ThreadTrackingBlocker blocker{};  // makes sure we cannot recursively track allocations
  // if allocations could happen here we would try to track that, which calls alloc

  // Read and check tracker before blocker as it wil temporarily reset the tracker
  auto *const tracker = blocker.GetPrevMemoryTracker();
  if (!tracker) return;

  tracker->TrackFree(size);
  auto *const user_resource = blocker.GetPrevUserTracker();
  if (user_resource) user_resource->DecrementTransactionsMemory(size);
}

void StartTrackingCurrentThread(utils::QueryMemoryTracker *tracker) { GetQueryTracker() = tracker; }

void StopTrackingCurrentThread() { GetQueryTracker() = nullptr; }

void StartTrackingUserResource(utils::UserResources *resource) { GetUserTracker() = resource; }

void StopTrackingUserResource() { GetUserTracker() = nullptr; }

bool IsQueryTracked() {
  return GetQueryTracker() != nullptr && !utils::detail::IsSkipListGcRunning();
  // GC is running, no way to control what gets deleted, just ignore this allocation;
}

void CreateOrContinueProcedureTracking(int64_t procedure_id, size_t limit) {
  // No need for user tracking at this level, if it was needed, it would have already been setup by this point
  DMG_ASSERT(GetQueryTracker(), "Query memory tracker was not set");
  GetQueryTracker()->TryCreateProcTracker(procedure_id, limit);
  GetQueryTracker()->SetActiveProc(procedure_id);
}

void PauseProcedureTracking() {
  DMG_ASSERT(GetQueryTracker(), "Query memory tracker was not set");
  GetQueryTracker()->StopProcTracking();
}

ThreadTrackingBlocker::ThreadTrackingBlocker() : prev_state_{GetQueryTracker()}, prev_user_state_(GetUserTracker()) {
  // Disable thread tracking
  GetQueryTracker() = nullptr;
  GetUserTracker() = nullptr;
}

ThreadTrackingBlocker::~ThreadTrackingBlocker() {
  // Reset thread tracking to previous state
  GetQueryTracker() = prev_state_;
  GetUserTracker() = prev_user_state_;
}

}  // namespace memgraph::memory
