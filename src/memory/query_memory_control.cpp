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

#include <cstdint>
#include <utility>

#include "query_memory_control.hpp"

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "utils/db_aware_allocator.hpp"

#include "utils/logging.hpp"
#include "utils/query_memory_tracker.hpp"
#include "utils/resource_monitoring.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::memory {

namespace {
constexpr bool kUseJemalloc = USE_JEMALLOC;

inline auto &QueryTrackerStorage() {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  constinit static thread_local utils::QueryMemoryTracker *query_memory_tracker_ [[gnu::tls_model("initial-exec")]] =
      nullptr;
  return query_memory_tracker_;
}

inline auto &UserTrackerStorage() {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  constinit static thread_local utils::UserResources *user_resource_ [[gnu::tls_model("initial-exec")]] = nullptr;
  return user_resource_;
}

inline auto GetQueryTracker() -> utils::QueryMemoryTracker * {
  if constexpr (kUseJemalloc) {
    return QueryTrackerStorage();
  }
  return nullptr;
}

inline void SetQueryTracker(utils::QueryMemoryTracker *tracker) {
  if constexpr (kUseJemalloc) {
    QueryTrackerStorage() = tracker;
  } else {
    (void)tracker;
  }
}

inline auto GetUserTracker() -> utils::UserResources * {
  if constexpr (kUseJemalloc) {
    return UserTrackerStorage();
  }
  return nullptr;
}

inline void SetUserTracker(utils::UserResources *resource) {
  if constexpr (kUseJemalloc) {
    UserTrackerStorage() = resource;
  } else {
    (void)resource;
  }
}

}  // namespace

#if USE_JEMALLOC

bool TrackAllocOnCurrentThread(size_t size) {
  const ThreadTrackingBlocker blocker{};  // makes sure we cannot recursively track allocations
  // if allocations could happen here we would try to track that, which calls alloc

  // Read the tracker from the blocker because it temporarily resets tracking.
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

  // Read the tracker from the blocker because it temporarily resets tracking.
  auto *const tracker = blocker.GetPrevMemoryTracker();
  if (!tracker) return;

  tracker->TrackFree(size);
  auto *const user_resource = blocker.GetPrevUserTracker();
  if (user_resource) user_resource->DecrementTransactionsMemory(size);
}

#else  // !USE_JEMALLOC

bool TrackAllocOnCurrentThread(size_t /*size*/) { return true; }

void TrackFreeOnCurrentThread(size_t /*size*/) {}

#endif  // USE_JEMALLOC

void StartTrackingCurrentThread(utils::QueryMemoryTracker *tracker) {
  SetQueryTracker(tracker);
}

void StopTrackingCurrentThread() { SetQueryTracker(nullptr); }

void StartTrackingUserResource(utils::UserResources *resource) { SetUserTracker(resource); }

void StopTrackingUserResource() { SetUserTracker(nullptr); }

bool IsQueryTracked() {
  if constexpr (kUseJemalloc) {
    // GC is running, no way to control what gets deleted, just ignore this allocation.
    return GetQueryTracker() != nullptr && !utils::detail::IsSkipListGcRunning();
  }
  return false;
}

void CreateOrContinueProcedureTracking(int64_t procedure_id, size_t limit) {
  if constexpr (kUseJemalloc) {
    // No need for user tracking at this level, if it was needed, it would have already been setup by this point
    DMG_ASSERT(GetQueryTracker(), "Query memory tracker was not set");
    GetQueryTracker()->CreateOrSetProcTracker(procedure_id, limit);
  } else {
    (void)procedure_id;
    (void)limit;
  }
}

void PauseProcedureTracking() {
  if constexpr (kUseJemalloc) {
    DMG_ASSERT(GetQueryTracker(), "Query memory tracker was not set");
    GetQueryTracker()->StopProcTracking();
  }
}

ThreadTrackingBlocker::ThreadTrackingBlocker() : prev_state_{GetQueryTracker()}, prev_user_state_(GetUserTracker()) {
  // Disable thread tracking
  SetQueryTracker(nullptr);
  SetUserTracker(nullptr);
}

ThreadTrackingBlocker::~ThreadTrackingBlocker() {
  // Reset thread tracking to previous state
  SetQueryTracker(prev_state_);
  SetUserTracker(prev_user_state_);
}

CrossThreadMemoryTracking::CrossThreadMemoryTracking(ArenaPool *arena_pool)
    : query_tracker(GetQueryTracker()), user_tracker(GetUserTracker()), db_arena_pool(arena_pool) {}

void CrossThreadMemoryTracking::StartTracking() {
  DMG_ASSERT(!started_, "CrossThreadMemoryTracking::StartTracking called twice");
  started_ = true;
  prev_query_tracker_ = GetQueryTracker();
  prev_user_tracker_ = GetUserTracker();
  SetQueryTracker(query_tracker);
  SetUserTracker(user_tracker);
  if (db_arena_pool) db_arena_scope_.emplace(db_arena_pool);
}

void CrossThreadMemoryTracking::StopTracking() {
  if (!started_) return;
  SetQueryTracker(prev_query_tracker_);
  SetUserTracker(prev_user_tracker_);
  prev_query_tracker_ = nullptr;
  prev_user_tracker_ = nullptr;
  db_arena_scope_.reset();
  started_ = false;
}

CrossThreadMemoryTracking::CrossThreadMemoryTracking(CrossThreadMemoryTracking &&other) noexcept
    : query_tracker(std::exchange(other.query_tracker, nullptr)),
      user_tracker(std::exchange(other.user_tracker, nullptr)),
      db_arena_pool(std::exchange(other.db_arena_pool, nullptr)),
      prev_query_tracker_(std::exchange(other.prev_query_tracker_, nullptr)),
      prev_user_tracker_(std::exchange(other.prev_user_tracker_, nullptr)),
      started_(false) {
  DMG_ASSERT(!other.started_ && !other.db_arena_scope_.has_value(), "Cannot move active CrossThreadMemoryTracking");
}

CrossThreadMemoryTracking &CrossThreadMemoryTracking::operator=(CrossThreadMemoryTracking &&other) noexcept {
  if (this != &other) {
    DMG_ASSERT(!started_, "Cannot overwrite active CrossThreadMemoryTracking");
    DMG_ASSERT(!other.started_ && !other.db_arena_scope_.has_value(), "Cannot move active CrossThreadMemoryTracking");
    query_tracker = std::exchange(other.query_tracker, nullptr);
    user_tracker = std::exchange(other.user_tracker, nullptr);
    db_arena_pool = std::exchange(other.db_arena_pool, nullptr);
    prev_query_tracker_ = std::exchange(other.prev_query_tracker_, nullptr);
    prev_user_tracker_ = std::exchange(other.prev_user_tracker_, nullptr);
    started_ = false;
  }
  return *this;
}
}  // namespace memgraph::memory
