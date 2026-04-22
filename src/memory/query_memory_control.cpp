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

#if USE_JEMALLOC

namespace {

inline auto &GetQueryTracker() {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  constinit static thread_local utils::QueryMemoryTracker *query_memory_tracker_ [[gnu::tls_model("initial-exec")]] =
      nullptr;
  return query_memory_tracker_;
}

inline auto &GetUserTracker() {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  constinit static thread_local utils::UserResources *user_resource_ [[gnu::tls_model("initial-exec")]] = nullptr;
  return user_resource_;
}

}  // namespace

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
#if USE_JEMALLOC
  GetQueryTracker() = tracker;
#else
  (void)tracker;
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
#else
  (void)resource;
#endif
}

void StopTrackingUserResource() {
#if USE_JEMALLOC
  GetUserTracker() = nullptr;
#endif
}

bool IsQueryTracked() {
#if USE_JEMALLOC
  // GC is running, no way to control what gets deleted, just ignore this allocation.
  return GetQueryTracker() != nullptr && !utils::detail::IsSkipListGcRunning();
#else
  return false;
#endif
}

void CreateOrContinueProcedureTracking(int64_t procedure_id, size_t limit) {
#if USE_JEMALLOC
  // No need for user tracking at this level, if it was needed, it would have already been setup by this point
  DMG_ASSERT(GetQueryTracker(), "Query memory tracker was not set");
  GetQueryTracker()->CreateOrSetProcTracker(procedure_id, limit);
#else
  (void)procedure_id;
  (void)limit;
#endif
}

void PauseProcedureTracking() {
#if USE_JEMALLOC
  DMG_ASSERT(GetQueryTracker(), "Query memory tracker was not set");
  GetQueryTracker()->StopProcTracking();
#endif
}

#if USE_JEMALLOC
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
#else
ThreadTrackingBlocker::ThreadTrackingBlocker() = default;
ThreadTrackingBlocker::~ThreadTrackingBlocker() = default;
#endif

#if USE_JEMALLOC
CrossThreadMemoryTracking::CrossThreadMemoryTracking(unsigned arena_idx)
    : query_tracker(GetQueryTracker()), user_tracker(GetUserTracker()), db_arena_idx(arena_idx) {}

void CrossThreadMemoryTracking::StartTracking() {
  DMG_ASSERT(!started_, "CrossThreadMemoryTracking::StartTracking called twice");
  started_ = true;
  prev_query_tracker_ = GetQueryTracker();
  prev_user_tracker_ = GetUserTracker();
  GetQueryTracker() = query_tracker;
  GetUserTracker() = user_tracker;
  if (db_arena_idx != 0) {
    prev_arena_ = memory::tls_db_arena_state.arena;
    memory::tls_db_arena_state.arena = db_arena_idx;
  }
}

void CrossThreadMemoryTracking::StopTracking() {
  if (!started_) return;
  GetQueryTracker() = prev_query_tracker_;
  GetUserTracker() = prev_user_tracker_;
  prev_query_tracker_ = nullptr;
  prev_user_tracker_ = nullptr;
  if (prev_arena_) {
    memory::tls_db_arena_state.arena = *prev_arena_;
    prev_arena_.reset();
  }
  started_ = false;
}

CrossThreadMemoryTracking::CrossThreadMemoryTracking(CrossThreadMemoryTracking &&other) noexcept
    : query_tracker(std::exchange(other.query_tracker, nullptr)),
      user_tracker(std::exchange(other.user_tracker, nullptr)),
      db_arena_idx(std::exchange(other.db_arena_idx, 0U)),
      prev_query_tracker_(std::exchange(other.prev_query_tracker_, nullptr)),
      prev_user_tracker_(std::exchange(other.prev_user_tracker_, nullptr)),
      prev_arena_(std::exchange(other.prev_arena_, std::nullopt)),
      started_(std::exchange(other.started_, false)) {}

CrossThreadMemoryTracking &CrossThreadMemoryTracking::operator=(CrossThreadMemoryTracking &&other) noexcept {
  if (this != &other) {
    DMG_ASSERT(!started_, "Cannot overwrite active CrossThreadMemoryTracking");
    query_tracker = std::exchange(other.query_tracker, nullptr);
    user_tracker = std::exchange(other.user_tracker, nullptr);
    db_arena_idx = std::exchange(other.db_arena_idx, 0U);
    prev_query_tracker_ = std::exchange(other.prev_query_tracker_, nullptr);
    prev_user_tracker_ = std::exchange(other.prev_user_tracker_, nullptr);
    prev_arena_ = std::exchange(other.prev_arena_, std::nullopt);
    started_ = std::exchange(other.started_, false);
  }
  return *this;
}
#else   // !USE_JEMALLOC
CrossThreadMemoryTracking::CrossThreadMemoryTracking(unsigned arena_idx) : db_arena_idx(arena_idx) {}

void CrossThreadMemoryTracking::StartTracking() {
  DMG_ASSERT(!started_, "CrossThreadMemoryTracking::StartTracking called twice");
  started_ = true;
  if (db_arena_idx != 0) {
    prev_arena_ = memory::tls_db_arena_state.arena;
    memory::tls_db_arena_state.arena = db_arena_idx;
  }
}

void CrossThreadMemoryTracking::StopTracking() {
  if (!started_) return;
  if (prev_arena_) {
    memory::tls_db_arena_state.arena = *prev_arena_;
    prev_arena_.reset();
  }
  started_ = false;
}

CrossThreadMemoryTracking::CrossThreadMemoryTracking(CrossThreadMemoryTracking &&other) noexcept
    : db_arena_idx(std::exchange(other.db_arena_idx, 0U)),
      prev_query_tracker_(std::exchange(other.prev_query_tracker_, nullptr)),
      prev_user_tracker_(std::exchange(other.prev_user_tracker_, nullptr)),
      prev_arena_(std::exchange(other.prev_arena_, std::nullopt)),
      started_(std::exchange(other.started_, false)) {}

CrossThreadMemoryTracking &CrossThreadMemoryTracking::operator=(CrossThreadMemoryTracking &&other) noexcept {
  if (this != &other) {
    DMG_ASSERT(!started_, "Cannot overwrite active CrossThreadMemoryTracking");
    db_arena_idx = std::exchange(other.db_arena_idx, 0U);
    prev_query_tracker_ = std::exchange(other.prev_query_tracker_, nullptr);
    prev_user_tracker_ = std::exchange(other.prev_user_tracker_, nullptr);
    prev_arena_ = std::exchange(other.prev_arena_, std::nullopt);
    started_ = std::exchange(other.started_, false);
  }
  return *this;
}
#endif  // USE_JEMALLOC
}  // namespace memgraph::memory
