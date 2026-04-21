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

#include <cstddef>
#include <cstdint>
#include <optional>

// Forward declaration
namespace memgraph::utils {
class QueryMemoryTracker;
struct UserResources;
}  // namespace memgraph::utils

namespace memgraph::memory {

static constexpr int64_t UNLIMITED_MEMORY{0};

// Find tracker for current thread if exists, track
// query allocation and procedure allocation if
// necessary.  In non-jemalloc builds this is a no-op
// that always returns true.
bool TrackAllocOnCurrentThread(size_t size);

// Find tracker for current thread if exists, track
// query allocation and procedure allocation if
// necessary.  In non-jemalloc builds this is a no-op.
void TrackFreeOnCurrentThread(size_t size);

// API function call to start tracking current thread.
// Does nothing if jemalloc is not enabled
void StartTrackingCurrentThread(utils::QueryMemoryTracker *tracker);

// API function call to stop tracking current thread.
// Does nothing if jemalloc is not enabled
void StopTrackingCurrentThread();

void StartTrackingUserResource(utils::UserResources *resource);
void StopTrackingUserResource();

// Is query's memory tracked
bool IsQueryTracked();

// Creates tracker on procedure if doesn't exist. Sets query tracker
// to track procedure with id.
void CreateOrContinueProcedureTracking(int64_t procedure_id, size_t limit);

// Pauses procedure tracking. This enables to continue
// tracking on procedure once procedure execution resumes.
void PauseProcedureTracking();

struct ThreadTrackingBlocker {
  ThreadTrackingBlocker();
  ~ThreadTrackingBlocker();

  ThreadTrackingBlocker(ThreadTrackingBlocker &) = delete;
  ThreadTrackingBlocker &operator=(ThreadTrackingBlocker &) = delete;
  ThreadTrackingBlocker(ThreadTrackingBlocker &&) = delete;
  ThreadTrackingBlocker &operator=(ThreadTrackingBlocker &&) = delete;

  utils::QueryMemoryTracker *GetPrevMemoryTracker() const { return prev_state_; }

  utils::UserResources *GetPrevUserTracker() const { return prev_user_state_; }

 private:
  utils::QueryMemoryTracker *prev_state_{nullptr};
  utils::UserResources *prev_user_state_{nullptr};
};

struct CrossThreadMemoryTracking {
  utils::QueryMemoryTracker *query_tracker{nullptr};
  utils::UserResources *user_tracker{nullptr};
  unsigned db_arena_idx{0};

  explicit CrossThreadMemoryTracking(unsigned arena_idx = 0);
  ~CrossThreadMemoryTracking() = default;

  void StartTracking();
  void StopTracking();

  CrossThreadMemoryTracking(CrossThreadMemoryTracking &) = delete;
  CrossThreadMemoryTracking &operator=(CrossThreadMemoryTracking &) = delete;
  CrossThreadMemoryTracking(CrossThreadMemoryTracking &&other) noexcept;
  CrossThreadMemoryTracking &operator=(CrossThreadMemoryTracking &&other) noexcept;

 private:
  std::optional<unsigned> prev_arena_;
};

}  // namespace memgraph::memory
