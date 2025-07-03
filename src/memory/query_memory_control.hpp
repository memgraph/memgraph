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
#pragma once

#include <cstddef>
#include <cstdint>

// Forward declaration
namespace memgraph::utils {
class QueryMemoryTracker;
struct UserResources;
}  // namespace memgraph::utils

namespace memgraph::memory {

static constexpr int64_t UNLIMITED_MEMORY{0};

#if USE_JEMALLOC

// Find tracker for current thread if exists, track
// query allocation and procedure allocation if
// necessary
bool TrackAllocOnCurrentThread(size_t size);

// Find tracker for current thread if exists, track
// query allocation and procedure allocation if
// necessary
void TrackFreeOnCurrentThread(size_t size);

bool IsThreadTracked();

#endif

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
#if USE_JEMALLOC
  ThreadTrackingBlocker();
  ~ThreadTrackingBlocker();

  ThreadTrackingBlocker(ThreadTrackingBlocker &) = delete;
  ThreadTrackingBlocker &operator=(ThreadTrackingBlocker &) = delete;
  ThreadTrackingBlocker(ThreadTrackingBlocker &&) = delete;
  ThreadTrackingBlocker &operator=(ThreadTrackingBlocker &&) = delete;

 private:
  utils::QueryMemoryTracker *prev_state_;
  utils::UserResources *prev_user_state_;
#endif
};

}  // namespace memgraph::memory
