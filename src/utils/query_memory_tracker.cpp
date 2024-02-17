// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/query_memory_tracker.hpp"
#include <atomic>
#include <optional>
#include "memory/query_memory_control.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::utils {

bool QueryMemoryTracker::TrackAlloc(size_t size) {
  if (query_tracker_.has_value()) [[likely]] {
    bool ok = query_tracker_->Alloc(static_cast<int64_t>(size));
    if (!ok) return false;
  }

  auto *proc_tracker = GetActiveProc();

  if (proc_tracker == nullptr) {
    return true;
  }

  return proc_tracker->Alloc(static_cast<int64_t>(size));
}
void QueryMemoryTracker::TrackFree(size_t size) {
  if (query_tracker_.has_value()) [[likely]] {
    query_tracker_->Free(static_cast<int64_t>(size));
  }

  auto *proc_tracker = GetActiveProc();

  if (proc_tracker == nullptr) {
    return;
  }

  proc_tracker->Free(static_cast<int64_t>(size));
}

void QueryMemoryTracker::SetQueryLimit(size_t size) {
  if (size == memgraph::memory::UNLIMITED_MEMORY) {
    return;
  }
  InitializeQueryTracker();
  query_tracker_->SetMaximumHardLimit(static_cast<int64_t>(size));
  query_tracker_->SetHardLimit(static_cast<int64_t>(size));
}

memgraph::utils::MemoryTracker *QueryMemoryTracker::GetActiveProc() {
  if (active_proc_id == NO_PROCEDURE) [[likely]] {
    return nullptr;
  }
  return &proc_memory_trackers_[active_proc_id];
}

void QueryMemoryTracker::SetActiveProc(int64_t new_active_proc) { active_proc_id = new_active_proc; }

void QueryMemoryTracker::StopProcTracking() { active_proc_id = QueryMemoryTracker::NO_PROCEDURE; }

void QueryMemoryTracker::TryCreateProcTracker(int64_t procedure_id, size_t limit) {
  if (proc_memory_trackers_.contains(procedure_id)) {
    return;
  }
  auto [it, inserted] = proc_memory_trackers_.emplace(procedure_id, utils::MemoryTracker{});
  it->second.SetMaximumHardLimit(static_cast<int64_t>(limit));
  it->second.SetHardLimit(static_cast<int64_t>(limit));
}

void QueryMemoryTracker::InitializeQueryTracker() { query_tracker_.emplace(MemoryTracker{}); }

}  // namespace memgraph::utils
