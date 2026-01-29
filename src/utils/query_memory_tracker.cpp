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

#include "utils/query_memory_tracker.hpp"

#include "memory/query_memory_control.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::utils {

namespace {
inline auto &GetProcTracker() {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  static thread_local memgraph::utils::MemoryTracker *proc_tracker = nullptr;
  return proc_tracker;
}
}  // namespace

bool QueryMemoryTracker::TrackAlloc(size_t size) {
  const bool ok = transaction_tracker_.Alloc(static_cast<int64_t>(size));
  if (!ok) return false;

  auto *proc_tracker = GetProcTracker();
  if (proc_tracker == nullptr) {
    return true;
  }
  return proc_tracker->Alloc(static_cast<int64_t>(size));
}

void QueryMemoryTracker::TrackFree(size_t size) {
  transaction_tracker_.Free(static_cast<int64_t>(size));

  auto *proc_tracker = GetProcTracker();
  if (proc_tracker == nullptr) {
    return;
  }
  proc_tracker->Free(static_cast<int64_t>(size));
}

// NOTE: Currently the transaction tracker does not limit the memory usage of the query.
void QueryMemoryTracker::SetQueryLimit(size_t size) {
  if (size == memgraph::memory::UNLIMITED_MEMORY) {
    transaction_tracker_.ResetLimit();
    return;
  }
  // Increment the limit by the requested size.
  // This is to allow the transaction tracker to grow its limit if needed.
  const auto current = transaction_tracker_.Amount();
  transaction_tracker_.SetMaximumHardLimit(current + static_cast<int64_t>(size));
  transaction_tracker_.SetHardLimit(current + static_cast<int64_t>(size));
}

int64_t QueryMemoryTracker::Amount() const { return transaction_tracker_.Amount(); }

void QueryMemoryTracker::StopProcTracking() { GetProcTracker() = nullptr; }

void QueryMemoryTracker::CreateOrSetProcTracker(int64_t procedure_id, size_t limit) {
  auto *&proc_tracker = GetProcTracker();
  if (procedure_id == NO_PROCEDURE) [[unlikely]] {
    proc_tracker = nullptr;
    return;
  }
  // Read-only access to the proc tracker
  {
    const std::shared_lock lock(proc_trackers_mutex_);
    auto it = proc_memory_trackers_.find(procedure_id);
    if (it != proc_memory_trackers_.end()) {
      proc_tracker = &it->second;
      return;
    }
  }
  // Write access to the proc tracker
  {
    const std::unique_lock lock(proc_trackers_mutex_);
    auto [it, inserted] = proc_memory_trackers_.emplace(
        std::piecewise_construct, std::forward_as_tuple(procedure_id), std::forward_as_tuple());
    // Only set limits if we actually inserted a new entry.
    // Another thread may have inserted between releasing the shared lock and acquiring the unique lock.
    // Setting limits on an already-in-use tracker would cause data races and incorrect behavior.
    if (inserted) {
      it->second.SetMaximumHardLimit(static_cast<int64_t>(limit));
      it->second.SetHardLimit(static_cast<int64_t>(limit));
    }
    proc_tracker = &it->second;
  }
}

}  // namespace memgraph::utils
