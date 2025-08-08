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

#include "utils/query_memory_tracker.hpp"
#include <atomic>
#include <optional>
#include "memory/query_memory_control.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::utils {

bool QueryMemoryTracker::TrackAlloc(size_t size) {
  if (transaction_tracker_.has_value()) [[likely]] {
    const bool ok = transaction_tracker_->Alloc(static_cast<int64_t>(size));
    if (!ok) return false;
  }

  auto *proc_tracker = GetActiveProc();

  if (proc_tracker == nullptr) {
    return true;
  }

  return proc_tracker->Alloc(static_cast<int64_t>(size));
}

void QueryMemoryTracker::TrackFree(size_t size) {
  if (transaction_tracker_.has_value()) [[likely]] {
    transaction_tracker_->Free(static_cast<int64_t>(size));
  }

  auto *proc_tracker = GetActiveProc();

  if (proc_tracker == nullptr) {
    return;
  }

  proc_tracker->Free(static_cast<int64_t>(size));
}

// NOTE: Currently the transaction tracker does not limit the memory usage of the query.
void QueryMemoryTracker::SetQueryLimit(size_t size) {
  if (!transaction_tracker_) InitializeTransactionTracker();
  if (size == memgraph::memory::UNLIMITED_MEMORY) {
    transaction_tracker_->ResetLimit();
    return;
  }
  // Increment the limit by the requested size.
  // This is to allow the transaction tracker to grow its limit if needed.
  const auto current = transaction_tracker_->Amount();
  transaction_tracker_->SetMaximumHardLimit(current + static_cast<int64_t>(size));
  transaction_tracker_->SetHardLimit(current + static_cast<int64_t>(size));
}

memgraph::utils::MemoryTracker *QueryMemoryTracker::GetActiveProc() {
  if (active_proc_id == NO_PROCEDURE) [[likely]] {
    return nullptr;
  }
  return &proc_memory_trackers_[active_proc_id];
}

void QueryMemoryTracker::SetActiveProc(int64_t new_active_proc) { active_proc_id = new_active_proc; }

void QueryMemoryTracker::StopProcTracking() { active_proc_id = QueryMemoryTracker::NO_PROCEDURE; }

int64_t QueryMemoryTracker::Amount() const {
  if (transaction_tracker_.has_value()) [[likely]] {
    return transaction_tracker_->Amount();
  }
  return 0;
}

void QueryMemoryTracker::TryCreateProcTracker(int64_t procedure_id, size_t limit) {
  if (proc_memory_trackers_.contains(procedure_id)) {
    return;
  }
  auto [it, inserted] = proc_memory_trackers_.emplace(std::piecewise_construct, std::forward_as_tuple(procedure_id),
                                                      std::forward_as_tuple());
  it->second.SetMaximumHardLimit(static_cast<int64_t>(limit));
  it->second.SetHardLimit(static_cast<int64_t>(limit));
}

void QueryMemoryTracker::InitializeTransactionTracker() { transaction_tracker_.emplace(); }

}  // namespace memgraph::utils
