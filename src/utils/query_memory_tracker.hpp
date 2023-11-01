// Copyright 2023 Memgraph Ltd.
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

#include <atomic>
#include <optional>
#include <unordered_map>
#include <utility>
#include "utils/memory_tracker.hpp"

namespace memgraph::utils {

class QueryMemoryTracker {
 public:
  QueryMemoryTracker() { proc_memory_trackers_.emplace(0, std::nullopt); }

  QueryMemoryTracker(QueryMemoryTracker &&other) noexcept
      : query_tracker_(std::move(other.query_tracker_)), proc_memory_trackers_(std::move(other.proc_memory_trackers_)) {
    active_proc_id.store(other.active_proc_id.load(std::memory_order_acquire), std::memory_order_acq_rel);
    other.active_proc_id.store(0, std::memory_order_acq_rel);
  }

  QueryMemoryTracker(const QueryMemoryTracker &other) = delete;

  QueryMemoryTracker &operator=(QueryMemoryTracker &&other) = delete;
  QueryMemoryTracker &operator=(const QueryMemoryTracker &other) = delete;

  ~QueryMemoryTracker() = default;

  void TrackAlloc(size_t);
  void TrackFree(size_t);

  void SetQueryLimit(size_t);

  void TryCreateProcTracker(int64_t, size_t);
  void SetActiveProc(int64_t);
  void StopProcTracking();

 protected:
 private:
  void InitializeQueryTracker();

  std::optional<memgraph::utils::MemoryTracker> query_tracker_{std::nullopt};
  std::unordered_map<int64_t, std::optional<memgraph::utils::MemoryTracker>> proc_memory_trackers_;
  std::atomic<int64_t> active_proc_id{0};

  std::optional<memgraph::utils::MemoryTracker> &GetActiveProc();
};

}  // namespace memgraph::utils
