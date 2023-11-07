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

// Class which handles tracking of query memory
// together with procedure memory. Default
// active procedure id is set to -1, as
// procedures start from id 1.
// This class is meant to be used in environment
// where we don't execute multiple procedures in parallel
// but procedure can have multiple threads which are doing allocations
class QueryMemoryTracker {
 public:
  QueryMemoryTracker() = default;

  QueryMemoryTracker(QueryMemoryTracker &&other) noexcept
      : query_tracker_(std::move(other.query_tracker_)),
        proc_memory_trackers_(std::move(other.proc_memory_trackers_)),
        active_proc_id(other.active_proc_id) {
    other.active_proc_id = NO_PROCEDURE;
  }

  QueryMemoryTracker(const QueryMemoryTracker &other) = delete;

  QueryMemoryTracker &operator=(QueryMemoryTracker &&other) = delete;
  QueryMemoryTracker &operator=(const QueryMemoryTracker &other) = delete;

  ~QueryMemoryTracker() = default;

  // Track allocation on query and procedure if active
  void TrackAlloc(size_t);

  // Track Free on query and procedure if active
  void TrackFree(size_t);

  // Set query limit
  void SetQueryLimit(size_t);

  // Create proc tracker if doesn't exist
  void TryCreateProcTracker(int64_t, size_t);

  // Set currently active procedure
  void SetActiveProc(int64_t);

  // Stop procedure tracking
  void StopProcTracking();

 private:
  static constexpr int64_t NO_PROCEDURE{-1};
  void InitializeQueryTracker();

  std::optional<memgraph::utils::MemoryTracker> query_tracker_{std::nullopt};
  std::unordered_map<int64_t, memgraph::utils::MemoryTracker> proc_memory_trackers_;

  // Procedure ids start from 1. Procedure id -1 means there is no procedure
  // to track.
  int64_t active_proc_id{NO_PROCEDURE};

  memgraph::utils::MemoryTracker *GetActiveProc();
};

}  // namespace memgraph::utils
