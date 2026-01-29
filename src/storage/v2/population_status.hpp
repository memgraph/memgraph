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

#include <atomic>

#include "storage/v2/transaction_constants.hpp"

namespace memgraph::storage {

/// MVCC status tracking for schema objects (indices, constraints).
///
/// Tracks the commit timestamp for proper snapshot isolation.
/// A schema object is visible to a transaction if:
///   commit_timestamp <= transaction_start_timestamp
///
/// Uses atomics to allow lock-free reads during query execution.
/// This enables indices/constraints to be checked without acquiring container locks,
/// improving performance for read-heavy workloads.
///
/// Timestamp space:
/// - [0, kTransactionInitialId): Committed transaction timestamps
/// - kTransactionInitialId (1<<63): POPULATING state sentinel
///
/// Recovery uses kTimestampInitialId (0) to make objects immediately visible.
struct PopulationStatus {
  // kTransactionInitialId used as sentinel - commit timestamps are always below this value
  static constexpr uint64_t kPopulating = kTransactionInitialId;

  PopulationStatus() = default;
  ~PopulationStatus() = default;
  PopulationStatus(const PopulationStatus &) = delete;
  PopulationStatus &operator=(const PopulationStatus &) = delete;
  PopulationStatus(PopulationStatus &&) = delete;
  PopulationStatus &operator=(PopulationStatus &&) = delete;

  bool IsPopulating() const { return commit_timestamp_.load(std::memory_order_acquire) == kPopulating; }

  bool IsReady() const { return !IsPopulating(); }

  /// A schema object is visible if it was committed at or before the given timestamp.
  bool IsVisible(uint64_t timestamp) const { return commit_timestamp_.load(std::memory_order_acquire) <= timestamp; }

  void Commit(uint64_t timestamp) { commit_timestamp_.store(timestamp, std::memory_order_release); }

 private:
  std::atomic<uint64_t> commit_timestamp_{kPopulating};
};

}  // namespace memgraph::storage
