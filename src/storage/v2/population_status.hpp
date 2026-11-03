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

#include "storage/v2/timestamp_types.hpp"
#include "storage/v2/transaction_constants.hpp"

namespace memgraph::storage {

/// MVCC status tracking for schema objects (indices, constraints).
///
/// A schema object is visible to a transaction if it committed at or before the boundary that
/// transaction reads at, so both sides of that test are commit timestamps.
///
/// The populating sentinel sits above the whole committed range, so an object still being built is
/// visible to no snapshot and a reader does not have to test readiness separately.
///
/// Reads are atomic so that indices and constraints can be checked during query execution without
/// acquiring container locks.
struct PopulationStatus {
  static constexpr CommitTimestamp kPopulating = CommitTimestamp::FromUint(kTransactionInitialId);

  PopulationStatus() = default;
  ~PopulationStatus() = default;
  PopulationStatus(const PopulationStatus &) = delete;
  PopulationStatus &operator=(const PopulationStatus &) = delete;
  PopulationStatus(PopulationStatus &&) = delete;
  PopulationStatus &operator=(PopulationStatus &&) = delete;

  bool IsPopulating() const { return commit_timestamp_.load(std::memory_order_acquire) == kPopulating; }

  bool IsReady() const { return !IsPopulating(); }

  bool IsVisible(CommitTimestamp bound) const { return commit_timestamp_.load(std::memory_order_acquire) <= bound; }

  void Commit(CommitTimestamp timestamp) { commit_timestamp_.store(timestamp, std::memory_order_release); }

 private:
  std::atomic<CommitTimestamp> commit_timestamp_{kPopulating};
};

// A lock would defeat the point of reading this during query execution.
static_assert(std::atomic<CommitTimestamp>::is_always_lock_free);

}  // namespace memgraph::storage
