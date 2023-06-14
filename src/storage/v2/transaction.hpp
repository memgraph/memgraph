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
#include <limits>
#include <list>
#include <memory>

#include "utils/skip_list.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::storage {

const uint64_t kTimestampInitialId = 0;
const uint64_t kTransactionInitialId = 1ULL << 63U;

struct Transaction {
  Transaction(uint64_t transaction_id, uint64_t start_timestamp, IsolationLevel isolation_level,
              StorageMode storage_mode)
      : transaction_id(transaction_id),
        start_timestamp(start_timestamp),
        command_id(0),
        must_abort(false),
        isolation_level(isolation_level),
        storage_mode(storage_mode) {}

  Transaction(Transaction &&other) noexcept
      : transaction_id(other.transaction_id.load(std::memory_order_acquire)),
        start_timestamp(other.start_timestamp),
        commit_timestamp(std::move(other.commit_timestamp)),
        command_id(other.command_id),
        deltas(std::move(other.deltas)),
        must_abort(other.must_abort),
        isolation_level(other.isolation_level),
        storage_mode(other.storage_mode) {}

  Transaction(const Transaction &) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&other) = delete;

  ~Transaction() {}

  /// @throw std::bad_alloc if failed to create the `commit_timestamp`
  void EnsureCommitTimestampExists() {
    if (commit_timestamp != nullptr) return;
    commit_timestamp = std::make_unique<std::atomic<uint64_t>>(transaction_id.load(std::memory_order_relaxed));
  }

  std::atomic<uint64_t> transaction_id;
  uint64_t start_timestamp;
  // The `Transaction` object is stack allocated, but the `commit_timestamp`
  // must be heap allocated because `Delta`s have a pointer to it, and that
  // pointer must stay valid after the `Transaction` is moved into
  // `committed_transactions_` list for GC.
  std::unique_ptr<std::atomic<uint64_t>> commit_timestamp;
  uint64_t command_id;
  std::list<Delta> deltas;
  bool must_abort;
  IsolationLevel isolation_level;
  StorageMode storage_mode;
};

inline bool operator==(const Transaction &first, const Transaction &second) {
  return first.transaction_id.load(std::memory_order_acquire) == second.transaction_id.load(std::memory_order_acquire);
}
inline bool operator<(const Transaction &first, const Transaction &second) {
  return first.transaction_id.load(std::memory_order_acquire) < second.transaction_id.load(std::memory_order_acquire);
}
inline bool operator==(const Transaction &first, const uint64_t &second) {
  return first.transaction_id.load(std::memory_order_acquire) == second;
}
inline bool operator<(const Transaction &first, const uint64_t &second) {
  return first.transaction_id.load(std::memory_order_acquire) < second;
}

}  // namespace memgraph::storage
