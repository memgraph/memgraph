// Copyright 2022 Memgraph Ltd.
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

#include <limits>
#include <list>
#include <memory>

#include "coordinator/hybrid_logical_clock.hpp"
#include "storage/v3/delta.hpp"
#include "storage/v3/edge.hpp"
#include "storage/v3/isolation_level.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/vertex.hpp"
#include "storage/v3/view.hpp"

namespace memgraph::storage::v3 {

struct CommitInfo {
  bool is_locally_committed{false};
  coordinator::Hlc start_or_commit_timestamp;
};

struct Transaction {
  Transaction(coordinator::Hlc start_timestamp, IsolationLevel isolation_level)
      : start_timestamp(start_timestamp),
        commit_info(std::make_unique<CommitInfo>(CommitInfo{false, {start_timestamp}})),
        command_id(0),
        must_abort(false),
        is_aborted(false),
        isolation_level(isolation_level) {}

  Transaction(Transaction &&other) noexcept
      : start_timestamp(other.start_timestamp),
        commit_info(std::move(other.commit_info)),
        command_id(other.command_id),
        deltas(std::move(other.deltas)),
        must_abort(other.must_abort),
        is_aborted(other.is_aborted),
        isolation_level(other.isolation_level) {}

  Transaction(const Transaction &) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&other) = delete;

  ~Transaction() {}

  coordinator::Hlc start_timestamp;
  std::unique_ptr<CommitInfo> commit_info;
  uint64_t command_id;
  std::list<Delta> deltas;
  bool must_abort;
  bool is_aborted;
  IsolationLevel isolation_level;
};

// Relies on start timestamps are unique
inline bool operator==(const Transaction &first, const Transaction &second) {
  return first.start_timestamp == second.start_timestamp;
}
inline bool operator<(const Transaction &first, const Transaction &second) {
  return first.start_timestamp < second.start_timestamp;
}
inline bool operator==(const Transaction &first, const uint64_t second) { return first.start_timestamp == second; }
inline bool operator<(const Transaction &first, const uint64_t second) { return first.start_timestamp < second; }

}  // namespace memgraph::storage::v3
