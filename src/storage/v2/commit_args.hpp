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

#include "storage/v2/database_access.hpp"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

#include <optional>
#include <variant>

namespace memgraph::storage {

class TransactionReplication;

struct CommitArgs {
  static auto make_main(DatabaseProtectorPtr protector) -> CommitArgs { return CommitArgs{Main{std::move(protector)}}; }

  static auto make_replica_write(uint64_t desired_commit_timestamp, bool two_phase_commit) -> CommitArgs {
    return CommitArgs{ReplicaWrite{desired_commit_timestamp, two_phase_commit}};
  }

  static auto make_replica_read() -> CommitArgs { return CommitArgs{ReplicaRead{}}; }

  auto durable_timestamp(uint64_t commit_timestamp) const -> uint64_t {
    auto const f = utils::Overloaded{[&](Main const &main) { return commit_timestamp; },
                                     [](ReplicaWrite const &replica) { return replica.desired_commit_timestamp; },
                                     [](ReplicaRead const &) -> uint64_t { MG_ASSERT(false, "invalid state"); }};
    return std::visit(f, data);
  }

  bool durability_allowed() const { return !std::holds_alternative<ReplicaRead>(data); }
  bool replication_allowed() const { return std::holds_alternative<Main>(data); }

  auto database_protector() const -> DatabaseProtector const & {
    auto const f =
        utils::Overloaded{[&](Main const &main) -> DatabaseProtector const & { return *main.db_acc; },
                          [](auto const &) -> DatabaseProtector const & { MG_ASSERT(false, "invalid state"); }};
    return std::visit(f, data);
  }

  bool two_phase_commit(TransactionReplication &replicating_txn) const;

  template <typename Func>
  bool apply_if_replica_write(Func &&func) const {
    auto const f = utils::Overloaded{[](auto const &) { return false; },
                                     [&](ReplicaWrite const &replica) {
                                       func(replica.two_phase_commit_, replica.desired_commit_timestamp);
                                       return true;
                                     }};
    return std::visit(f, data);
  }

  template <typename Func>
  auto apply_if_main(Func &&func) const
      -> std::pair<bool, std::optional<std::invoke_result_t<Func, DatabaseProtector const &>>> {
    // TODO: return just optional, not pair
    using retult_t = std::optional<std::invoke_result_t<Func, DatabaseProtector const &>>;
    auto const f = utils::Overloaded{[](auto const &) {
                                       return std::pair{false, retult_t{}};
                                     },
                                     [&](Main const &main) {
                                       return std::pair{true, std::optional{func(*main.db_acc)}};
                                     }};
    return std::visit(f, data);
  }

 private:
  struct ReplicaRead {};

  struct ReplicaWrite {
    // REPLICA on receipt of Deltas will have a desired commit timestamp
    uint64_t desired_commit_timestamp{};
    // false for SYNC/ASYNC replica, true for STRICT_SYNC replica
    bool two_phase_commit_ = false;
  };

  struct Main {
    // Needed for ASYNC replication tasks
    DatabaseProtectorPtr db_acc;
  };

  explicit CommitArgs(std::variant<Main, ReplicaWrite, ReplicaRead> data) : data(std::move(data)) {}

  std::variant<Main, ReplicaWrite, ReplicaRead> data;
};
}  // namespace memgraph::storage
