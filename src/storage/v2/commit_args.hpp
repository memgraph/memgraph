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

#include "storage/v2/database_protector.hpp"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

#include <optional>
#include <variant>

namespace memgraph::storage {

class TransactionReplication;

struct CommitArgs {
  static auto make_main(DatabaseProtectorPtr protector) -> CommitArgs { return CommitArgs{Main{std::move(protector)}}; }

  static auto make_replica_write(uint64_t const desired_commit_timestamp, bool const two_phase_commit,
                                 std::function<void()> in_progress_cb) -> CommitArgs {
    return CommitArgs{ReplicaWrite{.desired_commit_timestamp = desired_commit_timestamp,
                                   .two_phase_commit_ = two_phase_commit,
                                   .in_progress_cb_ = std::move(in_progress_cb)}};
  }

  static auto make_replica_read() -> CommitArgs { return CommitArgs{ReplicaRead{}}; }

  // Graph Versioning v1 durability (S3a, FIX C): recovery is replaying a WAL transaction that is
  // ALREADY durable (retained WAL, S1) at its ORIGINAL commit timestamp `desired_commit_timestamp`.
  // The caller (recovery, S3d) drives `Storage::timestamp_ := desired_commit_timestamp` immediately
  // before calling PrepareForCommitPhase, so the ordinary `GetCommitTimestamp()` (`timestamp_++`,
  // untouched by this variant -- see design doc FIX C simplification) naturally yields
  // `desired_commit_timestamp` as `commit_timestamp_`. `desired_commit_timestamp` is carried here
  // purely as a defense-in-depth cross-check (see the WAL-suppress early-return in
  // InMemoryStorage::InMemoryAccessor::PrepareForCommitPhase) that the caller actually primed
  // `timestamp_` correctly -- it is NOT used to override `commit_timestamp_`.
  static auto make_recovery_replay(uint64_t const desired_commit_timestamp) -> CommitArgs {
    return CommitArgs{RecoveryReplay{.desired_commit_timestamp = desired_commit_timestamp}};
  }

  auto durable_timestamp(uint64_t commit_timestamp) const -> uint64_t {
    auto const f = utils::Overloaded{[&](Main const &main) { return commit_timestamp; },
                                     [](ReplicaWrite const &replica) { return replica.desired_commit_timestamp; },
                                     [](ReplicaRead const &) -> uint64_t { MG_ASSERT(false, "invalid state"); },
                                     // Same shape as Main: the original commit timestamp already flows
                                     // through `commit_timestamp` (see this struct's doc-comment above).
                                     [&](RecoveryReplay const &) { return commit_timestamp; }};
    return std::visit(f, data);
  }

  bool durability_allowed() const { return !std::holds_alternative<ReplicaRead>(data); }

  bool replication_allowed() const { return std::holds_alternative<Main>(data); }

  // True only for the recovery-replay variant: the transaction is already durable in the retained
  // WAL (S1) at its original commit timestamp, so PrepareForCommitPhase must skip
  // InitializeWalFile/append/replication entirely (re-writing it would duplicate durability) while
  // still running FinalizeCommitPhase's delta-visibility + commit_log_ bookkeeping.
  bool suppress_wal() const { return std::holds_alternative<RecoveryReplay>(data); }

  // The original commit timestamp this replay commit is expected to land at, for the
  // defense-in-depth cross-check described above `make_recovery_replay`'s doc-comment. `nullopt`
  // for every other variant.
  auto recovery_replay_desired_timestamp() const -> std::optional<uint64_t> {
    if (auto const *replay = std::get_if<RecoveryReplay>(&data)) {
      return replay->desired_commit_timestamp;
    }
    return std::nullopt;
  }

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

  void apply_cb_if_replica_write() const {
    auto const f = utils::Overloaded{[](auto const &) {},
                                     [](ReplicaWrite const &replica) { std::invoke(replica.in_progress_cb_); }};
    std::visit(f, data);
  }

  template <typename Func>
  auto apply_if_main(Func &&func) const -> std::optional<std::invoke_result_t<Func, DatabaseProtector const &>> {
    using result_t = std::optional<std::invoke_result_t<Func, DatabaseProtector const &>>;
    auto const f = utils::Overloaded{[](auto const &) -> result_t { return {}; },
                                     [&](Main const &main) -> result_t { return func(*main.db_acc); }};
    return std::visit(f, data);
  }

 private:
  struct ReplicaRead {};

  struct ReplicaWrite {
    // REPLICA on receipt of Deltas will have a desired commit timestamp
    uint64_t desired_commit_timestamp{};
    // false for SYNC/ASYNC replica, true for STRICT_SYNC replica
    bool two_phase_commit_ = false;
    std::function<void()> in_progress_cb_;
  };

  struct Main {
    // Needed for ASYNC replication tasks
    DatabaseProtectorPtr db_acc;
  };

  // Graph Versioning v1 durability (S3a, FIX C). See make_recovery_replay's doc-comment above.
  struct RecoveryReplay {
    uint64_t desired_commit_timestamp{};
  };

  explicit CommitArgs(std::variant<Main, ReplicaWrite, ReplicaRead, RecoveryReplay> data) : data(std::move(data)) {}

  std::variant<Main, ReplicaWrite, ReplicaRead, RecoveryReplay> data;
};
}  // namespace memgraph::storage
