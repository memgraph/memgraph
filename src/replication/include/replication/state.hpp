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
#include <cstdint>
#include <variant>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "replication/config.hpp"
#include "replication/epoch.hpp"
#include "replication/mode.hpp"
#include "replication/role.hpp"
#include "utils/result.hpp"

namespace memgraph::replication {

enum class RolePersisted : uint8_t { UNKNOWN_OR_NO, YES };

struct ReplicationState {
  ReplicationState(std::optional<std::filesystem::path> durability_dir);

  ReplicationState(ReplicationState const &) = delete;
  ReplicationState(ReplicationState &&) = delete;
  ReplicationState &operator=(ReplicationState const &) = delete;
  ReplicationState &operator=(ReplicationState &&) = delete;

  void SetRole(ReplicationRole role) { return replication_role_.store(role); }
  auto GetRole() const -> ReplicationRole { return replication_role_.load(); }
  bool IsMain() const { return replication_role_ == ReplicationRole::MAIN; }
  bool IsReplica() const { return replication_role_ == ReplicationRole::REPLICA; }

  auto GetEpoch() const -> const ReplicationEpoch & { return epoch_; }
  auto GetEpoch() -> ReplicationEpoch & { return epoch_; }

  enum class FetchReplicationError : uint8_t {
    NOTHING_FETCHED,
    PARSE_ERROR,
  };
  using ReplicationDataReplica = ReplicationServerConfig;
  using ReplicationDataMain = std::vector<ReplicationClientConfig>;
  using ReplicationData = std::variant<ReplicationDataMain, ReplicationDataReplica>;
  using FetchReplicationResult = utils::BasicResult<FetchReplicationError, ReplicationData>;
  auto FetchReplicationData() -> FetchReplicationResult;

  bool ShouldPersist() const { return nullptr != durability_; }
  bool TryPersistRoleMain();
  bool TryPersistRoleReplica(const ReplicationServerConfig &config);
  bool TryPersistUnregisterReplica(std::string_view &name);
  bool TryPersistRegisteredReplica(const ReplicationClientConfig &config);

 private:
  ReplicationEpoch epoch_;
  std::atomic<ReplicationRole> replication_role_{ReplicationRole::MAIN};
  std::unique_ptr<kvstore::KVStore> durability_;
  std::atomic<RolePersisted> role_persisted = RolePersisted::UNKNOWN_OR_NO;
};

}  // namespace memgraph::replication
