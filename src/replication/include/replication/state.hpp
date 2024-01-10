// Copyright 2024 Memgraph Ltd.
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

#include "kvstore/kvstore.hpp"
#include "replication/config.hpp"
#include "replication/epoch.hpp"
#include "replication/mode.hpp"
#include "replication/replication_client.hpp"
#include "replication/role.hpp"
#include "replication_server.hpp"
#include "status.hpp"
#include "utils/result.hpp"
#include "utils/synchronized.hpp"

#include <atomic>
#include <cstdint>
#include <list>
#include <variant>
#include <vector>

namespace memgraph::replication {

enum class RolePersisted : uint8_t { UNKNOWN_OR_NO, YES };

enum class RegisterReplicaError : uint8_t { NAME_EXISTS, END_POINT_EXISTS, COULD_NOT_BE_PERSISTED, NOT_MAIN, SUCCESS };

struct RoleMainData {
  RoleMainData() = default;
  explicit RoleMainData(ReplicationEpoch e) : epoch_(std::move(e)) {}
  ~RoleMainData() = default;

  RoleMainData(RoleMainData const &) = delete;
  RoleMainData &operator=(RoleMainData const &) = delete;
  RoleMainData(RoleMainData &&) = default;
  RoleMainData &operator=(RoleMainData &&) = default;

  ReplicationEpoch epoch_;
  std::list<ReplicationClient> registered_replicas_{};
};

struct RoleReplicaData {
  ReplicationServerConfig config;
  std::unique_ptr<ReplicationServer> server;
};

// Global (instance) level object
struct ReplicationState {
  explicit ReplicationState(std::optional<std::filesystem::path> durability_dir);
  ~ReplicationState() = default;

  ReplicationState(ReplicationState const &) = delete;
  ReplicationState(ReplicationState &&) = delete;
  ReplicationState &operator=(ReplicationState const &) = delete;
  ReplicationState &operator=(ReplicationState &&) = delete;

  enum class FetchReplicationError : uint8_t {
    NOTHING_FETCHED,
    PARSE_ERROR,
  };

  using ReplicationData_t = std::variant<RoleMainData, RoleReplicaData>;
  using FetchReplicationResult_t = utils::BasicResult<FetchReplicationError, ReplicationData_t>;
  auto FetchReplicationData() -> FetchReplicationResult_t;

  auto GetRole() const -> ReplicationRole {
    return std::holds_alternative<RoleReplicaData>(replication_data_) ? ReplicationRole::REPLICA
                                                                      : ReplicationRole::MAIN;
  }
  bool IsMain() const { return GetRole() == ReplicationRole::MAIN; }
  bool IsReplica() const { return GetRole() == ReplicationRole::REPLICA; }

  bool ShouldPersist() const { return nullptr != durability_; }
  bool TryPersistRoleMain(std::string new_epoch);
  bool TryPersistRoleReplica(const ReplicationServerConfig &config);
  bool TryPersistUnregisterReplica(std::string_view name);
  bool TryPersistRegisteredReplica(const ReplicationClientConfig &config);

  // TODO: locked access
  auto ReplicationData() -> ReplicationData_t & { return replication_data_; }
  auto ReplicationData() const -> ReplicationData_t const & { return replication_data_; }
  utils::BasicResult<RegisterReplicaError, ReplicationClient *> RegisterReplica(const ReplicationClientConfig &config);

  bool SetReplicationRoleMain();

  bool SetReplicationRoleReplica(const ReplicationServerConfig &config);

 private:
  bool HandleVersionMigration(durability::ReplicationRoleEntry &data) const;

  std::unique_ptr<kvstore::KVStore> durability_;
  ReplicationData_t replication_data_;
  std::atomic<RolePersisted> role_persisted = RolePersisted::UNKNOWN_OR_NO;
};

}  // namespace memgraph::replication
