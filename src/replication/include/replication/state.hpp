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

// TODO: (andi) Dual definition of same error.

enum class RegisterReplicaError : uint8_t {
  NAME_EXISTS,
  END_POINT_EXISTS,
  COULD_NOT_BE_PERSISTED,
  IS_REPLICA,
  SUCCESS
};

enum class RegisterMainError : uint8_t {
  MAIN_ALREADY_EXISTS,
  END_POINT_EXISTS,
  COULD_NOT_BE_PERSISTED,
  NOT_COORDINATOR,
  SUCCESS
};

struct RoleMainData {
  // TODO: (andi) Currently, RoleMainData can exist without server and server_config_. Introducing new role should solve
  // this non-happy design decision.
  RoleMainData() = default;
  // Init constructor
  RoleMainData(ReplicationEpoch epoch, ReplicationServerConfig server_config)
      : epoch_(std::move(epoch)),
        server_config_(std::move(server_config)),
        server_(std::make_unique<ReplicationServer>(server_config_)) {}
  ~RoleMainData() = default;

  RoleMainData(RoleMainData const &) = delete;
  RoleMainData &operator=(RoleMainData const &) = delete;

  RoleMainData(RoleMainData &&other) noexcept
      : epoch_(std::move(other.epoch_)),
        registered_replicas_(std::move(other.registered_replicas_)),
        server_config_(std::move(other.server_config_)),
        server_(std::move(other.server_)) {}

  RoleMainData &operator=(RoleMainData &&other) noexcept {
    if (this != &other) {
      epoch_ = std::move(other.epoch_);
      registered_replicas_ = std::move(other.registered_replicas_);
      server_config_ = std::move(other.server_config_);
      server_ = std::move(other.server_);
    }
    return *this;
  }

  ReplicationEpoch epoch_;
  std::list<ReplicationClient> registered_replicas_;
  ReplicationServerConfig server_config_;
  std::unique_ptr<ReplicationServer> server_;
};

struct RoleReplicaData {
  explicit RoleReplicaData(ReplicationServerConfig config)
      : config_(std::move(config)), server_(std::make_unique<ReplicationServer>(config_)) {}

  ~RoleReplicaData() = default;

  RoleReplicaData(RoleReplicaData const &) = delete;
  RoleReplicaData &operator=(RoleReplicaData const &) = delete;
  RoleReplicaData(RoleReplicaData &&other) noexcept
      : config_(std::move(other.config_)), server_(std::move(other.server_)) {}

  RoleReplicaData &operator=(RoleReplicaData &&other) noexcept {
    if (this != &other) {
      config_ = std::move(other.config_);
      server_ = std::move(other.server_);
    }
    return *this;
  }

  ReplicationServerConfig config_;
  std::unique_ptr<ReplicationServer> server_;
};

struct RoleCoordinatorData {
  RoleCoordinatorData() = default;
  ~RoleCoordinatorData() = default;

  RoleCoordinatorData(RoleCoordinatorData const &) = delete;
  RoleCoordinatorData &operator=(RoleCoordinatorData const &) = delete;

  RoleCoordinatorData(RoleCoordinatorData &&other) noexcept
      : registered_replicas_(std::move(other.registered_replicas_)), main(std::move(other.main)) {}
  RoleCoordinatorData &operator=(RoleCoordinatorData &&other) noexcept {
    if (this != &other) {
      registered_replicas_ = std::move(other.registered_replicas_);
      main = std::move(other.main);
    }
    return *this;
  }

  // TODO: (andi) Does it need epoch or some other way or tracking what is going on?
  std::list<ReplicationClient> registered_replicas_;
  std::unique_ptr<ReplicationClient> main;
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

  using ReplicationData_t = std::variant<RoleMainData, RoleReplicaData, RoleCoordinatorData>;
  using FetchReplicationResult_t = utils::BasicResult<FetchReplicationError, ReplicationData_t>;
  auto FetchReplicationData() -> FetchReplicationResult_t;

  auto GetRole() const -> ReplicationRole {
    if (std::holds_alternative<RoleReplicaData>(replication_data_)) {
      return ReplicationRole::REPLICA;
    }
    if (std::holds_alternative<RoleMainData>(replication_data_)) {
      return ReplicationRole::MAIN;
    }
    return ReplicationRole::COORDINATOR;
  }
  bool IsMain() const { return GetRole() == ReplicationRole::MAIN; }
  bool IsReplica() const { return GetRole() == ReplicationRole::REPLICA; }
  bool IsCoordinator() const { return GetRole() == ReplicationRole::COORDINATOR; }

  bool ShouldPersist() const { return nullptr != durability_; }
  bool TryPersistRoleMain(std::string new_epoch, const ReplicationServerConfig &config);
  bool TryPersistRoleReplica(const ReplicationServerConfig &config);
  bool TryPersistRoleCoordinator();

  bool TryPersistRegisteredReplicaOnMain(const ReplicationClientConfig &config);
  bool TryPersistUnregisterReplicaOnMain(std::string_view name);

  bool TryPersistRegisteredReplicaOnCoordinator(const ReplicationClientConfig &config);
  bool TryPersistUnregisterReplicaOnCoordinator(std::string_view name);

  bool TryPersistRegisteredMainOnCoordinator(const ReplicationClientConfig &config);
  // TODO: (andi) Unregistering main from coordinator

  // TODO: locked access
  auto ReplicationData() -> ReplicationData_t & { return replication_data_; }
  auto ReplicationData() const -> ReplicationData_t const & { return replication_data_; }

  utils::BasicResult<RegisterReplicaError, ReplicationClient *> RegisterReplica(const ReplicationClientConfig &config);
  utils::BasicResult<RegisterMainError, ReplicationClient *> RegisterMain(const ReplicationClientConfig &config);

  bool SetReplicationRoleMain(const ReplicationServerConfig &config);
  bool SetReplicationRoleReplica(const ReplicationServerConfig &config);
  bool SetReplicationRoleCoordinator();

 private:
  bool HandleVersionMigration(durability::ReplicationRoleEntry &data) const;

  std::unique_ptr<kvstore::KVStore> durability_;
  ReplicationData_t replication_data_;
  std::atomic<RolePersisted> role_persisted = RolePersisted::UNKNOWN_OR_NO;
};

}  // namespace memgraph::replication
