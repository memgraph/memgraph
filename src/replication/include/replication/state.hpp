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

#include "kvstore/kvstore.hpp"
#include "replication/config.hpp"
#include "replication/replication_client.hpp"
#include "replication_coordination_glue/common.hpp"
#include "replication_coordination_glue/role.hpp"
#include "replication_server.hpp"
#include "status.hpp"
#include "utils/uuid.hpp"

#include <atomic>
#include <cstdint>
#include <list>
#include <optional>
#include <variant>

#include "nlohmann/json_fwd.hpp"

namespace memgraph::replication {

enum class RolePersisted : uint8_t { UNKNOWN_OR_NO, YES };

enum class RegisterReplicaStatus : uint8_t { NAME_EXISTS, ENDPOINT_EXISTS, COULD_NOT_BE_PERSISTED, NOT_MAIN, SUCCESS };

struct RoleMainData {
  RoleMainData() = default;

  explicit RoleMainData(bool const writing_enabled, utils::UUID const uuid)
      : uuid_(uuid), writing_enabled_(writing_enabled) {}

  ~RoleMainData() = default;

  RoleMainData(RoleMainData const &) = delete;
  RoleMainData &operator=(RoleMainData const &) = delete;
  RoleMainData(RoleMainData &&) = default;
  RoleMainData &operator=(RoleMainData &&) = default;

  std::list<ReplicationClient> registered_replicas_;
  utils::UUID uuid_;  // also used in ReplicationStorageClient but important thing is that at both places, the value is
  // immutable.
  bool writing_enabled_{false};
};

struct RoleReplicaData {
  ReplicationServerConfig config;
  std::unique_ptr<ReplicationServer> server;
  // uuid of main instance that replica is listening to
  utils::UUID uuid_;
};

struct ReplicationData_t {
  // Common data for both replica and main
  // The source value is stored in Raft distributed config, this is only a cached value
  // on the data instance's side. Needs to be accessed in a thread-safe way using ReplState lock because two threads are
  // using this value:
  // 1. RPC thread that handles communication with coordinators
  // 2. RPC thread used for communication with main (if instance is replica)
  uint64_t deltas_batch_progress_size_{replication_coordination_glue::kDefaultDeltasBatchProgressSize};
  std::variant<RoleMainData, RoleReplicaData> data_;
};

// Global (instance) level object
struct ReplicationState {
  explicit ReplicationState(std::optional<std::filesystem::path> durability_dir, bool ha_cluster = false);
  ~ReplicationState() = default;

  ReplicationState(ReplicationState const &) = delete;
  ReplicationState(ReplicationState &&) = delete;
  ReplicationState &operator=(ReplicationState const &) = delete;
  ReplicationState &operator=(ReplicationState &&) = delete;

  enum class FetchReplicationError : uint8_t {
    NOTHING_FETCHED,
    PARSE_ERROR,
  };

  using FetchReplicationResult_t = std::expected<ReplicationData_t, FetchReplicationError>;
  using FetchVariantResult_t = std::expected<std::variant<RoleMainData, RoleReplicaData>, FetchReplicationError>;
  auto FetchReplicationData() -> FetchReplicationResult_t;

  auto GetRole() const -> replication_coordination_glue::ReplicationRole {
    return std::holds_alternative<RoleReplicaData>(replication_data_.data_)
               ? replication_coordination_glue::ReplicationRole::REPLICA
               : replication_coordination_glue::ReplicationRole::MAIN;
  }

  bool IsMain() const { return GetRole() == replication_coordination_glue::ReplicationRole::MAIN; }

  bool IsReplica() const { return GetRole() == replication_coordination_glue::ReplicationRole::REPLICA; }

  auto IsMainWriteable() const -> bool {
    if (auto const *main = std::get_if<RoleMainData>(&replication_data_.data_)) {
      return !part_of_ha_cluster_ || main->writing_enabled_;
    }
    return false;
  }

  auto EnableWritingOnMain() -> bool {
    if (auto *main = std::get_if<RoleMainData>(&replication_data_.data_)) {
      main->writing_enabled_ = true;
      return true;
    }
    return false;
  }

  auto GetMainRole() -> RoleMainData & {
    MG_ASSERT(IsMain(), "Instance is not MAIN");
    return std::get<RoleMainData>(replication_data_.data_);
  }

  auto GetMainRole() const -> RoleMainData const & {
    MG_ASSERT(IsMain(), "Instance is not MAIN");
    return std::get<RoleMainData>(replication_data_.data_);
  }

  auto GetReplicaRole() -> RoleReplicaData & {
    MG_ASSERT(!IsMain(), "Instance is MAIN");
    return std::get<RoleReplicaData>(replication_data_.data_);
  }

  auto GetReplicaRole() const -> RoleReplicaData const & {
    MG_ASSERT(!IsMain(), "Instance is MAIN");
    return std::get<RoleReplicaData>(replication_data_.data_);
  }

  bool HasDurability() const { return nullptr != durability_; }

  bool TryPersistRoleMain(utils::UUID main_uuid);
  bool TryPersistRoleReplica(const ReplicationServerConfig &config, utils::UUID const &main_uuid);
  bool TryPersistUnregisterReplica(std::string_view name);
  bool TryPersistRegisteredReplica(const ReplicationClientConfig &config, utils::UUID main_uuid);

  auto ReplicationData() -> std::variant<RoleMainData, RoleReplicaData> & { return replication_data_.data_; }

  auto ReplicationData() const -> std::variant<RoleMainData, RoleReplicaData> const & {
    return replication_data_.data_;
  }

  std::expected<ReplicationClient *, RegisterReplicaStatus> RegisterReplica(const ReplicationClientConfig &config);

  bool SetReplicationRoleMain(const utils::UUID &main_uuid);
  bool SetReplicationRoleReplica(const ReplicationServerConfig &config,
                                 std::optional<utils::UUID> const &maybe_main_uuid);

  std::optional<nlohmann::json> GetTelemetryJson() const;

  void Shutdown();

  auto GetDeltasBatchProgressSize() const -> uint64_t;
  void UpdateDeltasBatchProgressSize(uint64_t new_value);

 private:
  bool HandleVersionMigration(durability::ReplicationRoleEntry &data) const;

  std::unique_ptr<kvstore::KVStore> durability_;
  ReplicationData_t replication_data_;
  std::atomic<RolePersisted> role_persisted_ = RolePersisted::UNKNOWN_OR_NO;
  bool part_of_ha_cluster_{false};
};

}  // namespace memgraph::replication
