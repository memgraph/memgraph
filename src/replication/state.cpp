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

#include "replication/state.hpp"

#include "replication/status.hpp"  //TODO: don't use status for durability
#include "utils/file.hpp"

constexpr auto kReplicationDirectory = std::string_view{"replication"};

namespace memgraph::replication {

ReplicationState::ReplicationState(std::optional<std::filesystem::path> durability_dir) {
  if (!durability_dir) return;
  auto repl_dir = *std::move(durability_dir);
  repl_dir /= kReplicationDirectory;
  utils::EnsureDirOrDie(repl_dir);
  durability_ = std::make_unique<kvstore::KVStore>(std::move(repl_dir));
}
bool ReplicationState::TryPersistRoleReplica(const ReplicationServerConfig &config) {
  if (!ShouldPersist()) return true;
  // Only thing that matters here is the role saved as REPLICA and the listening port
  auto data = ReplicationStatusToJSON(ReplicationStatus{.name = kReservedReplicationRoleName,
                                                        .ip_address = config.ip_address,
                                                        .port = config.port,
                                                        .sync_mode = ReplicationMode::SYNC,
                                                        .replica_check_frequency = std::chrono::seconds(0),
                                                        .ssl = std::nullopt,
                                                        .role = ReplicationRole::REPLICA});

  if (durability_->Put(kReservedReplicationRoleName, data.dump())) {
    role_persisted = RolePersisted::YES;
    return true;
  }
  spdlog::error("Error when saving REPLICA replication role in settings.");
  return false;
}
bool ReplicationState::TryPersistRoleMain() {
  if (!ShouldPersist()) return true;
  // Only thing that matters here is the role saved as MAIN
  auto data = ReplicationStatusToJSON(ReplicationStatus{.name = kReservedReplicationRoleName,
                                                        .ip_address = "",
                                                        .port = 0,
                                                        .sync_mode = ReplicationMode::SYNC,
                                                        .replica_check_frequency = std::chrono::seconds(0),
                                                        .ssl = std::nullopt,
                                                        .role = ReplicationRole::MAIN});

  if (durability_->Put(kReservedReplicationRoleName, data.dump())) {
    role_persisted = RolePersisted::YES;
    return true;
  }
  spdlog::error("Error when saving MAIN replication role in settings.");
  return false;
}
bool ReplicationState::TryPersistUnregisterReplica(std::string_view &name) {
  if (!ShouldPersist()) return true;
  if (durability_->Delete(name)) return true;
  spdlog::error("Error when removing replica {} from settings.", name);
  return false;
}
auto ReplicationState::FetchReplicationData() -> FetchReplicationResult {
  if (!ShouldPersist()) return FetchReplicationError::NOTHING_FETCHED;
  const auto replication_data = durability_->Get(kReservedReplicationRoleName);
  if (!replication_data.has_value()) {
    return FetchReplicationError::NOTHING_FETCHED;
  }

  const auto maybe_replication_status = JSONToReplicationStatus(nlohmann::json::parse(*replication_data));
  if (!maybe_replication_status.has_value()) {
    return FetchReplicationError::PARSE_ERROR;
  }

  // To get here this must be the case
  role_persisted = memgraph::replication::RolePersisted::YES;

  const auto replication_status = *maybe_replication_status;
  auto role = replication_status.role.value_or(ReplicationRole::MAIN);
  switch (role) {
    case ReplicationRole::REPLICA: {
      return {ReplicationServerConfig{
          .ip_address = kDefaultReplicationServerIp,
          .port = replication_status.port,
      }};
    }
    case ReplicationRole::MAIN: {
      auto res = ReplicationState::ReplicationDataMain{};
      res.reserve(durability_->Size() - 1);
      for (const auto &[replica_name, replica_data] : *durability_) {
        if (replica_name == kReservedReplicationRoleName) {
          continue;
        }

        const auto maybe_replica_status = JSONToReplicationStatus(nlohmann::json::parse(replica_data));
        if (!maybe_replica_status.has_value()) {
          return FetchReplicationError::PARSE_ERROR;
        }

        auto replica_status = *maybe_replica_status;
        if (replica_status.name != replica_name) {
          return FetchReplicationError::PARSE_ERROR;
        }
        res.emplace_back(ReplicationClientConfig{
            .name = replica_status.name,
            .mode = replica_status.sync_mode,
            .ip_address = replica_status.ip_address,
            .port = replica_status.port,
            .replica_check_frequency = replica_status.replica_check_frequency,
            .ssl = replica_status.ssl,
        });
      }
      return {std::move(res)};
    }
  }
}
bool ReplicationState::TryPersistRegisteredReplica(const ReplicationClientConfig &config) {
  if (!ShouldPersist()) return true;

  // If any replicas are persisted then Role must be persisted
  if (role_persisted != RolePersisted::YES) {
    DMG_ASSERT(IsMain(), "MAIN is expected");
    if (!TryPersistRoleMain()) return false;
  }

  auto data = ReplicationStatusToJSON(ReplicationStatus{.name = config.name,
                                                        .ip_address = config.ip_address,
                                                        .port = config.port,
                                                        .sync_mode = config.mode,
                                                        .replica_check_frequency = config.replica_check_frequency,
                                                        .ssl = config.ssl,
                                                        .role = ReplicationRole::REPLICA});
  if (durability_->Put(config.name, data.dump())) return true;
  spdlog::error("Error when saving replica {} in settings.", config.name);
  return false;
}
}  // namespace memgraph::replication
