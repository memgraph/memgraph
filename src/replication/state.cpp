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

#include "replication/state.hpp"
#include <optional>

#include "flags/replication.hpp"
#include "replication/replication_client.hpp"
#include "replication/replication_server.hpp"
#include "replication/status.hpp"
#include "utils/file.hpp"
#include "utils/result.hpp"
#include "utils/uuid.hpp"
#include "utils/variant_helpers.hpp"

constexpr auto kReplicationDirectory = std::string_view{"replication"};

namespace memgraph::replication {

auto BuildReplicaKey(std::string_view name) -> std::string {
  auto key = std::string{durability::kReplicationReplicaPrefix};
  key.append(name);
  return key;
}

ReplicationState::ReplicationState(std::optional<std::filesystem::path> durability_dir) {
  if (!durability_dir) return;
  auto repl_dir = *std::move(durability_dir);
  repl_dir /= kReplicationDirectory;
  utils::EnsureDirOrDie(repl_dir);
  durability_ = std::make_unique<kvstore::KVStore>(std::move(repl_dir));
  spdlog::info("Replication configuration will be stored and will be automatically restored in case of a crash.");

  auto fetched_replication_data = FetchReplicationData();
  if (fetched_replication_data.HasError()) {
    switch (fetched_replication_data.GetError()) {
      using enum ReplicationState::FetchReplicationError;
      case NOTHING_FETCHED: {
        spdlog::debug("Cannot find data needed for restore replication role in persisted metadata.");
        replication_data_ = RoleMainData{};
        return;
      }
      case PARSE_ERROR: {
        LOG_FATAL("Cannot parse previously saved configuration of replication role.");
        return;
      }
    }
  }
  auto replication_data = std::move(fetched_replication_data).GetValue();
  if (FLAGS_coordinator_server_port) {
    if (std::holds_alternative<RoleReplicaData>(replication_data)) {
      std::get<RoleReplicaData>(replication_data).uuid_ = std::nullopt;
    }
  }
  replication_data_ = std::move(replication_data);
}

bool ReplicationState::TryPersistRoleReplica(const ReplicationServerConfig &config,
                                             const std::optional<utils::UUID> &main_uuid) {
  if (!HasDurability()) return true;

  auto data =
      durability::ReplicationRoleEntry{.role = durability::ReplicaRole{.config = config, .main_uuid = main_uuid}};

  if (!durability_->Put(durability::kReplicationRoleName, nlohmann::json(data).dump())) {
    spdlog::error("Error when saving REPLICA replication role in settings.");
    return false;
  }
  role_persisted = RolePersisted::YES;

  // Cleanup remove registered replicas (assume successful delete)
  // NOTE: we could do the alternative which would be on REPLICA -> MAIN we recover these registered replicas
  auto b = durability_->begin(durability::kReplicationReplicaPrefix);
  auto e = durability_->end(durability::kReplicationReplicaPrefix);
  for (; b != e; ++b) {
    durability_->Delete(b->first);
  }

  return true;
}

bool ReplicationState::TryPersistRoleMain(std::string new_epoch, utils::UUID main_uuid) {
  if (!HasDurability()) return true;

  auto data = durability::ReplicationRoleEntry{
      .role = durability::MainRole{.epoch = ReplicationEpoch{std::move(new_epoch)}, .main_uuid = main_uuid}};

  if (durability_->Put(durability::kReplicationRoleName, nlohmann::json(data).dump())) {
    role_persisted = RolePersisted::YES;
    return true;
  }
  spdlog::error("Error when saving MAIN replication role in settings.");
  return false;
}

bool ReplicationState::TryPersistUnregisterReplica(std::string_view name) {
  if (!HasDurability()) return true;

  auto key = BuildReplicaKey(name);

  if (durability_->Delete(key)) return true;
  spdlog::error("Error when removing replica {} from settings.", name);
  return false;
}

// TODO: FetchEpochData (agnostic of FetchReplicationData, but should be done before)

auto ReplicationState::FetchReplicationData() -> FetchReplicationResult_t {
  if (!HasDurability()) return FetchReplicationError::NOTHING_FETCHED;
  const auto replication_data = durability_->Get(durability::kReplicationRoleName);
  if (!replication_data.has_value()) {
    return FetchReplicationError::NOTHING_FETCHED;
  }

  auto json = nlohmann::json::parse(*replication_data, nullptr, false);
  if (json.is_discarded()) {
    return FetchReplicationError::PARSE_ERROR;
  }
  try {
    durability::ReplicationRoleEntry data = json.get<durability::ReplicationRoleEntry>();

    if (!HandleVersionMigration(data)) {
      return FetchReplicationError::PARSE_ERROR;
    }

    // To get here this must be the case
    role_persisted = memgraph::replication::RolePersisted::YES;

    return std::visit(
        utils::Overloaded{
            [&](durability::MainRole &&r) -> FetchReplicationResult_t {
              auto res =
                  RoleMainData{std::move(r.epoch), r.main_uuid.has_value() ? r.main_uuid.value() : utils::UUID{}};
              auto b = durability_->begin(durability::kReplicationReplicaPrefix);
              auto e = durability_->end(durability::kReplicationReplicaPrefix);
              for (; b != e; ++b) {
                auto const &[replica_name, replica_data] = *b;
                auto json = nlohmann::json::parse(replica_data, nullptr, false);
                if (json.is_discarded()) return FetchReplicationError::PARSE_ERROR;
                try {
                  durability::ReplicationReplicaEntry data = json.get<durability::ReplicationReplicaEntry>();
                  auto key_name = std::string_view{replica_name}.substr(strlen(durability::kReplicationReplicaPrefix));
                  if (key_name != data.config.name) {
                    return FetchReplicationError::PARSE_ERROR;
                  }
                  // Instance clients
                  res.registered_replicas_.emplace_back(data.config);
                  // Bump for each replica uuid
                  res.registered_replicas_.back().try_set_uuid = !r.main_uuid.has_value();
                } catch (...) {
                  return FetchReplicationError::PARSE_ERROR;
                }
              }
              return {std::move(res)};
            },
            [&](durability::ReplicaRole &&r) -> FetchReplicationResult_t {
              return {RoleReplicaData{r.config, std::make_unique<ReplicationServer>(r.config), r.main_uuid}};
            },
        },
        std::move(data.role));
  } catch (...) {
    return FetchReplicationError::PARSE_ERROR;
  }
}

bool ReplicationState::HandleVersionMigration(durability::ReplicationRoleEntry &data) const {
  switch (data.version) {
    case durability::DurabilityVersion::V1: {
      // For each replica config, change key to use the prefix
      std::map<std::string, std::string> to_put;
      std::vector<std::string> to_delete;
      for (auto [old_key, old_data] : *durability_) {
        // skip reserved keys
        if (old_key == durability::kReplicationRoleName) {
          continue;
        }

        // Turn old data to new data
        auto old_json = nlohmann::json::parse(old_data, nullptr, false);
        if (old_json.is_discarded()) return false;  // Can not read old_data as json
        try {
          durability::ReplicationReplicaEntry new_data = old_json.get<durability::ReplicationReplicaEntry>();

          // Migrate to using new key
          to_put.emplace(BuildReplicaKey(old_key), nlohmann::json(new_data).dump());
        } catch (...) {
          return false;  // Can not parse as ReplicationReplicaEntry
        }
        to_delete.push_back(std::move(old_key));
      }
      // Set version
      data.version = durability::DurabilityVersion::V2;
      // Re-serialise (to include version + epoch)
      to_put.emplace(durability::kReplicationRoleName, nlohmann::json(data).dump());
      if (!durability_->PutAndDeleteMultiple(to_put, to_delete)) return false;  // some reason couldn't persist
      [[fallthrough]];
    }
    case durability::DurabilityVersion::V2: {
      if (std::holds_alternative<durability::ReplicaRole>(data.role)) {
        throw std::runtime_error("Migration for replica from older version to new version is not possible");
      } else {
        auto &main = std::get<durability::MainRole>(data.role);
        main.main_uuid = utils::UUID{};
      }
      data.version = durability::DurabilityVersion::V3;
      break;
    }
    case durability::DurabilityVersion::V3: {
      // do nothing - add code if V4 ever happens
      break;
    }
  }
  return true;
}

bool ReplicationState::TryPersistRegisteredReplica(const ReplicationClientConfig &config, utils::UUID main_uuid) {
  if (!HasDurability()) return true;

  // If any replicas are persisted then Role must be persisted
  if (role_persisted != RolePersisted::YES) {
    DMG_ASSERT(IsMain(), "MAIN is expected");
    auto epoch_str = std::string(std::get<RoleMainData>(replication_data_).epoch_.id());
    if (!TryPersistRoleMain(std::move(epoch_str), main_uuid)) return false;
  }

  auto data = durability::ReplicationReplicaEntry{.config = config};

  auto key = BuildReplicaKey(config.name);
  if (durability_->Put(key, nlohmann::json(data).dump())) return true;
  spdlog::error("Error when saving replica {} in settings.", config.name);
  return false;
}

bool ReplicationState::SetReplicationRoleMain(const utils::UUID &main_uuid) {
  auto new_epoch = utils::GenerateUUID();

  if (!TryPersistRoleMain(new_epoch, main_uuid)) {
    return false;
  }

  replication_data_ = RoleMainData{ReplicationEpoch{new_epoch}, main_uuid};

  return true;
}

bool ReplicationState::SetReplicationRoleReplica(const ReplicationServerConfig &config,
                                                 const std::optional<utils::UUID> &main_uuid) {
  if (!TryPersistRoleReplica(config, main_uuid)) {
    return false;
  }
  replication_data_ = RoleReplicaData{config, std::make_unique<ReplicationServer>(config), std::nullopt};
  return true;
}

utils::BasicResult<RegisterReplicaError, ReplicationClient *> ReplicationState::RegisterReplica(
    const ReplicationClientConfig &config) {
  auto const replica_handler = [](RoleReplicaData const &) { return RegisterReplicaError::NOT_MAIN; };

  ReplicationClient *client{nullptr};
  auto const main_handler = [&client, &config, this](RoleMainData &mainData) -> RegisterReplicaError {
    // name check
    auto name_check = [&config](auto const &replicas) {
      auto name_matches = [&name = config.name](auto const &replica) { return replica.name_ == name; };
      return std::any_of(replicas.begin(), replicas.end(), name_matches);
    };
    if (name_check(mainData.registered_replicas_)) {
      return RegisterReplicaError::NAME_EXISTS;
    }

    // endpoint check
    auto endpoint_check = [&](auto const &replicas) {
      auto endpoint_matches = [&config](auto const &replica) {
        const auto &ep = replica.rpc_client_.Endpoint();
        return ep.address == config.ip_address && ep.port == config.port;
      };
      return std::any_of(replicas.begin(), replicas.end(), endpoint_matches);
    };
    if (endpoint_check(mainData.registered_replicas_)) {
      return RegisterReplicaError::ENDPOINT_EXISTS;
    }

    // Durability
    if (!TryPersistRegisteredReplica(config, mainData.uuid_)) {
      return RegisterReplicaError::COULD_NOT_BE_PERSISTED;
    }

    // set
    client = &mainData.registered_replicas_.emplace_back(config);
    return RegisterReplicaError::SUCCESS;
  };

  const auto &res = std::visit(utils::Overloaded{main_handler, replica_handler}, replication_data_);
  if (res == RegisterReplicaError::SUCCESS) {
    return client;
  }
  return res;
}

}  // namespace memgraph::replication
