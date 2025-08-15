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

#include <optional>
#include <variant>

#include "flags/coord_flag_env_handler.hpp"
#include "replication/replication_client.hpp"
#include "replication/replication_server.hpp"
#include "replication/state.hpp"
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

ReplicationState::ReplicationState(std::optional<std::filesystem::path> durability_dir, bool part_of_ha_cluster)
    : part_of_ha_cluster_(part_of_ha_cluster) {
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
#ifdef MG_ENTERPRISE
  if (flags::CoordinationSetupInstance().IsDataInstanceManagedByCoordinator() &&
      std::holds_alternative<RoleReplicaData>(replication_data)) {
    std::get<RoleReplicaData>(replication_data).uuid_ = utils::UUID{};
    spdlog::trace("Replica's replication uuid for replica has been reset");
  }
#endif
  if (std::holds_alternative<RoleReplicaData>(replication_data)) {
    auto const &replica_uuid = std::get<RoleReplicaData>(replication_data).uuid_;
    auto const uuid = std::string(replica_uuid);
    spdlog::trace("Recovered main's uuid for replica {}", uuid);
  } else {
    spdlog::trace("Recovered uuid for main {}", std::string(std::get<RoleMainData>(replication_data).uuid_));
  }
  replication_data_ = std::move(replication_data);
}

bool ReplicationState::TryPersistRoleReplica(const ReplicationServerConfig &config, utils::UUID const &main_uuid) {
  if (!HasDurability()) return true;

  auto data =
      durability::ReplicationRoleEntry{.role = durability::ReplicaRole{.config = config, .main_uuid = main_uuid}};

  if (!durability_->Put(durability::kReplicationRoleName, nlohmann::json(data).dump())) {
    spdlog::error("Error when saving REPLICA replication role in settings.");
    return false;
  }
  role_persisted.store(RolePersisted::YES, std::memory_order_release);

  // Cleanup remove registered replicas (assume successful delete)
  // NOTE: we could do the alternative which would be on REPLICA -> MAIN we recover these registered replicas
  auto b = durability_->begin(durability::kReplicationReplicaPrefix);
  auto e = durability_->end(durability::kReplicationReplicaPrefix);
  for (; b != e; ++b) {
    durability_->Delete(b->first);
  }

  return true;
}

bool ReplicationState::TryPersistRoleMain(utils::UUID main_uuid) {
  if (!HasDurability()) return true;

  auto data = durability::ReplicationRoleEntry{.role = durability::MainRole{.main_uuid = main_uuid}};

  if (durability_->Put(durability::kReplicationRoleName, nlohmann::json(data).dump())) {
    role_persisted.store(RolePersisted::YES, std::memory_order_release);
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
    role_persisted.store(RolePersisted::YES, std::memory_order_release);

    return std::visit(
        utils::Overloaded{
            [&](durability::MainRole &&r) -> FetchReplicationResult_t {
              auto res = RoleMainData{false, r.main_uuid.has_value() ? r.main_uuid.value() : utils::UUID{}};
              auto b = durability_->begin(durability::kReplicationReplicaPrefix);
              auto e = durability_->end(durability::kReplicationReplicaPrefix);
              for (; b != e; ++b) {
                auto const &[replica_name, replica_data] = *b;
                auto json_data = nlohmann::json::parse(replica_data, nullptr, false);
                if (json_data.is_discarded()) return FetchReplicationError::PARSE_ERROR;
                try {
                  durability::ReplicationReplicaEntry const local_data =
                      json_data.get<durability::ReplicationReplicaEntry>();

                  auto key_name = std::string_view{replica_name}.substr(strlen(durability::kReplicationReplicaPrefix));
                  if (key_name != local_data.config.name) {
                    return FetchReplicationError::PARSE_ERROR;
                  }
                  // Instance clients
                  res.registered_replicas_.emplace_back(local_data.config);
                  // Bump for each replica uuid
                  res.registered_replicas_.back().try_set_uuid = !r.main_uuid.has_value();
                } catch (...) {
                  return FetchReplicationError::PARSE_ERROR;
                }
              }
              return {std::move(res)};
            },
            [&](durability::ReplicaRole &&r) -> FetchReplicationResult_t {
              // False positive report for the std::make_unique
              // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
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
      data.version = durability::DurabilityVersion::V3;
      if (std::holds_alternative<durability::MainRole>(data.role)) {
        auto &main = std::get<durability::MainRole>(data.role);
        main.main_uuid = utils::UUID{};
      }
      if (!durability_->Put(durability::kReplicationRoleName, nlohmann::json(data).dump())) return false;
      [[fallthrough]];
    }
    case durability::DurabilityVersion::V3: {
      std::map<std::string, std::string> to_put;
      for (auto [old_key, old_data] : *durability_) {
        if (old_key == durability::kReplicationRoleName) {
          data.version = durability::DurabilityVersion::V4;
          to_put.emplace(durability::kReplicationRoleName, nlohmann::json(data).dump());
        } else {
          auto old_json = nlohmann::json::parse(old_data, nullptr, false);
          if (old_json.is_discarded()) return false;
          try {
            durability::ReplicationReplicaEntry const new_data = old_json.get<durability::ReplicationReplicaEntry>();
            to_put.emplace(old_key, nlohmann::json(new_data).dump());
          } catch (...) {
            return false;
          }
        }
      }
      if (!durability_->PutMultiple(to_put)) return false;  // some reason couldn't persist
      [[fallthrough]];
    }
    case durability::DurabilityVersion::V4: {
      // In v5 epoch was removed from MainRole, hence we only need to bump DurabilityVersion
      data.version = durability::DurabilityVersion::V5;
      if (!durability_->Put(durability::kReplicationRoleName, nlohmann::json(data).dump())) return false;
      break;
    }
    case durability::DurabilityVersion::V5: {
      // do nothing - add code if V6 ever happens
      break;
    }
  }
  return true;
}

bool ReplicationState::TryPersistRegisteredReplica(const ReplicationClientConfig &config, utils::UUID main_uuid) {
  if (!HasDurability()) return true;

  // If any replicas are persisted then Role must be persisted
  if (role_persisted.load(std::memory_order_acquire) != RolePersisted::YES) {
    DMG_ASSERT(IsMain(), "MAIN is expected");
    if (!TryPersistRoleMain(main_uuid)) return false;
  }

  auto data = durability::ReplicationReplicaEntry{.config = config};

  auto key = BuildReplicaKey(config.name);
  if (durability_->Put(key, nlohmann::json(data).dump())) return true;
  spdlog::error("Error when saving replica {} in settings.", config.name);
  return false;
}

bool ReplicationState::SetReplicationRoleMain(const utils::UUID &main_uuid) {
  if (!TryPersistRoleMain(main_uuid)) {
    return false;
  }

  // By default, writing on MAIN is disabled until cluster is in healthy state
  replication_data_ = RoleMainData{/*is_writing enabled*/ false, main_uuid};

  return true;
}

bool ReplicationState::SetReplicationRoleReplica(const ReplicationServerConfig &config) {
  // False positive report for the std::make_unique
  // Random UUID when first setting replica rol
  auto const main_uuid = utils::UUID{};
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
  if (!TryPersistRoleReplica(config, main_uuid)) {
    return false;
  }
  // False positive report for the std::make_unique
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
  replication_data_ =
      RoleReplicaData{.config = config, .server = std::make_unique<ReplicationServer>(config), .uuid_ = main_uuid};
  return true;
}

utils::BasicResult<RegisterReplicaStatus, ReplicationClient *> ReplicationState::RegisterReplica(
    const ReplicationClientConfig &config) {
  auto const replica_handler = [](RoleReplicaData const &) { return RegisterReplicaStatus::NOT_MAIN; };

  ReplicationClient *client{nullptr};
  auto const main_handler = [&client, &config, this](RoleMainData &mainData) -> RegisterReplicaStatus {
    // name check
    auto name_check = [&config](auto const &replicas) {
      auto name_matches = [&name = config.name](auto const &replica) { return replica.name_ == name; };
      return std::any_of(replicas.begin(), replicas.end(), name_matches);
    };
    if (name_check(mainData.registered_replicas_)) {
      return RegisterReplicaStatus::NAME_EXISTS;
    }

    // endpoint check
    auto endpoint_check = [&](auto const &replicas) {
      auto endpoint_matches = [&config](auto const &replica) {
        const auto &ep = replica.rpc_client_.Endpoint();
        return ep == config.repl_server_endpoint;
      };
      return std::any_of(replicas.begin(), replicas.end(), endpoint_matches);
    };
    if (endpoint_check(mainData.registered_replicas_)) {
      return RegisterReplicaStatus::ENDPOINT_EXISTS;
    }

    // Durability
    if (!TryPersistRegisteredReplica(config, mainData.uuid_)) {
      return RegisterReplicaStatus::COULD_NOT_BE_PERSISTED;
    }

    // set
    client = &mainData.registered_replicas_.emplace_back(config);
    return RegisterReplicaStatus::SUCCESS;
  };

  const auto &res = std::visit(utils::Overloaded{main_handler, replica_handler}, replication_data_);
  if (res == RegisterReplicaStatus::SUCCESS) {
    return client;
  }
  return res;
}

}  // namespace memgraph::replication
