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
#include "utils/variant_helpers.hpp"

constexpr auto kReplicationDirectory = std::string_view{"replication"};

namespace memgraph::replication {

auto BuildReplicaKey(std::string_view name) -> std::string {
  auto key = std::string{durability::kReplicationReplicaPrefix};
  key.append(name.begin(), name.end());
  return key;
}

ReplicationState::ReplicationState(std::optional<std::filesystem::path> durability_dir) {
  if (!durability_dir) return;
  auto repl_dir = *std::move(durability_dir);
  repl_dir /= kReplicationDirectory;
  utils::EnsureDirOrDie(repl_dir);
  durability_ = std::make_unique<kvstore::KVStore>(std::move(repl_dir));

  auto replicationData = FetchReplicationData();
  if (replicationData.HasError()) {
    switch (replicationData.GetError()) {
      using enum ReplicationState::FetchReplicationError;
      case NOTHING_FETCHED: {
        spdlog::debug("Cannot find data needed for restore replication role in persisted metadata.");
        replication_data_ = ReplicationState::ReplicationDataMain_t{};
        return;
      }
      case PARSE_ERROR: {
        LOG_FATAL("Cannot parse previously saved configuration of replication role.");
        return;
      }
    }
  }
  replication_data_ = std::move(replicationData).GetValue();
}
bool ReplicationState::TryPersistRoleReplica(const ReplicationServerConfig &config) {
  if (!ShouldPersist()) return true;

  auto data = durability::ReplicationRoleEntry{.role = durability::ReplicaRole{
                                                   .config = config,
                                               }};

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
bool ReplicationState::TryPersistRoleMain() {
  if (!ShouldPersist()) return true;

  auto data = durability::ReplicationRoleEntry{.role = durability::MainRole{}};

  if (durability_->Put(durability::kReplicationRoleName, nlohmann::json(data).dump())) {
    role_persisted = RolePersisted::YES;
    return true;
  }
  spdlog::error("Error when saving MAIN replication role in settings.");
  return false;
}

bool ReplicationState::TryPersistUnregisterReplica(std::string_view name) {
  if (!ShouldPersist()) return true;

  auto key = BuildReplicaKey(name);

  if (durability_->Delete(key)) return true;
  spdlog::error("Error when removing replica {} from settings.", name);
  return false;
}

// TODO: FetchEpochData (agnostic of FetchReplicationData, but should be done before)

auto ReplicationState::FetchReplicationData() -> FetchReplicationResult_t {
  if (!ShouldPersist()) return FetchReplicationError::NOTHING_FETCHED;
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
              auto res = ReplicationState::ReplicationDataMain_t{};
              auto b = durability_->begin(durability::kReplicationReplicaPrefix);
              auto e = durability_->end(durability::kReplicationReplicaPrefix);
              res.reserve(durability_->Size(durability::kReplicationReplicaPrefix));
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
                  res.emplace_back(std::move(data.config));
                } catch (...) {
                  return FetchReplicationError::PARSE_ERROR;
                }
              }
              return {std::move(res)};
            },
            [&](durability::ReplicaRole &&r) -> FetchReplicationResult_t {
              return {ReplicationDataReplica_t{std::move(r.config)}};
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
        if (old_key == durability::kReplicationRoleName || old_key == durability::kReplicationEpoch) {
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
      data.version = durability::DurabilityVersion::V2;
      to_put.emplace(durability::kReplicationRoleName, nlohmann::json(data).dump());
      if (!durability_->PutAndDeleteMultiple(to_put, to_delete)) return false;  // some reason couldn't persist
      [[fallthrough]];
    }
    case durability::DurabilityVersion::V2: {
      // do nothing - add code if V3 ever happens
      break;
    }
  }
  return true;
}

bool ReplicationState::TryPersistRegisteredReplica(const ReplicationClientConfig &config) {
  if (!ShouldPersist()) return true;

  // If any replicas are persisted then Role must be persisted
  if (role_persisted != RolePersisted::YES) {
    DMG_ASSERT(IsMain(), "MAIN is expected");
    if (!TryPersistRoleMain()) return false;
  }

  auto data = durability::ReplicationReplicaEntry{.config = config};

  auto key = BuildReplicaKey(config.name);
  if (durability_->Put(key, nlohmann::json(data).dump())) return true;
  spdlog::error("Error when saving replica {} in settings.", config.name);
  return false;
}
bool ReplicationState::SetReplicationRoleMain() {
  auto prev_epoch = NewEpoch();  // TODO: need Epoch to be part of the durability state
  if (!TryPersistRoleMain()) {
    SetEpoch(std::move(prev_epoch));
    return false;
  }
  replication_data_ = ReplicationDataMain_t{};
  return true;
}
bool ReplicationState::SetReplicationRoleReplica(const ReplicationServerConfig &config) {
  if (!TryPersistRoleReplica(config)) {
    return false;
  }
  replication_data_ = ReplicationDataReplica_t{config};
  return true;
}
auto ReplicationState::RegisterReplica(const ReplicationClientConfig &config) -> RegisterReplicaError {
  auto const replica_handler = [](ReplicationState::ReplicationDataReplica_t &) -> RegisterReplicaError {
    return RegisterReplicaError::NOT_MAIN;
  };
  auto const main_handler = [this, &config](ReplicationState::ReplicationDataMain_t &mainData) -> RegisterReplicaError {
    // name check
    auto name_check = [&config](ReplicationState::ReplicationDataMain_t &replicas) {
      auto name_matches = [&name = config.name](ReplicationClientConfig const &registered_config) {
        return registered_config.name == name;
      };
      return std::any_of(replicas.begin(), replicas.end(), name_matches);
    };
    if (name_check(mainData)) {
      return RegisterReplicaError::NAME_EXISTS;
    }

    // endpoint check
    auto endpoint_check = [&](ReplicationState::ReplicationDataMain_t &replicas) {
      auto endpoint_matches = [&config](ReplicationClientConfig const &registered_config) {
        return registered_config.ip_address == config.ip_address && registered_config.port == config.port;
      };
      return std::any_of(replicas.begin(), replicas.end(), endpoint_matches);
    };
    if (endpoint_check(mainData)) {
      return RegisterReplicaError::END_POINT_EXISTS;
    }

    // Durability
    if (!TryPersistRegisteredReplica(config)) {
      return RegisterReplicaError::COULD_NOT_BE_PERSISTED;
    }

    // set
    mainData.emplace_back(config);
    return RegisterReplicaError::SUCCESS;
  };

  return std::visit(utils::Overloaded{main_handler, replica_handler}, replication_data_);
}
}  // namespace memgraph::replication
