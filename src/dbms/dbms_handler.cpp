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

#include "dbms/dbms_handler.hpp"

#include <cstdint>
#include <filesystem>

#include "dbms/constants.hpp"
#include "dbms/global.hpp"
#include "flags/experimental.hpp"
#include "spdlog/spdlog.h"
#include "system/include/system/system.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

namespace {
constexpr std::string_view kDBPrefix = "database:";  // Key prefix for database durability

std::string RegisterReplicaErrorToString(query::RegisterReplicaError error) {
  switch (error) {
    using enum query::RegisterReplicaError;
    case NAME_EXISTS:
      return "NAME_EXISTS";
    case ENDPOINT_EXISTS:
      return "ENDPOINT_EXISTS";
    case CONNECTION_FAILED:
      return "CONNECTION_FAILED";
    case COULD_NOT_BE_PERSISTED:
      return "COULD_NOT_BE_PERSISTED";
    case ERROR_ACCEPTING_MAIN:
      return "ERROR_ACCEPTING_MAIN";
  }
}

// Per storage
// NOTE Storage will connect to all replicas. Future work might change this
void RestoreReplication(replication::RoleMainData &mainData, DatabaseAccess db_acc) {
  spdlog::info("Restoring replication role.");

  // Each individual client has already been restored and started. Here we just go through each database and start its
  // client
  for (auto &instance_client : mainData.registered_replicas_) {
    spdlog::info("Replica {} restoration started for {}.", instance_client.name_, db_acc->name());
    const auto &ret = db_acc->storage()->repl_storage_state_.replication_clients_.WithLock(
        [&, db_acc](auto &storage_clients) mutable -> utils::BasicResult<query::RegisterReplicaError> {
          auto client = std::make_unique<storage::ReplicationStorageClient>(instance_client, mainData.uuid_);
          auto *storage = db_acc->storage();
          client->Start(storage, std::move(db_acc));
          // After start the storage <-> replica state should be READY or RECOVERING (if correctly started)
          // MAYBE_BEHIND isn't a statement of the current state, this is the default value
          // Failed to start due to branching of MAIN and REPLICA
          if (client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
            spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.",
                         instance_client.name_);
          }
          storage_clients.push_back(std::move(client));
          return {};
        });

    if (ret.HasError()) {
      MG_ASSERT(query::RegisterReplicaError::CONNECTION_FAILED != ret.GetError());
      LOG_FATAL("Failure when restoring replica {}: {}.", instance_client.name_,
                RegisterReplicaErrorToString(ret.GetError()));
    }
    spdlog::info("Replica {} restored for {}.", instance_client.name_, db_acc->name());
  }
  spdlog::info("Replication role restored to MAIN.");
}
}  // namespace

#ifdef MG_ENTERPRISE
struct Durability {
  enum class DurabilityVersion : uint8_t {
    V0 = 0,
    V1,
  };

  struct VersionException : public utils::BasicException {
    VersionException() : utils::BasicException("Unsupported durability version!") {}
  };

  struct UnknownVersionException : public utils::BasicException {
    UnknownVersionException() : utils::BasicException("Unable to parse the durability version!") {}
  };

  struct MigrationException : public utils::BasicException {
    MigrationException() : utils::BasicException("Failed to migrate to the current durability version!") {}
  };

  static DurabilityVersion VersionCheck(std::optional<std::string_view> val) {
    if (!val) {
      return DurabilityVersion::V0;
    }
    if (val == "V1") {
      return DurabilityVersion::V1;
    }
    throw UnknownVersionException();
  };

  static auto GenKey(std::string_view name) -> std::string { return fmt::format("{}{}", kDBPrefix, name); }

  static auto GenVal(utils::UUID uuid, std::filesystem::path rel_dir) {
    nlohmann::json json;
    json["uuid"] = uuid;
    json["rel_dir"] = rel_dir;
    // TODO: Serialize the configuration
    return json.dump();
  }

  static void Migrate(kvstore::KVStore *durability, const std::filesystem::path &root) {
    const auto ver_val = durability->Get("version");
    const auto ver = VersionCheck(ver_val);

    std::map<std::string, std::string> to_put;
    std::vector<std::string> to_delete;

    // Update from V0 to V1
    if (ver == DurabilityVersion::V0) {
      for (const auto &[key, val] : *durability) {
        if (key == "version") continue;  // Reserved key
        // Generate a UUID
        auto const uuid = utils::UUID();
        // New json values
        auto new_key = GenKey(key);
        auto path = root;
        if (key != kDefaultDB) {  // Special case for non-default DBs
          // Move directory to new UUID dir
          path = root / kMultiTenantDir / std::string{uuid};
          std::filesystem::path old_dir(root / kMultiTenantDir / key);
          std::error_code ec;
          std::filesystem::rename(old_dir, path, ec);
          MG_ASSERT(!ec, "Failed to upgrade durability: cannot move default directory.");
        }
        // Generate json and update value
        auto new_data = GenVal(uuid, std::filesystem::relative(path, root));
        to_put.emplace(std::move(new_key), std::move(new_data));
        to_delete.emplace_back(key);
      }
    }

    // Set version
    durability->Put("version", "V1");
    // Update to the new key-value pairs
    if (!durability->PutAndDeleteMultiple(to_put, to_delete)) {
      throw MigrationException();
    }
  }
};

DbmsHandler::DbmsHandler(storage::Config config, replication::ReplicationState &repl_state, auth::SynchedAuth &auth,
                         bool recovery_on_startup)
    : default_config_{std::move(config)}, auth_{auth}, repl_state_{repl_state} {
  // TODO: Decouple storage config from dbms config
  // TODO: Save individual db configs inside the kvstore and restore from there

  /*
   * FILESYSTEM MANIPULATION
   */
  const auto &root = default_config_.durability.storage_directory;
  storage::UpdatePaths(default_config_, root);
  const auto &db_dir = default_config_.durability.storage_directory / kMultiTenantDir;
  // TODO: Unify durability and wal
  const auto durability_dir = db_dir / ".durability";
  utils::EnsureDirOrDie(db_dir);
  utils::EnsureDirOrDie(durability_dir);
  durability_ = std::make_unique<kvstore::KVStore>(durability_dir);

  /*
   * DURABILITY
   */
  // Migrate durability
  Durability::Migrate(durability_.get(), root);
  auto directories = std::set{std::string{kDefaultDB}};

  // Recover previous databases
  if (flags::AreExperimentsEnabled(flags::Experiments::SYSTEM_REPLICATION) && !recovery_on_startup) {
    // This will result in dropping databases on SystemRecoveryHandler
    // for MT case, and for single DB case we might not even set replication as commit timestamp is checked
    spdlog::warn(
        "Data recovery on startup not set, this will result in dropping database in case of multi-tenancy enabled.");
  }

  // TODO: Problem is if user doesn't set this up "database" name won't be recovered
  // but if storage-recover-on-startup is true storage will be recovered which is an issue
  spdlog::info("Data recovery on startup set to {}", recovery_on_startup);
  if (recovery_on_startup) {
    auto it = durability_->begin(std::string(kDBPrefix));
    auto end = durability_->end(std::string(kDBPrefix));
    for (; it != end; ++it) {
      const auto &[key, config_json] = *it;
      const auto name = key.substr(kDBPrefix.size());
      auto json = nlohmann::json::parse(config_json);
      const auto uuid = json.at("uuid").get<utils::UUID>();
      const auto rel_dir = json.at("rel_dir").get<std::filesystem::path>();
      spdlog::info("Restoring database {} at {}.", name, rel_dir);
      auto new_db = New_(name, uuid, nullptr, rel_dir);
      MG_ASSERT(!new_db.HasError(), "Failed while creating database {}.", name);
      directories.emplace(rel_dir.filename());
      spdlog::info("Database {} restored.", name);
    }
  } else {  // Clear databases from the durability list and auth
    auto locked_auth = auth_.Lock();
    auto it = durability_->begin(std::string{kDBPrefix});
    auto end = durability_->end(std::string{kDBPrefix});
    for (; it != end; ++it) {
      const auto &[key, _] = *it;
      const auto name = key.substr(kDBPrefix.size());
      if (name == kDefaultDB) continue;
      locked_auth->DeleteDatabase(name);
      durability_->Delete(key);
    }
  }

  /*
   * DATABASES CLEAN UP
   */
  // Clean the unused directories
  for (const auto &entry : std::filesystem::directory_iterator(db_dir)) {
    const auto &name = entry.path().filename().string();
    if (entry.is_directory() && !name.empty() && name.front() != '.') {
      auto itr = directories.find(name);
      if (itr == directories.end()) {
        std::error_code dummy;
        std::filesystem::remove_all(entry, dummy);
      } else {
        directories.erase(itr);
      }
    }
  }

  /*
   * DEFAULT DB SETUP
   */
  // Setup the default DB
  SetupDefault_();
}

struct DropDatabase : memgraph::system::ISystemAction {
  explicit DropDatabase(utils::UUID uuid) : uuid_{uuid} {}
  void DoDurability() override { /* Done during DBMS execution */
  }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     replication::ReplicationEpoch const &epoch,
                     memgraph::system::Transaction const &txn) const override {
    auto check_response = [](const storage::replication::DropDatabaseRes &response) {
      return response.result != storage::replication::DropDatabaseRes::Result::FAILURE;
    };

    return client.SteamAndFinalizeDelta<storage::replication::DropDatabaseRpc>(
        check_response, main_uuid, std::string(epoch.id()), txn.last_committed_system_timestamp(), txn.timestamp(),
        uuid_);
  }
  void PostReplication(replication::RoleMainData &mainData) const override {}

 private:
  utils::UUID uuid_;
};

DbmsHandler::DeleteResult DbmsHandler::TryDelete(std::string_view db_name, system::Transaction *transaction) {
  std::lock_guard<LockT> wr(lock_);
  if (db_name == kDefaultDB) {
    // MSG cannot delete the default db
    return DeleteError::DEFAULT_DB;
  }

  // Get DB config for the UUID and disk clean up
  const auto conf = db_handler_.GetConfig(db_name);
  if (!conf) {
    return DeleteError::NON_EXISTENT;
  }
  const auto &storage_path = conf->durability.storage_directory;
  const auto &uuid = conf->salient.uuid;

  // Check if db exists
  try {
    // Low level handlers
    if (!db_handler_.TryDelete(db_name)) {
      return DeleteError::USING;
    }
  } catch (utils::BasicException &) {
    return DeleteError::NON_EXISTENT;
  }

  // Remove from durability list
  if (durability_) durability_->Delete(Durability::GenKey(db_name));

  // Delete disk storage
  std::error_code ec;
  (void)std::filesystem::remove_all(storage_path, ec);
  if (ec) {
    spdlog::error(R"(Failed to clean disk while deleting database "{}" stored in {})", db_name, storage_path);
  }

  // Success
  // Save delta
  if (transaction) {
    transaction->AddAction<DropDatabase>(uuid);
  }

  return {};
}

DbmsHandler::DeleteResult DbmsHandler::Delete(std::string_view db_name) {
  auto wr = std::lock_guard(lock_);
  return Delete_(db_name);
}

DbmsHandler::DeleteResult DbmsHandler::Delete(utils::UUID uuid) {
  auto wr = std::lock_guard(lock_);
  std::string db_name;
  try {
    const auto db = Get_(uuid);
    db_name = db->name();
  } catch (const UnknownDatabaseException &) {
    return DeleteError::NON_EXISTENT;
  }
  return Delete_(db_name);
}

struct CreateDatabase : memgraph::system::ISystemAction {
  explicit CreateDatabase(storage::SalientConfig config, DatabaseAccess db_acc)
      : config_{std::move(config)}, db_acc(db_acc) {}

  void DoDurability() override {
    // Done during dbms execution
  }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     replication::ReplicationEpoch const &epoch,
                     memgraph::system::Transaction const &txn) const override {
    auto check_response = [](const storage::replication::CreateDatabaseRes &response) {
      return response.result != storage::replication::CreateDatabaseRes::Result::FAILURE;
    };

    return client.SteamAndFinalizeDelta<storage::replication::CreateDatabaseRpc>(
        check_response, main_uuid, std::string(epoch.id()), txn.last_committed_system_timestamp(), txn.timestamp(),
        config_);
  }

  void PostReplication(replication::RoleMainData &mainData) const override {
    // Sync database with REPLICAs
    // NOTE: The function bellow is used to create ReplicationStorageClient, so it must be called on a new storage
    // We don't need to have it here, since the function won't fail even if the replication client fails to
    // connect We will just have everything ready, for recovery at some point.
    dbms::DbmsHandler::RecoverStorageReplication(db_acc, mainData);
  }

 private:
  storage::SalientConfig config_;
  DatabaseAccess db_acc;
};

DbmsHandler::NewResultT DbmsHandler::New_(storage::Config storage_config, system::Transaction *txn) {
  auto new_db = db_handler_.New(storage_config, repl_state_);

  if (new_db.HasValue()) {  // Success
                            // Save delta
    UpdateDurability(storage_config);
    if (txn) {
      txn->AddAction<CreateDatabase>(storage_config.salient, new_db.GetValue());
    }
  }
  return new_db;
}

DbmsHandler::DeleteResult DbmsHandler::Delete_(std::string_view db_name) {
  if (db_name == kDefaultDB) {
    // MSG cannot delete the default db
    return DeleteError::DEFAULT_DB;
  }

  const auto storage_path = StorageDir_(db_name);
  if (!storage_path) return DeleteError::NON_EXISTENT;

  {
    auto db = db_handler_.Get(db_name);
    if (!db) return DeleteError::NON_EXISTENT;
    // TODO: ATM we assume REPLICA won't have streams,
    //       this is a best effort approach just in case they do
    //       there is still subtle data race we stream manipulation
    //       can occur while we are dropping the database
    db->prepare_for_deletion();
    auto &database = *db->get();
    database.streams()->StopAll();
    database.streams()->DropAll();
    database.thread_pool()->Shutdown();
  }

  // Remove from durability list
  if (durability_) durability_->Delete(Durability::GenKey(db_name));

  // Check if db exists
  // Low level handlers
  db_handler_.DeferDelete(db_name, [storage_path = *storage_path, db_name = std::string{db_name}]() {
    // Delete disk storage
    std::error_code ec;
    (void)std::filesystem::remove_all(storage_path, ec);
    if (ec) {
      spdlog::error(R"(Failed to clean disk while deleting database "{}" stored in {})", db_name, storage_path);
    }
  });

  return {};  // Success
}

void DbmsHandler::UpdateDurability(const storage::Config &config, std::optional<std::filesystem::path> rel_dir) {
  if (!durability_) return;
  // Save database in a list of active databases
  const auto &key = Durability::GenKey(config.salient.name);
  if (rel_dir == std::nullopt) {
    rel_dir =
        std::filesystem::relative(config.durability.storage_directory, default_config_.durability.storage_directory);
  }
  const auto &val = Durability::GenVal(config.salient.uuid, *rel_dir);
  durability_->Put(key, val);
}

#endif

void DbmsHandler::RecoverStorageReplication(DatabaseAccess db_acc, replication::RoleMainData &role_main_data) {
  using enum memgraph::flags::Experiments;
  auto const is_enterprise = license::global_license_checker.IsEnterpriseValidFast();
  auto experimental_system_replication = flags::AreExperimentsEnabled(SYSTEM_REPLICATION);
  if ((is_enterprise && experimental_system_replication) || db_acc->name() == dbms::kDefaultDB) {
    // Handle global replication state
    spdlog::info("Replication configuration will be stored and will be automatically restored in case of a crash.");
    // RECOVER REPLICA CONNECTIONS
    memgraph::dbms::RestoreReplication(role_main_data, db_acc);
  } else if (!role_main_data.registered_replicas_.empty()) {
    spdlog::warn("Multi-tenant replication is currently not supported!");
  }
}
}  // namespace memgraph::dbms
