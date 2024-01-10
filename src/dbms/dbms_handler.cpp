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
#include "dbms/replication_client.hpp"
#include "spdlog/spdlog.h"
#include "utils/exceptions.hpp"
#include "utils/uuid.hpp"

namespace memgraph::dbms {

#ifdef MG_ENTERPRISE

namespace {
constexpr std::string_view kDBPrefix = "database:";                               // Key prefix for database durability
constexpr std::string_view kLastCommitedSystemTsKey = "last_commited_system_ts";  // Key for timestamp durability
}  // namespace

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

  static DurabilityVersion StringToVersion(std::string_view str) {
    if (str == "V0") return DurabilityVersion::V0;
    if (str == "V1") return DurabilityVersion::V1;
    throw VersionException();
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
          std::filesystem::rename(old_dir, path);
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

DbmsHandler::DbmsHandler(
    storage::Config config,
    memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
    bool recovery_on_startup)
    : default_config_{std::move(config)}, repl_state_{ReplicationStateRootPath(default_config_)} {
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
      auto new_db = New_(name, uuid, rel_dir);
      MG_ASSERT(!new_db.HasError(), "Failed while creating database {}.", name);
      directories.emplace(rel_dir.filename());
      spdlog::info("Database {} restored.", name);
    }
    // Read the last timestamp
    auto lcst = durability_->Get(kLastCommitedSystemTsKey);
    if (lcst) {
      last_commited_system_timestamp_ = std::stoul(*lcst);
      system_timestamp_ = last_commited_system_timestamp_;
    }
  } else {  // Clear databases from the durability list and auth
    auto locked_auth = auth->Lock();
    auto it = durability_->begin(std::string{kDBPrefix});
    auto end = durability_->end(std::string{kDBPrefix});
    for (; it != end; ++it) {
      const auto &[key, _] = *it;
      const auto name = key.substr(kDBPrefix.size());
      if (name == kDefaultDB) continue;
      locked_auth->DeleteDatabase(name);
      durability_->Delete(key);
    }
    // Delete the last timestamp
    durability_->Delete(kLastCommitedSystemTsKey);
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
        std::filesystem::remove_all(entry);
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

  /*
   * REPLICATION RECOVERY AND STARTUP
   */
  // Startup replication state (if recovered at startup)
  auto replica = [this](replication::RoleReplicaData const &data) { return StartRpcServer(*this, data); };
  // Replication recovery and frequent check start
  auto main = [this](replication::RoleMainData &data) {
    for (auto &client : data.registered_replicas_) {
      SystemRestore(client);
    }
    ForEach([this](DatabaseAccess db) { RecoverReplication(db); });
    for (auto &client : data.registered_replicas_) {
      StartReplicaClient(*this, client);
    }
    return true;
  };
  // Startup proccess for main/replica
  MG_ASSERT(std::visit(memgraph::utils::Overloaded{replica, main}, repl_state_.ReplicationData()),
            "Replica recovery failure!");

  // Warning
  if (default_config_.durability.snapshot_wal_mode == storage::Config::Durability::SnapshotWalMode::DISABLED &&
      repl_state_.IsMain()) {
    spdlog::warn(
        "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please "
        "consider "
        "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
        "without write-ahead logs this instance is not replicating any data.");
  }
}

DbmsHandler::DeleteResult DbmsHandler::TryDelete(std::string_view db_name) {
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
  if (system_transaction_) {
    system_transaction_->delta.emplace(SystemTransaction::Delta::drop_database, uuid);
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

DbmsHandler::NewResultT DbmsHandler::New_(storage::Config storage_config) {
  auto new_db = db_handler_.New(storage_config, repl_state_);

  if (new_db.HasValue()) {  // Success
                            // Save delta
    if (system_transaction_) {
      system_transaction_->delta.emplace(SystemTransaction::Delta::create_database, storage_config.salient);
    }
    UpdateDurability(storage_config);
    return new_db.GetValue();
  }
  return new_db.GetError();
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
  if (rel_dir == std::nullopt)
    rel_dir =
        std::filesystem::relative(config.durability.storage_directory, default_config_.durability.storage_directory);
  const auto &val = Durability::GenVal(config.salient.uuid, *rel_dir);
  durability_->Put(key, val);
}

void DbmsHandler::Commit() {
  if (system_transaction_ == std::nullopt || system_transaction_->delta == std::nullopt) return;  // Nothing to commit
  const auto &delta = *system_transaction_->delta;

  // TODO Create a system client that can handle all of this automatically
  switch (delta.action) {
    using enum SystemTransaction::Delta::Action;
    case CREATE_DATABASE: {
      // Replication
      auto main_handler = [&](memgraph::replication::RoleMainData &main_data) {
        // TODO: data race issue? registered_replicas_ access not protected
        // This is sync in any case, as this is the startup
        for (auto &client : main_data.registered_replicas_) {
          SteamAndFinalizeDelta<storage::replication::CreateDatabaseRpc>(
              client,
              [](const storage::replication::CreateDatabaseRes &response) {
                return response.result == storage::replication::CreateDatabaseRes::Result::FAILURE;
              },
              std::string(main_data.epoch_.id()), system_transaction_->system_timestamp, delta.config);
        }
        // Sync database with REPLICAs
        RecoverReplication(Get_(delta.config.name));
      };
      auto replica_handler = [](memgraph::replication::RoleReplicaData &) { /* Nothing to do */ };
      std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
    } break;
    case DROP_DATABASE: {
      // Replication
      auto main_handler = [&](memgraph::replication::RoleMainData &main_data) {
        // TODO: data race issue? registered_replicas_ access not protected
        // This is sync in any case, as this is the startup
        for (auto &client : main_data.registered_replicas_) {
          SteamAndFinalizeDelta<storage::replication::DropDatabaseRpc>(
              client,
              [](const storage::replication::DropDatabaseRes &response) {
                return response.result == storage::replication::DropDatabaseRes::Result::FAILURE;
              },
              std::string(main_data.epoch_.id()), system_transaction_->system_timestamp, delta.uuid);
        }
      };
      auto replica_handler = [](memgraph::replication::RoleReplicaData &) { /* Nothing to do */ };
      std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
    } break;
  }

  last_commited_system_timestamp_ = system_transaction_->system_timestamp;
  durability_->Put(kLastCommitedSystemTsKey, std::to_string(last_commited_system_timestamp_));
  ResetSystemTransaction();
}

#else  // not MG_ENTERPRISE

void DbmsHandler::Commit() {
  if (system_transaction_ == std::nullopt || system_transaction_->delta == std::nullopt) return;  // Nothing to commit
  const auto &delta = *system_transaction_->delta;

  // TODO Create a system client that can handle all of this automatically
  switch (delta.action) {
    using enum SystemTransaction::Delta::Action;
    case CREATE_DATABASE:
    case DROP_DATABASE:
      /* Community edition doesn't support multi-tenant replication */
      break;
  }

  last_commited_system_timestamp_ = system_transaction_->system_timestamp;
  // TODO Durability once community needs it
  ResetSystemTransaction();
}

#endif
}  // namespace memgraph::dbms
