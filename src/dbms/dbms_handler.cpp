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

#include "dbms/dbms_handler.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <shared_mutex>
#include <thread>

#include "dbms/constants.hpp"
#include "dbms/global.hpp"
#include "dbms/rpc.hpp"
#include "license/license.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

#include <mutex>

namespace memgraph::dbms {

namespace {
#ifdef MG_ENTERPRISE
constexpr std::string_view kDBPrefix = "database:";  // Key prefix for database durability
#endif

// Per storage
// NOTE Storage will connect to all replicas. Future work might change this
void RestoreReplication(replication::RoleMainData &mainData, DatabaseAccess db_acc) {
  spdlog::info("Restoring replication role.");

  // Each individual client has already been restored and started. Here we just go through each database and start its
  // client
  for (auto &instance_client : mainData.registered_replicas_) {
    spdlog::info("Replica {} restoration started for {}.", instance_client.name_, db_acc->name());
    auto client = std::make_unique<storage::ReplicationStorageClient>(instance_client, mainData.uuid_);
    auto *storage = db_acc->storage();
    auto protector = dbms::DatabaseProtector{db_acc};
    client->Start(storage, protector);
    // After start the storage <-> replica state should be READY or RECOVERING (if correctly started)
    // MAYBE_BEHIND isn't a statement of the current state, this is the default value
    // Failed to start due to branching of MAIN and REPLICA
    if (client->State() == storage::replication::ReplicaState::MAYBE_BEHIND) {
      spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.",
                   instance_client.name_);
    }
    db_acc->storage()->repl_storage_state_.replication_storage_clients_.WithLock(
        [client = std::move(client)](auto &storage_clients) mutable { storage_clients.push_back(std::move(client)); });
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
      for (const auto &[key, _] : *durability) {
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

DbmsHandler::DbmsHandler(storage::Config config) : default_config_{std::move(config)} {
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

  // Restore databases
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
    MG_ASSERT(new_db.has_value(), "Failed while creating database {}.", name);
    directories.emplace(rel_dir.filename());
    spdlog::info("Database {} restored.", name);
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

  /*
   * TENANT PROFILES
   */
  tenant_profiles_ = std::make_unique<TenantProfiles>(*durability_);
  RestoreTenantProfiles_();
}

struct DropDatabase : memgraph::system::ISystemAction {
  explicit DropDatabase(utils::UUID uuid) : uuid_{uuid} {}

  void DoDurability() override { /* Done during DBMS execution */ }

  bool ShouldReplicateInCommunity() const override { return false; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check_response = [](const storage::replication::DropDatabaseRes &response) {
      return response.result != storage::replication::DropDatabaseRes::Result::FAILURE;
    };

    return client.StreamAndFinalizeDelta<storage::replication::DropDatabaseRpc>(
        check_response, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), uuid_);
  }

  void PostReplication(replication::RoleMainData &mainData) const override {}

 private:
  utils::UUID uuid_;
};

struct RenameDatabase : memgraph::system::ISystemAction {
  explicit RenameDatabase(std::string old_name, std::string new_name)
      : old_name_{std::move(old_name)}, new_name_{std::move(new_name)} {}

  void DoDurability() override { /* Done during DBMS execution */ }

  bool ShouldReplicateInCommunity() const override { return false; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check_response = [](const storage::replication::RenameDatabaseRes &response) {
      return response.result != storage::replication::RenameDatabaseRes::Result::FAILURE;
    };

    return client.StreamAndFinalizeDelta<storage::replication::RenameDatabaseRpc>(
        check_response, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), uuid_, old_name_, new_name_);
  }

  void PostReplication(replication::RoleMainData &mainData) const override {}

 private:
  std::string old_name_;
  std::string new_name_;
  utils::UUID uuid_;
};

DbmsHandler::DeleteResult DbmsHandler::TryDelete(std::string_view db_name, system::Transaction *transaction) {
  auto wr = std::lock_guard{lock_};
  if (db_name == kDefaultDB) {
    // MSG cannot delete the default db
    return std::unexpected{DeleteError::DEFAULT_DB};
  }

  // Get DB config for the UUID and disk clean up
  const auto conf = db_handler_.GetConfig(db_name);
  if (!conf) {
    return std::unexpected{DeleteError::NON_EXISTENT};
  }
  const auto &storage_path = conf->durability.storage_directory;
  const auto &uuid = conf->salient.uuid;

  // Check if db exists
  try {
    // Low level handlers
    if (!db_handler_.TryDelete(db_name)) {
      return std::unexpected{DeleteError::USING};
    }
  } catch (utils::BasicException &) {
    return std::unexpected{DeleteError::NON_EXISTENT};
  }

  // Remove from durability list
  if (durability_) durability_->Delete(Durability::GenKey(db_name));

  // Delete disk storage
  std::error_code ec;
  (void)std::filesystem::remove_all(storage_path, ec);
  if (ec) {
    spdlog::error(R"(Failed to clean disk while deleting database "{}" stored in {})", db_name, storage_path);
  }

  // Detach from tenant profile. Return value is safe to ignore here because this
  // code path (TryDelete) is exclusive with the DetachFromDatabase call in Delete_
  // below. If the DB is not attached, detaching is a no-op.
  if (tenant_profiles_) {
    [[maybe_unused]] auto detached = tenant_profiles_->DetachFromDatabase(db_name);
  }

  // Success
  // Save delta
  if (transaction) {
    transaction->AddAction<DropDatabase>(uuid);
  }

  return {};
}

DbmsHandler::DeleteResult DbmsHandler::Delete(std::string_view db_name, system::Transaction *transaction) {
  auto wr = std::lock_guard(lock_);

  // Get DB config for the UUID and disk clean up
  const auto conf = db_handler_.GetConfig(db_name);
  if (!conf) {
    return std::unexpected{DeleteError::NON_EXISTENT};
  }

  // Force delete
  const auto res = Delete_(db_name);
  if (res) {
    // Success; save delta
    if (transaction) {
      transaction->AddAction<DropDatabase>(conf->salient.uuid);
    }
  }
  return res;
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
    return std::unexpected{DeleteError::NON_EXISTENT};
  }
  return Delete_(db_name);
}

DbmsHandler::RenameResult DbmsHandler::Rename(std::string_view old_name, std::string_view new_name,
                                              system::Transaction *txn) {
  auto wr = std::lock_guard{lock_};

  // Check if trying to rename default database
  if (old_name == kDefaultDB) {
    return std::unexpected{RenameError::DEFAULT_DB};
  }

  if (old_name == new_name) {
    return std::unexpected{RenameError::SAME_NAME};
  }

  // Perform the rename operation in the handler
  if (auto rename_result = db_handler_.Rename(old_name, new_name); !rename_result.has_value()) {
    return std::unexpected{rename_result.error()};
  }

  // Update current db config
  auto new_db = db_handler_.Get(new_name);
  MG_ASSERT(new_db, "Database {} not found after rename.", new_name);
  (*new_db)->storage()->config_.salient.name = new_name;

  // Update durability metadata
  if (durability_) {
    const auto old_key = Durability::GenKey(old_name);
    const auto new_key = Durability::GenKey(new_name);
    const auto old_val = durability_->Get(old_key);

    if (old_val) {
      // Parse the existing value and update the name
      auto json = nlohmann::json::parse(*old_val);
      json["name"] = new_name;

      // Update in durability store
      durability_->Put(new_key, json.dump());
      durability_->Delete(old_key);
    }
  }

  // Update tenant profile membership (no-op if database had no profile attached).
  if (tenant_profiles_) {
    [[maybe_unused]] auto renamed = tenant_profiles_->RenameDatabase(old_name, new_name);
  }

  // Add system action for replication
  if (txn) {
    txn->AddAction<RenameDatabase>(std::string{old_name}, std::string{new_name});
  }

  return {};  // Success
}

struct CreateDatabase : memgraph::system::ISystemAction {
  explicit CreateDatabase(storage::SalientConfig config, DatabaseAccess db_acc)
      : config_{std::move(config)}, db_acc(db_acc) {}

  void DoDurability() override {
    // Done during dbms execution
  }

  bool ShouldReplicateInCommunity() const override { return false; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check_response = [](const storage::replication::CreateDatabaseRes &response) {
      return response.result != storage::replication::CreateDatabaseRes::Result::FAILURE;
    };

    return client.StreamAndFinalizeDelta<storage::replication::CreateDatabaseRpc>(
        check_response, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), config_);
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

constexpr int64_t kUnusedMemoryLimit = 0;

struct TenantProfileAction : memgraph::system::ISystemAction {
  using Action = storage::replication::TenantProfileReq::Action;

  TenantProfileAction(Action action, std::string_view profile_name, std::string_view db_name, int64_t memory_limit)
      : action_{action}, profile_{.name = std::string{profile_name}, .memory_limit = memory_limit}, db_name_{db_name} {}

  void DoDurability() override {}

  bool ShouldReplicateInCommunity() const override { return false; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    return client.StreamAndFinalizeDelta<storage::replication::TenantProfileRpc>(
        [](const storage::replication::TenantProfileRes &response) { return response.success; },
        main_uuid,
        txn.last_committed_system_timestamp(),
        txn.timestamp(),
        action_,
        profile_,
        db_name_);
  }

  void PostReplication(replication::RoleMainData & /*mainData*/) const override {}

 private:
  Action action_;
  TenantProfiles::Profile profile_;
  std::string db_name_;
};

std::expected<void, TenantProfiles::CreateError> DbmsHandler::CreateTenantProfile(std::string_view name,
                                                                                  int64_t memory_limit,
                                                                                  system::Transaction *sys_txn) {
  auto result = tenant_profiles_->Create(name, memory_limit);
  if (!result) return std::unexpected{result.error()};
  if (sys_txn) {
    sys_txn->AddAction<TenantProfileAction>(TenantProfileAction::Action::CREATE, name, "", memory_limit);
  }
  return {};
}

std::expected<void, TenantProfiles::AlterError> DbmsHandler::AlterTenantProfile(std::string_view name,
                                                                                int64_t memory_limit,
                                                                                system::Transaction *sys_txn) {
  auto result = tenant_profiles_->Alter(name, memory_limit);
  if (!result) return std::unexpected{result.error()};
  for (const auto &db_name : *result) {
    try {
      auto db_acc = Get(db_name);
      db_acc.get()->SetTenantMemoryLimit(memory_limit);
    } catch (const UnknownDatabaseException &) {
      // DB was dropped concurrently — the profile change is already durable and will not
      // be re-applied on restart (the DB no longer exists). Skip gracefully.
      spdlog::warn("AlterTenantProfile: database '{}' not found while applying profile '{}' — skipping", db_name, name);
    }
  }
  if (sys_txn) {
    sys_txn->AddAction<TenantProfileAction>(TenantProfileAction::Action::ALTER, name, "", memory_limit);
  }
  return {};
}

std::expected<void, TenantProfiles::DropError> DbmsHandler::DropTenantProfile(std::string_view name,
                                                                              system::Transaction *sys_txn) {
  auto result = tenant_profiles_->Drop(name);
  if (!result) return std::unexpected{result.error()};
  if (sys_txn) {
    sys_txn->AddAction<TenantProfileAction>(TenantProfileAction::Action::DROP, name, "", kUnusedMemoryLimit);
  }
  return {};
}

std::expected<void, TenantProfiles::AttachError> DbmsHandler::SetTenantProfileOnDatabase(std::string_view profile_name,
                                                                                         std::string_view db_name,
                                                                                         system::Transaction *sys_txn) {
  auto db_acc = Get(db_name);
  auto result = tenant_profiles_->AttachToDatabase(profile_name, db_name);
  if (!result) return std::unexpected{result.error()};
  db_acc.get()->SetTenantMemoryLimit(*result);
  if (sys_txn) {
    sys_txn->AddAction<TenantProfileAction>(
        TenantProfileAction::Action::SET_ON_DATABASE, profile_name, db_name, *result);
  }
  return {};
}

std::expected<void, TenantProfiles::DetachError> DbmsHandler::RemoveTenantProfileFromDatabase(
    std::string_view db_name, system::Transaction *sys_txn) {
  auto result = tenant_profiles_->DetachFromDatabase(db_name);
  if (!result) return std::unexpected{result.error()};
  auto db_acc = Get(db_name);
  db_acc.get()->SetTenantMemoryLimit(0);
  if (sys_txn) {
    sys_txn->AddAction<TenantProfileAction>(
        TenantProfileAction::Action::REMOVE_FROM_DATABASE, "", db_name, kUnusedMemoryLimit);
  }
  return {};
}

DbmsHandler::NewResultT DbmsHandler::New_(storage::Config storage_config, system::Transaction *txn) {
  auto new_db = db_handler_.New(storage_config);

  if (new_db) {  // Success
                 // Save delta
    UpdateDurability(storage_config);
    if (txn) {
      txn->AddAction<CreateDatabase>(storage_config.salient, new_db.value());
    }
  }
  return new_db;
}

DbmsHandler::DeleteResult DbmsHandler::Delete_(std::string_view db_name) {
  if (db_name == kDefaultDB) {
    // MSG cannot delete the default db
    return std::unexpected{DeleteError::DEFAULT_DB};
  }

  const auto storage_path = StorageDir_(db_name);
  if (!storage_path) return std::unexpected{DeleteError::NON_EXISTENT};

  {
    auto db = db_handler_.Get(db_name);
    if (!db) return std::unexpected{DeleteError::NON_EXISTENT};
    // TODO: ATM we assume REPLICA won't have streams,
    //       this is a best effort approach just in case they do
    //       there is still subtle data race we stream manipulation
    //       can occur while we are dropping the database
    db->prepare_for_deletion();
    auto &database = *db->get();
    database.StopAllBackgroundTasks();
    database.streams()->DropAll();
  }

  // Remove from durability list
  if (durability_) durability_->Delete(Durability::GenKey(db_name));

  // Detach from tenant profile. Return value is safe to ignore here because this
  // code path (Delete_) is exclusive with the TryDelete path above. If the DB is
  // not attached, detaching is a no-op.
  if (tenant_profiles_) {
    [[maybe_unused]] auto detached = tenant_profiles_->DetachFromDatabase(db_name);
  }

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
  const auto &key = Durability::GenKey(*config.salient.name.str_view());
  if (rel_dir == std::nullopt) {
    rel_dir =
        std::filesystem::relative(config.durability.storage_directory, default_config_.durability.storage_directory);
  }
  const auto &val = Durability::GenVal(config.salient.uuid, *rel_dir);
  durability_->Put(key, val);
}

#endif

std::optional<memgraph::metrics::StorageSnapshot> DbmsHandler::TryGetStorageSnapshotForMetrics(utils::UUID const &uuid
                                                                                               [[maybe_unused]]) {
#ifdef MG_ENTERPRISE
  try {
    auto rd = std::shared_lock{lock_};
    auto db = Get_(uuid);
    auto const info = db->storage()->GetBaseInfo();
    return memgraph::metrics::StorageSnapshot{.vertex_count = info.vertex_count,
                                              .edge_count = info.edge_count,
                                              .disk_usage = info.disk_usage,
                                              .db_memory_tracked = db->DbMemoryUsage(),
                                              .db_peak_memory_tracked = db->DbPeakMemoryUsage(),
                                              .db_storage_memory_tracked = db->DbStorageMemoryUsage(),
                                              .db_embedding_memory_tracked = db->DbEmbeddingMemoryUsage(),
                                              .db_query_memory_tracked = db->DbQueryMemoryUsage()};
  } catch (UnknownDatabaseException const &) {
    return std::nullopt;
  }
#else
  auto db_opt = db_gatekeeper_.access();
  if (!db_opt) return std::nullopt;
  auto const info = (*db_opt)->storage()->GetBaseInfo();
  return memgraph::metrics::StorageSnapshot{.vertex_count = info.vertex_count,
                                            .edge_count = info.edge_count,
                                            .disk_usage = info.disk_usage,
                                            .db_memory_tracked = (*db_opt)->DbMemoryUsage(),
                                            .db_peak_memory_tracked = (*db_opt)->DbPeakMemoryUsage(),
                                            .db_storage_memory_tracked = (*db_opt)->DbStorageMemoryUsage(),
                                            .db_embedding_memory_tracked = (*db_opt)->DbEmbeddingMemoryUsage(),
                                            .db_query_memory_tracked = (*db_opt)->DbQueryMemoryUsage()};
#endif
}

void DbmsHandler::RecoverStorageReplication(DatabaseAccess db_acc, replication::RoleMainData &role_main_data) {
  auto const is_enterprise = license::global_license_checker.IsEnterpriseValidFast();
  if (is_enterprise || db_acc->name() == dbms::kDefaultDB) {
    // Handle global replication state
    spdlog::info("Replication configuration will be stored and will be automatically restored in case of a crash.");
    // RECOVER REPLICA CONNECTIONS
    memgraph::dbms::RestoreReplication(role_main_data, db_acc);
  } else if (!role_main_data.registered_replicas_.empty()) {
    spdlog::warn("Multi-tenant replication is currently not supported!");
  }
}

void DbmsHandler::RestoreTriggers(query::InterpreterContext *ic) {
#ifdef MG_ENTERPRISE
  auto wr = std::lock_guard{lock_};
  for (auto &[_, db_gk] : db_handler_) {
#else
  {
    auto &db_gk = db_gatekeeper_;
#endif
    if (auto db_acc_opt = db_gk.access()) {
      auto &db_acc = *db_acc_opt;
      spdlog::debug("Restoring trigger for database \"{}\"", db_acc->name());
      auto storage_accessor = db_acc->Access(memgraph::storage::WRITE);
      auto dba = memgraph::query::DbAccessor{storage_accessor.get()};
      db_acc->trigger_store()->RestoreTriggers(
          &ic->ast_cache, &dba, ic->config.query, ic->auth_checker, db_acc->name(), ic->parameters);
    }
  }
}

#ifdef MG_ENTERPRISE
// Hot/cold SUSPEND as a system action. Recording it makes SUSPEND participate in the system
// transaction — serialized + system-timestamped exactly like CREATE/DROP DATABASE. The replica
// wire (SuspendDatabaseRpc + replica apply handler) lands in C7; until then DoReplication is a
// no-op and suspend stays node-local. This is safe: SUSPEND is rejected while the tenant replicates
// (SuspendError::REPLICATING), so a MAIN with replicas never reaches this path and no delta is
// silently dropped.
struct SuspendDatabase : memgraph::system::ISystemAction {
  explicit SuspendDatabase(utils::UUID uuid) : uuid_{uuid} {}

  void DoDurability() override { /* COLD marker persisted during dbms execution (C9) */ }

  bool ShouldReplicateInCommunity() const override { return false; }

  bool DoReplication(replication::ReplicationClient & /*client*/, const utils::UUID & /*main_uuid*/,
                     memgraph::system::Transaction const & /*txn*/) const override {
    (void)uuid_;  // C7 replaces this stub with:
    // client.StreamAndFinalizeDelta<storage::replication::SuspendDatabaseRpc>(check, main_uuid,
    //     txn.last_committed_system_timestamp(), txn.timestamp(), uuid_);
    return true;
  }

  void PostReplication(replication::RoleMainData & /*mainData*/) const override {}

 private:
  utils::UUID uuid_;  // tenant id; consumed by the C7 replica wire
};

// Hot/cold RESUME as a system action. See SuspendDatabase above. The replica resumes from its own
// on-disk artifacts by UUID; the wire + apply handler land in C7.
struct ResumeDatabase : memgraph::system::ISystemAction {
  explicit ResumeDatabase(utils::UUID uuid) : uuid_{uuid} {}

  void DoDurability() override { /* COLD marker cleared during dbms execution (C9) */ }

  bool ShouldReplicateInCommunity() const override { return false; }

  bool DoReplication(replication::ReplicationClient & /*client*/, const utils::UUID & /*main_uuid*/,
                     memgraph::system::Transaction const & /*txn*/) const override {
    (void)uuid_;  // C7 replaces this stub with:
    // client.StreamAndFinalizeDelta<storage::replication::ResumeDatabaseRpc>(check, main_uuid,
    //     txn.last_committed_system_timestamp(), txn.timestamp(), uuid_);
    return true;
  }

  void PostReplication(replication::RoleMainData & /*mainData*/) const override {}

 private:
  utils::UUID uuid_;  // tenant id; consumed by the C7 replica wire
};

DbmsHandler::SuspendResult DbmsHandler::Suspend_(std::string_view name, system::Transaction *txn) {
  if (name == kDefaultDB) return std::unexpected{SuspendError::DEFAULT_DB};

  SuspendedEntry entry;
  utils::Gatekeeper<Database> *gk = nullptr;
  std::optional<DatabaseAccess> acc;
  {
    // PHASE A — eligibility check + metadata capture + freeze, all under the shared lock_.
    //
    // SU-1 / BUG #1 FIX: try_begin_suspend() is called INSIDE this shared_lock scope (not after).
    // Rationale: between releasing the shared lock and try_begin_suspend(), a concurrent
    // DROP DATABASE (Delete -> DeferDelete) sees state HOT, moves `gk` out of the map, and
    // erases it -> gk dangling -> heap-UAF on try_begin_suspend(). By calling
    // try_begin_suspend() under the shared lock, the state transitions to SUSPENDING while
    // lock_ is held; a concurrent Drop then acquires the exclusive lock_ (mutually exclusive
    // with our shared lock), sees SUSPENDING, and returns DeleteError::USING without touching
    // `gk` — so `gk` remains valid through the lock-free Phase B/C below.
    //
    // NOTE: try_begin_suspend() waits up to 100ms for count==1. Holding the shared lock_
    // during that wait is acceptable: suspend is rare/background; accessor release and
    // access() use the gatekeeper's own mutex, not lock_, so count can still drain; only
    // exclusive-lock writers (Drop/New) briefly wait on lock_.
    auto rd = std::shared_lock{lock_};
    auto a = db_handler_.Get(name);  // nullopt if absent OR not HOT (already cold)
    if (!a) return std::unexpected{SuspendError::NON_EXISTENT};
    auto *db = a->get();
    auto *st = db->storage();
    // SU-3: Suspend requires {periodic snapshot + WAL} durability AND a RUNTIME
    // storage mode of IN_MEMORY_TRANSACTIONAL. The runtime-mode check is load-bearing and must NOT
    // be replaced by the config: a tenant switched to IN_MEMORY_ANALYTICAL at runtime suppresses WAL
    // (InitializeWalFile() returns false for analytical) and pauses the snapshot runner, yet
    // IsDurabilityCompleteForSuspend() only inspects the creation-time config.
    // Rejecting anything not in-memory transactional covers both ON_DISK_TRANSACTIONAL and
    // IN_MEMORY_ANALYTICAL.
    if (st->GetStorageMode() != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      return std::unexpected{SuspendError::NOT_IN_MEMORY};
    }
    if (!st->IsDurabilityCompleteForSuspend()) return std::unexpected{SuspendError::DURABILITY_INCOMPLETE};
    if (st->IsReplicationParticipant()) return std::unexpected{SuspendError::REPLICATING};

    entry.salient = db->config().salient;
    entry.rel_dir = std::filesystem::relative(db->config().durability.storage_directory,
                                              default_config_.durability.storage_directory);
    entry.last_used_ns = db->LastUsedNs();

    gk = db_handler_.GetGatekeeper(name);  // stable pointer to the in-map gatekeeper
    acc = std::move(*a);                   // hold the accessor across phases (count includes it)

    // SU-1 / BUG #1 FIX: transition HOT -> SUSPENDING under the shared lock (LAST step in Phase A).
    if (!gk->try_begin_suspend()) return std::unexpected{SuspendError::ACTIVE_CONNECTIONS};
  }
  // State is now SUSPENDING. Concurrent Drop sees SUSPENDING and returns USING without
  // erasing gk, so gk is valid for the rest of this function (lock-free Phase B/C).

  // SU-2 / BUG #5 FIX: RAII rollback guard. If anything in the SUSPENDING window throws (or we
  // return early on the repl re-check path below), abort_suspend() restores HOT so the
  // gatekeeper is not permanently stuck SUSPENDING (which would hang ~Gatekeeper).
  auto rollback = utils::OnScopeExit{[&] { gk->abort_suspend(); }};

  // PHASE B — post-freeze checks (lock-free; gk is SUSPENDING so Drop is rejected).

  // SU-3 continued: Post-freeze replication re-check (a RegisterReplica may have committed
  // in the A->freeze window). On REPLICATING the rollback guard runs abort_suspend() automatically.
  if ((*acc)->storage()->IsReplicationParticipant()) {
    return std::unexpected{SuspendError::REPLICATING};
  }

  // SU-4 / A7: Consolidating snapshot at suspend. The tenant is frozen (no in-flight txns), so
  // this captures the full committed state. On resume, recovery loads this snapshot and skips WAL
  // files at/below its durable timestamp -> near-zero WAL replay (fast reheat).
  if (auto *inmem = dynamic_cast<storage::InMemoryStorage *>((*acc)->storage())) {
    using CreateSnapshotError = storage::InMemoryStorage::CreateSnapshotError;
    // A7: the periodic snapshot scheduler is NOT paused by the freeze, so a periodic
    // snapshot can be mid-flight -> CreateSnapshot returns AlreadyRunning. Briefly wait it out:
    // once it finishes our call writes the small frozen-state delta or returns NothingNewToWrite.
    constexpr auto kSnapPollStep = std::chrono::milliseconds(10);
    constexpr auto kSnapWaitTimeout = std::chrono::seconds(10);
    auto snap = inmem->CreateSnapshot(/*force=*/false, "suspend");
    if (!snap.has_value() && snap.error() == CreateSnapshotError::AlreadyRunning) {
      const auto snap_deadline = std::chrono::steady_clock::now() + kSnapWaitTimeout;
      while (!snap.has_value() && snap.error() == CreateSnapshotError::AlreadyRunning &&
             std::chrono::steady_clock::now() < snap_deadline) {
        std::this_thread::sleep_for(kSnapPollStep);
        snap = inmem->CreateSnapshot(/*force=*/false, "suspend");
      }
    }
    if (!snap.has_value() && snap.error() != CreateSnapshotError::NothingNewToWrite) {
      spdlog::warn(
          "hot/cold suspend: consolidating snapshot for '{}' was not written ({}); durability "
          "intact via WAL, resume will replay the WAL delta.",
          name,
          storage::InMemoryStorage::CreateSnapshotErrorToString(snap.error()));
    }
    // Disable exit snapshot: we just took the consolidating one; dtor must NOT take another.
    inmem->DisableExitSnapshot();
  }

  // SU-5: Capture heartbeat metadata POST-freeze (count==1, no concurrent commit can advance
  // ldt/epoch between this read and teardown).
  {
    auto *st = (*acc)->storage();
    auto const commit_info = st->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);
    entry.last_durable_timestamp = commit_info.ldt_;
    entry.num_committed_txns = commit_info.num_committed_txns_;
    entry.last_epoch = st->repl_storage_state_.LastEpochWithCommit(commit_info.ldt_);
    // P4 — last-hot stats snapshot (for C7 replication / coordinator history).
    entry.cold_stats = st->GetBaseInfo();
  }

  // PHASE C — record metadata (lock_), then heavy teardown OUTSIDE lock_.
  const auto tenant_uuid = entry.salient.uuid;  // capture before `entry` is moved into suspended_
  acc.reset();                                  // drop our accessor -> count == 0 (state still SUSPENDING)
  {
    auto wr = std::lock_guard{lock_};
    suspended_.insert_or_assign(std::string{name}, std::move(entry));
    // NOTE: durable COLD marker persisted in C9 (cross-restart durability commit).
  }

  // SU-2: Disable the rollback guard BEFORE calling finish_suspend(). finish_suspend() transitions
  // SUSPENDING -> COLD; if the guard fired afterwards it would abort_suspend() on a non-SUSPENDING
  // state and trip the debug assert.
  rollback.Disable();

  // OUTSIDE lock_: value_.reset() -> ~Database (stop threads, FinalizeWal, NO exit snapshot).
  // The COLD shell stays in db_handler_ so a later resume can move-assign a fresh gatekeeper.
  gk->finish_suspend();

  // Record the system action so the suspend is ordered + replicated like CREATE/DROP DATABASE.
  // Done only after the local teardown commits; the replica wire is filled in C7.
  if (txn) txn->AddAction<SuspendDatabase>(tenant_uuid);

  spdlog::info("hot/cold: tenant '{}' suspended (HOT -> COLD)", name);
  return {};
}

DbmsHandler::ResumeResult DbmsHandler::Resume_(std::string_view name, bool rewire_replication,
                                               system::Transaction *txn) {
  // Outer loop: a loser that observes a COLD-fallback (the winner aborted) restarts the whole
  // attempt from Phase A. Every restart re-initializes gk/entry/won_resume from the map under a
  // fresh shared lock. All early-exit paths (NON_EXISTENT, already-HOT, timeout, build failure)
  // are `return`s; only the COLD-fallback path `continue`s.
  while (true) {
    utils::Gatekeeper<Database> *gk = nullptr;
    SuspendedEntry entry;
    bool won_resume = false;
    {
      // PHASE A — decide + acquire the single-flight token, all under the shared lock_.
      //
      // begin_resume() is called INSIDE this shared-lock scope (not after): between releasing the
      // lock and begin_resume(), a concurrent DROP of a COLD tenant (under exclusive lock_) could
      // erase the COLD shell, leaving `gk` dangling -> begin_resume() would be a UAF. By transitioning
      // COLD -> RESUMING while lock_ is held, a concurrent Drop then takes the exclusive lock, sees
      // RESUMING, and refuses to erase — so `gk` stays valid through the slow off-lock build/publish.
      auto rd = std::shared_lock{lock_};
      if (auto a = db_handler_.Get(name)) return std::move(*a);  // already HOT (raced) — share its accessor
      gk = db_handler_.GetGatekeeper(name);                      // stable pointer to the in-map gatekeeper
      if (!gk) return std::unexpected{ResumeError::NON_EXISTENT};
      auto it = suspended_.find(name);
      if (it == suspended_.end()) return std::unexpected{ResumeError::NON_EXISTENT};
      entry = it->second;               // copy rebuild metadata (salient + rel_dir) for the off-lock build
      won_resume = gk->begin_resume();  // COLD -> RESUMING (LAST step in Phase A)
    }

    // SINGLE-FLIGHT — loser path: someone else holds RESUMING. Poll Get(name) until HOT, bounded.
    if (!won_resume) {
      constexpr auto kPollStep = std::chrono::milliseconds(5);
      constexpr auto kPollTimeout = std::chrono::seconds(10);
      const auto deadline = std::chrono::steady_clock::now() + kPollTimeout;
      while (std::chrono::steady_clock::now() < deadline) {
        bool retry_after_cold_fallback = false;
        {
          auto rd = std::shared_lock{lock_};
          if (auto a = db_handler_.Get(name)) return std::move(*a);  // winner published HOT
          // Re-fetch the gatekeeper by name under THIS shared lock rather than dereferencing the raw
          // `gk` captured in Phase A: a concurrent DROP of the (now-COLD) tenant during a sleep window
          // can free the GKInternals `gk` points at. nullptr (dropped) or COLD => treat as a fallback
          // and restart Phase A (which then observes NON_EXISTENT and returns cleanly). We must NOT
          // restart while holding this shared lock (the winner path takes the exclusive lock_ on the
          // same thread -> self-deadlock on the PREFER_READER rwlock): set a flag, drop the lock, then
          // continue the outer loop.
          auto *live_gk = db_handler_.GetGatekeeper(name);
          if (!live_gk || live_gk->state() == utils::GatekeeperState::COLD) retry_after_cold_fallback = true;
        }
        if (retry_after_cold_fallback) break;  // exit poll loop; outer `continue` restarts Phase A
        std::this_thread::sleep_for(kPollStep);
      }
      {
        auto rd = std::shared_lock{lock_};
        auto *live_gk = db_handler_.GetGatekeeper(name);
        if (!live_gk) return std::unexpected{ResumeError::NON_EXISTENT};  // dropped mid-resume
        if (live_gk->state() != utils::GatekeeperState::COLD) {
          // Still RESUMING (timed out) or now HOT (raced just now — check once more).
          if (auto a = db_handler_.Get(name)) return std::move(*a);
          return std::unexpected{ResumeError::RECOVERY_FAILED};
        }
      }
      continue;  // COLD-fallback confirmed (lock released): restart Phase A
    }

    // WE are the winner (state RESUMING). Build OFF the map and recover (SLOW, NO lock_ held).
    // `fresh` lives in this scope so that, on the throw path, the catch can release `acc` BEFORE
    // `fresh` destructs: ~Gatekeeper waits for count == 0, so an outstanding `acc` would self-deadlock
    // the fresh gatekeeper's destruction.
    try {
      storage::Config cfg = default_config_;
      cfg.salient = entry.salient;
      cfg.durability.recover_on_startup = true;
      storage::UpdatePaths(cfg, default_config_.durability.storage_directory / entry.rel_dir);
      auto fresh = db_handler_.BuildDetached(std::move(cfg));  // Database ctor recovers from disk
      DatabaseAccess acc = fresh.access().value();             // fresh gk is HOT => has_value
      try {
        if (on_resume_) on_resume_(acc);  // PRE-PUBLISH arm (triggers/streams/TTL re-arm)
        {
          // PUBLISH — move-assign the fresh HOT gatekeeper over the RESUMING shell, under lock_.
          // `acc` survives the move (it points at fresh's pimpl, which the unique_ptr move transfers
          // into the shell). The shell is RESUMING (access() refuses) and we hold lock_, so no
          // concurrent accessor observes the half-moved state. The erase uses a transparent find +
          // erase(iterator) — both nothrow — so the publish block cannot throw after the noexcept
          // move commits the gatekeeper to HOT.
          auto wr = std::unique_lock{lock_};
          *gk = std::move(fresh);
          if (auto it = suspended_.find(name); it != suspended_.end()) suspended_.erase(it);
        }
      } catch (...) {
        // PRE-PUBLISH failure: on_resume_ threw and the publish has NOT happened — `fresh` is still
        // local and HOT. Release `acc` BEFORE `fresh` unwinds so fresh's ~Gatekeeper sees count == 0
        // instead of self-deadlocking on the outstanding accessor, then roll back to COLD.
        acc.reset();
        gk->abort_resume();  // RESUMING -> COLD; suspended_ untouched => retriable
        return std::unexpected{ResumeError::RECOVERY_FAILED};
      }
      // POST-PUBLISH arm (replication). The tenant is ALREADY published HOT and erased from
      // suspended_; we must NOT abort_resume() here (the gatekeeper is no longer RESUMING). The arm
      // is independently retriable, so on failure we log and return the live accessor. Skipped when
      // rewire_replication == false (the replica replication-apply caller holds the repl_state read
      // lock and on_resume_repl_ would re-take it as a write lock -> self-deadlock). Wired in a later
      // (replication) commit; on_resume_repl_ is empty by default here, so this is a no-op.
      try {
        if (rewire_replication && on_resume_repl_) on_resume_repl_(acc);
      } catch (...) {
        spdlog::warn(
            "hot/cold resume: replication re-wiring for tenant '{}' threw; tenant is live (HOT) and data is "
            "intact, but it may run un-replicated until replication is re-established.",
            name);
      }
      // Record the system action so the resume is ordered + replicated like CREATE/DROP DATABASE.
      // Reached only on the winner's successful publish; the replica wire is filled in C7.
      if (txn) txn->AddAction<ResumeDatabase>(entry.salient.uuid);

      spdlog::info("hot/cold: tenant '{}' resumed (COLD -> HOT)", name);
      return acc;
    } catch (...) {
      // Recovery (BuildDetached) threw before `acc`/`fresh` existed. No live accessor to release.
      gk->abort_resume();  // RESUMING -> COLD; suspended_ untouched => retriable
      return std::unexpected{ResumeError::RECOVERY_FAILED};
    }
  }  // end outer while(true) — all winner exits are `return`, so falling out is unreachable
}
#endif  // MG_ENTERPRISE

}  // namespace memgraph::dbms
