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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "dbms/constants.hpp"
#include "dbms/global.hpp"
#include "dbms/rpc.hpp"
#include "flags/experimental.hpp"
#include "flags/memory_limit.hpp"
#include "flags/run_time_configurable.hpp"
#include "license/license.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/stat.hpp"
#include "utils/synchronized.hpp"
#include "utils/timer.hpp"
#include "utils/uuid.hpp"

#include "metrics/prometheus_metrics.hpp"

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

DbmsHandler::DbmsHandler(storage::Config config)
    : default_config_{std::move(config)},
      // Resume executor width from the flag (>= 1). resume_pool_ is the LAST member, so it is the
      // FIRST destroyed (reverse-order) — stopping+joining in-flight resume jobs before db_handler_
      // (which they hold raw gatekeeper pointers into) is torn down.
      resume_pool_{std::max<size_t>(1, FLAGS_storage_hot_cold_max_concurrent_resumes)} {
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

  // State-aware dispatch (hot/cold tenants). A COLD tenant has no readable config (GetConfig
  // returns nullopt), so route it to the cold-drop path before the hot path below.
  if (auto *gk = db_handler_.GetGatekeeper(db_name)) {
    switch (gk->state()) {
      case utils::GatekeeperState::HOT:
        break;  // fall through to the existing hot path
      case utils::GatekeeperState::COLD:
        return DropCold_(db_name, transaction);
      case utils::GatekeeperState::SUSPENDING:
      case utils::GatekeeperState::RESUMING:
        // Transitional state — reject as retryable ("database busy, try again"). We reuse the
        // existing DeleteError::USING rather than adding a dedicated TRANSITIONING value: adding a
        // DeleteError enumerator would break the interpreter's no-default switch on DeleteError and
        // stop commit 6 from building standalone. A dedicated TRANSITIONING error is a later
        // refinement.
        return std::unexpected{DeleteError::USING};
    }
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

  // State-aware dispatch (hot/cold tenants). See TryDelete for rationale on reusing DeleteError::USING
  // for the transitional (SUSPENDING/RESUMING) states.
  if (db_name != kDefaultDB) {
    if (auto *gk = db_handler_.GetGatekeeper(db_name)) {
      switch (gk->state()) {
        case utils::GatekeeperState::HOT:
          break;  // fall through to the existing hot path
        case utils::GatekeeperState::COLD:
          return DropCold_(db_name, transaction);
        case utils::GatekeeperState::SUSPENDING:
        case utils::GatekeeperState::RESUMING:
          return std::unexpected{DeleteError::USING};
      }
    }
  }

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

  // State-aware dispatch (hot/cold tenants). No system transaction in this overload -> pass nullptr.
  if (db_name != kDefaultDB) {
    if (auto *gk = db_handler_.GetGatekeeper(db_name)) {
      switch (gk->state()) {
        case utils::GatekeeperState::HOT:
          break;  // fall through to Delete_ (hot path)
        case utils::GatekeeperState::COLD:
          return DropCold_(db_name, /*txn=*/nullptr);
        case utils::GatekeeperState::SUSPENDING:
        case utils::GatekeeperState::RESUMING:
          return std::unexpected{DeleteError::USING};
      }
    }
  }

  return Delete_(db_name);
}

DbmsHandler::DeleteResult DbmsHandler::Delete(utils::UUID uuid) {
  auto wr = std::lock_guard(lock_);
  std::string db_name;
  try {
    // NOTE: cold-drop-by-uuid is intentionally NOT supported here. Get_(uuid) resolves the name by
    // iterating the map and minting accessors; a COLD shell mints none (access() == nullopt in
    // non-HOT states), so a COLD tenant is not reachable by uuid through this overload. That is
    // acceptable for now: drop-by-uuid is the replication-only path, and a COLD tenant is
    // non-replicated by construction (Suspend_ rejects replication participants). Left as-is; the
    // name-based TryDelete / Delete(name, txn) carry the cold dispatch.
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

  // State-aware guard (hot/cold tenants): only a HOT tenant can be renamed. A COLD shell has no
  // live storage and is not re-fetchable via db_handler_.Get(new_name) after the rename, which
  // would trip the MG_ASSERT(new_db, ...) below (crash) and leave a dangling gatekeeper pointer in
  // the suspended_ rebuild metadata (its name no longer matches the map key). Reject non-HOT states
  // as retryable using the existing RenameError::USING ("cannot rename, try again") — adding a
  // dedicated enumerator would break the interpreter's no-default switch on RenameError. A dedicated
  // TRANSITIONING error is a later refinement.
  if (auto *gk = db_handler_.GetGatekeeper(old_name); gk && gk->state() != utils::GatekeeperState::HOT) {
    return std::unexpected{RenameError::USING};
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
    // For COLD/RESUMING tenants the in-memory apply is a no-op here; Resume_ re-applies the
    // durable profile limit on the next resume (FIX 1). No throwing Get() needed.
    ApplyTenantMemoryLimitIfHot_(db_name, memory_limit);
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
  // Durable attach FIRST: on a COLD tenant the previous Get(db_name) threw UnknownDatabaseException
  // before the profile was durably attached, meaning COLD tenants could never have a profile set.
  // AttachToDatabase is a KVStore op independent of any DatabaseAccess.
  auto result = tenant_profiles_->AttachToDatabase(profile_name, db_name);
  if (!result) return std::unexpected{result.error()};
  // Apply limit in-memory only if the tenant is currently HOT. For COLD/RESUMING tenants this is a
  // no-op; Resume_ re-applies the durable profile on next resume (FIX 1).
  ApplyTenantMemoryLimitIfHot_(db_name, *result);
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
  // Reset limit in-memory only if the tenant is currently HOT. For COLD/RESUMING tenants this is a
  // no-op; the detach is already durable and Resume_ will see no profile attached (limit == 0).
  ApplyTenantMemoryLimitIfHot_(db_name, 0);
  if (sys_txn) {
    sys_txn->AddAction<TenantProfileAction>(
        TenantProfileAction::Action::REMOVE_FROM_DATABASE, "", db_name, kUnusedMemoryLimit);
  }
  return {};
}

void DbmsHandler::ApplyTenantMemoryLimitIfHot_(std::string_view db_name, int64_t limit) {
  auto rd = std::shared_lock{lock_};
  if (auto *gk = db_handler_.GetGatekeeper(db_name)) {
    if (auto acc = gk->access()) {  // nullopt unless HOT
      acc->get()->SetTenantMemoryLimit(limit);
    }
  }
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
    // Disable any pending exit snapshot so the Database dtor does NOT write a full snapshot
    // that remove_all (called from the DeferDelete callback below) would immediately delete.
    // Idempotent: calling it on a live storage just clears the exit_snapshot_enabled_ flag.
    // Both the suspend path (Suspend_) and the hot-DROP path (this function) now disable the
    // exit snapshot before teardown.
    database.storage()->DisableExitSnapshot();
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

DbmsHandler::DeleteResult DbmsHandler::DropCold_(std::string_view db_name, system::Transaction *txn) {
  // CALLER HOLDS lock_ (exclusive). Drop a COLD (suspended) tenant: its config is NOT readable via
  // GetConfig() (nullopt for COLD), so uuid + path come from the suspended_ rebuild metadata.
  auto it = suspended_.find(db_name);
  if (it == suspended_.end()) {
    // Gatekeeper said COLD but no rebuild metadata — treat as non-existent (not actually cold).
    return std::unexpected{DeleteError::NON_EXISTENT};
  }
  const auto uuid = it->second.salient.uuid;
  const auto path = default_config_.durability.storage_directory / it->second.rel_dir;

  // Erase the COLD gatekeeper shell. ~Gatekeeper waits for TERMINAL state (COLD qualifies) AND
  // count == 0 (a cold shell has none), so this returns promptly without quiescing any live tenant.
  if (!db_handler_.Erase(db_name)) {
    return std::unexpected{DeleteError::NON_EXISTENT};
  }

  // Drop the rebuild metadata IMMEDIATELY after Erase, before the durability/disk ops below.
  // Rationale: if durability_->Delete throws after the gatekeeper shell is gone but while the
  // suspended_ entry still exists, Contains(db_name) would lie (report the tenant present) until
  // restart. db_handler_.Erase touches db_handler_.items_, not suspended_, so the iterator `it`
  // is still valid here; uuid/path were already copied above, so nothing below needs `it`.
  suspended_.erase(it);
  metrics::Metrics().global.hot_cold_suspended_tenants->Set(static_cast<double>(suspended_.size()));

  // Remove from durability list (KVStore).
  if (durability_) durability_->Delete(Durability::GenKey(db_name));

  // Delete disk storage.
  std::error_code ec;
  (void)std::filesystem::remove_all(path, ec);
  if (ec) {
    spdlog::error(R"(Failed to clean disk while deleting cold database "{}" stored in {})", db_name, path);
  }

  // Detach from tenant profile (no-op if not attached).
  if (tenant_profiles_) {
    [[maybe_unused]] auto detached = tenant_profiles_->DetachFromDatabase(db_name);
  }

  // Save delta (system transaction scope).
  if (txn) {
    txn->AddAction<DropDatabase>(uuid);
  }

  return {};  // Success
}

DbmsHandler::SuspendResult DbmsHandler::Suspend_(std::string_view name) {
  if (name == kDefaultDB) return std::unexpected{SuspendError::DEFAULT_DB};
  // NOTE: a REPLICA may suspend tenants independently under its own memory pressure. This is safe
  // because incoming replication traffic reheats a COLD tenant on demand (data/recovery RPCs resume
  // it via GetDatabaseAccessorOrResume; HeartbeatRpc answers from suspend-time metadata without
  // reheating). There is therefore no role gate here anymore.

  SuspendedEntry entry;
  utils::Gatekeeper<Database> *gk = nullptr;
  std::optional<DatabaseAccess> acc;
  {
    // PHASE A — eligibility check + metadata capture + freeze, all under the shared lock_.
    //
    // BUG #1 FIX: try_begin_suspend() is called INSIDE this shared_lock scope (not after).
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
    // Suspend requires {periodic snapshot + WAL} durability (checked just below) AND a RUNTIME
    // storage mode of IN_MEMORY_TRANSACTIONAL. The runtime-mode check is load-bearing and must NOT
    // be replaced by the config: a tenant switched to IN_MEMORY_ANALYTICAL at runtime suppresses WAL
    // (InitializeWalFile() returns false for analytical) and pauses the snapshot runner, yet
    // IsDurabilityCompleteForSuspend() only inspects the creation-time config.durability.snapshot_wal_mode
    // (which SetStorageMode never updates). Without this check, analytical-mode commits would pass the
    // durability gate and be SILENTLY LOST when the storage is torn down on suspend. Rejecting anything
    // that is not in-memory transactional covers both ON_DISK_TRANSACTIONAL and IN_MEMORY_ANALYTICAL.
    if (st->GetStorageMode() != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      return std::unexpected{SuspendError::NOT_IN_MEMORY};
    }
    if (!st->IsDurabilityCompleteForSuspend()) return std::unexpected{SuspendError::DURABILITY_INCOMPLETE};
    if (st->IsReplicationParticipant()) return std::unexpected{SuspendError::REPLICATING};

    // Min-hot-residency debounce: tenant must have stayed hot long enough since last use.
    const auto min_ns = static_cast<int64_t>(FLAGS_storage_hot_cold_min_hot_residency_sec) * 1'000'000'000LL;
    const auto now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
    if (db->LastUsedNs() + min_ns > now_ns) return std::unexpected{SuspendError::MIN_RESIDENCY};

    entry.salient = db->config().salient;
    entry.rel_dir = std::filesystem::relative(db->config().durability.storage_directory,
                                              default_config_.durability.storage_directory);
    entry.last_used_ns = db->LastUsedNs();

    gk = db_handler_.GetGatekeeper(name);  // stable pointer to the in-map gatekeeper
    acc = std::move(*a);                   // hold the accessor across phases (count includes it)

    // BUG #1 FIX: transition HOT -> SUSPENDING under the shared lock (LAST step in Phase A).
    // On ACTIVE_CONNECTIONS the lock releases on scope exit — no transition happened, gk
    // was not erased (Drop/New wait for exclusive lock_), so the raw pointer is not used
    // again after the early return — safe.
    if (!gk->try_begin_suspend()) return std::unexpected{SuspendError::ACTIVE_CONNECTIONS};
  }
  // State is now SUSPENDING. Concurrent Drop sees SUSPENDING and returns USING without
  // erasing gk, so gk is valid for the rest of this function (lock-free Phase B/C).

  // BUG #5 FIX: RAII rollback guard. If anything in the SUSPENDING window throws (or we
  // return early on the repl re-check path below), abort_suspend() restores HOT so the
  // gatekeeper is not permanently stuck SUSPENDING (which would hang ~Gatekeeper).
  auto rollback = utils::OnScopeExit{[&] { gk->abort_suspend(); }};

  // PHASE B — post-freeze checks (lock-free; gk is SUSPENDING so Drop is rejected).

  // Post-freeze replication re-check (a RegisterReplica may have committed in the A->freeze window).
  // On REPLICATING the rollback guard runs abort_suspend() automatically — do NOT call it manually.
  if ((*acc)->storage()->IsReplicationParticipant()) {
    return std::unexpected{SuspendError::REPLICATING};
  }

  // Consolidating snapshot at suspend: the tenant is frozen (no in-flight txns), so this captures the
  // full committed state. On resume, recovery loads this snapshot and skips WAL files at/below its
  // durable timestamp -> near-zero WAL replay (fast reheat). Best-effort: on failure the WAL is still
  // finalized in the dtor (durability intact), reheat just replays the WAL delta. CreateSnapshot(force=
  // false) returns NothingNewToWrite (no I/O) when nothing changed since the last periodic snapshot.
  if (auto *inmem = dynamic_cast<storage::InMemoryStorage *>((*acc)->storage())) {
    if (auto snap = inmem->CreateSnapshot(/*force=*/false, "suspend"); !snap.has_value()) {
      spdlog::warn(
          "hot/cold suspend: consolidating snapshot for '{}' was not written ({}); durability "
          "intact via WAL, resume will replay the WAL delta.",
          name,
          storage::InMemoryStorage::CreateSnapshotErrorToString(snap.error()));
    }
  }
  // The Database dtor must NOT take an exit snapshot; abort any in-flight one.
  // We just took the consolidating one above — a second exit snapshot from the dtor is unnecessary.
  (*acc)->storage()->DisableExitSnapshot();

  // Capture heartbeat metadata for the COLD-tenant heartbeat answer (replica path), so a suspended
  // tenant answers HeartbeatRpc from this snapshot without reheating. Done HERE (post-freeze) rather
  // than in Phase A: the tenant is now SUSPENDING with count == 1 (try_begin_suspend drained all
  // other accessors and access() refuses new ones), so no concurrent commit can advance ldt/epoch
  // between this read and teardown — the captured values are exactly the last durable state. Uses
  // the same ReplicationStorageState::LastEpochWithCommit the live HeartbeatHandler uses.
  {
    auto *st = (*acc)->storage();
    auto const commit_info = st->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);
    entry.last_durable_timestamp = commit_info.ldt_;
    entry.num_committed_txns = commit_info.num_committed_txns_;
    entry.last_epoch = st->repl_storage_state_.LastEpochWithCommit(commit_info.ldt_);
  }

  // PHASE C — record metadata (lock_), then heavy teardown OUTSIDE lock_.
  acc.reset();  // drop our accessor -> count == 0 (state still SUSPENDING)
  {
    auto wr = std::lock_guard{lock_};
    suspended_.insert_or_assign(std::string{name}, std::move(entry));
    // Metrics: the tenant is now committed to suspended_ (the suspend is effectively done — the
    // following finish_suspend() is non-throwing). Bump the counter here, under lock_, rather than
    // after finish_suspend(): if finish_suspend() were to throw, the entry is already in suspended_
    // and the gauge below already reflects it, so the counter must agree. NOTE on the gauge: it is a
    // process-global singleton; production runs exactly one DbmsHandler, so Set(suspended_.size())
    // is the authoritative cold count (the multi-handler case exists only in unit tests).
    metrics::Metrics().global.hot_cold_suspends_total->Increment();
    metrics::Metrics().global.hot_cold_suspended_tenants->Set(static_cast<double>(suspended_.size()));
    // NOTE: insert_or_assign may throw bad_alloc (exactly under the memory pressure that
    // triggers eviction). If it throws here, the rollback guard above calls abort_suspend()
    // before propagating, preventing a permanently-stuck SUSPENDING state.
  }

  // Disable the rollback guard BEFORE calling finish_suspend(). finish_suspend() transitions
  // SUSPENDING -> COLD; if the guard fired afterwards it would abort_suspend() on a non-SUSPENDING
  // state and trip the debug assert.
  rollback.Disable();

  // OUTSIDE lock_: value_.reset() -> ~Database (stop threads, FinalizeWal, NO exit snapshot).
  // The COLD shell stays in db_handler_ so a later resume can move-assign a fresh gatekeeper.
  gk->finish_suspend();
  return {};
}

int64_t DbmsHandler::SuspendColdestIdleTenants(int64_t bytes_to_free, uint64_t max_evictions,
                                               std::string_view exclude) {
#ifdef MG_ENTERPRISE
  if (max_evictions == 0 || bytes_to_free <= 0) return 0;

  struct Candidate {
    std::string name;
    int64_t last_used_ns;
    int64_t bytes;
  };

  std::vector<Candidate> candidates;
  {
    // Snapshot eligible HOT tenants under a shared lock. Do NOT call Suspend_ here — it takes lock_.
    auto rd = std::shared_lock{lock_};
    for (auto &[name, db_gk] : db_handler_) {
      if (name == kDefaultDB) continue;
      if (!exclude.empty() && name == exclude) continue;  // e.g. the just-resumed tenant (held by the resume thread)
      if (db_gk.state() != utils::GatekeeperState::HOT) continue;
      auto acc = db_gk.access();  // transient; dropped at end of iteration
      if (!acc) continue;         // raced out of HOT
      candidates.push_back({std::string{name}, (*acc)->LastUsedNs(), (*acc)->DbMemoryUsage()});
    }
  }
  // Coldest first = smallest last_used_ns.
  std::ranges::sort(candidates, [](const auto &a, const auto &b) { return a.last_used_ns < b.last_used_ns; });
  int64_t freed = 0;
  uint64_t evicted = 0;
  for (const auto &c : candidates) {
    if (evicted >= max_evictions || freed >= bytes_to_free) break;
    if (Suspend_(c.name).has_value()) {
      freed += c.bytes;
      ++evicted;
      spdlog::info("Hot/cold eviction: suspended idle tenant \"{}\" (~{} bytes).", c.name, c.bytes);
      metrics::Metrics().global.hot_cold_evictions_total->Increment();
    }
    // On failure (active connections / min-residency / etc.) skip and try the next-coldest.
  }
  return freed;
#else
  (void)bytes_to_free;
  (void)max_evictions;
  (void)exclude;
  return 0;
#endif
}

int64_t DbmsHandler::EvictForMemoryPressure(std::string_view exclude) {
#ifdef MG_ENTERPRISE
  // Self-gating so the periodic HC-Evict scheduler and the make-room-on-resume path share ONE policy.
  // Cheap early-outs first (this is called on every resume, so it must be ~free when eviction is off).
  if (!FLAGS_storage_hot_cold_eviction_enabled) return 0;
  if (!flags::AreExperimentsEnabled(flags::Experiments::HOT_COLD_TENANTS)) return 0;
  // Eviction acts on non-default tenants (multi-tenancy = enterprise). Re-check each call so eviction
  // goes dormant if the license lapses and self-resumes when it returns.
  //
  // TODO(hot-cold): a lapsed license stops *new* evictions but strands already-COLD tenants (the only
  // path back is resume-on-cold, itself multi-tenancy-gated). Decide intended behaviour: (a) resume all
  // COLD on license loss; (b) grace-path resume of existing COLD tenants; or (c) document + surface it.
  if (!license::global_license_checker.IsEnterpriseValidFast()) return 0;

  const int64_t limit = flags::GetMemoryLimit();
  if (limit <= 0) return 0;
  // Memory signal depends on the allocator: jemalloc build -> the allocator-hooked tracker is precise
  // (RSS would over-report retained/decaying pages); otherwise -> real resident memory (RSS).
#if USE_JEMALLOC
  const int64_t usage = static_cast<int64_t>(utils::total_memory_tracker.Amount());
#else
  const int64_t usage = static_cast<int64_t>(utils::GetMemoryRES());
#endif
  const int64_t high = (limit / 100) * static_cast<int64_t>(flags::run_time::GetHotColdEvictionHighWatermarkPercent());
  if (usage <= high) return 0;  // below the high watermark — nothing to do
  const int64_t low = (limit / 100) * static_cast<int64_t>(flags::run_time::GetHotColdEvictionLowWatermarkPercent());
  const int64_t to_free = usage - low;  // aim to drop back to the low watermark
  if (to_free <= 0) return 0;
  const int64_t freed = SuspendColdestIdleTenants(to_free, flags::run_time::GetHotColdEvictionMaxPerCycle(), exclude);
  if (freed > 0) {
    spdlog::info(
        "Hot/cold eviction cycle: usage {} > high {}, freed ~{} bytes (target {}).", usage, high, freed, to_free);
  }
  return freed;
#else
  (void)exclude;
  return 0;
#endif
}

DbmsHandler::ResumeResult DbmsHandler::Resume_(std::string_view name, bool rewire_replication, bool make_room) {
  // Outer loop: converts the former tail-recursion (loser sees COLD-fallback -> retry whole
  // function) into an iterative restart.  Each iteration is one full attempt: Phase A acquires
  // the single-flight token; the winner builds and publishes; a loser polls until the winner
  // finishes or times out and, on COLD-fallback, `continue`s the outer loop to retry Phase A
  // from scratch.  All early-exit paths (NON_EXISTENT, already-HOT, timeout, build failure)
  // stay as `return` to exit the function.  `gk`/`entry`/`won_resume` are declared INSIDE the
  // loop so every restart re-initializes them from the map under the fresh shared lock.
  while (true) {
    utils::Gatekeeper<Database> *gk = nullptr;
    SuspendedEntry entry;
    bool won_resume = false;
    {
      // PHASE A — decide + single-flight token, all under the shared lock_.
      //
      // BUG #2 FIX: begin_resume() is called INSIDE this shared_lock scope (not after).
      // Rationale: between releasing the shared lock and begin_resume(), a concurrent
      // DROP DATABASE on a COLD tenant (TryDelete -> DropCold_ -> db_handler_.Erase(name)
      // under exclusive lock_) can erase the COLD shell -> gk dangling -> begin_resume() is
      // a heap-UAF. Also, a concurrent move-assign (*gk = std::move(fresh)) under exclusive
      // lock_ would race a lock-free begin_resume() on the same pimpl_.
      //
      // By calling begin_resume() under the shared lock, the state transitions to RESUMING
      // while lock_ is held; a concurrent Drop then acquires the exclusive lock_ (mutually
      // exclusive with our shared lock), sees RESUMING, and returns DeleteError::USING without
      // erasing gk — so gk stays valid through the slow off-lock build and publish.
      auto rd = std::shared_lock{lock_};
      if (auto a = db_handler_.Get(name)) return std::move(*a);  // already HOT (raced) — return its accessor
      gk = db_handler_.GetGatekeeper(name);                      // stable pointer to the in-map gatekeeper
      if (!gk) return std::unexpected{ResumeError::NON_EXISTENT};
      auto it = suspended_.find(name);
      if (it == suspended_.end()) return std::unexpected{ResumeError::NON_EXISTENT};
      entry = it->second;  // copy metadata for the build (rebuild needs salient + rel_dir)
      // BUG #2 FIX: transition COLD -> RESUMING under the shared lock (LAST step in Phase A).
      won_resume = gk->begin_resume();
    }

    // SINGLE-FLIGHT — poll path for the loser (someone else already holds RESUMING).
    if (!won_resume) {
      // Someone else is RESUMING (or it just went HOT). Poll Get(name) until HOT, bounded.
      // No futures by design: a short sleep loop with a timeout.
      constexpr auto kPollStep = std::chrono::milliseconds(5);
      constexpr auto kPollTimeout = std::chrono::seconds(10);
      const auto deadline = std::chrono::steady_clock::now() + kPollTimeout;
      while (std::chrono::steady_clock::now() < deadline) {
        bool retry_after_cold_fallback = false;
        {
          auto rd = std::shared_lock{lock_};
          if (auto a = db_handler_.Get(name)) return std::move(*a);  // winner published HOT
          // If the winner failed and rolled back to COLD, retry the whole resume from scratch.
          // BUG #1 FIX: re-fetch the gatekeeper by name under THIS shared_lock rather than
          // dereferencing the raw `gk` captured back in Phase A. During the poll's sleep windows
          // a concurrent DROP of the (now-COLD) tenant can run DropCold_ -> db_handler_.Erase(name)
          // and free the GKInternals `gk` points at; reading gk->state() would then be a UAF.
          // GetGatekeeper returns nullptr if the tenant was dropped -> treat as COLD-fallback and
          // restart Phase A, which then observes NON_EXISTENT and returns cleanly.
          // We must NOT recurse here: the winner path takes std::unique_lock{lock_}
          // (write) while we still hold this shared_lock (read) on the SAME thread -> self-deadlock
          // on the PREFER_READER rwlock. Set a flag, break out of the shared_lock scope, and only
          // then restart the outer loop with NO lock held.
          auto *live_gk = db_handler_.GetGatekeeper(name);
          if (!live_gk || live_gk->state() == utils::GatekeeperState::COLD) retry_after_cold_fallback = true;
        }  // <-- shared_lock destroyed here, before the outer-loop restart or unique_lock acquisition.
        if (retry_after_cold_fallback) break;  // exit poll loop; outer `continue` restarts Phase A
        std::this_thread::sleep_for(kPollStep);
      }
      // Did the poll loop exit because of a COLD-fallback (winner aborted, need new attempt)?
      // Re-check: if we timed out without a COLD-fallback, the winner is still alive but slow;
      // give up.
      {
        auto rd = std::shared_lock{lock_};
        // BUG #1 FIX: re-fetch under the lock; the raw `gk` may have been freed by a concurrent
        // DROP of a COLD tenant during the poll's sleep windows.
        auto *live_gk = db_handler_.GetGatekeeper(name);
        if (!live_gk) return std::unexpected{ResumeError::NON_EXISTENT};  // dropped mid-resume
        if (live_gk->state() != utils::GatekeeperState::COLD) {
          // Still RESUMING (timed out) or now HOT (raced just now — check once more).
          if (auto a = db_handler_.Get(name)) return std::move(*a);
          // NOTE: do NOT bump hot_cold_resume_failures_total here. This is the LOSER giving up after
          // waiting > kPollTimeout for another thread's in-flight resume — a wait-timeout, not a
          // recovery failure. The actual recovery-failure paths (below) own that counter; counting
          // here would raise false positives on every slow concurrent resume and double-count when
          // the winner also fails.
          return std::unexpected{ResumeError::RECOVERY_FAILED};
        }
      }
      // COLD-fallback confirmed (with shared_lock released): restart the whole Resume_ logic.
      continue;  // outer while(true) — re-runs Phase A
    }

    // WE are the winner (state RESUMING). Build OFF-map and recover (SLOW, NO lock_).
    // `fresh` is declared in the enclosing scope so that, on the throw path, the catch block can
    // release `acc` BEFORE `fresh` destructs. `~Gatekeeper` waits for count == 0, so an outstanding
    // `acc` (count == 1) would self-deadlock the fresh gatekeeper's destruction.
    // Timer starts here — measures the full winner build + recovery + publish path.
    utils::Timer resume_timer;
    try {
      storage::Config cfg = default_config_;
      cfg.salient = entry.salient;
      cfg.durability.recover_on_startup = true;
      storage::UpdatePaths(cfg, default_config_.durability.storage_directory / entry.rel_dir);
      // BuildDetached runs the Database ctor (which recovers) WITHOUT inserting into the map.
      auto fresh = db_handler_.BuildDetached(std::move(cfg));
      DatabaseAccess acc = fresh.access().value();  // fresh gk is HOT => has_value
      try {
        if (on_resume_) on_resume_(acc);  // PRE-PUBLISH arm (triggers/streams/TTL)
        {
          // PUBLISH — move-assign the fresh HOT gatekeeper over the RESUMING shell.
          // `acc` (raw GKInternals*) survives the move (it points at fresh's pimpl, which the
          // unique_ptr move transfers into the shell). The shell is RESUMING (access() refuses) and
          // we hold lock_, so no concurrent accessor can observe the half-moved state.
          auto wr = std::unique_lock{lock_};
          // Re-apply the durable tenant-profile memory limit: a COLD resume rebuilds the storage via
          // BuildDetached with hard-limit 0 (unlimited), so without this the resumed tenant would run
          // unlimited until process restart. Done under lock_ (held here) so a concurrent profile
          // mutation that skipped its in-memory apply (it saw the tenant RESUMING) cannot leave a stale
          // limit. acc points at the fresh Database (not yet in db_handler_), so SetTenantMemoryLimit
          // applies before the tenant is observable HOT.
          if (tenant_profiles_) {
            if (auto const profile_name = tenant_profiles_->GetProfileForDatabase(name)) {
              if (auto const profile = tenant_profiles_->Get(*profile_name); profile) {
                // Apply unconditionally when a profile is attached: 0 == unlimited must still be
                // asserted on the freshly-rebuilt storage. A `> 0` guard would silently skip an
                // ALTER-to-unlimited that was made durable while the tenant was COLD.
                acc.get()->SetTenantMemoryLimit(profile->memory_limit);
              }
            }
          }
          // BUG #2 FIX: the previous `suspended_.erase(std::string{name})` heap-allocated a
          // temporary std::string that could throw bad_alloc *after* the noexcept move-publish
          // committed the gatekeeper to HOT. The inner catch would then call abort_resume() on a
          // now-HOT gatekeeper (MG_ASSERT failure / silent HOT->COLD corruption + stranded tenant).
          // Erase via transparent find (string_view key, no allocation) + erase(iterator) — both
          // nothrow — so the publish block cannot throw after the move commits.
          *gk = std::move(fresh);
          if (auto it = suspended_.find(name); it != suspended_.end()) suspended_.erase(it);
          metrics::Metrics().global.hot_cold_suspended_tenants->Set(static_cast<double>(suspended_.size()));
        }
      } catch (...) {
        // PRE-PUBLISH failure: on_resume_ (or the publish itself) threw and the publish has NOT
        // happened — `fresh` is still local and HOT. Roll back to COLD. Release `acc` BEFORE `fresh`
        // unwinds: reset it here so fresh's ~Gatekeeper sees count == 0 instead of self-deadlocking
        // on the outstanding accessor.
        acc.reset();
        gk->abort_resume();  // RESUMING -> COLD; suspended_ untouched => retriable
        metrics::Metrics().global.hot_cold_resume_failures_total->Increment();
        return std::unexpected{ResumeError::RECOVERY_FAILED};
      }
      // Publish succeeded: record the resume latency (excludes make-room eviction below) and
      // increment the success counter.
      metrics::Metrics().global.hot_cold_resumes_total->Increment();
      metrics::Metrics().global.hot_cold_resume_latency_seconds->Observe(
          resume_timer.Elapsed<std::chrono::duration<double>>().count());
      // POST-PUBLISH arm (replication). The tenant is ALREADY published HOT and erased from
      // suspended_; we must NOT abort_resume() here (the gatekeeper is no longer RESUMING — that
      // would assert in debug / force the tenant to COLD with no suspended_ entry = lost tenant).
      // Replication re-wiring is independently retriable, so on failure we log and return the live
      // accessor.
      try {
        // Skip on the replica replication-apply path (rewire_replication == false): the caller holds
        // the repl_state read lock and this hook takes the repl_state write lock -> self-deadlock.
        if (rewire_replication && on_resume_repl_) on_resume_repl_(acc);  // POST-PUBLISH arm (replication)
      } catch (...) {
        spdlog::error(
            "Resume: replication re-wiring failed for tenant '{}' after publish; the tenant is live "
            "(HOT) and data is intact, but replication wiring must be retried.",
            name);
      }
      // MAKE-ROOM-ON-RESUME: the tenant is now HOT. If memory is over the high watermark, evict the
      // coldest idle tenants to bound peak after reheating a COLD tenant. `name` is excluded (we still
      // hold `acc`, so the resumed tenant's count is >= 1 and it could not be suspended anyway). No
      // lock_ is held here, and the eviction path (Suspend_) never re-enters Resume_, so there is no
      // recursion. Best-effort: a failure must not fail the resume (the tenant is already live/intact).
      if (make_room) {
        try {
          EvictForMemoryPressure(name);
        } catch (...) {
          spdlog::error("Resume: make-room eviction after reheating tenant '{}' threw; tenant is live (HOT).", name);
        }
      }
      return acc;
    } catch (...) {
      // Recovery (BuildDetached) threw before `acc`/`fresh` existed (or after they were already
      // unwound by the inner catch's rethrow path — but the inner catch does not rethrow). No live
      // accessor to release here.
      gk->abort_resume();  // RESUMING -> COLD; suspended_ untouched => retriable
      metrics::Metrics().global.hot_cold_resume_failures_total->Increment();
      return std::unexpected{ResumeError::RECOVERY_FAILED};
    }
  }  // end outer while(true) — all winner exits are `return`, so this is unreachable but required
}

void DbmsHandler::KickResume(std::string_view name) {
  // Fire-and-forget on the resume executor. Errors are swallowed; the tenant stays COLD + retriable.
  resume_pool_.AddTask([this, key = std::string{name}]() { (void)Resume_(key); });
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

std::optional<DbmsHandler::SuspendedHeartbeatInfo> DbmsHandler::GetSuspendedHeartbeatInfo(
    const utils::UUID &uuid) const {
  auto rd = std::shared_lock{lock_};
  for (auto const &[_, entry] : suspended_) {
    if (entry.salient.uuid == uuid) {
      return SuspendedHeartbeatInfo{entry.last_durable_timestamp, entry.num_committed_txns, entry.last_epoch};
    }
  }
  return std::nullopt;
}

std::optional<DatabaseAccess> DbmsHandler::ResumeByUUID(const utils::UUID &uuid) {
  // Find the COLD tenant's name under the shared lock, then resume by name OUTSIDE the lock.
  // Resume_ takes its own shared_lock; holding ours would self-deadlock on the rwlock.
  std::string name;
  {
    auto rd = std::shared_lock{lock_};
    bool found = false;
    for (auto const &[n, entry] : suspended_) {
      if (entry.salient.uuid == uuid) {
        name = n;
        found = true;
        break;
      }
    }
    if (!found) return std::nullopt;
  }
  // rewire_replication=false: we are on the replication RPC thread which already holds the
  // repl_state read lock; running on_resume_repl_ (which takes the repl_state write lock) here would
  // self-deadlock. The hook is a no-op on a replica anyway.
  auto res = Resume_(name, /*rewire_replication=*/false);  // single-flight; recovers from durability
  if (!res) return std::nullopt;
  return std::optional{std::move(*res)};
}

void DbmsHandler::ResumeColdTenantsForPromotion() {
  // Snapshot COLD-tenant names under the lock, then resume outside it (Resume_ takes lock_ itself).
  std::vector<std::string> cold_names;
  {
    auto rd = std::shared_lock{lock_};
    cold_names.reserve(suspended_.size());
    for (auto const &[name, _] : suspended_) cold_names.emplace_back(name);
  }
  for (auto const &name : cold_names) {
    // rewire_replication=false: caller holds repl_state write lock. make_room=false: don't re-evict peers.
    if (auto res = Resume_(name, /*rewire_replication=*/false, /*make_room=*/false); !res.has_value()) {
      spdlog::warn(
          "MAIN promotion: could not resume COLD tenant '{}' to update its epoch; it will keep "
          "its pre-promotion epoch until next resume.",
          name);
    }
  }
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
}  // namespace memgraph::dbms
