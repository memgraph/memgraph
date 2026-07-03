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
#include <ranges>
#include <shared_mutex>
#include <thread>
#include <utility>

#include "dbms/constants.hpp"
#include "dbms/global.hpp"
#include "dbms/rpc.hpp"
#include "license/license.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/enum.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"  // utils::OutOfMemoryException (boot-OOM detection)
#include "utils/on_scope_exit.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

#include <mutex>

namespace memgraph::dbms {

namespace {
#ifdef MG_ENTERPRISE
constexpr std::string_view kDBPrefix = "database:";  // Key prefix for database durability
constexpr std::string_view kUuidKey = "uuid";        // JSON key for a durable entry's tenant uuid
constexpr std::string_view kRelDirKey = "rel_dir";   // JSON key for a durable entry's relative data dir
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
    V2,  //!< hot/cold: COLD entries gain a `cold` marker + heartbeat metadata + cold_stats
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
    if (val == "V2") {
      return DurabilityVersion::V2;
    }
    throw UnknownVersionException();
  };

  static auto GenKey(std::string_view name) -> std::string { return fmt::format("{}{}", kDBPrefix, name); }

  static auto GenVal(utils::UUID uuid, std::filesystem::path rel_dir) {
    nlohmann::json json;
    json[kUuidKey] = uuid;
    json[kRelDirKey] = rel_dir;
    // TODO: Serialize the configuration
    return json.dump();
  }

  // Round-trippable JSON for a captured StorageInfo (hot/cold cold_stats). Distinct from
  // storage::ToJson (a lossy SHOW-presentation form): this stores every field by struct name so the
  // exact StorageInfo survives a restart. Driven by the single StorageInfoForEachField field list
  // (storage.hpp) so adding a field updates this path AND the V3 SLK wire at once. Enums persist as
  // their underlying integer: nlohmann's built-in enum (de)serialization is integer-based, and
  // storage::StorageMode's own to_json/from_json (storage_mode.hpp) keep that integer encoding while
  // range-checking the value on read. The human-readable string form lives only in storage::ToJson.
  static nlohmann::json StatsToJson(const storage::StorageInfo &s) {
    nlohmann::json j;
    storage::StorageInfoForEachField(s, [&](const char *key, const auto &v) { j[key] = v; });
    return j;
  }

  // Reads back a StatsToJson object. Tolerant of missing keys (defaults to a value-initialized field):
  // a V1 COLD entry never existed, but a forward-compatible read of a partial object must not throw.
  // A present-but-out-of-range storage_mode is range-checked by storage::from_json (falls back to
  // IN_MEMORY_TRANSACTIONAL); isolation/compression direct-cast via nlohmann's default enum from_json.
  static storage::StorageInfo StatsFromJson(const nlohmann::json &j) {
    storage::StorageInfo s{};
    storage::StorageInfoForEachField(
        s, [&](const char *key, auto &v) { v = j.value(key, std::remove_cvref_t<decltype(v)>{}); });
    return s;
  }

  // Durable entry for a COLD (suspended) tenant. Carries the HOT entry's identity (uuid, rel_dir)
  // plus the cold marker and the as-of-suspend cold_stats. On restart a `cold:true` entry is restored
  // as a no-value COLD shell (no storage build) — see the restore loop in the DbmsHandler ctor. A
  // resumed cold tenant trusts its own on-disk epoch (BuildDetached); no epoch is persisted here.
  static auto GenColdVal(utils::UUID uuid, std::filesystem::path rel_dir, const storage::StorageInfo &cold_stats)
      -> std::string {
    nlohmann::json json;
    json[kUuidKey] = uuid;
    json[kRelDirKey] = rel_dir;
    json["cold"] = true;
    json["cold_stats"] = StatsToJson(cold_stats);
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

    // V1 -> V2 is purely additive: V1 entries are all HOT ({uuid, rel_dir}) and remain valid as-is;
    // V2 only ADDS optional COLD entries (written by SUSPEND). Reads default `cold` to false and
    // `cold_stats` to zeros, so an unmigrated V1 entry is read correctly with no data movement.

    // Bump the version inside the SAME atomic batch as the key rewrite: a crash must never leave
    // version=V2 persisted while the V0->V1 key rewrite is missing (that would permanently skip
    // migration on the next boot and orphan the un-rewritten keys). Version advances iff the batch lands.
    to_put.emplace("version", "V2");
    if (!durability->PutAndDeleteMultiple(to_put, to_delete)) {
      throw MigrationException();
    }
  }
};

DbmsHandler::DbmsHandler(storage::Config config, ResumeRetryPolicy resume_retry_policy)
    : default_config_{std::move(config)}, resume_retry_policy_{resume_retry_policy} {
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

  // Reconstruct a COLD tenant's rebuild metadata from a durable entry. The salient is rebuilt from
  // the instance defaults (per-tenant salient persistence is out of scope for now) with the durable
  // name + uuid overlaid — identical to how New_(name, uuid, ...) builds it for a HOT tenant.
  auto make_cold_entry =
      [&](std::string_view nm, utils::UUID id, std::filesystem::path rel_dir, const nlohmann::json &json) {
        SuspendedEntry entry;
        entry.salient = default_config_.salient;
        entry.salient.name = nm;
        entry.salient.uuid = id;
        entry.rel_dir = std::move(rel_dir);
        if (json.contains("cold_stats")) entry.cold_stats = Durability::StatsFromJson(json.at("cold_stats"));
        return entry;
      };

  // Restore databases. A tenant is restored HOT (storage built + recovered) unless its durable entry
  // carries `cold:true`, in which case only a no-value COLD shell + suspended_ metadata is restored
  // (no storage build). A HOT recovery that fails ABORTS the process (fail loud, like master) — a HOT
  // database is never silently demoted to COLD at boot.
  // If a durable entry fails to parse we cannot learn its data directory, so we must NOT run the
  // cleanup pass below (it would delete every dir not in the keep-set, including the one this corrupt
  // entry owned). Track that the durable view is incomplete and skip cleanup for this boot.
  bool durability_view_incomplete = false;
  auto it = durability_->begin(std::string(kDBPrefix));
  auto end = durability_->end(std::string(kDBPrefix));
  for (; it != end; ++it) {
    const auto &[key, config_json] = *it;
    const auto name = key.substr(kDBPrefix.size());
    // Parse defensively. A corrupt/truncated entry must leave the instance starting DEGRADED
    // (skip the entry, keep the data on disk) rather than throwing out of the ctor and bricking boot.
    nlohmann::json json;
    utils::UUID uuid;
    std::filesystem::path rel_dir;
    try {
      json = nlohmann::json::parse(config_json);
      uuid = json.at(kUuidKey).get<utils::UUID>();
      rel_dir = json.at(kRelDirKey).get<std::filesystem::path>();
    } catch (const nlohmann::json::exception &e) {
      // Catch ONLY JSON parse/type/missing-key errors (genuine corruption). A non-JSON exception
      // (e.g. std::bad_alloc under boot memory pressure) must propagate, not be mislabeled corruption
      // and silently skip a valid tenant — mirrors the OOM-aware classification of the HOT path below.
      durability_view_incomplete = true;
      spdlog::error(
          "Corrupt durable database entry '{}' ({}); skipping it. Its on-disk data is left untouched and "
          "directory cleanup is SKIPPED this boot so no data is deleted while the durable view is incomplete.",
          key,
          e.what());
      continue;
    }
    // The on-disk directory must survive the cleanup pass below for BOTH hot and cold tenants — a
    // COLD shell's data dir is exactly what a later resume rebuilds from.
    directories.emplace(rel_dir.filename());

    const bool is_cold = json.value("cold", false);
    if (is_cold) {
      // COLD: metadata-only shell, no storage build.
      // make_cold_entry reads the optional cold_stats field via j.value() / j.at(). nlohmann's j.value() throws
      // nlohmann::json::type_error (302) when a key is PRESENT BUT WRONG-TYPED (the default only
      // covers MISSING keys). Guard here with the same degradation pattern as the parse guard above:
      // a wrong-typed sub-field must leave the instance booting DEGRADED, not abort it. We catch
      // only nlohmann::json::exception so std::bad_alloc / OOM keep their existing propagation.
      spdlog::info("Restoring suspended (cold) database {} at {}.", name, rel_dir);
      try {
        db_handler_.EmplaceColdShell(name);
        suspended_.insert_or_assign(std::string{name}, make_cold_entry(name, uuid, rel_dir, json));
        spdlog::info("Suspended (cold) database {} restored.", name);
      } catch (const nlohmann::json::exception &e) {
        durability_view_incomplete = true;
        db_handler_.EraseColdShell(name);
        spdlog::warn(
            "Durable cold entry '{}' has a wrong-typed metadata field ({}); skipping it. "
            "Its on-disk data is left untouched and directory cleanup is SKIPPED this boot.",
            name,
            e.what());
      }
      continue;
    }

    // HOT: build + recover the storage. A recovery failure here is FATAL — the instance aborts, exactly
    // as on master and on a single-tenant instance. A HOT database that cannot be brought up at boot is
    // NEVER silently demoted to COLD: failing loud surfaces the problem (corruption / OOM / bad config)
    // and refuses to serve a tenant that has silently lost data. (A user-initiated RESUME, by contrast,
    // is recoverable — it fails + returns an error and leaves the tenant COLD/retriable; see Resume_.)
    // A genuinely suspended tenant never reaches here: it is restored as a COLD shell above (cold:true).
    // Recovery can fail two ways: New_ returns an `unexpected` (the assert below) OR it throws
    // (on-disk corruption, bad_alloc, OutOfMemoryException). Both are fatal; the catch turns a throw into
    // the same clean fatal log instead of an opaque uncaught-exception terminate out of the ctor.
    spdlog::info("Restoring database {} at {}.", name, rel_dir);
    try {
      // New_ registers the database as a side effect; the returned handle is intentionally dropped and
      // only its success is checked here (do not "fix" the unused value by storing it).
      auto new_db = New_(name, uuid, nullptr, rel_dir);
      MG_ASSERT(new_db.has_value(),
                "Failed while recovering database {} (error {}); aborting.",
                name,
                static_cast<int>(new_db.error()));
    } catch (const std::exception &e) {
      LOG_FATAL("Failed while recovering database {} ({}); aborting.", name, e.what());
    }
    spdlog::info("Database {} restored.", name);
  }
  // Set the cold-tenant gauge ONCE after the restore loop (a per-iteration Set would be repeatedly
  // overwritten; no scrape endpoint is live during construction anyway). Counts the cold-shell tenants
  // restored above (a failed HOT recovery aborts, so it never contributes a COLD entry here).
  UpdateColdGauge_();

  /*
   * DATABASES CLEAN UP
   */
  // Clean the unused directories. If any durable entry failed to parse, the keep-set is
  // incomplete — skip cleanup entirely so we never delete the data dir of a tenant we could not read.
  // The orphan dirs are merely disk overhead and get reclaimed on the next clean boot.
  if (durability_view_incomplete) {
    spdlog::warn(
        "Skipping unused-directory cleanup: a durable database entry failed to parse, so the set of live "
        "directories is incomplete and no data directory will be removed this boot.");
  } else {
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

std::optional<DbmsHandler::DeleteResult> DbmsHandler::TryDeleteColdFastPath_(std::string_view name,
                                                                             system::Transaction *transaction) {
  if (!suspended_.contains(name)) return std::nullopt;
  auto res = DeleteCold_(name);
  if (!res) return DeleteResult{std::unexpected{res.error()}};
  if (transaction) {
    transaction->AddAction<DropDatabase>(*res);
  }
  return DeleteResult{};
}

DbmsHandler::DeleteResult DbmsHandler::TryDelete(std::string_view db_name, system::Transaction *transaction) {
  auto wr = std::lock_guard{lock_};
  if (db_name == kDefaultDB) {
    // MSG cannot delete the default db
    return std::unexpected{DeleteError::DEFAULT_DB};
  }

  // Cold-tenant fast path: drop a COLD (suspended) tenant directly without going through the HOT
  // gatekeeper path (which returns NON_EXISTENT for a no-value shell).
  if (auto cold_res = TryDeleteColdFastPath_(db_name, transaction)) {
    return *cold_res;
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

  // Cold-tenant fast path.
  if (auto cold_res = TryDeleteColdFastPath_(db_name, transaction)) {
    return *cold_res;
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
  if (auto cold_res = TryDeleteColdFastPath_(db_name, /*transaction=*/nullptr)) {
    return *cold_res;
  }
  return Delete_(db_name);
}

DbmsHandler::DeleteResult DbmsHandler::Delete(utils::UUID uuid) {
  auto wr = std::lock_guard(lock_);
  // COLD first, matching the rest of the Delete/TryDelete family (a DROP treats a COLD tenant as an
  // equally valid target, unlike the Get_ read path which privileges HOT to surface a "RESUME it"
  // error). Ordering is correctness-neutral: under lock_ a tenant is HOT xor in suspended_, never both
  // (Resume_ publishes HOT and erases the suspended_ entry in one nothrow scope). Both lookups are
  // in-memory map scans — neither touches disk. (Replica apply path — no transaction arg.)
  if (auto it = FindSuspendedByUuid_(uuid); it != suspended_.end()) {
    auto dropped = DeleteCold_(it->first);
    if (!dropped) return std::unexpected{dropped.error()};
    return {};
  }
  auto it = FindHotByUuid_(uuid);
  if (it == db_handler_.end()) return std::unexpected{DeleteError::NON_EXISTENT};
  return Delete_(it->first);
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

  // A COLD (suspended) tenant cannot be renamed. Its gatekeeper is a no-value shell, so
  // db_handler_.Rename would move the shell and the subsequent Get(new_name) MG_ASSERT would abort
  // the process. Renaming a suspended tenant is not supported — RESUME it first.
  if (suspended_.contains(old_name)) {
    return std::unexpected{RenameError::SUSPENDED};
  }

  // A tenant mid-transition must not be renamed either. Suspend_ holds a raw pointer to the in-map
  // gatekeeper across its lock-free phase; db_handler_.Rename would move-erase that node out from
  // under it (use-after-free, or the post-rename Get MG_ASSERT). The suspended_ check above already
  // covers COLD and RESUMING (their suspended_ entry is live), so this closes the remaining SUSPENDING
  // window. Mirrors the DROP sibling DeleteCold_, which likewise refuses a non-HOT gatekeeper under
  // this same exclusive lock_. Safety here is otherwise external (all DDL is serialized by the system
  // transaction) — this makes it local and robust to any future finer-grained DDL locking.
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

std::expected<utils::UUID, DeleteError> DbmsHandler::DeleteCold_(std::string_view name) {
  auto it = suspended_.find(name);
  if (it == suspended_.end()) return std::unexpected{DeleteError::NON_EXISTENT};

  // A DROP may only proceed when the tenant is strictly COLD. If it is mid-transition
  // (RESUMING or SUSPENDING) a concurrent operation holds or has handed off the gatekeeper;
  // erasing now would block ~Gatekeeper waiting for a terminal state while the caller holds
  // lock_ -> permanent deadlock. Reject as retriable (USING): the in-flight transition will
  // finish (RESUMING -> HOT, SUSPENDING -> COLD or HOT) and the DROP can be retried.
  //
  // This is the load-bearing enforcement of the invariant the Resume_ comment at Phase A
  // (~1300-1304) previously only claimed: "a concurrent DROP sees RESUMING and refuses to erase."
  // That invariant is NOW enforced here under the same exclusive lock_ the DROP path holds.
  auto *gk = db_handler_.GetGatekeeper(name);
  if (!gk || gk->state() != utils::GatekeeperState::COLD) {
    return std::unexpected{DeleteError::USING};
  }

  const auto uuid = it->second.salient.uuid;
  const auto data_dir = default_config_.durability.storage_directory / it->second.rel_dir;
  suspended_.erase(it);
  db_handler_.EraseColdShell(name);  // guaranteed to succeed: we just verified state==COLD above
  if (durability_) durability_->Delete(Durability::GenKey(name));
  std::error_code ec;
  (void)std::filesystem::remove_all(data_dir, ec);
  if (ec) {
    spdlog::error(R"(Failed to clean disk while dropping suspended database "{}" at {})", name, data_dir.string());
  }
  if (tenant_profiles_) {
    [[maybe_unused]] auto detached = tenant_profiles_->DetachFromDatabase(name);
  }
  UpdateColdGauge_();
  return uuid;
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
    if (!db) {
      // Get() is HOT-gated, so a tenant caught mid-SUSPEND (state SUSPENDING, not yet in `suspended_`)
      // lands here too. Mirrors the Rename guard: report USING for any non-HOT gatekeeper instead of
      // misreporting NON_EXISTENT for a tenant that in fact exists.
      auto *gk = db_handler_.GetGatekeeper(db_name);
      if (gk && gk->state() != utils::GatekeeperState::HOT) return std::unexpected{DeleteError::USING};
      return std::unexpected{DeleteError::NON_EXISTENT};
    }
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

void DbmsHandler::RestoreTriggersFor(DatabaseAccess db_acc, query::InterpreterContext *ic) {
  // Single-db trigger restore for the hot/cold resume arm (mirrors the per-db body of RestoreTriggers but
  // on a caller-held accessor, with no lock_). The resumed Database's TriggerStore is constructed empty by
  // BuildDetached (the ctor does not auto-arm), so this re-parses + arms the durably-persisted triggers.
  auto storage_accessor = db_acc->Access(memgraph::storage::WRITE);
  auto dba = memgraph::query::DbAccessor{storage_accessor.get()};
  db_acc->trigger_store()->RestoreTriggers(
      &ic->ast_cache, &dba, ic->config.query, ic->auth_checker, db_acc->name(), ic->parameters);
}

#ifdef MG_ENTERPRISE
// Hot/cold SUSPEND as a system action. Recording it makes SUSPEND participate in the system
// transaction — serialized + system-timestamped exactly like CREATE/DROP DATABASE. DoReplication
// streams a SuspendDatabaseRpc to each replica, which applies its own teardown (in system-timestamp
// order) so the replica's copy is torn down to a COLD shell to match MAIN.
struct SuspendDatabase : memgraph::system::ISystemAction {
  explicit SuspendDatabase(utils::UUID uuid) : uuid_{uuid} {}

  void DoDurability() override { /* COLD marker persisted during Suspend_ (before this action runs) */ }

  bool ShouldReplicateInCommunity() const override { return false; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check_response = [](const storage::replication::SuspendDatabaseRes &response) {
      return response.result != storage::replication::SuspendDatabaseRes::Result::FAILURE;
    };

    return client.StreamAndFinalizeDelta<storage::replication::SuspendDatabaseRpc>(
        check_response, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), uuid_);
  }

  void PostReplication(replication::RoleMainData & /*mainData*/) const override {}

 private:
  utils::UUID uuid_;
};

// Hot/cold RESUME as a system action. See SuspendDatabase above. DoReplication streams a
// ResumeDatabaseRpc; the replica rebuilds its own copy (COLD -> HOT) from its on-disk artifacts
// identified by UUID, in system-timestamp order.
struct ResumeDatabase : memgraph::system::ISystemAction {
  explicit ResumeDatabase(utils::UUID uuid) : uuid_{uuid} {}

  void DoDurability() override { /* COLD marker cleared during Resume_ (before this action runs) */ }

  bool ShouldReplicateInCommunity() const override { return false; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check_response = [](const storage::replication::ResumeDatabaseRes &response) {
      return response.result != storage::replication::ResumeDatabaseRes::Result::FAILURE;
    };

    return client.StreamAndFinalizeDelta<storage::replication::ResumeDatabaseRpc>(
        check_response, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), uuid_);
  }

  void PostReplication(replication::RoleMainData & /*mainData*/) const override {}

 private:
  utils::UUID uuid_;
};

DbmsHandler::SuspendResult DbmsHandler::Suspend_(std::string_view name, system::Transaction *txn, bool for_recovery) {
  if (name == kDefaultDB) return std::unexpected{SuspendError::DEFAULT_DB};

  // Wall-clock start for the suspend-latency histogram; observed only on the success path below so a
  // rejected/aborted attempt (e.g. an ACTIVE_CONNECTIONS timeout) does not pollute the distribution.
  const auto suspend_start = std::chrono::steady_clock::now();

  SuspendedEntry entry;
  utils::Gatekeeper<Database> *gk = nullptr;
  std::optional<DatabaseAccess> acc;

  // PHASE A-pre (OFF-lock): stop the per-database features that keep the tenant pinned HOT.
  // Each Kafka/Pulsar stream consumer captures a shared_ptr<Interpreter> that owns a DatabaseAccess, so
  // the freeze in Phase A could never reach sole-accessor (count==1) while any stream exists -> suspend
  // would always fail ACTIVE_CONNECTIONS. on_suspend_ runs Streams::Shutdown() which stops the consumers
  // and releases those accessors WITHOUT deleting the durable stream metadata (resume rebuilds it).
  //
  // This MUST run with lock_ NOT held: Streams::Shutdown() performs an unbounded blocking join() of the
  // consumer threads, and joining while we hold lock_ would stall ALL db_handler_ access for however long
  // that join takes (and on a replica's single-threaded replication-apply thread it would stall
  // replication entirely). We take a brief shared lock_ only to mint the accessor, then release it before
  // joining.
  // Tracks whether on_suspend_ ran (not specifically "streams" — the callback's scope may grow beyond
  // stopping streams in the future).
  bool on_suspend_run = false;
  {
    std::optional<DatabaseAccess> sacc;
    {
      auto rd = std::shared_lock{lock_};
      auto a = db_handler_.Get(name);
      if (!a) return std::unexpected{SuspendError::NON_EXISTENT};
      sacc = std::move(*a);
    }  // release lock_ BEFORE joining consumer threads
    if (on_suspend_) {
      on_suspend_(*sacc);
      on_suspend_run = true;
    }
  }  // sacc released -> its count contribution is gone before Phase A mints the freeze accessor
  // UNDO guard: a failed/aborted suspend must NOT silently leave the streams stopped. Fires on every path
  // that does not commit the suspend (early return below, or a post-freeze throw after the abort_suspend
  // rollback restores HOT). Declared BEFORE the abort_suspend rollback so it fires AFTER it (LIFO) -> the
  // tenant is HOT again by the time we restore streams. Disabled once finish_suspend() commits the COLD.
  auto stream_restore = utils::OnScopeExit{[&] {
    if (!on_suspend_run || !restore_streams_) return;
    auto rd = std::shared_lock{lock_};
    if (auto a = db_handler_.Get(name)) restore_streams_(*a);
  }};
  {
    // PHASE A — eligibility check + metadata capture + freeze, all under the shared lock_.
    //
    // try_begin_suspend() is called INSIDE this shared_lock scope (not after).
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
    //
    // CONCURRENCY ASSUMPTION: at this primitive level, two concurrent Suspend_ calls for the SAME
    // tenant would each mint their own accessor above, so each sees the other's accessor and both fail
    // try_begin_suspend()'s ACTIVE_CONNECTIONS check — single-winner exclusivity is NOT enforced by this
    // primitive itself. It relies on DDL being serialized upstream by the system transaction. A future
    // caller that bypasses that serialization (e.g. an asynchronous/direct resume-triggered suspend) must
    // add its own per-tenant exclusion before reaching this point.
    auto rd = std::shared_lock{lock_};
    auto a = db_handler_.Get(name);  // nullopt if absent OR not HOT (already cold)
    if (!a) return std::unexpected{SuspendError::NON_EXISTENT};
    auto *db = a->get();
    auto *st = db->storage();
    // Suspend requires {periodic snapshot + WAL} durability AND a RUNTIME storage mode of
    // IN_MEMORY_TRANSACTIONAL. The runtime-mode check is load-bearing and must NOT be replaced by
    // the config: a tenant switched to IN_MEMORY_ANALYTICAL at runtime suppresses WAL
    // (InitializeWalFile() returns false for analytical) and pauses the snapshot runner, yet
    // IsDurabilityCompleteForSuspend() only inspects the creation-time config.
    // Rejecting anything not in-memory transactional covers both ON_DISK_TRANSACTIONAL and
    // IN_MEMORY_ANALYTICAL.
    if (st->GetStorageMode() != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      return std::unexpected{SuspendError::NOT_IN_MEMORY};
    }
    if (!st->IsDurabilityCompleteForSuspend()) {
      if (!for_recovery) return std::unexpected{SuspendError::DURABILITY_INCOMPLETE};
      // In recovery the replica converges to MAIN's AUTHORITATIVE cold set. The durability-complete
      // gate protects a USER-initiated SUSPEND from creating an unrecoverable cold tenant; in recovery
      // the tenant is already COLD on MAIN, so we bypass the gate and suspend here regardless. Suspend
      // itself no longer writes a snapshot, so recoverability of the resulting local cold shell rests
      // entirely on THIS replica's own periodic-snapshot+WAL durability state, not on anything suspend
      // does — if that state is incomplete, this cold shell may not be locally recoverable (no
      // snapshot, no/partial WAL). The cluster stays consistent regardless via the higher-level
      // SystemRecoveryHandler / replica resync from MAIN, not via a suspend-time snapshot.
      // Reachable only when this replica's durability differs from MAIN's (e.g. the WAL/snapshot config
      // is inconsistent across the cluster) — bypass with a loud warning rather than failing recovery
      // and stalling the replica in a BEHIND retry loop.
      spdlog::warn(
          "Force-suspending database {} during recovery although its local durability is incomplete "
          "(periodic snapshot + WAL not enabled here). MAIN is authoritative for the cold set, so this "
          "replica converges regardless, but the resulting local cold shell may not be locally "
          "recoverable (no snapshot, no/partial WAL) — this usually means the durability config differs "
          "from MAIN — align the WAL/snapshot durability flags across the cluster.",
          name);
    }
    // Suspend does NOT gate on whether replicas are registered. Hot/cold is a system-replicated operation (sibling of
    // CREATE/DROP DATABASE): suspending a tenant that has replicas is the SUPPORTED flow — the
    // SuspendDatabase system action streams a SuspendDatabaseRpc so each replica tears down its own
    // copy in system-timestamp order. The earlier "reject while replicating" guard encoded only the
    // v1 node-local limitation (no wire to tell replicas) and protects no MAIN-local invariant:
    // IsDurabilityCompleteForSuspend() + the freeze-to-count==1 below already guarantee MAIN's state
    // is fully durable before teardown. On a replica this predicate is false anyway (replication
    // storage clients live on MAIN), so the path is identical there.

    entry.salient = db->config().salient;
    entry.rel_dir = std::filesystem::relative(db->config().durability.storage_directory,
                                              default_config_.durability.storage_directory);

    gk = db_handler_.GetGatekeeper(name);  // stable pointer to the in-map gatekeeper
    acc = std::move(*a);                   // hold the accessor across phases (count includes it)

    // Transition HOT -> SUSPENDING under the shared lock (LAST step in Phase A).
    if (!gk->try_begin_suspend()) return std::unexpected{SuspendError::ACTIVE_CONNECTIONS};
  }
  // State is now SUSPENDING. Concurrent Drop sees SUSPENDING and returns USING without
  // erasing gk, so gk is valid for the rest of this function (lock-free Phase B/C).

  // RAII rollback guard. If anything in the SUSPENDING window throws, abort_suspend() restores HOT
  // so the gatekeeper is not permanently stuck SUSPENDING (which would hang ~Gatekeeper).
  auto rollback = utils::OnScopeExit{[&] { gk->abort_suspend(); }};

  // PHASE B — post-freeze work (lock-free; gk is SUSPENDING so Drop is rejected).
  // No post-freeze replica-registration re-check either — see Phase A rationale above. A
  // RegisterReplica racing into the A->freeze window only adds replicas, which is now allowed.

  // Suspend takes NO proactive/consolidating snapshot. Durability at suspend is whatever the
  // periodic snapshot + WAL already provide (guaranteed complete by IsDurabilityCompleteForSuspend()
  // above), plus — if --storage-snapshot-on-exit is enabled — the snapshot InMemoryStorage's own
  // destructor writes during its normal teardown below (finish_suspend() -> ~InMemoryStorage).
  // Resume recovers via recover_on_startup (snapshot-if-any + WAL replay from the tenant's own
  // durability directory); it does not depend on a suspend-time snapshot existing.

  // Capture the last-hot stats snapshot POST-freeze (count==1, no concurrent commit can advance it
  // between this read and teardown). Served by cold SHOW STORAGE INFO / SHOW DATABASES. Use GetInfo()
  // (not GetBaseInfo()): the latter only fills vertex/edge/memory/disk counters and leaves
  // storage_mode, isolation_level, index/constraint counts, and the durability/compression flags
  // value-initialized, so cold SHOW STORAGE INFO would report wrong values for those fields.
  // GetInfo() layers them on top of GetBaseInfo() and is deadlock-safe here: it only takes storage's
  // main_lock_ shared (via Access(READ)), which is free in this frozen SUSPENDING window. `st`'s
  // concrete type is InMemoryStorage (guaranteed by the IN_MEMORY_TRANSACTIONAL check earlier), so
  // GetInfo() dispatches correctly.
  {
    auto *st = (*acc)->storage();
    entry.cold_stats = st->GetInfo();
  }

  // PHASE C — record metadata (lock_), then heavy teardown OUTSIDE lock_.
  const auto tenant_uuid = entry.salient.uuid;  // capture before `entry` is moved into suspended_
  // Serialize the durable COLD marker (key + value) from `entry` BEFORE it is moved into suspended_ and
  // BEFORE the point of no return. GenKey (fmt::format) allocates, so computing it here — while the
  // rollback guard can still cleanly restore HOT — keeps the only throwing durability work out of the
  // post-commit region (HC-2: a throw must not leave a stale suspended_ entry behind).
  std::string cold_key;
  std::string cold_val;
  if (durability_) {
    cold_key = Durability::GenKey(name);
    cold_val = Durability::GenColdVal(entry.salient.uuid, entry.rel_dir, entry.cold_stats);
  }
  acc.reset();  // drop our accessor -> count == 0 (state still SUSPENDING)
  {
    auto wr = std::lock_guard{lock_};
    // Persist the durable COLD marker FIRST, before the in-memory commit below. Both run under the SAME
    // lock_ so a concurrent Resume_ cannot observe a half state (order within the lock is invisible to
    // observers, who cannot acquire lock_ until we release it). The marker is best-effort: a Put failure
    // — whether it returns false OR throws (KVStore::Put is not noexcept; bad_alloc under memory pressure)
    // — is caught locally right below and degrades to a warning; it never reaches the outer rollback
    // guard (`rollback` above, which calls gk->abort_suspend()) and never rolls back the suspend. The
    // throwing durability work that the rollback guard actually protects is the PRE-LOCK GenKey/
    // GenColdVal serialization above (before acc.reset()/this lock_ scope) — that is what must not run
    // after suspended_ is populated. Keeping suspended_.insert_or_assign AFTER this Put attempt is
    // defense-in-depth: it means even a hypothetical future change that let a Put-throw escape this
    // try/catch still could not leave suspended_ populated for a tenant that stayed HOT. A missing
    // marker only makes the tenant recover HOT on restart; MAIN's data stays durable on disk regardless.
    if (durability_) {
      bool marker_persisted = false;
      std::string put_error;  // populated only on a thrown Put(); empty on a clean `false` return
      try {
        marker_persisted = durability_->Put(cold_key, cold_val);
      } catch (const std::exception &e) {  // best-effort marker; fall through to the warning below
        marker_persisted = false;
        put_error = e.what();
      } catch (...) {  // NOLINT(bugprone-empty-catch) — non-std throw; still best-effort
        marker_persisted = false;
      }
      if (!marker_persisted) {
        spdlog::warn(
            "hot/cold suspend: failed to persist the cold durability marker for '{}'{}{}; the database is "
            "suspended in memory but will recover HOT on restart.",
            name,
            put_error.empty() ? "" : ": ",
            put_error);
      }
    }
    // In-memory commit — the tenant is now COLD. Counts every successful Suspend_ (user SUSPEND, replica
    // RPC apply, and recovery force-suspend all funnel through here); the gauge tracks the live cold-set
    // size. insert_or_assign / Increment / UpdateColdGauge_ are all noexcept, so once we reach here the
    // suspend cannot throw-and-roll-back with a stale suspended_ entry.
    suspended_.insert_or_assign(std::string{name}, std::move(entry));
    metrics::Metrics().global.database_suspends->Increment();
    UpdateColdGauge_();
  }

  // Disable the rollback guard BEFORE calling finish_suspend(). finish_suspend() transitions
  // SUSPENDING -> COLD; if the guard fired afterwards it would abort_suspend() on a non-SUSPENDING
  // state and trip the debug assert.
  rollback.Disable();

  // OUTSIDE lock_: value_.reset() -> ~Database (stop threads, FinalizeWal, NO exit snapshot).
  // The COLD shell stays in db_handler_ so a later resume can move-assign a fresh gatekeeper.
  gk->finish_suspend();

  // The suspend is committed (tenant is COLD, in-memory storage and stream consumers dropped;
  // durable stream metadata persists for resume). Keep the streams stopped — do NOT run the undo guard.
  //
  // On the gap between rollback.Disable() above and this Disable(): finish_suspend() CANNOT throw-and-unwind
  // into it — its only fallible step is value_.reset() (~Database -> ~InMemoryStorage), and destructors are
  // noexcept by default, so a throwing teardown calls std::terminate rather than unwinding. The hypothetical
  // "stuck SUSPENDING + streams left stopped" state is therefore not exception-reachable here (and a throwing
  // storage destructor is already a terminate-level invariant violation, independent of this arm).
  stream_restore.Disable();

  // Record the system action so the suspend is ordered + replicated like CREATE/DROP DATABASE.
  // Done only after the local teardown commits; the replica wire is filled by DoReplication.
  if (txn) txn->AddAction<SuspendDatabase>(tenant_uuid);

  // Successful suspend: record end-to-end latency (includes teardown).
  metrics::Metrics().global.database_suspend_latency_seconds->Observe(
      std::chrono::duration<double>(std::chrono::steady_clock::now() - suspend_start).count());

  spdlog::info("hot/cold: database '{}' suspended (HOT -> COLD)", name);
  return {};
}

DbmsHandler::ResumeResult DbmsHandler::Resume_(std::string_view name, bool rewire_replication, system::Transaction *txn,
                                               bool *already_hot) {
  // Outer loop: a loser keeps `continue`-ing only while the winner is demonstrably alive
  // (RESUMING/SUSPENDING), re-initializing gk/salient/rel_dir/won_resume from the map under a fresh
  // shared lock on each pass. All other exits (NON_EXISTENT, already-HOT, timeout, build failure, and
  // a CONFIRMED-COLD winner abort) are `return`s — a loser that confirms the winner aborted fails fast
  // with RECOVERY_FAILED rather than auto-retrying; RESUME is idempotent/retriable, so the caller
  // re-issues.
  // Wall-clock start for the resume-latency histogram; observed only on the winner's successful publish
  // below (the path that bumps the resume counter), so loser/error returns do not skew the distribution.
  const auto resume_start = std::chrono::steady_clock::now();
  while (true) {
    utils::Gatekeeper<Database> *gk = nullptr;
    // Copy only the two fields the off-lock build needs (salient + rel_dir) instead of the full
    // SuspendedEntry (which also holds cold_stats).
    storage::SalientConfig salient;
    std::filesystem::path rel_dir;
    bool won_resume = false;
    {
      // PHASE A — decide + acquire the single-flight token, all under the shared lock_.
      //
      // begin_resume() is called INSIDE this shared-lock scope (not after): between releasing the
      // lock and begin_resume(), a concurrent DROP of a COLD tenant (under exclusive lock_) could
      // erase the COLD shell, leaving `gk` dangling -> begin_resume() would be a UAF. By transitioning
      // COLD -> RESUMING while lock_ is held, a concurrent DROP then takes the exclusive lock and calls
      // DeleteCold_ which checks gk->state() — if not strictly COLD (it is RESUMING) DeleteCold_
      // returns USING and refuses to erase — so `gk` stays valid through the slow off-lock build/publish.
      // This invariant is ENFORCED inside DeleteCold_/EraseColdShell (not merely claimed): both functions
      // check state() under the caller's exclusive lock_ before touching items_ or suspended_.
      auto rd = std::shared_lock{lock_};
      if (auto a = db_handler_.Get(name)) {
        // Already HOT (raced) — share its accessor. This is the ONLY already_hot=true exit: the
        // tenant was HOT before this call did anything (idempotent no-op), as opposed to the
        // loser-poll / winner-publish exits below which all observe a real COLD -> HOT transition.
        if (already_hot) *already_hot = true;
        return std::move(*a);
      }
      gk = db_handler_.GetGatekeeper(name);  // stable pointer to the in-map gatekeeper
      if (!gk) return std::unexpected{ResumeError::NON_EXISTENT};
      auto it = suspended_.find(name);
      if (it == suspended_.end()) return std::unexpected{ResumeError::NON_EXISTENT};
      salient = it->second.salient;  // copy only the two fields the off-lock build needs
      rel_dir = it->second.rel_dir;
      won_resume = gk->begin_resume();  // COLD -> RESUMING (LAST step in Phase A)
    }

    // SINGLE-FLIGHT — loser path: someone else holds RESUMING. Poll Get(name) until HOT.
    // The deadline is re-armed on every iteration that confirms the winner is still RESUMING
    // (demonstrably alive and building), so a healthy large-tenant rebuild that takes longer than the
    // liveness window does NOT spuriously fail. kMaxWait is an absolute ceiling that still fires if the
    // winner is stuck-alive (holds RESUMING but never completes). We poll by NAME (not the gatekeeper
    // cv_): the winner publishes via `*gk = std::move(fresh)`, which destroys the old GKInternals (and
    // its cv_), so waiting on that cv_ would be a use-after-free.
    //
    // KNOWN LIMITATION: liveness is inferred purely from the RESUMING state, not an independent probe of
    // the winner thread. A winner that is alive but HUNG in recovery I/O (e.g. a stuck disk) keeps every
    // loser re-arming its deadline and waiting up to resume_retry_policy_.max_wait, repeatedly, forever.
    // This is an accepted blocking-I/O limitation (same class as finish_suspend()'s unbounded teardown
    // join, see the comment above Suspend_'s on_suspend_ call) — a stuck-RESUMING observability metric is
    // a planned follow-up, not implemented here.
    if (!won_resume) {
      constexpr auto kPollStep = std::chrono::milliseconds(5);
      // How long to wait after the LAST confirmed-RESUMING observation before concluding the winner
      // is dead. Large enough for a healthy rebuild under IO pressure, small enough to surface a
      // crashed winner before a client request timeout.
      // Absolute ceiling from poll entry: bounds a stuck-alive winner (RESUMING forever).
      const auto absolute_ceiling = std::chrono::steady_clock::now() + resume_retry_policy_.max_wait;
      auto deadline = std::chrono::steady_clock::now() + resume_retry_policy_.winner_liveness_window;
      while (std::chrono::steady_clock::now() < deadline && std::chrono::steady_clock::now() < absolute_ceiling) {
        bool retry_after_cold_fallback = false;
        {
          auto rd = std::shared_lock{lock_};
          if (auto a = db_handler_.Get(name)) {
            // Winner published HOT — this loser observed a real COLD -> HOT transition, not an
            // already-hot no-op, so already_hot stays false.
            if (already_hot) *already_hot = false;
            return std::move(*a);
          }
          // Re-fetch the gatekeeper by name under THIS shared lock rather than dereferencing the raw
          // `gk` captured in Phase A: a concurrent DROP of the (now-COLD) tenant during a sleep window
          // can free the GKInternals `gk` points at. nullptr (dropped) or COLD => treat as a fallback
          // and break out of polling to the authoritative post-poll re-check below, which returns
          // NON_EXISTENT (dropped) or RECOVERY_FAILED (winner aborted). We must NOT re-check while
          // holding this shared lock (the winner path takes the exclusive lock_ on the same thread ->
          // self-deadlock on the PREFER_READER rwlock): set a flag, drop the lock, then break.
          auto *live_gk = db_handler_.GetGatekeeper(name);
          if (!live_gk || live_gk->state() == utils::GatekeeperState::COLD) {
            retry_after_cold_fallback = true;
          } else if (live_gk->state() == utils::GatekeeperState::RESUMING ||
                     live_gk->state() == utils::GatekeeperState::SUSPENDING) {
            // Winner is demonstrably still alive — extend the liveness deadline. SUSPENDING is a live,
            // bounded wait-state too (not a COLD-fallback): Suspend_ inserts into `suspended_` (Phase B,
            // before finish_suspend()) BEFORE the SUSPENDING -> COLD flip, so by the time any observer
            // sees COLD the `suspended_` entry is guaranteed present. Treating SUSPENDING as a fallback
            // here would restart Phase A into the freeze-visible-but-not-yet-in-`suspended_` window,
            // which would spuriously return NON_EXISTENT for a tenant that in fact exists (racing a
            // concurrent SUSPEND against this RESUME).
            deadline = std::chrono::steady_clock::now() + resume_retry_policy_.winner_liveness_window;
          }
        }
        if (retry_after_cold_fallback) break;  // exit poll loop; fall through to the post-poll decision
        std::this_thread::sleep_for(kPollStep);
      }
      bool winner_still_resuming = false;
      {
        auto rd = std::shared_lock{lock_};
        auto *live_gk = db_handler_.GetGatekeeper(name);
        if (!live_gk) return std::unexpected{ResumeError::NON_EXISTENT};  // dropped mid-resume
        // HOT: the winner just published — share its accessor. Real COLD -> HOT transition, so
        // already_hot stays false (same reasoning as the in-poll HOT check above).
        if (auto a = db_handler_.Get(name)) {
          if (already_hot) *already_hot = false;
          return std::move(*a);
        }
        // RESUMING: a concurrent winner is still actively rebuilding. SUSPENDING: a DIFFERENT tenant
        // transition is live (see the loser-poll SUSPENDING note above) — its `suspended_` entry is
        // guaranteed present once it reaches COLD, so this is also just "still alive, keep waiting", not
        // a fallback. A demonstrably-live winner must NEVER be turned into a RECOVERY_FAILED, however
        // long its rebuild takes (a huge tenant replaying a long WAL is legitimately slow) — only restart
        // Phase A to keep waiting; a CONFIRMED-COLD (aborted) winner is the only case that fails fast.
        winner_still_resuming = (live_gk->state() == utils::GatekeeperState::RESUMING ||
                                 live_gk->state() == utils::GatekeeperState::SUSPENDING);
        // COLD / any other non-terminal state: the winner aborted (or a SUSPEND completed) — fail fast
        // below with RECOVERY_FAILED instead of auto-retrying.
      }
      // A live (RESUMING/SUSPENDING) winner: keep waiting, bounded by max_wait via the outer loop
      // re-arming the poll (this is the HC-1 fix: the absolute_ceiling can fire purely because the
      // winner stayed RESUMING, and that must not be treated as an abort).
      if (winner_still_resuming) continue;
      // Winner aborted -> tenant went COLD: the resume we were waiting on failed. Fail fast — RESUME is
      // idempotent and retriable, so the caller re-issues rather than us auto-retrying in-call (which was
      // a livelock risk under concurrent failing resumes, and needed a bounded counter). suspended_ is
      // untouched by abort_resume(), so the tenant stays retriable.
      return std::unexpected{ResumeError::RECOVERY_FAILED};
    }

    // WE are the winner (state RESUMING). Build OFF the map and recover (SLOW, NO lock_ held).
    // `fresh` lives in this scope so that, on the throw path, the catch can release `acc` BEFORE
    // `fresh` destructs: ~Gatekeeper waits for count == 0, so an outstanding `acc` would self-deadlock
    // the fresh gatekeeper's destruction.
    try {
      storage::Config cfg = default_config_;
      cfg.salient = salient;
      cfg.durability.recover_on_startup = true;
      storage::UpdatePaths(cfg, default_config_.durability.storage_directory / rel_dir);
      auto fresh = db_handler_.BuildDetached(std::move(cfg));  // Database ctor recovers from disk
      DatabaseAccess acc = fresh.access().value();             // fresh gk is HOT => has_value
      // Roll a failed pre-publish rebuild back to COLD (retriable). CRITICAL ORDERING: fully tear down
      // `fresh` (its InMemoryStorage closes WAL/snapshot fds; ~Database -> RemoveDatabase deregisters the
      // uuid-keyed prometheus metrics) BEFORE abort_resume() releases the single-flight RESUMING token.
      // Releasing the token first would let the next winner BuildDetached a same-uuid Database that
      // pointer-aliases `fresh`'s not-yet-freed metrics (heap-UAF), or let DeleteCold_ remove_all() the
      // data dir while `fresh` still holds it open.
      auto rollback_to_cold = [&] {
        acc.reset();  // drop our accessor: fresh.count_ -> 0
        // ~Gatekeeper<Database> runs now (count==0, HOT => no wait): ~Database -> RemoveDatabase(uuid)
        // + storage teardown complete.
        {
          auto dying = std::move(fresh);
        }
        gk->abort_resume();  // RESUMING -> COLD; suspended_ untouched => retriable
      };
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
          // The resumed tenant runs its own on-disk epoch+history (recovered by BuildDetached) — the
          // cold entry carries no epoch to restore (cold-tenant epoch machinery was intentionally removed).
          auto it = suspended_.find(name);  // valid across the move below (it touches db_handler_, not suspended_)
          // Overwriting the RESUMING shell here is deadlock-free because Gatekeeper's move-assign tears
          // the old state down via ~GKInternals (non-blocking), NOT the count-waiting ~Gatekeeper — see
          // the load-bearing note on Gatekeeper::operator=(Gatekeeper&&) in utils/gatekeeper.hpp.
          *gk = std::move(fresh);
          if (it != suspended_.end()) suspended_.erase(it);
          // The tenant is now HOT. Counter increment + gauge set are atomic/non-throwing, so they
          // do not break the publish block's no-throw guarantee; done under lock_ so the gauge reads a
          // consistent suspended_ size.
          metrics::Metrics().global.database_resumes->Increment();
          UpdateColdGauge_();
        }
      } catch (const std::exception &e) {
        // PRE-PUBLISH failure: on_resume_ threw and the publish has NOT happened — `fresh` is still
        // local and HOT. rollback_to_cold() releases `acc` and fully tears `fresh` down BEFORE
        // releasing the single-flight RESUMING token (see the load-bearing comment on the lambda).
        spdlog::warn(
            "hot/cold resume: the pre-publish on_resume_ arm for '{}' threw ({}); rolling back to COLD "
            "(retriable).",
            name,
            e.what());
        rollback_to_cold();
        return std::unexpected{ResumeError::RECOVERY_FAILED};
      } catch (...) {
        // Same as above, for a non-std::exception throw.
        rollback_to_cold();
        return std::unexpected{ResumeError::RECOVERY_FAILED};
      }
      // Publish committed: the tenant is HOT and queryable. Everything below is best-effort
      // bookkeeping — the gatekeeper is already HOT, so a throw escaping any of these arms must NEVER
      // reach the outer catch(...): that catch calls abort_resume() on the gatekeeper, and abort_resume()
      // asserts state == RESUMING, so it would terminate on a HOT gatekeeper instead of degrading
      // gracefully. `best_effort` wraps each arm in its own try/catch so a failure here only logs — the
      // database stays live (HOT) and data is intact regardless of whether the bookkeeping succeeds.
      auto best_effort = [&](std::string_view what, auto &&fn) {
        try {
          fn();
        } catch (const std::exception &e) {
          spdlog::warn("hot/cold resume: {} for '{}' threw ({}); database is live (HOT).", what, name, e.what());
        } catch (...) {
          spdlog::warn("hot/cold resume: {} for '{}' threw; database is live (HOT).", what, name);
        }
      };

      // Record resume latency (recovery + publish) before the rest of the best-effort post-publish
      // bookkeeping so that work is excluded from the metric. Histogram::Observe takes a std::mutex
      // lock, whose lock() may throw std::system_error under OS resource exhaustion.
      best_effort("recording resume latency", [&] {
        metrics::Metrics().global.database_resume_latency_seconds->Observe(
            std::chrono::duration<double>(std::chrono::steady_clock::now() - resume_start).count());
      });

      // Flip the durable entry back to HOT (drop the cold marker) so a restart recovers it HOT.
      // Done in a SEPARATE short lock_ scope AFTER the publish. The write is guarded by
      // !suspended_.contains: if a SUSPEND raced in after our publish-erase and re-suspended the tenant,
      // its under-lock COLD marker must stand. GenKey/GenVal allocate (fmt/nlohmann) and can throw
      // bad_alloc. A failure (bool or throw) degrades to a warning: the tenant is live HOT regardless;
      // on the next restart a stale cold marker only makes it recover COLD, and it is resumable again.
      best_effort("clearing the cold durability marker (recoverable as COLD on restart)", [&] {
        if (durability_) {
          auto wr = std::lock_guard{lock_};
          if (!suspended_.contains(name)) {
            if (!durability_->Put(Durability::GenKey(name), Durability::GenVal(salient.uuid, rel_dir))) {
              spdlog::warn(
                  "hot/cold resume: failed to clear the cold durability marker for '{}'; the database is live "
                  "(HOT) but may recover COLD on restart (resumable).",
                  name);
            }
          }
        }
      });

      // POST-PUBLISH arm (replication). The tenant is ALREADY published HOT and erased from
      // suspended_; we must NOT abort_resume() here (the gatekeeper is no longer RESUMING). The arm
      // is independently retriable, so on failure we log and return the live accessor. Skipped when
      // rewire_replication == false (the replica replication-apply caller holds the repl_state read
      // lock and on_resume_repl_ would re-take it as a write lock -> self-deadlock). Wired in a later
      // (replication) commit; on_resume_repl_ is empty by default here, so this is a no-op.
      best_effort("replication re-wiring (may run un-replicated until re-established)", [&] {
        if (rewire_replication && on_resume_repl_) on_resume_repl_(acc);
      });

      // Record the system action so the resume is ordered + replicated like CREATE/DROP DATABASE.
      // Reached only on the winner's successful publish; the replica wire is filled by DoReplication.
      best_effort("recording the resume system action (may not replicate until the next system sync)", [&] {
        if (txn) txn->AddAction<ResumeDatabase>(salient.uuid);
        spdlog::info("hot/cold: database '{}' resumed (COLD -> HOT)", name);
      });
      // Winner's own successful publish: a real COLD -> HOT rebuild, so already_hot is false.
      if (already_hot) *already_hot = false;
      return acc;
    } catch (const std::exception &e) {
      // Recovery (BuildDetached) threw before `acc`/`fresh` existed. No live accessor to release.
      // Guarded by state(): reachable today only from a pre-publish BuildDetached throw, where state is
      // still RESUMING (behavior unchanged). The guard is defense-in-depth against a hypothetical future
      // post-publish escape reaching this outer catch — calling abort_resume() on an already-HOT
      // gatekeeper would (under NDEBUG) silently force it back to COLD.
      spdlog::warn("hot/cold resume: recovering database '{}' failed ({}); staying COLD (retriable).", name, e.what());
      // RESUMING -> COLD; suspended_ untouched => retriable.
      if (gk->state() == utils::GatekeeperState::RESUMING) gk->abort_resume();
      return std::unexpected{ResumeError::RECOVERY_FAILED};
    } catch (...) {
      // Same as above (non-std throw): no exception text to log.
      // RESUMING -> COLD; suspended_ untouched => retriable.
      if (gk->state() == utils::GatekeeperState::RESUMING) gk->abort_resume();
      return std::unexpected{ResumeError::RECOVERY_FAILED};
    }
  }  // end outer while(true) — all winner exits are `return`, so falling out is unreachable
}

DbmsHandler::SuspendResult DbmsHandler::SuspendByUUID(utils::UUID uuid, system::Transaction *txn) {
  // Resolve UUID -> name from the HOT set, then drop the lock before delegating to Suspend_ (which
  // re-acquires lock_ and freezes the tenant; holding an accessor here would keep count > 1 and make
  // try_begin_suspend() fail). A COLD shell has no accessor, so it is correctly not matched — a
  // SuspendDatabaseRpc for an already-COLD tenant resolves to NON_EXISTENT and the handler treats it
  // as NO_NEED (idempotent re-apply).
  std::string name;
  {
    auto rd = std::shared_lock{lock_};
    auto it = FindHotByUuid_(uuid);  // access() is nullopt for a non-HOT (COLD) shell => skipped
    if (it == db_handler_.end()) return std::unexpected{SuspendError::NON_EXISTENT};
    name = it->first;
  }
  return Suspend_(name, txn);
}

DbmsHandler::ResumeResult DbmsHandler::ResumeByUUID(utils::UUID uuid, system::Transaction *txn) {
  // Resolve UUID -> name from the suspended-set, then delegate to Resume_ with rewire_replication
  // false — the replication-apply caller holds the repl_state read lock, so on_resume_repl_ must
  // not re-take it as a write lock. The lock is dropped before Resume_ re-acquires it; Resume_
  // re-validates the COLD shell under its own lock, so the brief TOCTOU is benign (a racing resume
  // just makes Resume_ observe HOT and share the accessor).
  std::string name;
  {
    auto rd = std::shared_lock{lock_};
    auto it = FindSuspendedByUuid_(uuid);
    if (it == suspended_.end()) return std::unexpected{ResumeError::NON_EXISTENT};
    name = it->first;
  }
  return Resume_(name, /*rewire_replication=*/false, txn);
}

void DbmsHandler::ApplyColdRecoveryMeta(std::string_view name, const storage::ColdTenantRecovery &meta) {
  auto wr = std::lock_guard{lock_};
  auto it = suspended_.find(name);
  if (it == suspended_.end()) return;
  // StorageInfo is a trivially-copyable flat POD, so this assignment allocates nothing and cannot
  // throw — it->second is never left partially updated.
  it->second.cold_stats = meta.stats;

  // Persist the refreshed COLD marker so MAIN's authoritative stats survive a restart. The Put runs
  // UNDER lock_: releasing the lock between the in-memory mutation and the durable write would let a
  // concurrent DROP erase the tenant in the gap, after which this trailing Put would resurrect a
  // durable COLD entry for a tenant that no longer exists. Best-effort: the in-memory entry already
  // carries MAIN's stats (display is correct in this process); only a restart-before-persist would
  // recover the stale stats, which then self-heal on the next SystemRecovery round.
  if (durability_) {
    const auto &entry = it->second;
    if (!durability_->Put(Durability::GenKey(name),
                          Durability::GenColdVal(entry.salient.uuid, entry.rel_dir, entry.cold_stats))) {
      spdlog::warn("hot/cold recovery: failed to persist cold stats for database '{}'.", name);
    }
  }
}

#endif  // MG_ENTERPRISE

}  // namespace memgraph::dbms
