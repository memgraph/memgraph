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
    json["uuid"] = uuid;
    json["rel_dir"] = rel_dir;
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
    json["uuid"] = uuid;
    json["rel_dir"] = rel_dir;
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

    // Set version to the current schema (V2).
    durability->Put("version", "V2");
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
  // (no storage build). A HOT recovery that fails is left COLD rather than aborting the process:
  // OOM failures get specific detection/metrics on top of this leave-cold infrastructure.
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
      uuid = json.at("uuid").get<utils::UUID>();
      rel_dir = json.at("rel_dir").get<std::filesystem::path>();
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

    // HOT: build + recover the storage. On failure, leave the tenant COLD instead of aborting.
    spdlog::info("Restoring database {} at {}.", name, rel_dir);
    bool recovered = false;
    // Classify the failure. OOM (out-of-memory during recovery — bad_alloc or the tracker's
    // OutOfMemoryException) is the case the boot-OOM safety valve exists for: the instance comes up
    // DEGRADED with the tenant left cold + loudly logged + marked in SHOW, rather than bricking.
    auto reason = SuspendedEntry::ColdReason::kRecoveryFailed;
    try {
      auto new_db = New_(name, uuid, nullptr, rel_dir);
      recovered = new_db.has_value();
      if (!recovered) {
        spdlog::error("Failed while recovering database {} (error {}); leaving it suspended (cold).",
                      name,
                      static_cast<int>(new_db.error()));
      }
    } catch (const utils::OutOfMemoryException &e) {
      reason = SuspendedEntry::ColdReason::kRecoveryFailedOom;
      spdlog::error(
          "OUT OF MEMORY while recovering database {} ({}). Leaving it SUSPENDED (cold): the instance is "
          "starting DEGRADED — this database is unavailable until memory is freed and it is RESUMEd (or the "
          "instance is restarted with more memory). SHOW DATABASES marks it 'recovery failed: out of memory'.",
          name,
          e.what());
    } catch (const std::bad_alloc &e) {
      reason = SuspendedEntry::ColdReason::kRecoveryFailedOom;
      spdlog::error(
          "OUT OF MEMORY (bad_alloc) while recovering database {} ({}). Leaving it SUSPENDED (cold); the "
          "instance is starting DEGRADED and this database is resumable once memory frees up.",
          name,
          e.what());
    } catch (const std::exception &e) {
      spdlog::error("Exception while recovering database {} ({}); leaving it suspended (cold).", name, e.what());
    }
    if (recovered) {
      spdlog::info("Database {} restored.", name);
    } else if (name == kDefaultDB) {
      // The default database is the system database (it backs auth, multi-tenancy metadata, etc.) and is
      // never suspendable. It MUST come up or the instance must fail loudly — leaving it cold would make
      // SetupDefault_ silently recreate it with a fresh UUID. A recovery failure here is fatal, exactly as
      // on a single-tenant instance.
      MG_ASSERT(recovered, "Failed while recovering the default database; aborting.");
    } else {
      // Leave-cold: emplace a COLD shell so the process stays up and the tenant is resumable later.
      // The instance starts DEGRADED rather than aborting. The durable entry is left HOT (no cold marker)
      // so a future restart retries HOT recovery; the metadata is rebuilt from defaults (no
      // heartbeat/cold_stats for a failed recovery). Tag the in-memory entry with WHY it is cold so SHOW
      // surfaces the degraded marker.
      //
      // make_cold_entry can throw nlohmann::json::type_error when an optional metadata field is
      // present but wrong-typed. Guard it: if it throws, erase the shell we just emplaced (no
      // orphan), mark the view incomplete, and continue — the same degradation pattern as the
      // parse guard above. std::bad_alloc / OOM are not caught here so they keep their propagation.
      db_handler_.EmplaceColdShell(name);
      try {
        auto entry = make_cold_entry(name, uuid, rel_dir, json);
        entry.cold_reason = reason;
        suspended_.insert_or_assign(std::string{name}, std::move(entry));
      } catch (const nlohmann::json::exception &e) {
        durability_view_incomplete = true;
        db_handler_.EraseColdShell(name);
        spdlog::warn(
            "HOT-recovery-failed tenant '{}' has a wrong-typed metadata field ({}); skipping cold-shell "
            "registration. Its on-disk data is left untouched and directory cleanup is SKIPPED this boot.",
            name,
            e.what());
        // Increment the boot-recovery failure metric before continuing — the degraded-boot event
        // must be counted even though the cold-shell registration was skipped.
        if (reason == SuspendedEntry::ColdReason::kRecoveryFailedOom) {
          metrics::Metrics().global.database_boot_recovery_oom_failures->Increment();
        } else {
          metrics::Metrics().global.database_boot_recovery_failures->Increment();
        }
        continue;
      }
      // Surface tenants left cold by a failed hot recovery; the OOM split mirrors the degraded marker
      // so ops can alert on a degraded boot.
      if (reason == SuspendedEntry::ColdReason::kRecoveryFailedOom) {
        metrics::Metrics().global.database_boot_recovery_oom_failures->Increment();
      } else {
        metrics::Metrics().global.database_boot_recovery_failures->Increment();
      }
    }
  }
  // Set the cold-tenant gauge ONCE after the restore loop (a per-iteration Set would be repeatedly
  // overwritten; no scrape endpoint is live during construction anyway). Covers both the cold-shell and
  // failed-recovery leave-cold paths above.
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

DbmsHandler::DeleteResult DbmsHandler::TryDelete(std::string_view db_name, system::Transaction *transaction) {
  auto wr = std::lock_guard{lock_};
  if (db_name == kDefaultDB) {
    // MSG cannot delete the default db
    return std::unexpected{DeleteError::DEFAULT_DB};
  }

  // Cold-tenant fast path: drop a COLD (suspended) tenant directly without going through the HOT
  // gatekeeper path (which returns NON_EXISTENT for a no-value shell).
  if (suspended_.contains(db_name)) {
    auto res = DeleteCold_(db_name);
    if (!res) return std::unexpected{res.error()};
    if (transaction) {
      transaction->AddAction<DropDatabase>(*res);
    }
    return {};
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
  if (suspended_.contains(db_name)) {
    auto res = DeleteCold_(db_name);
    if (!res) return std::unexpected{res.error()};
    if (transaction) {
      transaction->AddAction<DropDatabase>(*res);
    }
    return {};
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
  if (suspended_.contains(db_name)) {
    auto res = DeleteCold_(db_name);
    if (!res) return std::unexpected{res.error()};
    return {};
  }
  return Delete_(db_name);
}

DbmsHandler::DeleteResult DbmsHandler::Delete(utils::UUID uuid) {
  auto wr = std::lock_guard(lock_);
  // Cold-tenant fast path: scan suspended_ by uuid (replica apply path — no transaction arg).
  {
    auto it = FindSuspendedByUuid_(uuid);
    if (it != suspended_.end()) {
      auto res = DeleteCold_(it->first);
      if (!res) return std::unexpected{res.error()};
      return {};
    }
  }
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

  // A COLD (suspended) tenant cannot be renamed. Its gatekeeper is a no-value shell, so
  // db_handler_.Rename would move the shell and the subsequent Get(new_name) MG_ASSERT would abort
  // the process. Renaming a suspended tenant is not supported — RESUME it first.
  if (suspended_.contains(old_name)) {
    return std::unexpected{RenameError::SUSPENDED};
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
  // This MUST run with lock_ NOT held: Shutdown() joins consumer threads, and a consumer's in-flight batch
  // can re-enter db_handler_.Get() (a shared lock_) — joining while we hold lock_ would deadlock (and on a
  // replica's single-threaded apply thread it would stall all replication). We take a brief shared lock_
  // only to mint the accessor, then release it before joining.
  bool streams_stopped = false;
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
      streams_stopped = true;
    }
  }  // sacc released -> its count contribution is gone before Phase A mints the freeze accessor
  // UNDO guard: a failed/aborted suspend must NOT silently leave the streams stopped. Fires on every path
  // that does not commit the suspend (early return below, or a post-freeze throw after the abort_suspend
  // rollback restores HOT). Declared BEFORE the abort_suspend rollback so it fires AFTER it (LIFO) -> the
  // tenant is HOT again by the time we restore streams. Disabled once finish_suspend() commits the COLD.
  auto stream_restore = utils::OnScopeExit{[&] {
    if (!streams_stopped || !restore_streams_) return;
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
      // the tenant is already COLD on MAIN, and the consolidating snapshot at suspend is written
      // unconditionally and read back on resume, so the tenant stays recoverable.
      // Reachable only when this replica's durability differs from MAIN's (e.g. the WAL/snapshot config
      // is inconsistent across the cluster) — bypass with a loud warning rather than failing recovery
      // and stalling the replica in a BEHIND retry loop.
      spdlog::warn(
          "Force-suspending database {} during recovery although its local durability is incomplete "
          "(periodic snapshot + WAL not enabled here). It remains recoverable (the consolidating "
          "snapshot is still written), but this usually means the durability config differs "
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

  // Consolidating snapshot at suspend. The tenant is frozen (no in-flight txns), so this captures
  // the full committed state. On resume, recovery loads this snapshot and skips WAL files at/below
  // its durable timestamp -> near-zero WAL replay (fast reheat).
  //
  // Capture the InMemoryStorage pointer in a wider scope so DisableExitSnapshot() can be called
  // just before finish_suspend() (past the abort point-of-no-return). If DisableExitSnapshot were
  // called here and the suspend aborted between this point and rollback.Disable(), the rollback
  // re-HOTs the tenant with exit-snapshot permanently disabled (no EnableExitSnapshot() API).
  storage::InMemoryStorage *inmem_for_disable = nullptr;
  if (auto *inmem = dynamic_cast<storage::InMemoryStorage *>((*acc)->storage())) {
    using CreateSnapshotError = storage::InMemoryStorage::CreateSnapshotError;
    // The periodic snapshot scheduler is NOT paused by the freeze, so a periodic
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
      if (inmem->IsDurabilityCompleteForSuspend()) {
        spdlog::warn(
            "hot/cold suspend: consolidating snapshot for '{}' was not written ({}); durability "
            "intact via WAL, resume will replay the WAL delta.",
            name,
            storage::InMemoryStorage::CreateSnapshotErrorToString(snap.error()));
      } else {
        // On the recovery bypass path durability is incomplete (no WAL), so a failed consolidating
        // snapshot leaves NOTHING to recover from — do not claim a WAL fallback. The cold shell may
        // be unrecoverable; the replica re-syncs from MAIN on the next recovery round.
        spdlog::error(
            "hot/cold suspend (recovery): consolidating snapshot for '{}' was not written ({}) and "
            "durability is incomplete (no WAL fallback); the cold shell may be unrecoverable — the "
            "replica will re-sync from MAIN.",
            name,
            storage::InMemoryStorage::CreateSnapshotErrorToString(snap.error()));
      }
    }
    inmem_for_disable = inmem;  // valid until finish_suspend() destroys the storage
  }

  // Capture the last-hot stats snapshot POST-freeze (count==1, no concurrent commit can advance it
  // between this read and teardown). Served by cold SHOW STORAGE INFO / SHOW DATABASES.
  {
    auto *st = (*acc)->storage();
    entry.cold_stats = st->GetBaseInfo();
  }

  // PHASE C — record metadata (lock_), then heavy teardown OUTSIDE lock_.
  const auto tenant_uuid = entry.salient.uuid;  // capture before `entry` is moved into suspended_
  // Serialize the durable COLD marker from `entry` BEFORE it is moved into suspended_.
  std::string cold_val;
  if (durability_) {
    cold_val = Durability::GenColdVal(entry.salient.uuid, entry.rel_dir, entry.cold_stats);
  }
  acc.reset();  // drop our accessor -> count == 0 (state still SUSPENDING)
  {
    auto wr = std::lock_guard{lock_};
    suspended_.insert_or_assign(std::string{name}, std::move(entry));
    // The tenant is now COLD. Counts every successful Suspend_ (user SUSPEND, replica RPC apply, and
    // recovery force-suspend all funnel through here); the gauge tracks the live cold-set size.
    metrics::Metrics().global.database_suspends->Increment();
    UpdateColdGauge_();
    // Persist the durable COLD marker under the SAME lock_ as the suspended_ insert so a concurrent
    // Resume_ cannot observe a half state. A Put failure degrades to a warning and never rolls back
    // or throws (the in-memory teardown still proceeds; MAIN's data stays durable on disk).
    if (durability_) {
      if (!durability_->Put(Durability::GenKey(name), cold_val)) {
        spdlog::warn(
            "hot/cold suspend: failed to persist the cold durability marker for '{}'; the database is "
            "suspended in memory but will recover HOT on restart.",
            name);
      }
    }
  }

  // Disable the rollback guard BEFORE calling finish_suspend(). finish_suspend() transitions
  // SUSPENDING -> COLD; if the guard fired afterwards it would abort_suspend() on a non-SUSPENDING
  // state and trip the debug assert.
  rollback.Disable();

  // Disable exit snapshot NOW — past the abort point-of-no-return (rollback is disabled, so the
  // tenant is committed to teardown). The storage object lives in the gatekeeper until
  // finish_suspend() destroys it, so inmem_for_disable is still valid here. Doing this after
  // rollback.Disable() ensures an aborted suspend never leaves the tenant re-HOT with exit-snapshot
  // permanently disabled (there is no EnableExitSnapshot() API to undo it).
  if (inmem_for_disable) inmem_for_disable->DisableExitSnapshot();

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

  // Successful suspend: record end-to-end latency (includes the consolidating snapshot + teardown).
  metrics::Metrics().global.database_suspend_latency_seconds->Observe(
      std::chrono::duration<double>(std::chrono::steady_clock::now() - suspend_start).count());

  spdlog::info("hot/cold: database '{}' suspended (HOT -> COLD)", name);
  return {};
}

DbmsHandler::ResumeResult DbmsHandler::Resume_(std::string_view name, bool rewire_replication,
                                               system::Transaction *txn) {
  // Outer loop: a loser that observes a COLD-fallback (the winner aborted) restarts the whole
  // attempt from Phase A. Every restart re-initializes gk/salient/rel_dir/won_resume from the map
  // under a fresh shared lock. All early-exit paths (NON_EXISTENT, already-HOT, timeout, build
  // failure) are `return`s; only the COLD-fallback path `continue`s.
  // Bound the retry count: a transient single abort retries; a persistent on_resume_ failure is
  // surfaced as RECOVERY_FAILED instead of looping forever.
  constexpr int kMaxColdFallbackRestarts = 16;
  int cold_fallback_restarts = 0;
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
      if (auto a = db_handler_.Get(name)) return std::move(*a);  // already HOT (raced) — share its accessor
      gk = db_handler_.GetGatekeeper(name);                      // stable pointer to the in-map gatekeeper
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
    if (!won_resume) {
      constexpr auto kPollStep = std::chrono::milliseconds(5);
      // How long to wait after the LAST confirmed-RESUMING observation before concluding the winner
      // is dead. Large enough for a healthy rebuild under IO pressure, small enough to surface a
      // crashed winner before a client request timeout.
      constexpr auto kWinnerLivenessWindow = std::chrono::seconds(30);
      // Absolute ceiling from poll entry: bounds a stuck-alive winner (RESUMING forever).
      constexpr auto kMaxWait = std::chrono::minutes(10);
      const auto absolute_ceiling = std::chrono::steady_clock::now() + kMaxWait;
      auto deadline = std::chrono::steady_clock::now() + kWinnerLivenessWindow;
      while (std::chrono::steady_clock::now() < deadline && std::chrono::steady_clock::now() < absolute_ceiling) {
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
          if (!live_gk || live_gk->state() == utils::GatekeeperState::COLD) {
            retry_after_cold_fallback = true;
          } else if (live_gk->state() == utils::GatekeeperState::RESUMING) {
            // Winner is demonstrably still alive — extend the liveness deadline.
            deadline = std::chrono::steady_clock::now() + kWinnerLivenessWindow;
          }
        }
        if (retry_after_cold_fallback) break;  // exit poll loop; outer `continue` restarts Phase A
        std::this_thread::sleep_for(kPollStep);
      }
      {
        auto rd = std::shared_lock{lock_};
        auto *live_gk = db_handler_.GetGatekeeper(name);
        if (!live_gk) return std::unexpected{ResumeError::NON_EXISTENT};  // dropped mid-resume
        // HOT: the winner just published — share its accessor.
        if (auto a = db_handler_.Get(name)) return std::move(*a);
        // RESUMING: a second concurrent winner has taken over and is actively rebuilding — do not
        // return RECOVERY_FAILED for a live winner; fall through to the bounded retry instead.
        // COLD: the winner aborted and the COLD-fallback path restarts Phase A (bounded retry).
        // Any other state is treated as COLD (bounded retry).
        // Only RECOVERY_FAILED if the retry bound is exceeded (handled just below).
      }
      // Bound the retry — a single transient abort is retried; a persistent on_resume_ failure
      // (e.g. corrupt trigger metadata) is surfaced as RECOVERY_FAILED instead of looping forever.
      if (++cold_fallback_restarts > kMaxColdFallbackRestarts) return std::unexpected{ResumeError::RECOVERY_FAILED};
      continue;  // COLD-fallback confirmed (lock released): restart Phase A
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
          // cold entry carries no epoch to restore (see hot_cold_review_responses #20).
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
      } catch (...) {
        // PRE-PUBLISH failure: on_resume_ threw and the publish has NOT happened — `fresh` is still
        // local and HOT. Release `acc` BEFORE `fresh` unwinds so fresh's ~Gatekeeper sees count == 0
        // instead of self-deadlocking on the outstanding accessor, then roll back to COLD.
        acc.reset();
        gk->abort_resume();  // RESUMING -> COLD; suspended_ untouched => retriable
        return std::unexpected{ResumeError::RECOVERY_FAILED};
      }
      // Publish committed: the tenant is HOT and queryable. Record resume latency (recovery + publish)
      // before the best-effort post-publish bookkeeping below so that work is excluded from the metric.
      // Wrapped in its OWN try/catch like the sibling post-publish arms below: we are STILL inside the
      // outer try whose catch(...) calls abort_resume() on a now-HOT gatekeeper (DMG_ASSERT(state ==
      // RESUMING) -> terminate). Histogram::Observe takes a std::mutex lock, whose lock() may throw
      // std::system_error under OS resource exhaustion, so it must not be allowed to escape to that catch.
      try {
        metrics::Metrics().global.database_resume_latency_seconds->Observe(
            std::chrono::duration<double>(std::chrono::steady_clock::now() - resume_start).count());
      } catch (...) {
        spdlog::warn("hot/cold resume: recording resume latency for '{}' threw; database is live (HOT).", name);
      }

      // Flip the durable entry back to HOT (drop the cold marker) so a restart recovers it HOT.
      // Done in a SEPARATE short lock_ scope AFTER the publish. The write is guarded by
      // !suspended_.contains: if a SUSPEND raced in after our publish-erase and re-suspended the tenant,
      // its under-lock COLD marker must stand. Wrapped in its OWN try/catch (like the two arms below):
      // the gatekeeper is already HOT here, so any throw escaping to the outer catch(...) would call
      // abort_resume() on a HOT gatekeeper and trip its DMG_ASSERT(state == RESUMING) -> terminate.
      // GenKey/GenVal allocate (fmt/nlohmann) and can throw bad_alloc. A failure (bool or throw)
      // degrades to a warning: the tenant is live HOT regardless; on the next restart a stale cold
      // marker only makes it recover COLD, and it is resumable again.
      try {
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
      } catch (...) {
        spdlog::warn(
            "hot/cold resume: clearing the cold durability marker for '{}' threw; the database is live (HOT) "
            "and data is intact, but it may recover COLD on restart (resumable).",
            name);
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
            "hot/cold resume: replication re-wiring for database '{}' threw; database is live (HOT) and data is "
            "intact, but it may run un-replicated until replication is re-established.",
            name);
      }
      // Record the system action so the resume is ordered + replicated like CREATE/DROP DATABASE.
      // Reached only on the winner's successful publish; the replica wire is filled by DoReplication.
      // Wrapped in its own try/catch because the gatekeeper is already HOT after the publish block
      // above: any throw that escaped to the outer catch(...) would call abort_resume() on a HOT
      // gatekeeper and trip its DMG_ASSERT(state == RESUMING). The database is live regardless of
      // whether recording or logging succeeds here.
      try {
        if (txn) txn->AddAction<ResumeDatabase>(salient.uuid);
        spdlog::info("hot/cold: database '{}' resumed (COLD -> HOT)", name);
      } catch (...) {
        spdlog::warn(
            "hot/cold resume: recording the resume system action for '{}' threw; the database is live (HOT) "
            "and data is intact, but this resume may not replicate until the next system sync.",
            name);
      }
      return acc;
    } catch (...) {
      // Recovery (BuildDetached) threw before `acc`/`fresh` existed. No live accessor to release.
      gk->abort_resume();  // RESUMING -> COLD; suspended_ untouched => retriable
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
    bool found = false;
    for (auto &[n, db_gk] : db_handler_) {
      auto acc = db_gk.access();  // nullopt for a non-HOT (COLD) shell
      if (acc && acc->get()->uuid() == uuid) {
        name = n;
        found = true;
        break;
      }
    }
    if (!found) return std::unexpected{SuspendError::NON_EXISTENT};
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
  // Strong exception guarantee: copy into a local first, then commit with a noexcept move. A bad_alloc
  // during the copy leaves it->second untouched (no partial update).
  auto stats = meta.stats;
  it->second.cold_stats = std::move(stats);

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
