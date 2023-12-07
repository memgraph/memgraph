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

#include <iterator>
#include "license/license.hpp"
#include "spdlog/spdlog.h"
#include "utils/exceptions.hpp"
#ifdef MG_ENTERPRISE

#include "dbms/dbms_handler.hpp"

#include <cstdint>
#include <filesystem>

#include "dbms/constants.hpp"
#include "utils/uuid.hpp"

namespace {
constexpr std::string_view kDBPrefix = "database:";  // Key prefix for database durability
}  // namespace

namespace memgraph::dbms {

namespace replication {

struct CreateDatabaseReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CreateDatabaseReq *self, memgraph::slk::Reader *reader);
  static void Save(const CreateDatabaseReq &self, memgraph::slk::Builder *builder);
  CreateDatabaseReq() = default;
  // ABCReq() {}
};

constexpr utils::TypeInfo CreateDatabaseReq::kType{utils::TypeId::REP_CREATEDATABASE_REQ, "CreateDatabaseReq", nullptr};

struct CreateDatabaseRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CreateDatabaseRes *self, memgraph::slk::Reader *reader);
  static void Save(const CreateDatabaseRes &self, memgraph::slk::Builder *builder);
  CreateDatabaseRes() = default;
  CreateDatabaseRes(bool success) : success(success) {}

  bool success;
  std::string epoch_id;      // Indicates history around who was the replication group's MAIN
  uint64_t group_timestamp;  // System event in group governed by group_clock
};
constexpr utils::TypeInfo CreateDatabaseRes::kType{utils::TypeId::REP_CREATEDATABASE_RES, "CreateDatabaseRes", nullptr};

using CreateDatabaseRpc = rpc::RequestResponse<CreateDatabaseReq, CreateDatabaseRes>;

}  // namespace replication

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

  static auto GenVal(utils::UUID uuid) {
    nlohmann::json json;
    json["uuid"] = uuid;
    // TODO: Serialize the configuration
    return json.dump();
  }

  static void Migrate(kvstore::KVStore *durability, const std::filesystem::path &storage_dir) {
    const auto ver_val = durability->Get("version");
    const auto ver = VersionCheck(ver_val);

    std::map<std::string, std::string> to_put;
    std::vector<std::string> to_delete;

    // Update from V0 to V1
    if (ver == DurabilityVersion::V0) {
      for (const auto &[key, val] : *durability) {
        if (key == "version") continue;   // Reserved key
        if (key == kDefaultDB) continue;  // Already set
        // Generate a UUID
        // move directory to new UUID dir
        // generate json and update value
        auto const uuid = utils::UUID();
        std::filesystem::path old_dir(storage_dir / key);
        std::filesystem::rename(old_dir, storage_dir / std::string{uuid});
        auto new_key = GenKey(key);
        auto new_data = GenVal(uuid);
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
  storage::UpdatePaths(default_config_, default_config_.durability.storage_directory / "databases");
  const auto &db_dir = default_config_.durability.storage_directory;
  const auto durability_dir = db_dir / ".durability";
  utils::EnsureDirOrDie(db_dir);
  utils::EnsureDirOrDie(durability_dir);
  durability_ = std::make_unique<kvstore::KVStore>(durability_dir);

  // Migrate durability
  Durability::Migrate(durability_.get(), default_config_.durability.storage_directory);

  // Generate the default database

  auto default_config_json = durability_->Get(Durability::GenKey(kDefaultDB));
  if (!default_config_json) {
    // Nothing to restore, use new uuid
    MG_ASSERT(!NewDefault_().HasError(), "Failed while creating the default DB.");
  } else {
    // Restore with correct uuid
    auto json = nlohmann::json::parse(*default_config_json);
    const auto uuid = json.at("uuid").get<utils::UUID>();
    MG_ASSERT(!NewDefault_(uuid).HasError(), "Failed while creating the default DB.");
  }

  auto directories = std::set{std::string{kDefaultDB}};

  // Recover previous databases
  if (recovery_on_startup) {
    auto it = durability_->begin(std::string(kDBPrefix));
    auto end = durability_->end(std::string(kDBPrefix));
    for (; it != end; ++it) {
      const auto &[key, config_json] = *it;
      const auto name = key.substr(kDBPrefix.size());
      if (name == kDefaultDB) continue;  // Already set
      auto json = nlohmann::json::parse(config_json);
      const auto uuid = json.at("uuid").get<utils::UUID>();
      spdlog::info("Restoring database {} at {}.", name, std::string{uuid});
      MG_ASSERT(!New_(name, uuid).HasError(), "Failed while creating database {}.", name);
      directories.emplace(uuid);
      spdlog::info("Database {} restored.", name);
    }
  } else {  // Clear databases from the durability list and auth
    auto locked_auth = auth->Lock();
    auto it = durability_->begin("database");
    auto end = durability_->end("database");
    for (; it != end; ++it) {
      const auto &[key, _] = *it;
      const auto name = key.substr(kDBPrefix.size());
      if (name == kDefaultDB) continue;
      locked_auth->DeleteDatabase(name);
      durability_->Delete(key);
    }
  }

  // Clean the unused directories
  for (const auto &entry : std::filesystem::directory_iterator(default_config_.durability.storage_directory)) {
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

  if (default_config_.durability.snapshot_wal_mode == storage::Config::Durability::SnapshotWalMode::DISABLED &&
      repl_state_.IsMain()) {
    spdlog::warn(
        "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please "
        "consider "
        "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
        "without write-ahead logs this instance is not replicating any data.");
  }

  // Startup replication state (if recovered at startup)
  auto replica = [this](replication::RoleReplicaData const &data) {
    // Register handlers
    InMemoryReplicationHandlers::Register(this, *data.server);
    // TODO: Add more handlers -> system
    if (!data.server->Start()) {
      spdlog::error("Unable to start the replication server.");
      return false;
    }
    return true;
  };
  // Replication frequent check start
  auto main = [this](replication::RoleMainData &data) {
    for (auto &client : data.registered_replicas_) {
      StartReplicaClient(*this, client);
    }
    return true;
  };
  // Startup proccess for main/replica
  MG_ASSERT(std::visit(memgraph::utils::Overloaded{replica, main}, repl_state_.ReplicationData()),
            "Replica recovery failure!");
}

void DbmsHandler::EnsureReplicaHasDatabase(const storage::Config &config) {
  // Only on MAIN -> make replica have it
  // Restore on MAIN -> replica also has it (noop)
  // TODO: have to strip MAIN relevent info out, REPLICA will add its
  //  path prefix

  auto main_handler = [](memgraph::replication::RoleMainData &main_data) {
    // TODO: datarace issue? registered_replicas_ access not protected
    for (memgraph::replication::ReplicationClient &client : main_data.registered_replicas_) {
      try {
        auto stream = client.rpc_client_.Stream<replication::CreateDatabaseRpc>(
            main_data.epoch_,
            0,  // current_group_clock,//TODO: make actual clock
            config);

        auto stream = client.rpc_client_.Stream<replication::CreateDatabaseRpc>();

        // already uptodate -- storage recovery
        // done
        // failure

        const auto response = stream.AwaitResponse();
        if (!response.success) {
          // This replica needs SYSTEM recovery
          continue;
        }

        // TODO: more thinking....about error handling (out of sync)
        //  + response.epoch_id; also

        // CHECK: if this is memgraph recovery, MAIN and REPLICA will have same group_clock
        //        hence no need to ensure database with `config` exists on REPLICA (because it already does)
        if (response.group_timestamp == current_group_clock) continue;

        // CHECK: this should be the next group_clock event
        if (response.group_timestamp != current_group_clock - 1) {
          // This replica needs SYSTEM recovery,
          continue;
        }

      } catch (memgraph::rpc::GenericRpcFailedException const &e) {
        // This replica needs SYSTEM recovery
      }
      auto stream = client.rpc_client_.Stream<replication::CreateDatabase>(config);
      const auto response2 = stream.AwaitResponse();
    }
  };
  auto replica_handler = [](memgraph::replication::RoleReplicaData &) {};

  std::visit(utils::Overloaded{main_handler, replica_handler}, repl_state_.ReplicationData());
}

DbmsHandler::NewResultT DbmsHandler::New_(storage::Config &&storage_config) {
  auto new_db = db_handler_.New(storage_config, repl_state_);

  if (new_db.HasValue()) {  // Success
    // Recover replication (if durable)
    if (storage_config.name != kDefaultDB) {
      EnsureReplicaHasDatabase(storage_config);

    } else {
      // TODO:
      // set REPLICA kDefaultDB uuid / config
    }
    RecoverReplication(new_db.GetValue());
    // Save database in a list of active databases
    const auto &key = Durability::GenKey(storage_config.name);
    const auto &val = Durability::GenVal(storage_config.uuid);
    if (durability_) durability_->Put(key, val);
    return new_db.GetValue();
  }
  return new_db.GetError();
}
DbmsHandler::DeleteResult DbmsHandler::TryDelete(std::string_view db_name) {
  std::lock_guard<LockT> wr(lock_);
  if (db_name == kDefaultDB) {
    // MSG cannot delete the default db
    return DeleteError::DEFAULT_DB;
  }

  const auto storage_path = StorageDir_(db_name);
  if (!storage_path) return DeleteError::NON_EXISTENT;

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
  (void)std::filesystem::remove_all(*storage_path, ec);
  if (ec) {
    spdlog::error(R"(Failed to clean disk while deleting database "{}" stored in {})", db_name, *storage_path);
  }
  return {};  // Success
}

DbmsHandler::DeleteResult DbmsHandler::Delete(std::string_view db_name) {
  auto wr = std::lock_guard(lock_);
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
  db_handler_.DeferDelete(db_name, [storage_path = *storage_path, db_name]() {
    // Delete disk storage
    std::error_code ec;
    (void)std::filesystem::remove_all(storage_path, ec);
    if (ec) {
      spdlog::error(R"(Failed to clean disk while deleting database "{}" stored in {})", db_name, storage_path);
    }
  });

  return {};  // Success
}

}  // namespace memgraph::dbms

#endif
