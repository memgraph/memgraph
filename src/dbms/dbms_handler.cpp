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

#include "dbms/dbms_handler.hpp"
#include "replication/state.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::dbms {
#ifdef MG_ENTERPRISE
DbmsHandler::DbmsHandler(
    storage::Config config,
    memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
    bool recovery_on_startup, bool delete_on_drop)
    : default_config_{std::move(config)},
      delete_on_drop_(delete_on_drop),
      repl_state_{ReplicationStateRootPath(default_config_)} {
  // TODO: Decouple storage config from dbms config
  // TODO: Save individual db configs inside the kvstore and restore from there
  storage::UpdatePaths(default_config_, default_config_.durability.storage_directory / "databases");
  const auto &db_dir = default_config_.durability.storage_directory;
  const auto durability_dir = db_dir / ".durability";
  utils::EnsureDirOrDie(db_dir);
  utils::EnsureDirOrDie(durability_dir);
  durability_ = std::make_unique<kvstore::KVStore>(durability_dir);

  // Generate the default database
  MG_ASSERT(!NewDefault_().HasError(), "Failed while creating the default DB.");

  // Recover previous databases
  if (recovery_on_startup) {
    for (const auto &[name, _] : *durability_) {
      if (name == kDefaultDB) continue;  // Already set
      spdlog::info("Restoring database {}.", name);
      MG_ASSERT(!New_(name).HasError(), "Failed while creating database {}.", name);
      spdlog::info("Database {} restored.", name);
    }
  } else {  // Clear databases from the durability list and auth
    auto locked_auth = auth->Lock();
    for (const auto &[name, _] : *durability_) {
      if (name == kDefaultDB) continue;
      locked_auth->DeleteDatabase(name);
      durability_->Delete(name);
    }
  }

  // Startup replication state (if recovered at startup)
  auto replica = [this](replication::RoleReplicaData const &data) {
    // Register handlers
    InMemoryReplicationHandlers::Register(this, *data.server_);
    if (!data.server_->Start()) {
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
  auto coordinator = [](replication::RoleCoordinatorData &) -> bool {
    throw utils::NotYetImplemented("Not yet implemented");
  };
  // Startup proccess for main/replica
  MG_ASSERT(std::visit(memgraph::utils::Overloaded{replica, main, coordinator}, repl_state_.ReplicationData()),
            "Replica recovery failure!");
}
#endif

}  // namespace memgraph::dbms
