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

#include "dbms/replication_handlers.hpp"

#include "dbms/database.hpp"
#include "dbms/dbms_handler.hpp"
#include "storage/v2/storage.hpp"
#include "system/state.hpp"

namespace memgraph::dbms {

#ifdef MG_ENTERPRISE

void CreateDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           DbmsHandler &dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder) {
  using memgraph::storage::replication::CreateDatabaseRes;
  CreateDatabaseRes res(CreateDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error("Handling CreateDatabase, an enterprise RPC message, without license.");
    memgraph::slk::Save(res, res_builder);
    return;
  }

  memgraph::storage::replication::CreateDatabaseReq req;
  memgraph::slk::Load(&req, req_reader);

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("CreateDatabaseHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    memgraph::slk::Save(res, res_builder);
    return;
  }

  try {
    // Create new
    auto new_db = dbms_handler.Update(req.config);
    if (new_db.HasValue()) {
      // Successfully create db
      system_state_access.SetLastCommitedTS(req.new_group_timestamp);
      res = CreateDatabaseRes(CreateDatabaseRes::Result::SUCCESS);
      spdlog::debug("CreateDatabaseHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
    }
  } catch (...) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}

void DropDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access, DbmsHandler &dbms_handler,
                         slk::Reader *req_reader, slk::Builder *res_builder) {
  using memgraph::storage::replication::DropDatabaseRes;
  DropDatabaseRes res(DropDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error("Handling DropDatabase, an enterprise RPC message, without license.");
    memgraph::slk::Save(res, res_builder);
    return;
  }

  memgraph::storage::replication::DropDatabaseReq req;
  memgraph::slk::Load(&req, req_reader);

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("DropDatabaseHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    memgraph::slk::Save(res, res_builder);
    return;
  }

  try {
    // NOTE: Single communication channel can exist at a time, no other database can be deleted/created at the moment.
    auto new_db = dbms_handler.Delete(req.uuid);
    if (new_db.HasError()) {
      if (new_db.GetError() == DeleteError::NON_EXISTENT) {
        // Nothing to drop
        system_state_access.SetLastCommitedTS(req.new_group_timestamp);
        res = DropDatabaseRes(DropDatabaseRes::Result::NO_NEED);
      }
    } else {
      // Successfully drop db
      system_state_access.SetLastCommitedTS(req.new_group_timestamp);
      res = DropDatabaseRes(DropDatabaseRes::Result::SUCCESS);
      spdlog::debug("DropDatabaseHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
    }
  } catch (...) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}

bool SystemRecoveryHandler(DbmsHandler &dbms_handler, const std::vector<storage::SalientConfig> &database_configs) {
  /*
   * NO LICENSE
   */
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error("Handling SystemRecovery, an enterprise RPC message, without license.");
    for (const auto &config : database_configs) {
      // Only handle default DB
      if (config.name != kDefaultDB) continue;
      try {
        if (dbms_handler.Update(config).HasError()) {
          return false;
        }
      } catch (const UnknownDatabaseException &) {
        return false;
      }
    }
    return true;
  }

  /*
   * MULTI-TENANCY
   */
  // Get all current dbs
  auto old = dbms_handler.All();
  // Check/create the incoming dbs
  for (const auto &config : database_configs) {
    // Missing db
    try {
      if (dbms_handler.Update(config).HasError()) {
        spdlog::debug("SystemRecoveryHandler: Failed to update database \"{}\".", config.name);
        return false;
      }
    } catch (const UnknownDatabaseException &) {
      spdlog::debug("SystemRecoveryHandler: UnknownDatabaseException");
      return false;
    }
    const auto it = std::find(old.begin(), old.end(), config.name);
    if (it != old.end()) old.erase(it);
  }

  // Delete all the leftover old dbs
  for (const auto &remove_db : old) {
    const auto del = dbms_handler.Delete(remove_db);
    if (del.HasError()) {
      // Some errors are not terminal
      if (del.GetError() == DeleteError::DEFAULT_DB || del.GetError() == DeleteError::NON_EXISTENT) {
        spdlog::debug("SystemRecoveryHandler: Dropped database \"{}\".", remove_db);
        continue;
      }
      spdlog::debug("SystemRecoveryHandler: Failed to drop database \"{}\".", remove_db);
      return false;
    }
  }

  /*
   * SUCCESS
   */
  return true;
}

void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              dbms::DbmsHandler &dbms_handler) {
  // NOTE: Register even without license as the user could add a license at run-time
  data.server->rpc_server_.Register<storage::replication::CreateDatabaseRpc>(
      [system_state_access, &dbms_handler](auto *req_reader, auto *res_builder) mutable {
        spdlog::debug("Received CreateDatabaseRpc");
        CreateDatabaseHandler(system_state_access, dbms_handler, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::DropDatabaseRpc>(
      [system_state_access, &dbms_handler](auto *req_reader, auto *res_builder) mutable {
        spdlog::debug("Received DropDatabaseRpc");
        DropDatabaseHandler(system_state_access, dbms_handler, req_reader, res_builder);
      });
}
#endif
}  // namespace memgraph::dbms
