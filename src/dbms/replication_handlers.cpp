// Copyright 2025 Memgraph Ltd.
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
#include "dbms/rpc.hpp"
#include "storage/v2/storage.hpp"
#include "system/state.hpp"

#include "rpc/file_replication_handler.hpp"
#include "rpc/utils.hpp"

namespace memgraph::dbms {

#ifdef MG_ENTERPRISE

void CreateDatabaseHandler(system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                           uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using storage::replication::CreateDatabaseRes;
  CreateDatabaseRes res(CreateDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling CreateDatabase, an enterprise RPC message, without license. Check your license status by running "
        "SHOW LICENSE INFO.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  storage::replication::CreateDatabaseReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::CreateDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence, no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("CreateDatabaseHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // Create new
    if (auto const new_db = dbms_handler.Update(req.config); new_db.HasValue()) {
      // Successfully create db
      res = CreateDatabaseRes(CreateDatabaseRes::Result::SUCCESS);
      spdlog::debug("CreateDatabaseHandler: SUCCESS");
    }
  } catch (...) {
    // Failure
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

void DropDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                         const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                         uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using memgraph::storage::replication::DropDatabaseRes;
  DropDatabaseRes res(DropDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling DropDatabase, an enterprise RPC message, without license. Check your license status by running SHOW "
        "LICENSE INFO.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  memgraph::storage::replication::DropDatabaseReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, memgraph::storage::replication::DropDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("DropDatabaseHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // NOTE: Single communication channel can exist at a time, no other database can be deleted/created at the moment.
    auto new_db = dbms_handler.Delete(req.uuid);
    if (new_db.HasError()) {
      if (new_db.GetError() == DeleteError::NON_EXISTENT) {
        // Nothing to drop
        res = DropDatabaseRes(DropDatabaseRes::Result::NO_NEED);
      }
    } else {
      // Successfully drop db
      res = DropDatabaseRes(DropDatabaseRes::Result::SUCCESS);
      spdlog::debug("DropDatabaseHandler: SUCCESS");
    }
  } catch (...) {
    // Failure
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

void RenameDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                           uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using memgraph::storage::replication::RenameDatabaseRes;
  RenameDatabaseRes res(RenameDatabaseRes::Result::FAILURE);

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling RenameDatabase, an enterprise RPC message, without license. Check your license status by running "
        "SHOW "
        "LICENSE INFO.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  memgraph::storage::replication::RenameDatabaseReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, memgraph::storage::replication::RenameDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("RenameDatabaseHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // NOTE: Single communication channel can exist at a time, no other database can be renamed/created/deleted at the
    // moment.
    auto rename_result = dbms_handler.Rename(req.old_name, req.new_name);
    if (rename_result.HasError()) {
      if (rename_result.GetError() == RenameError::NON_EXISTENT) {
        // Nothing to rename
        system_state_access.SetLastCommitedTS(req.new_group_timestamp);
        res = RenameDatabaseRes(RenameDatabaseRes::Result::NO_NEED);
      }
    } else {
      // Successfully renamed db
      system_state_access.SetLastCommitedTS(req.new_group_timestamp);
      res = RenameDatabaseRes(RenameDatabaseRes::Result::SUCCESS);
      spdlog::debug("RenameDatabaseHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
    }
  } catch (...) {
    // Failure
    spdlog::trace(R"(RenameDatabaseHandler: Failed to rename database "{}" to "{}".)", req.old_name, req.new_name);
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

bool SystemRecoveryHandler(DbmsHandler &dbms_handler, const std::vector<storage::SalientConfig> &database_configs) {
  /*
   * NO LICENSE
   */
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error(
        "Handling SystemRecovery, an enterprise RPC message, without license. Check your license status by running "
        "SHOW LICENSE INFO.");
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
        spdlog::debug("SystemRecoveryHandler: Failed to update database \"{}\".", *config.name.str_view());
        return false;
      }
    } catch (const UnknownDatabaseException &) {
      spdlog::debug("SystemRecoveryHandler: UnknownDatabaseException");
      return false;
    }
    const auto it = std::find(old.begin(), old.end(), *config.name.str_view());
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
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version, auto *req_reader, auto *res_builder) mutable {
        CreateDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::DropDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version, auto *req_reader, auto *res_builder) mutable {
        DropDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::RenameDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version, auto *req_reader, auto *res_builder) mutable {
        RenameDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
}
#endif
}  // namespace memgraph::dbms
