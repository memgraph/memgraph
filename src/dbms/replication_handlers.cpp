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

#include "dbms/replication_handlers.hpp"

#include "dbms/dbms_handler.hpp"
#include "dbms/rpc.hpp"
#include "license/license.hpp"
#include "system/state.hpp"

#include "rpc/utils.hpp"

namespace memgraph::rpc {
class FileReplicationHandler;
}  // namespace memgraph::rpc

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

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::CreateDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence, no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("CreateDatabaseHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // Create new
    if (auto const new_db = dbms_handler.Update(req.config); new_db.has_value()) {
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

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, memgraph::storage::replication::DropDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("DropDatabaseHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // NOTE: Single communication channel can exist at a time, no other database can be deleted/created at the moment.
    auto new_db = dbms_handler.Delete(req.uuid);
    if (!new_db) {
      if (new_db.error() == DeleteError::NON_EXISTENT) {
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

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, memgraph::storage::replication::RenameDatabaseReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("RenameDatabaseHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // NOTE: Single communication channel can exist at a time, no other database can be renamed/created/deleted at the
    // moment.
    auto rename_result = dbms_handler.Rename(req.old_name, req.new_name);
    if (!rename_result) {
      if (rename_result.error() == RenameError::NON_EXISTENT) {
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
        if (!dbms_handler.Update(config)) {
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
      if (!dbms_handler.Update(config)) {
        spdlog::debug("SystemRecoveryHandler: Failed to update database \"{}\".", *config.name.str_view());
        return false;
      }
    } catch (const UnknownDatabaseException &) {
      spdlog::debug("SystemRecoveryHandler: UnknownDatabaseException");
      return false;
    }
    std::erase(old, *config.name.str_view());
  }

  // Delete all the leftover old dbs
  for (const auto &remove_db : old) {
    const auto del = dbms_handler.Delete(remove_db);
    if (!del) {
      // Some errors are not terminal
      if (del.error() == DeleteError::DEFAULT_DB || del.error() == DeleteError::NON_EXISTENT) {
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

namespace {

void TenantProfileHandler(system::ReplicaHandlerAccessToState &system_state_access,
                          const std::optional<utils::UUID> &current_main_uuid, dbms::DbmsHandler &dbms_handler,
                          uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::TenantProfileReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  storage::replication::TenantProfileRes res(false);

  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error("Handling TenantProfile RPC without enterprise license.");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (current_main_uuid != req.main_uuid) {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::TenantProfileReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("TenantProfileHandler: bad expected timestamp");
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  using Action = storage::replication::TenantProfileReq::Action;
  res = storage::replication::TenantProfileRes(true);
  try {
    switch (req.action) {
      case Action::CREATE: {
        auto result = dbms_handler.CreateTenantProfile(req.profile.name, req.profile.memory_limit, /*sys_txn=*/nullptr);
        if (!result && result.error() == TenantProfiles::CreateError::DURABILITY_ERROR) {
          spdlog::error("TenantProfileHandler: CREATE for profile '{}' failed — KVStore I/O error", req.profile.name);
          res.success = false;
        }
        break;
      }
      case Action::ALTER: {
        auto result = dbms_handler.AlterTenantProfile(req.profile.name, req.profile.memory_limit, /*sys_txn=*/nullptr);
        if (!result) {
          if (result.error() == TenantProfiles::AlterError::NOT_FOUND) {
            spdlog::warn("TenantProfileHandler: ALTER for non-existent tenant profile '{}'", req.profile.name);
          } else {
            spdlog::error("TenantProfileHandler: ALTER for profile '{}' failed — KVStore I/O error", req.profile.name);
          }
          res.success = false;
        }
        break;
      }
      case Action::DROP: {
        auto result = dbms_handler.DropTenantProfile(req.profile.name, /*sys_txn=*/nullptr);
        if (!result) {
          switch (result.error()) {
            case TenantProfiles::DropError::NOT_FOUND:
              // Benign: replica may have already pruned this profile or never had it.
              break;
            case TenantProfiles::DropError::HAS_ATTACHED_DATABASES:
              spdlog::warn("TenantProfileHandler: DROP for profile '{}' rejected — has attached databases",
                           req.profile.name);
              res.success = false;
              break;
            case TenantProfiles::DropError::DURABILITY_ERROR:
              spdlog::error("TenantProfileHandler: DROP for profile '{}' failed — KVStore I/O error", req.profile.name);
              res.success = false;
              break;
          }
        }
        break;
      }
      case Action::SET_ON_DATABASE: {
        auto result = dbms_handler.SetTenantProfileOnDatabase(req.profile.name, req.db_name, /*sys_txn=*/nullptr);
        if (!result) {
          if (result.error() == TenantProfiles::AttachError::PROFILE_NOT_FOUND) {
            spdlog::warn("TenantProfileHandler: SET_ON_DATABASE for non-existent tenant profile '{}'",
                         req.profile.name);
          } else {
            spdlog::error("TenantProfileHandler: SET_ON_DATABASE for profile '{}' failed — KVStore I/O error",
                          req.profile.name);
          }
          res.success = false;
        }
        break;
      }
      case Action::REMOVE_FROM_DATABASE: {
        auto result = dbms_handler.RemoveTenantProfileFromDatabase(req.db_name, /*sys_txn=*/nullptr);
        if (!result) {
          if (result.error() == TenantProfiles::DetachError::DURABILITY_ERROR) {
            spdlog::error("TenantProfileHandler: REMOVE_FROM_DATABASE for db '{}' failed — KVStore I/O error",
                          req.db_name);
            res.success = false;
          } else {
            spdlog::warn("TenantProfileHandler: REMOVE_FROM_DATABASE — db '{}' not attached to any profile on replica",
                         req.db_name);
          }
        }
        break;
      }
      default:
        spdlog::warn("TenantProfileHandler: unknown action {}", static_cast<uint8_t>(req.action));
        res.success = false;
        break;
    }
  } catch (const std::exception &e) {
    spdlog::warn("TenantProfileHandler failed: {}", e.what());
    res.success = false;
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

}  // namespace

void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              dbms::DbmsHandler &dbms_handler) {
  // NOTE: Register even without license as the user could add a license at run-time
  data.server->rpc_server_.Register<storage::replication::CreateDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        CreateDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::DropDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        DropDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::RenameDatabaseRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        RenameDatabaseHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::TenantProfileRpc>(
      [&data, system_state_access, &dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) mutable {
        TenantProfileHandler(system_state_access, data.uuid_, dbms_handler, request_version, req_reader, res_builder);
      });
}
#endif
}  // namespace memgraph::dbms
