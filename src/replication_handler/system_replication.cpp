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

#include "replication_handler/system_replication.hpp"

#include <spdlog/spdlog.h>

#include "auth/replication_handlers.hpp"
#include "dbms/replication_handlers.hpp"
#include "replication_handler/system_rpc.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen
#include "system/rpc.hpp"

namespace memgraph::replication {

#ifdef MG_ENTERPRISE

void SystemRecoveryHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, dbms::DbmsHandler &dbms_handler,
                           auth::SynchedAuth &auth, uint64_t const request_version, slk::Reader *req_reader,
                           slk::Builder *res_builder) {
  using memgraph::replication::SystemRecoveryRes;
  SystemRecoveryRes res(SystemRecoveryRes::Result::FAILURE);

  utils::OnScopeExit const send_on_exit([&]() { rpc::SendFinalResponse(res, res_builder); });

  memgraph::replication::SystemRecoveryReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  // validate
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, SystemRecoveryReq::kType.name);
    return;
  }

  /*
   * DBMS
   */
  if (!dbms::SystemRecoveryHandler(dbms_handler, req.database_configs)) return;  // Failure sent on exit

  /*
   * AUTH
   */
  if (!auth::SystemRecoveryHandler(auth, req.auth_config, req.users, req.roles, req.profiles))
    return;  // Failure sent on exit

  /*
   * SUCCESSFUL RECOVERY
   */
  system_state_access.SetLastCommitedTS(req.forced_group_timestamp);
  spdlog::debug("SystemRecoveryHandler: SUCCESS updated LCTS to {}", req.forced_group_timestamp);
  res = SystemRecoveryRes(SystemRecoveryRes::Result::SUCCESS);
}

void FinalizeSystemTxHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                             const std::optional<utils::UUID> &current_main_uuid, slk::Reader *req_reader,
                             slk::Builder *res_builder) {
  using memgraph::replication::FinalizeSystemTxRes;
  FinalizeSystemTxRes res(false);

  utils::OnScopeExit const send_on_exit([&]() { rpc::SendFinalResponse(res, res_builder); });

  memgraph::replication::FinalizeSystemTxReq req;
  memgraph::slk::Load(&req, req_reader);

  // validate MAIN
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, SystemRecoveryReq::kType.name);
    return;
  }

  // validate delta
  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.
  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::error("Received system delta with expected ts: {} != last commited ts: {}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    return;
  }

  system_state_access.SetLastCommitedTS(req.new_group_timestamp);
  spdlog::debug("FinalizeSystemTxHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
  res = FinalizeSystemTxRes(true);
}

void Register(replication::RoleReplicaData const &data, system::System &system, dbms::DbmsHandler &dbms_handler,
              auth::SynchedAuth &auth) {
  // NOTE: Register even without license as the user could add a license at run-time

  auto system_state_access = system.CreateSystemStateAccess();

  // need to tell REPLICA the uuid to use for "memgraph" default database
  data.server->rpc_server_.Register<replication::SystemRecoveryRpc>(
      [&data, system_state_access, &dbms_handler, &auth](uint64_t const request_version, auto *req_reader,
                                                         auto *res_builder) mutable {
        SystemRecoveryHandler(system_state_access, data.uuid_, dbms_handler, auth, request_version, req_reader,
                              res_builder);
      });

  // Generic finalize message
  data.server->rpc_server_.Register<replication::FinalizeSystemTxRpc>(
      [&data, system_state_access](auto *req_reader, auto *res_builder) mutable {
        FinalizeSystemTxHandler(system_state_access, data.uuid_, req_reader, res_builder);
      });

  // DBMS
  dbms::Register(data, system_state_access, dbms_handler);

  // Auth
  auth::Register(data, system_state_access, auth);
}
#endif

#ifdef MG_ENTERPRISE
bool StartRpcServer(dbms::DbmsHandler &dbms_handler, replication::RoleReplicaData &data, auth::SynchedAuth &auth,
                    system::System &system) {
#else
bool StartRpcServer(dbms::DbmsHandler &dbms_handler, replication::RoleReplicaData &data) {
#endif
  // Register storage handlers
  dbms::InMemoryReplicationHandlers::Register(&dbms_handler, data);
#ifdef MG_ENTERPRISE
  // Register system handlers
  Register(data, system, dbms_handler, auth);
#endif
  // Start server
  if (!data.server->Start()) {
    spdlog::error("Unable to start the replication server.");
    return false;
  }
  return true;
}

}  // namespace memgraph::replication
