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

#include "replication_handler/system_replication.hpp"

#include <spdlog/spdlog.h>

#include "auth/replication_handlers.hpp"
#include "dbms/replication_handlers.hpp"
#include "license/license.hpp"
#include "replication_handler/system_rpc.hpp"

namespace memgraph::replication {

#ifdef MG_ENTERPRISE
void SystemHeartbeatHandler(const uint64_t ts, slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::SystemHeartbeatRes res{0};

  // Ignore if no license
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    spdlog::error("Handling SystemHeartbeat, an enterprise RPC message, without license.");
    memgraph::slk::Save(res, res_builder);
    return;
  }

  replication::SystemHeartbeatReq req;
  replication::SystemHeartbeatReq::Load(&req, req_reader);

  res = replication::SystemHeartbeatRes{ts};
  memgraph::slk::Save(res, res_builder);
}

void SystemRecoveryHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           dbms::DbmsHandler &dbms_handler, auth::SynchedAuth &auth, slk::Reader *req_reader,
                           slk::Builder *res_builder) {
  using memgraph::replication::SystemRecoveryRes;
  SystemRecoveryRes res(SystemRecoveryRes::Result::FAILURE);

  utils::OnScopeExit send_on_exit([&]() { memgraph::slk::Save(res, res_builder); });

  memgraph::replication::SystemRecoveryReq req;
  memgraph::slk::Load(&req, req_reader);

  /*
   * DBMS
   */
  if (!dbms::SystemRecoveryHandler(dbms_handler, req.database_configs)) return;  // Failure sent on exit

  /*
   * AUTH
   */
  if (!auth::SystemRecoveryHandler(auth, req.auth_config, req.users, req.roles)) return;  // Failure sent on exit

  /*
   * SUCCESSFUL RECOVERY
   */
  system_state_access.SetLastCommitedTS(req.forced_group_timestamp);
  spdlog::debug("SystemRecoveryHandler: SUCCESS updated LCTS to {}", req.forced_group_timestamp);
  res = SystemRecoveryRes(SystemRecoveryRes::Result::SUCCESS);
}

void Register(replication::RoleReplicaData const &data, dbms::DbmsHandler &dbms_handler, auth::SynchedAuth &auth) {
  // NOTE: Register even without license as the user could add a license at run-time
  // TODO: fix Register when system is removed from DbmsHandler

  auto system_state_access = dbms_handler.system_->CreateSystemStateAccess();

  // System
  data.server->rpc_server_.Register<replication::SystemHeartbeatRpc>(
      [system_state_access](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received SystemHeartbeatRpc");
        SystemHeartbeatHandler(system_state_access.LastCommitedTS(), req_reader, res_builder);
      });
  data.server->rpc_server_.Register<replication::SystemRecoveryRpc>(
      [system_state_access, &dbms_handler, &auth](auto *req_reader, auto *res_builder) mutable {
        spdlog::debug("Received SystemRecoveryRpc");
        SystemRecoveryHandler(system_state_access, dbms_handler, auth, req_reader, res_builder);
      });

  // DBMS
  dbms::Register(data, system_state_access, dbms_handler);

  // Auth
  auth::Register(data, system_state_access, auth);
}
#endif

#ifdef MG_ENTERPRISE
bool StartRpcServer(dbms::DbmsHandler &dbms_handler, const replication::RoleReplicaData &data,
                    auth::SynchedAuth &auth) {
#else
bool StartRpcServer(dbms::DbmsHandler &dbms_handler, const replication::RoleReplicaData &data) {
#endif
  // Register storage handlers
  dbms::InMemoryReplicationHandlers::Register(&dbms_handler, *data.server);
#ifdef MG_ENTERPRISE
  // Register system handlers
  Register(data, dbms_handler, auth);
#endif
  // Start server
  if (!data.server->Start()) {
    spdlog::error("Unable to start the replication server.");
    return false;
  }
  return true;
}
}  // namespace memgraph::replication
