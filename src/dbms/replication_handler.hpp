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

#pragma once

#include "auth/auth.hpp"
#include "dbms/database.hpp"
#include "query/replication_query_handler.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/result.hpp"

namespace memgraph::dbms {

class DbmsHandler;

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
/// TODO: extend to do multiple storages
struct ReplicationHandler : public query::ReplicationQueryHandler {
  explicit ReplicationHandler(DbmsHandler &dbms_handler, auth::SynchedAuth &auth);

  // as REPLICA, become MAIN
  bool SetReplicationRoleMain() override;

  // as MAIN, become REPLICA
  bool SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config) override;

  // as MAIN, define and connect to REPLICAs
  auto RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
      -> utils::BasicResult<query::RegisterReplicaError> override;

  // as MAIN, remove a REPLICA connection
  auto UnregisterReplica(std::string_view name) -> query::UnregisterReplicaResult override;

  // Helper pass-through (TODO: remove)
  auto GetRole() const -> memgraph::replication_coordination_glue::ReplicationRole override;
  bool IsMain() const override;
  bool IsReplica() const override;

 private:
  DbmsHandler &dbms_handler_;
  auth::SynchedAuth &auth_;
};

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
/// TODO: extend to do multiple storages
void RestoreReplication(replication::ReplicationState &repl_state, DatabaseAccess db_acc);

namespace system_replication {
// System handlers
#ifdef MG_ENTERPRISE
void CreateDatabaseHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder);
void SystemHeartbeatHandler(uint64_t ts, slk::Reader *req_reader, slk::Builder *res_builder);
void SystemRecoveryHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder);
#endif

/// Register all DBMS level RPC handlers
void Register(replication::RoleReplicaData const &data, DbmsHandler &dbms_handler, auth::SynchedAuth &auth);
}  // namespace system_replication

bool StartRpcServer(DbmsHandler &dbms_handler, const replication::RoleReplicaData &data, auth::SynchedAuth &auth);

}  // namespace memgraph::dbms
