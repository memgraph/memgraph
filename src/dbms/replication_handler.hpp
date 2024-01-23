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

#include "dbms/database.hpp"
#include "replication/role.hpp"
#include "utils/result.hpp"

namespace memgraph::replication {
struct ReplicationState;
struct ReplicationServerConfig;
struct ReplicationClientConfig;
}  // namespace memgraph::replication

namespace memgraph::dbms {

class DbmsHandler;

enum class RegisterReplicaError : uint8_t { NAME_EXISTS, ENDPOINT_EXISTS, CONNECTION_FAILED, COULD_NOT_BE_PERSISTED };

enum class UnregisterReplicaResult : uint8_t {
  NOT_MAIN,
  COULD_NOT_BE_PERSISTED,
  CAN_NOT_UNREGISTER,
  SUCCESS,
};

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
/// TODO: extend to do multiple storages
struct ReplicationHandler {
  explicit ReplicationHandler(DbmsHandler &dbms_handler);

  // as REPLICA, become MAIN
  bool SetReplicationRoleMain();

  // as MAIN, become REPLICA
  bool SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config);

  // as MAIN, define and connect to REPLICAs
  auto RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
      -> utils::BasicResult<RegisterReplicaError>;

  // as MAIN, remove a REPLICA connection
  auto UnregisterReplica(std::string_view name) -> UnregisterReplicaResult;

  // Helper pass-through (TODO: remove)
  auto GetRole() const -> memgraph::replication::ReplicationRole;
  bool IsMain() const;
  bool IsReplica() const;

 private:
  DbmsHandler &dbms_handler_;
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
void Register(replication::RoleReplicaData const &data, DbmsHandler &dbms_handler);
}  // namespace system_replication

bool StartRpcServer(DbmsHandler &dbms_handler, const replication::RoleReplicaData &data);

}  // namespace memgraph::dbms
