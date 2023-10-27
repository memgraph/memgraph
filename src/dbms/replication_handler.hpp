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

#pragma once

#include "replication/role.hpp"
#include "storage/v2/storage.hpp"
#include "utils/result.hpp"

// BEGIN fwd declares
namespace memgraph::replication {
struct ReplicationState;
struct ReplicationServerConfig;
struct ReplicationClientConfig;
}  // namespace memgraph::replication

namespace memgraph::dbms {
class DbmsHandler;

enum class RegisterReplicaError : uint8_t { NAME_EXISTS, END_POINT_EXISTS, CONNECTION_FAILED, COULD_NOT_BE_PERSISTED };
enum class UnregisterReplicaResult : uint8_t {
  NOT_MAIN,
  COULD_NOT_BE_PERSISTED,
  CAN_NOT_UNREGISTER,
  SUCCESS,
};

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
/// TODO: extend to do multiple storages
struct ReplicationHandler {
  ReplicationHandler(memgraph::replication::ReplicationState &replState, DbmsHandler &dbms_handler)
      : repl_state_(replState), dbms_handler_(dbms_handler) {}

  // as REPLICA, become MAIN
  bool SetReplicationRoleMain();

  // as MAIN, become REPLICA
  bool SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config);

  // as MAIN, define and connect to REPLICAs
  auto RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
      -> utils::BasicResult<RegisterReplicaError>;

  // as MAIN, remove a REPLICA connection
  auto UnregisterReplica(std::string_view registered_config) -> UnregisterReplicaResult;

  // Helper pass-through (TODO: remove)
  auto GetRole() const -> memgraph::replication::ReplicationRole;
  bool IsMain() const;
  bool IsReplica() const;

 private:
  memgraph::replication::ReplicationState &repl_state_;
  DbmsHandler &dbms_handler_;
};

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
/// TODO: extend to do multiple storages
void RestoreReplication(const replication::ReplicationState &repl_state, storage::Storage &storage);

}  // namespace memgraph::dbms
