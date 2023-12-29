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

/// TODO: (andi) Two definitions of the same enum
enum class RegisterReplicaError : uint8_t { NAME_EXISTS, END_POINT_EXISTS, CONNECTION_FAILED, COULD_NOT_BE_PERSISTED };

#ifdef MG_ENTERPRISE
enum class RegisterMainError : uint8_t { MAIN_ALREADY_EXISTS, END_POINT_EXISTS, COULD_NOT_BE_PERSISTED };
#endif

enum class UnregisterReplicaResult : uint8_t {
  IS_REPLICA,
  COULD_NOT_BE_PERSISTED,
  CAN_NOT_UNREGISTER,
  SUCCESS,
};

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
/// TODO: extend to do multiple storages
struct ReplicationHandler {
  explicit ReplicationHandler(DbmsHandler &dbms_handler);

#ifdef MG_ENTERPRISE
  // as default main, add replication server to the main.
  // As replica become main
  bool SetReplicationRoleMain(const memgraph::replication::ReplicationServerConfig &config);
#else
  // as replica, become main.
  bool SetReplicationRoleMain();
#endif

  // as main, become replica
  bool SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config);

#ifdef MG_ENTERPRISE
  // as default main, become coordinator
  bool SetReplicationRoleCoordinator();
#endif

  // as main, define and connect to replicas
  auto RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
      -> utils::BasicResult<RegisterReplicaError>;

#ifdef MG_ENTERPRISE
  // as coordinator, connect to main
  auto RegisterMain(const memgraph::replication::ReplicationClientConfig &config)
      -> utils::BasicResult<RegisterMainError>;
#endif

  // as MAIN, remove a REPLICA connection
  auto UnregisterReplica(std::string_view name) -> UnregisterReplicaResult;

  // Helper pass-through (TODO: remove)
  auto GetRole() const -> memgraph::replication::ReplicationRole;
  bool IsMain() const;
  bool IsReplica() const;
#ifdef MG_ENTERPRISE
  bool IsCoordinator() const;
#endif

 private:
  DbmsHandler &dbms_handler_;
};

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
/// TODO: extend to do multiple storages
void RestoreReplication(replication::ReplicationState &repl_state, storage::Storage &storage);

}  // namespace memgraph::dbms
