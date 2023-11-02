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
#include "utils/result.hpp"

// BEGIN fwd declares
namespace memgraph::replication {
struct ReplicationState;
struct ReplicationServerConfig;
struct ReplicationClientConfig;
}  // namespace memgraph::replication
namespace memgraph::storage {
class Storage;
}
// END fwd declares

namespace memgraph::storage {

enum class RegistrationMode : std::uint8_t { MUST_BE_INSTANTLY_VALID, RESTORE };
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
  ReplicationHandler(memgraph::replication::ReplicationState &replState, Storage &storage)
      : repl_state_(replState), storage_(storage) {}

  // as REPLICA, become MAIN
  bool SetReplicationRoleMain();

  // as MAIN, become REPLICA
  bool SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config);

  // as MAIN, define and connect to REPLICAs
  auto RegisterReplica(RegistrationMode registration_mode, const memgraph::replication::ReplicationClientConfig &config)
      -> utils::BasicResult<RegisterReplicaError>;

  // as MAIN, remove a REPLICA connection
  auto UnregisterReplica(std::string_view name) -> UnregisterReplicaResult;

  // Generic restoration
  // TODO: decouple storage restoration from epoch restoration
  void RestoreReplication();

  // Helper pass-through (TODO: remove)
  auto GetRole() const -> memgraph::replication::ReplicationRole;
  bool IsMain() const;
  bool IsReplica() const;

 private:
  memgraph::replication::ReplicationState &repl_state_;
  Storage &storage_;
};
}  // namespace memgraph::storage
