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

#include "replication_coordination_glue/mode.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/result.hpp"
#include "utils/uuid.hpp"

// BEGIN fwd declares
namespace memgraph::replication {
struct ReplicationState;
struct ReplicationServerConfig;
struct ReplicationClientConfig;
}  // namespace memgraph::replication

namespace memgraph::query {

enum class RegisterReplicaError : uint8_t {
  NAME_EXISTS,
  ENDPOINT_EXISTS,
  CONNECTION_FAILED,
  COULD_NOT_BE_PERSISTED,
  ERROR_ACCEPTING_MAIN
};

enum class UnregisterReplicaResult : uint8_t {
  NOT_MAIN,
  COULD_NOT_BE_PERSISTED,
  CAN_NOT_UNREGISTER,
  SUCCESS,
};

enum class ShowReplicaError : uint8_t {
  NOT_MAIN,
};

struct ReplicaInfoState {
  ReplicaInfoState(uint64_t ts, uint64_t behind, storage::replication::ReplicaState state)
      : ts_(ts), behind_(behind), state_(state) {}

  uint64_t ts_;
  uint64_t behind_;
  storage::replication::ReplicaState state_;
};

struct ReplicasInfo {
  ReplicasInfo(std::string name, std::string socket_address, replication_coordination_glue::ReplicationMode sync_mode,
               std::map<std::string, ReplicaInfoState> data_info)
      : name_(std::move(name)),
        socket_address_(std::move(socket_address)),
        sync_mode_(sync_mode),
        data_info_(std::move(data_info)) {}

  std::string name_;
  std::string socket_address_;
  memgraph::replication_coordination_glue::ReplicationMode sync_mode_;
  std::map<std::string, ReplicaInfoState> data_info_;
};

struct ReplicasInfos {
  explicit ReplicasInfos(std::vector<ReplicasInfo> entries) : entries_(std::move(entries)) {}

  std::vector<ReplicasInfo> entries_;
};

/// A handler type that keep in sync current ReplicationState and the MAIN/REPLICA-ness of Storage
struct ReplicationQueryHandler {
  virtual ~ReplicationQueryHandler() = default;

  // as REPLICA, become MAIN
  virtual bool SetReplicationRoleMain() = 0;

  // as MAIN, become REPLICA
  virtual bool SetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config,
                                         const std::optional<utils::UUID> &main_uuid) = 0;

  virtual bool TrySetReplicationRoleReplica(const memgraph::replication::ReplicationServerConfig &config,
                                            const std::optional<utils::UUID> &main_uuid) = 0;

  // as MAIN, define and connect to REPLICAs
  virtual auto TryRegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
      -> utils::BasicResult<RegisterReplicaError> = 0;

  virtual auto RegisterReplica(const memgraph::replication::ReplicationClientConfig &config)
      -> utils::BasicResult<RegisterReplicaError> = 0;

  // as MAIN, remove a REPLICA connection
  virtual auto UnregisterReplica(std::string_view name) -> UnregisterReplicaResult = 0;

  // Helper pass-through (TODO: remove)
  virtual auto GetRole() const -> memgraph::replication_coordination_glue::ReplicationRole = 0;
  virtual bool IsMain() const = 0;
  virtual bool IsReplica() const = 0;

  virtual auto ShowReplicas() const -> utils::BasicResult<ShowReplicaError, ReplicasInfos> = 0;
};

}  // namespace memgraph::query
