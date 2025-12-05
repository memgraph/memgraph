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

#pragma once

#ifdef MG_ENTERPRISE

#include <optional>

#include "coordination/coordinator_observer.hpp"
#include "coordination/coordinator_state_machine.hpp"
#include "coordination/coordinator_state_manager.hpp"

#include <libnuraft/logger.hxx>
#include <libnuraft/nuraft.hxx>

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.utils;

namespace memgraph::coordination {

using BecomeLeaderCb = std::function<void()>;
using BecomeFollowerCb = std::function<void()>;

using nuraft::asio_service;
using nuraft::buffer;
using nuraft::context;
using nuraft::delayed_task_scheduler;
using nuraft::logger;
using nuraft::ptr;
using nuraft::raft_launcher;
using nuraft::raft_server;
using nuraft::rpc_client_factory;
using nuraft::rpc_listener;
using nuraft::srv_config;
using nuraft::state_machine;
using nuraft::state_mgr;
using raft_result = nuraft::cmd_result<ptr<buffer>>;

class RaftState {
 public:
  auto InitRaftServer() -> void;
  explicit RaftState(CoordinatorInstanceInitConfig const &config, BecomeLeaderCb become_leader_cb,
                     BecomeFollowerCb become_follower_cb,
                     std::optional<CoordinationClusterChangeObserver> observer = std::nullopt);
  RaftState() = delete;
  RaftState(RaftState const &other) = default;
  RaftState &operator=(RaftState const &other) = default;
  RaftState(RaftState &&other) noexcept = default;
  RaftState &operator=(RaftState &&other) noexcept = default;
  ~RaftState();

  auto GetCoordinatorEndpoint(int32_t coordinator_id) const -> std::string;
  auto GetMyCoordinatorEndpoint() const -> std::string;
  auto GetMyCoordinatorId() const -> int32_t;
  auto InstanceName() const -> std::string;

  // Only called when adding new coordinator instance, not itself.
  auto AddCoordinatorInstance(CoordinatorInstanceConfig const &config) const -> void;
  auto RemoveCoordinatorInstance(int32_t coordinator_id) const -> void;

  auto IsLeader() const -> bool;
  auto GetLeaderId() const -> int32_t;

  auto AppendClusterUpdate(CoordinatorClusterStateDelta const &delta_state) const -> bool;

  auto GetDataInstancesContext() const -> std::vector<DataInstanceContext>;
  auto GetCoordinatorInstancesContext() const -> std::vector<CoordinatorInstanceContext>;
  auto GetCoordinatorInstancesAux() const -> std::vector<CoordinatorInstanceAux>;
  auto GetMyCoordinatorInstanceAux() const -> CoordinatorInstanceAux;

  // TODO: (andi) Ideally we delete this and rely just on one thing.
  auto MainExists() const -> bool;
  auto HasMainState(std::string_view instance_name) const -> bool;
  auto IsCurrentMain(std::string_view instance_name) const -> bool;
  auto TryGetCurrentMainName() const -> std::optional<std::string>;
  auto GetCurrentMainUUID() const -> utils::UUID;

  auto GetLeaderCoordinatorData() const -> std::optional<LeaderCoordinatorData>;
  auto YieldLeadership() const -> void;
  auto GetRoutingTable(std::string_view db_name,
                       std::map<std::string, std::map<std::string, int64_t>> const &replicas_lag) const -> RoutingTable;

  // Returns elapsed time in ms since last successful response from the coordinator with id srv_id
  auto CoordLastSuccRespMs(int32_t srv_id) const -> std::chrono::milliseconds;
  // Return empty optional in the case when user didn't add coordinator on which setup of the cluster has been done
  auto GetBoltServer(int32_t coordinator_id) const -> std::optional<std::string>;
  auto GetMyBoltServer() const -> std::optional<std::string>;

  auto GetEnabledReadsOnMain() const -> bool;
  auto GetSyncFailoverOnly() const -> bool;
  auto GetMaxFailoverReplicaLag() const -> uint64_t;
  auto GetMaxReplicaReadLag() const -> uint64_t;

 private:
  uint16_t coordinator_port_;
  int32_t coordinator_id_;

  ptr<logger> logger_;
  ptr<raft_server> raft_server_;
  ptr<asio_service> asio_service_;
  ptr<rpc_listener> asio_listener_;

  ptr<CoordinatorStateMachine> state_machine_;
  ptr<CoordinatorStateManager> state_manager_;
  BecomeLeaderCb become_leader_cb_;
  BecomeFollowerCb become_follower_cb_;
};

}  // namespace memgraph::coordination
#endif
