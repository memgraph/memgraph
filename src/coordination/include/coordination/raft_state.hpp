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

#ifdef MG_ENTERPRISE
#include <optional>

#include <flags/replication.hpp>
#include "coordination/coordinator_communication_config.hpp"
#include "io/network/endpoint.hpp"
#include "nuraft/coordinator_state_machine.hpp"
#include "nuraft/coordinator_state_manager.hpp"

#include <libnuraft/nuraft.hxx>

namespace memgraph::coordination {

class CoordinatorInstance;
struct CoordinatorToReplicaConfig;

using BecomeLeaderCb = std::function<void()>;
using BecomeFollowerCb = std::function<void()>;
using RoutingTable = std::vector<std::pair<std::vector<std::string>, std::string>>;

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
                     BecomeFollowerCb become_follower_cb);
  RaftState() = delete;
  RaftState(RaftState const &other) = default;
  RaftState &operator=(RaftState const &other) = default;
  RaftState(RaftState &&other) noexcept = default;
  RaftState &operator=(RaftState &&other) noexcept = default;
  ~RaftState();

  auto InstanceName() const -> std::string;
  auto RaftSocketAddress() const -> std::string;

  auto AddCoordinatorInstance(CoordinatorToCoordinatorConfig const &config) -> void;
  auto GetCoordinatorInstances() const -> std::vector<CoordinatorToCoordinatorConfig>;

  auto IsLeader() const -> bool;

  auto AppendRegisterReplicationInstanceLog(CoordinatorToReplicaConfig const &config) -> bool;
  auto AppendUnregisterReplicationInstanceLog(std::string_view instance_name) -> bool;
  auto AppendSetInstanceAsMainLog(std::string_view instance_name, utils::UUID const &uuid) -> bool;
  auto AppendSetInstanceAsReplicaLog(std::string_view instance_name) -> bool;

  auto AppendUpdateUUIDForNewMainLog(utils::UUID const &uuid) -> bool;
  auto AppendUpdateUUIDForInstanceLog(std::string_view instance_name, utils::UUID const &uuid) -> bool;
  auto AppendOpenLock() -> bool;
  auto AppendCloseLock() -> bool;
  auto AppendInstanceNeedsDemote(std::string_view) -> bool;

  auto GetReplicationInstances() const -> std::vector<ReplicationInstanceState>;

  auto MainExists() const -> bool;
  auto HasMainState(std::string_view instance_name) const -> bool;
  auto HasReplicaState(std::string_view instance_name) const -> bool;
  auto IsCurrentMain(std::string_view instance_name) const -> bool;

  auto GetCurrentMainUUID() const -> utils::UUID;
  auto GetInstanceUUID(std::string_view) const -> utils::UUID;

  auto GetLeaderCoordinatorData() const -> std::optional<CoordinatorToCoordinatorConfig>;

  auto IsLockOpened() const -> bool;
  auto GetRoutingTable() const -> RoutingTable;

  auto TryGetCurrentMainName() const -> std::optional<std::string>;

  // Returns elapsed time in ms since last successful response from the coordinator with id srv_id
  auto CoordLastSuccRespMs(uint32_t srv_id) -> std::chrono::milliseconds;

 private:
  io::network::Endpoint raft_endpoint_;
  uint32_t coordinator_id_;

  ptr<CoordinatorStateMachine> state_machine_;
  ptr<CoordinatorStateManager> state_manager_;
  ptr<logger> logger_;
  ptr<raft_server> raft_server_;
  ptr<asio_service> asio_service_;
  ptr<rpc_listener> asio_listener_;

  BecomeLeaderCb become_leader_cb_;
  BecomeFollowerCb become_follower_cb_;
  std::atomic<bool> first_cb_executed_{false};
};

}  // namespace memgraph::coordination
#endif
