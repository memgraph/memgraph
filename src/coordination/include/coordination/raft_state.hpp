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

#include <flags/replication.hpp>
#include "io/network/endpoint.hpp"
#include "nuraft/coordinator_state_machine.hpp"
#include "nuraft/coordinator_state_manager.hpp"

#include <libnuraft/nuraft.hxx>

namespace memgraph::coordination {

class CoordinatorInstance;
struct CoordinatorToReplicaConfig;

using BecomeLeaderCb = std::function<void()>;
using BecomeFollowerCb = std::function<void()>;

using nuraft::buffer;
using nuraft::logger;
using nuraft::ptr;
using nuraft::raft_launcher;
using nuraft::raft_server;
using nuraft::srv_config;
using nuraft::state_machine;
using nuraft::state_mgr;
using raft_result = nuraft::cmd_result<ptr<buffer>>;

class RaftState {
 private:
  explicit RaftState(BecomeLeaderCb become_leader_cb, BecomeFollowerCb become_follower_cb, uint32_t raft_server_id,
                     uint32_t raft_port, std::string raft_address);

  auto InitRaftServer() -> void;

 public:
  RaftState() = delete;
  RaftState(RaftState const &other) = default;
  RaftState &operator=(RaftState const &other) = default;
  RaftState(RaftState &&other) noexcept = default;
  RaftState &operator=(RaftState &&other) noexcept = default;
  ~RaftState();

  static auto MakeRaftState(BecomeLeaderCb &&become_leader_cb, BecomeFollowerCb &&become_follower_cb) -> RaftState;

  auto InstanceName() const -> std::string;
  auto RaftSocketAddress() const -> std::string;

  auto AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config) -> void;
  auto GetAllCoordinators() const -> std::vector<ptr<srv_config>>;

  auto RequestLeadership() -> bool;
  auto IsLeader() const -> bool;

  auto MainExists() const -> bool;
  auto IsMain(std::string_view instance_name) const -> bool;
  auto IsReplica(std::string_view instance_name) const -> bool;

  auto AppendRegisterReplicationInstanceLog(CoordinatorToReplicaConfig const &config) -> bool;
  auto AppendUnregisterReplicationInstanceLog(std::string_view instance_name) -> bool;
  auto AppendSetInstanceAsMainLog(std::string_view instance_name) -> bool;
  auto AppendSetInstanceAsReplicaLog(std::string_view instance_name) -> bool;
  auto AppendUpdateUUIDLog(utils::UUID const &uuid) -> bool;

  auto GetReplicationInstances() const -> std::vector<ReplicationInstanceState>;

  auto GetUUID() const -> utils::UUID;

 private:
  // TODO: (andi) I think variables below can be abstracted/clean them.
  io::network::Endpoint raft_endpoint_;
  uint32_t raft_server_id_;

  ptr<CoordinatorStateMachine> state_machine_;
  ptr<CoordinatorStateManager> state_manager_;
  ptr<raft_server> raft_server_;
  ptr<logger> logger_;
  raft_launcher launcher_;

  BecomeLeaderCb become_leader_cb_;
  BecomeFollowerCb become_follower_cb_;
};

}  // namespace memgraph::coordination
#endif
