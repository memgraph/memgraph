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

#include <libnuraft/nuraft.hxx>

namespace memgraph::coordination {

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

class RaftInstance {
 public:
  RaftInstance(BecomeLeaderCb become_leader_cb, BecomeFollowerCb become_follower_cb);

  RaftInstance(RaftInstance const &other) = delete;
  RaftInstance &operator=(RaftInstance const &other) = delete;
  RaftInstance(RaftInstance &&other) noexcept = delete;
  RaftInstance &operator=(RaftInstance &&other) noexcept = delete;
  ~RaftInstance() = default;

  auto InstanceName() const -> std::string;
  auto RaftSocketAddress() const -> std::string;

  auto AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string raft_address) -> void;
  auto GetAllCoordinators() const -> std::vector<ptr<srv_config>>;

  auto RequestLeadership() -> bool;
  auto IsLeader() const -> bool;

  auto AppendRegisterReplicationInstance(std::string const &instance) -> ptr<raft_result>;

 private:
  ptr<state_machine> state_machine_;
  ptr<state_mgr> state_manager_;
  ptr<raft_server> raft_server_;
  ptr<logger> logger_;
  raft_launcher launcher_;

  // TODO: (andi) I think variables below can be abstracted
  uint32_t raft_server_id_;
  uint32_t raft_port_;
  std::string raft_address_;

  BecomeLeaderCb become_leader_cb_;
  BecomeFollowerCb become_follower_cb_;
};

}  // namespace memgraph::coordination
#endif
