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

using nuraft::logger;
using nuraft::ptr;
using nuraft::raft_launcher;
using nuraft::raft_server;
using nuraft::srv_config;
using nuraft::state_machine;
using nuraft::state_mgr;

class CoordinatorInstance {
 public:
  CoordinatorInstance();
  CoordinatorInstance(CoordinatorInstance const &other) = delete;
  CoordinatorInstance &operator=(CoordinatorInstance const &other) = delete;
  CoordinatorInstance(CoordinatorInstance &&other) noexcept = delete;
  CoordinatorInstance &operator=(CoordinatorInstance &&other) noexcept = delete;
  ~CoordinatorInstance() = default;

  auto InstanceName() const -> std::string;
  auto RaftSocketAddress() const -> std::string;
  auto AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string raft_address) -> void;
  auto GetAllCoordinators() const -> std::vector<ptr<srv_config>>;

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
};

}  // namespace memgraph::coordination
#endif
