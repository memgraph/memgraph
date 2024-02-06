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

#ifdef MG_ENTERPRISE

#include "coordination/raft_instance.hpp"

#include "coordination/coordinator_exceptions.hpp"
#include "nuraft/coordinator_state_machine.hpp"
#include "nuraft/coordinator_state_manager.hpp"
#include "utils/counter.hpp"

namespace memgraph::coordination {

using nuraft::asio_service;
using nuraft::cb_func;
using nuraft::CbReturnCode;
using nuraft::cmd_result;
using nuraft::cs_new;
using nuraft::ptr;
using nuraft::raft_params;
using nuraft::raft_server;
using nuraft::srv_config;
using raft_result = cmd_result<ptr<buffer>>;

RaftInstance::RaftInstance(BecomeLeaderCb become_leader_cb, BecomeFollowerCb become_follower_cb)
    : raft_server_id_(FLAGS_raft_server_id),
      raft_port_(FLAGS_raft_server_port),
      raft_address_("127.0.0.1"),
      become_leader_cb_(std::move(become_leader_cb)),
      become_follower_cb_(std::move(become_follower_cb)) {
  auto raft_endpoint = raft_address_ + ":" + std::to_string(raft_port_);
  state_manager_ = cs_new<CoordinatorStateManager>(raft_server_id_, raft_endpoint);
  state_machine_ = cs_new<CoordinatorStateMachine>();
  logger_ = nullptr;

  // TODO: (andi) Maybe params file

  asio_service::options asio_opts;
  asio_opts.thread_pool_size_ = 1;  // TODO: (andi) Improve this

  raft_params params;
  params.heart_beat_interval_ = 100;
  params.election_timeout_lower_bound_ = 200;
  params.election_timeout_upper_bound_ = 400;
  // 5 logs are preserved before the last snapshot
  params.reserved_log_items_ = 5;
  // Create snapshot for every 5 log appends
  params.snapshot_distance_ = 5;
  params.client_req_timeout_ = 3000;
  params.return_method_ = raft_params::blocking;

  raft_server::init_options init_opts;
  init_opts.raft_callback_ = [this](cb_func::Type event_type, cb_func::Param *param) -> nuraft::CbReturnCode {
    if (event_type == cb_func::BecomeLeader) {
      spdlog::info("Node {} became leader", param->leaderId);
      become_leader_cb_();
    } else if (event_type == cb_func::BecomeFollower) {
      spdlog::info("Node {} became follower", param->myId);
      become_follower_cb_();
    }
    return CbReturnCode::Ok;
  };

  raft_server_ = launcher_.init(state_machine_, state_manager_, logger_, static_cast<int>(raft_port_), asio_opts,
                                params, init_opts);

  if (!raft_server_) {
    throw RaftServerStartException("Failed to launch raft server on {}", raft_endpoint);
  }

  auto maybe_stop = utils::ResettableCounter<20>();
  while (!raft_server_->is_initialized() && !maybe_stop()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  if (!raft_server_->is_initialized()) {
    throw RaftServerStartException("Failed to initialize raft server on {}", raft_endpoint);
  }

  spdlog::info("Raft server started on {}", raft_endpoint);
}

auto RaftInstance::InstanceName() const -> std::string { return "coordinator_" + std::to_string(raft_server_id_); }

auto RaftInstance::RaftSocketAddress() const -> std::string { return raft_address_ + ":" + std::to_string(raft_port_); }

auto RaftInstance::AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string raft_address)
    -> void {
  auto const endpoint = raft_address + ":" + std::to_string(raft_port);
  srv_config const srv_config_to_add(static_cast<int>(raft_server_id), endpoint);
  if (!raft_server_->add_srv(srv_config_to_add)->get_accepted()) {
    throw RaftAddServerException("Failed to add server {} to the cluster", endpoint);
  }
  spdlog::info("Request to add server {} to the cluster accepted", endpoint);
}

auto RaftInstance::GetAllCoordinators() const -> std::vector<ptr<srv_config>> {
  std::vector<ptr<srv_config>> all_srv_configs;
  raft_server_->get_srv_config_all(all_srv_configs);
  return all_srv_configs;
}

auto RaftInstance::IsLeader() const -> bool { return raft_server_->is_leader(); }

}  // namespace memgraph::coordination
#endif
