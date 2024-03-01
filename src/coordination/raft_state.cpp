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

#include "coordination/raft_state.hpp"

#include "coordination/coordinator_config.hpp"
#include "coordination/coordinator_exceptions.hpp"
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

RaftState::RaftState(BecomeLeaderCb become_leader_cb, BecomeFollowerCb become_follower_cb, uint32_t raft_server_id,
                     uint32_t raft_port, std::string raft_address)
    : raft_server_id_(raft_server_id),
      raft_port_(raft_port),
      raft_address_(std::move(raft_address)),
      state_machine_(cs_new<CoordinatorStateMachine>()),
      state_manager_(
          cs_new<CoordinatorStateManager>(raft_server_id_, raft_address_ + ":" + std::to_string(raft_port_))),
      logger_(nullptr),
      become_leader_cb_(std::move(become_leader_cb)),
      become_follower_cb_(std::move(become_follower_cb)) {}

auto RaftState::InitRaftServer() -> void {
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

  raft_launcher launcher;

  raft_server_ = launcher.init(state_machine_, state_manager_, logger_, static_cast<int>(raft_port_), asio_opts, params,
                               init_opts);

  if (!raft_server_) {
    throw RaftServerStartException("Failed to launch raft server on {}:{}", raft_address_, raft_port_);
  }

  auto maybe_stop = utils::ResettableCounter<20>();
  do {
    if (raft_server_->is_initialized()) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  } while (!maybe_stop());

  throw RaftServerStartException("Failed to initialize raft server on {}:{}", raft_address_, raft_port_);
}

auto RaftState::MakeRaftState(BecomeLeaderCb &&become_leader_cb, BecomeFollowerCb &&become_follower_cb) -> RaftState {
  uint32_t raft_server_id = FLAGS_raft_server_id;
  uint32_t raft_port = FLAGS_raft_server_port;

  auto raft_state =
      RaftState(std::move(become_leader_cb), std::move(become_follower_cb), raft_server_id, raft_port, "127.0.0.1");

  raft_state.InitRaftServer();
  return raft_state;
}

RaftState::~RaftState() { launcher_.shutdown(); }

auto RaftState::InstanceName() const -> std::string { return "coordinator_" + std::to_string(raft_server_id_); }

auto RaftState::RaftSocketAddress() const -> std::string { return raft_address_ + ":" + std::to_string(raft_port_); }

auto RaftState::AddCoordinatorInstance(uint32_t raft_server_id, uint32_t raft_port, std::string_view raft_address)
    -> void {
  auto const endpoint = fmt::format("{}:{}", raft_address, raft_port);
  srv_config const srv_config_to_add(static_cast<int>(raft_server_id), endpoint);
  if (!raft_server_->add_srv(srv_config_to_add)->get_accepted()) {
    throw RaftAddServerException("Failed to add server {} to the cluster", endpoint);
  }
  spdlog::info("Request to add server {} to the cluster accepted", endpoint);
}

auto RaftState::GetAllCoordinators() const -> std::vector<ptr<srv_config>> {
  std::vector<ptr<srv_config>> all_srv_configs;
  raft_server_->get_srv_config_all(all_srv_configs);
  return all_srv_configs;
}

auto RaftState::IsLeader() const -> bool { return raft_server_->is_leader(); }

auto RaftState::RequestLeadership() -> bool { return raft_server_->is_leader() || raft_server_->request_leadership(); }

auto RaftState::AppendRegisterReplicationInstanceLog(CoordinatorClientConfig const &config) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeRegisterInstance(config);
  auto const res = raft_server_->append_entries({new_log});

  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for registering instance {}. Most likely the reason is that the instance is not "
        "the "
        "leader.",
        config.instance_name);
    return false;
  }

  spdlog::info("Request for registering instance {} accepted", config.instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to register instance {} with error code {}", config.instance_name, res->get_result_code());
    return false;
  }

  return true;
}

auto RaftState::AppendUnregisterReplicationInstanceLog(std::string_view instance_name) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeUnregisterInstance(instance_name);
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for unregistering instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return false;
  }

  spdlog::info("Request for unregistering instance {} accepted", instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to unregister instance {} with error code {}", instance_name, res->get_result_code());
    return false;
  }
  return true;
}

auto RaftState::AppendSetInstanceAsMainLog(std::string_view instance_name) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeSetInstanceAsMain(instance_name);
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for promoting instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return false;
  }

  spdlog::info("Request for promoting instance {} accepted", instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", instance_name, res->get_result_code());
    return false;
  }
  return true;
}

auto RaftState::AppendSetInstanceAsReplicaLog(std::string_view instance_name) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeSetInstanceAsReplica(instance_name);
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for demoting instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return false;
  }
  spdlog::info("Request for demoting instance {} accepted", instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", instance_name, res->get_result_code());
    return false;
  }

  return true;
}

auto RaftState::AppendUpdateUUIDLog(utils::UUID const &uuid) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeUpdateUUID(uuid);
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for updating UUID. Most likely the reason is that the instance is not "
        "the leader.");
    return false;
  }
  spdlog::info("Request for updating UUID accepted");

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to update UUID with error code {}", res->get_result_code());
    return false;
  }

  return true;
}

auto RaftState::FindCurrentMainInstanceName() const -> std::optional<std::string> {
  return state_machine_->FindCurrentMainInstanceName();
}

auto RaftState::MainExists() const -> bool { return state_machine_->MainExists(); }

auto RaftState::IsMain(std::string_view instance_name) const -> bool { return state_machine_->IsMain(instance_name); }

auto RaftState::IsReplica(std::string_view instance_name) const -> bool {
  return state_machine_->IsReplica(instance_name);
}

auto RaftState::GetInstances() const -> std::vector<InstanceState> { return state_machine_->GetInstances(); }

auto RaftState::GetUUID() const -> utils::UUID { return state_machine_->GetUUID(); }

}  // namespace memgraph::coordination
#endif
