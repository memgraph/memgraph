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
#include <chrono>

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "coordination/raft_state.hpp"
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

RaftState::RaftState(BecomeLeaderCb become_leader_cb, BecomeFollowerCb become_follower_cb, uint32_t coordinator_id,
                     uint32_t raft_port, std::string raft_address)
    : raft_endpoint_(raft_address, raft_port),
      coordinator_id_(coordinator_id),
      state_machine_(cs_new<CoordinatorStateMachine>()),
      state_manager_(cs_new<CoordinatorStateManager>(coordinator_id_, raft_endpoint_.SocketAddress())),
      logger_(nullptr),
      become_leader_cb_(std::move(become_leader_cb)),
      become_follower_cb_(std::move(become_follower_cb)) {}

auto RaftState::InitRaftServer() -> void {
  asio_service::options asio_opts;
  asio_opts.thread_pool_size_ = 1;

  raft_params params;
  params.heart_beat_interval_ = 150;
  params.election_timeout_lower_bound_ = 200;
  params.election_timeout_upper_bound_ = 400;
  params.reserved_log_items_ = 5;
  params.snapshot_distance_ = 5;
  params.client_req_timeout_ = 3000;
  params.return_method_ = raft_params::blocking;

  // If the leader doesn't receive any response from quorum nodes
  // in 200ms, it will step down.
  // This allows us to achieve strong consistency even if network partition
  // happens between the current leader and followers.
  // The value must be <= election_timeout_lower_bound_ so that cluster can never
  // have multiple leaders.
  params.leadership_expiry_ = 200;

  raft_server::init_options init_opts;

  init_opts.raft_callback_ = [this](cb_func::Type event_type, cb_func::Param *param) -> nuraft::CbReturnCode {
    if (event_type == cb_func::BecomeLeader) {
      spdlog::info("Node {} became leader", param->leaderId);
      become_leader_cb_();
    } else if (event_type == cb_func::BecomeFollower) {
      // TODO(antoniofilipovic) Check what happens when becoming follower while doing failover
      // There is no way to stop becoming a follower:
      // https://github.com/eBay/NuRaft/blob/188947bcc73ce38ab1c3cf9d01015ca8a29decd9/src/raft_server.cxx#L1334-L1335
      spdlog::trace("Got request to become follower");
      become_follower_cb_();
      spdlog::trace("Node {} became follower", param->myId);
    }
    return CbReturnCode::Ok;
  };

  raft_launcher launcher;

  raft_server_ =
      launcher.init(state_machine_, state_manager_, logger_, raft_endpoint_.port, asio_opts, params, init_opts);

  if (!raft_server_) {
    throw RaftServerStartException("Failed to launch raft server on {}", raft_endpoint_.SocketAddress());
  }
  auto maybe_stop = utils::ResettableCounter<20>();
  do {
    if (raft_server_->is_initialized()) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  } while (!maybe_stop());

  throw RaftServerStartException("Failed to initialize raft server on {}", raft_endpoint_.SocketAddress());
}

auto RaftState::MakeRaftState(BecomeLeaderCb &&become_leader_cb, BecomeFollowerCb &&become_follower_cb) -> RaftState {
  uint32_t coordinator_id = FLAGS_coordinator_id;
  uint32_t raft_port = FLAGS_coordinator_port;

  auto raft_state =
      RaftState(std::move(become_leader_cb), std::move(become_follower_cb), coordinator_id, raft_port, "127.0.0.1");

  raft_state.InitRaftServer();
  return raft_state;
}

RaftState::~RaftState() { launcher_.shutdown(); }

auto RaftState::InstanceName() const -> std::string {
  return fmt::format("coordinator_{}", std::to_string(coordinator_id_));
}

auto RaftState::RaftSocketAddress() const -> std::string { return raft_endpoint_.SocketAddress(); }

auto RaftState::AddCoordinatorInstance(coordination::CoordinatorToCoordinatorConfig const &config) -> void {
  auto const endpoint = config.coordinator_server.SocketAddress();
  srv_config const srv_config_to_add(static_cast<int>(config.coordinator_server_id), endpoint);

  auto cmd_result = raft_server_->add_srv(srv_config_to_add);

  if (cmd_result->get_result_code() == nuraft::cmd_result_code::OK) {
    spdlog::info("Request to add server {} to the cluster accepted", endpoint);
  } else {
    throw RaftAddServerException("Failed to accept request to add server {} to the cluster with error code {}",
                                 endpoint, int(cmd_result->get_result_code()));
  }

  // Waiting for server to join
  constexpr int max_tries{10};
  auto maybe_stop = utils::ResettableCounter<max_tries>();
  constexpr int waiting_period{200};
  bool added{false};
  while (!maybe_stop()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(waiting_period));
    const auto server_config = raft_server_->get_srv_config(static_cast<nuraft::int32>(config.coordinator_server_id));
    if (server_config) {
      spdlog::trace("Server with id {} added to cluster", config.coordinator_server_id);
      added = true;
      break;
    }
  }

  if (!added) {
    throw RaftAddServerException("Failed to add server {} to the cluster in {}ms", endpoint,
                                 max_tries * waiting_period);
  }
}

auto RaftState::GetAllCoordinators() const -> std::vector<ptr<srv_config>> {
  std::vector<ptr<srv_config>> all_srv_configs;
  raft_server_->get_srv_config_all(all_srv_configs);
  return all_srv_configs;
}

auto RaftState::IsLeader() const -> bool { return raft_server_->is_leader(); }

auto RaftState::RequestLeadership() -> bool { return raft_server_->is_leader() || raft_server_->request_leadership(); }

auto RaftState::AppendOpenLock() -> bool {
  auto new_log = CoordinatorStateMachine::SerializeOpenLock();
  auto const res = raft_server_->append_entries({new_log});

  if (!res->get_accepted()) {
    spdlog::error("Failed to accept request to open lock");
    return false;
  }
  spdlog::trace("Request for opening lock accepted");

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to open lock with error code {}", int(res->get_result_code()));
    return false;
  }

  return true;
}

auto RaftState::AppendCloseLock() -> bool {
  auto new_log = CoordinatorStateMachine::SerializeCloseLock();
  auto const res = raft_server_->append_entries({new_log});

  if (!res->get_accepted()) {
    spdlog::error("Failed to accept request to close lock");
    return false;
  }

  spdlog::trace("Request for closing lock accepted");

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to close lock with error code {}", int(res->get_result_code()));
    return false;
  }

  return true;
}

auto RaftState::AppendRegisterReplicationInstanceLog(CoordinatorToReplicaConfig const &config) -> bool {
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

  spdlog::trace("Request for registering instance {} accepted", config.instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to register instance {} with error code {}", config.instance_name,
                  int(res->get_result_code()));
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

  spdlog::trace("Request for unregistering instance {} accepted", instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to unregister instance {} with error code {}", instance_name, int(res->get_result_code()));
    return false;
  }
  return true;
}

auto RaftState::AppendSetInstanceAsMainLog(std::string_view instance_name, utils::UUID const &uuid) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeSetInstanceAsMain(
      InstanceUUIDUpdate{.instance_name = std::string{instance_name}, .uuid = uuid});
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for promoting instance {}. Most likely the reason is that the instance is not "
        "the leader.",
        instance_name);
    return false;
  }

  spdlog::trace("Request for promoting instance {} accepted", instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", instance_name, int(res->get_result_code()));
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
  spdlog::trace("Request for demoting instance {} accepted", instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to promote instance {} with error code {}", instance_name, int(res->get_result_code()));
    return false;
  }

  return true;
}

auto RaftState::AppendUpdateUUIDForNewMainLog(utils::UUID const &uuid) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeUpdateUUIDForNewMain(uuid);
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for updating UUID. Most likely the reason is that the instance is not "
        "the leader.");
    return false;
  }
  spdlog::trace("Request for updating UUID accepted");

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to update UUID with error code {}", int(res->get_result_code()));
    return false;
  }

  return true;
}

auto RaftState::AppendAddCoordinatorInstanceLog(CoordinatorToCoordinatorConfig const &config) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeAddCoordinatorInstance(config);
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error(
        "Failed to accept request for adding coordinator instance {}. Most likely the reason is that the instance is "
        "not the leader.",
        config.coordinator_server_id);
    return false;
  }

  spdlog::trace("Request for adding coordinator instance {} accepted", config.coordinator_server_id);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to add coordinator instance {} with error code {}", config.coordinator_server_id,
                  static_cast<int>(res->get_result_code()));
    return false;
  }

  return true;
}

auto RaftState::AppendInstanceNeedsDemote(std::string_view instance_name) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeInstanceNeedsDemote(instance_name);
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error("Failed to accept request that instance {} needs demote", instance_name);
    return false;
  }

  spdlog::trace("Request that instance {} needs demote accepted", instance_name);

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to add instance {} needs demote with error code {}", instance_name,
                  static_cast<int>(res->get_result_code()));
    return false;
  }

  return true;
}

auto RaftState::AppendUpdateUUIDForInstanceLog(std::string_view instance_name, const utils::UUID &uuid) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeUpdateUUIDForInstance(
      {.instance_name = std::string{instance_name}, .uuid = uuid});
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error("Failed to accept request for updating UUID of instance.");
    return false;
  }
  spdlog::trace("Request for updating UUID of instance accepted");

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to update UUID of instance with error code {}", int(res->get_result_code()));
    return false;
  }

  return true;
}

auto RaftState::MainExists() const -> bool { return state_machine_->MainExists(); }

auto RaftState::HasMainState(std::string_view instance_name) const -> bool {
  return state_machine_->HasMainState(instance_name);
}

auto RaftState::HasReplicaState(std::string_view instance_name) const -> bool {
  return state_machine_->HasReplicaState(instance_name);
}

auto RaftState::GetReplicationInstances() const -> std::vector<ReplicationInstanceState> {
  return state_machine_->GetReplicationInstances();
}

auto RaftState::GetCurrentMainUUID() const -> utils::UUID { return state_machine_->GetCurrentMainUUID(); }

auto RaftState::IsCurrentMain(std::string_view instance_name) const -> bool {
  return state_machine_->IsCurrentMain(instance_name);
}

auto RaftState::IsLockOpened() const -> bool { return state_machine_->IsLockOpened(); }

auto RaftState::GetInstanceUUID(std::string_view instance_name) const -> utils::UUID {
  return state_machine_->GetInstanceUUID(instance_name);
}

auto RaftState::GetCoordinatorInstances() const -> std::vector<CoordinatorInstanceState> {
  return state_machine_->GetCoordinatorInstances();
}

}  // namespace memgraph::coordination
#endif
