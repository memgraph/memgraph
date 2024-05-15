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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "coordination/raft_state.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"

#include <spdlog/spdlog.h>
#include "json/json.hpp"

namespace memgraph::coordination {

using nuraft::asio_service;
using nuraft::cb_func;
using nuraft::CbReturnCode;
using nuraft::cs_new;
using nuraft::ptr;
using nuraft::raft_params;
using nuraft::raft_server;
using nuraft::srv_config;

RaftState::RaftState(CoordinatorInstanceInitConfig const &config, BecomeLeaderCb become_leader_cb,
                     BecomeFollowerCb become_follower_cb)
    : raft_endpoint_("0.0.0.0", config.coordinator_port),
      coordinator_id_(config.coordinator_id),
      state_machine_(cs_new<CoordinatorStateMachine>()),
      state_manager_(cs_new<CoordinatorStateManager>(config)),
      logger_(nullptr),
      become_leader_cb_(std::move(become_leader_cb)),
      become_follower_cb_(std::move(become_follower_cb)) {}

// Call to this function results in call to
// coordinator instance, make sure everything is initialized in coordinator instance
// prior to calling InitRaftServer. To be specific, this function
// will call `become_leader_cb_`
auto RaftState::InitRaftServer() -> void {
  asio_service::options asio_opts;
  asio_opts.thread_pool_size_ = 1;

  raft_params params;
  params.heart_beat_interval_ = 100;
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

  init_opts.start_server_in_constructor_ = false;
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
    first_cb_executed_ = true;
    return CbReturnCode::Ok;
  };

  asio_service_ = nuraft::cs_new<asio_service>(asio_opts, logger_);

  ptr<delayed_task_scheduler> scheduler = asio_service_;
  ptr<rpc_client_factory> rpc_cli_factory = asio_service_;

  nuraft::ptr<nuraft::state_mgr> casted_state_manager = state_manager_;
  nuraft::ptr<nuraft::state_machine> casted_state_machine = state_machine_;

  asio_listener_ = asio_service_->create_rpc_listener(raft_endpoint_.GetPort(), logger_);
  if (!asio_listener_) {
    throw RaftServerStartException("Failed to create rpc listener on port {}", raft_endpoint_.GetPort());
  }

  auto *ctx = new nuraft::context(casted_state_manager, casted_state_machine, asio_listener_, logger_, rpc_cli_factory,
                                  scheduler, params);

  raft_server_ = nuraft::cs_new<raft_server>(ctx, init_opts);

  if (!raft_server_) {
    throw RaftServerStartException("Failed to allocate raft server on {}", raft_endpoint_.SocketAddress());
  }
  spdlog::trace("Raft server allocated on {}", raft_endpoint_.SocketAddress());

  // If set to true, server won't be created and exception will be thrown.
  // By setting it to false, all coordinators are started as leaders.
  bool const skip_initial_election_timeout{false};
  raft_server_->start_server(skip_initial_election_timeout);
  spdlog::trace("Raft server started on {}", raft_endpoint_.SocketAddress());

  asio_listener_->listen(raft_server_);
  spdlog::trace("Asio listener active on {}", raft_endpoint_.SocketAddress());

  // Don't return until role is set
  auto maybe_stop = utils::ResettableCounter<500>();
  while (!maybe_stop()) {
    if (first_cb_executed_) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  if (!raft_server_->is_leader()) {
    throw RaftServerStartException("Failed to start raft server as leader on {}", raft_endpoint_.SocketAddress());
  }
}

RaftState::~RaftState() {
  spdlog::trace("Shutting down RaftState for coordinator_{}", coordinator_id_);
  state_machine_.reset();
  state_manager_.reset();
  logger_.reset();

  if (!raft_server_) {
    spdlog::warn("Raft server not initialized for coordinator_{}, shutdown not necessary", coordinator_id_);
    return;
  }
  raft_server_->shutdown();
  raft_server_.reset();

  spdlog::trace("Raft server closed");

  if (asio_listener_) {
    asio_listener_->stop();
    asio_listener_->shutdown();
    spdlog::trace("Asio listener closed");
  }

  if (asio_service_) {
    asio_service_->stop();
    size_t count = 0;
    while (asio_service_->get_active_workers() != 0 && count < 500) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      count++;
    }
  }
  if (asio_service_->get_active_workers() > 0) {
    spdlog::warn("Failed to shutdown raft server correctly for coordinator_{} in 5s", coordinator_id_);
  }
  spdlog::trace("Asio service closed");
}

auto RaftState::InstanceName() const -> std::string { return fmt::format("coordinator_{}", coordinator_id_); }

auto RaftState::RaftSocketAddress() const -> std::string { return raft_endpoint_.SocketAddress(); }

auto RaftState::AddCoordinatorInstance(CoordinatorToCoordinatorConfig const &config) -> void {
  spdlog::trace("Adding coordinator instance {} start in RaftState for coordinator_{}", config.coordinator_id,
                coordinator_id_);
  auto const endpoint = config.coordinator_server.SocketAddress();
  auto const aux = nlohmann::json(config).dump();
  srv_config const srv_config_to_add(static_cast<int>(config.coordinator_id), 0, endpoint, aux, false);

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
    const auto server_config = raft_server_->get_srv_config(static_cast<nuraft::int32>(config.coordinator_id));
    if (server_config) {
      spdlog::trace("Server with id {} added to cluster", config.coordinator_id);
      added = true;
      break;
    }
  }

  if (!added) {
    throw RaftAddServerException("Failed to add server {} to the cluster in {}ms", endpoint,
                                 max_tries * waiting_period);
  }
}

auto RaftState::CoordLastSuccRespMs(uint32_t srv_id) -> std::chrono::milliseconds {
  using std::chrono::duration_cast;
  using std::chrono::microseconds;
  using std::chrono::milliseconds;

  auto const peer_info = raft_server_->get_peer_info(static_cast<int>(srv_id));
  auto const elapsed_time_ms = duration_cast<milliseconds>(microseconds(peer_info.last_succ_resp_us_));
  spdlog::trace("Elapsed time in miliseconds since last successful response from coordinator_{}: {}",
                static_cast<int>(srv_id), elapsed_time_ms.count());
  return elapsed_time_ms;
}

auto RaftState::GetCoordinatorInstances() const -> std::vector<CoordinatorToCoordinatorConfig> {
  std::vector<ptr<srv_config>> srv_configs;
  raft_server_->get_srv_config_all(srv_configs);

  return ranges::views::transform(
             srv_configs,
             [](auto const &srv_config) {
               return nlohmann::json::parse(srv_config->get_aux()).template get<CoordinatorToCoordinatorConfig>();
             }) |
         ranges::to<std::vector>();
}

auto RaftState::GetLeaderCoordinatorData() const -> std::optional<CoordinatorToCoordinatorConfig> {
  std::vector<ptr<srv_config>> srv_configs;
  raft_server_->get_srv_config_all(srv_configs);
  auto const leader_id = raft_server_->get_leader();
  auto const transform_func = [](auto const &srv_config) -> CoordinatorToCoordinatorConfig {
    return nlohmann::json::parse(srv_config->get_aux()).template get<CoordinatorToCoordinatorConfig>();
  };
  auto maybe_leader_srv_config =
      std::ranges::find_if(srv_configs, [&](auto const &srv_config) { return leader_id == srv_config->get_id(); });
  return maybe_leader_srv_config == srv_configs.end() ? std::nullopt
                                                      : std::make_optional(transform_func(*maybe_leader_srv_config));
}

auto RaftState::IsLeader() const -> bool { return raft_server_->is_leader(); }

auto RaftState::AppendOpenLock() -> bool {
  auto new_log = CoordinatorStateMachine::SerializeOpenLock();
  auto const res = raft_server_->append_entries({new_log});

  if (!res->get_accepted()) {
    spdlog::error("Failed to accept request to open lock");
    return false;
  }
  spdlog::trace("Request for opening lock in thread {} accepted", std::this_thread::get_id());

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

  spdlog::trace("Request for closing lock in thread {} accepted", std::this_thread::get_id());

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
  spdlog::trace("Request for updating UUID to {} accepted", std::string{uuid});

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to update UUID with error code {}", int(res->get_result_code()));
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
  spdlog::trace("Request for updating UUID of instance {} accepted", instance_name);

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

auto RaftState::TryGetCurrentMainName() const -> std::optional<std::string> {
  return state_machine_->TryGetCurrentMainName();
}

auto RaftState::GetRoutingTable() const -> RoutingTable {
  auto res = RoutingTable{};

  auto const repl_instance_to_bolt = [](ReplicationInstanceState const &instance) {
    return instance.config.BoltSocketAddress();
  };

  // TODO: (andi) This is wrong check, Fico will correct in #1819.
  auto const is_instance_main = [&](ReplicationInstanceState const &instance) {
    return instance.status == ReplicationRole::MAIN;
  };

  auto const is_instance_replica = [&](ReplicationInstanceState const &instance) {
    return instance.status == ReplicationRole::REPLICA;
  };

  auto const &raft_log_repl_instances = GetReplicationInstances();

  auto bolt_mains = raft_log_repl_instances | ranges::views::filter(is_instance_main) |
                    ranges::views::transform(repl_instance_to_bolt) | ranges::to<std::vector>();
  MG_ASSERT(bolt_mains.size() <= 1, "There can be at most one main instance active!");

  if (!std::ranges::empty(bolt_mains)) {
    res.emplace_back(std::move(bolt_mains), "WRITE");
  }

  auto bolt_replicas = raft_log_repl_instances | ranges::views::filter(is_instance_replica) |
                       ranges::views::transform(repl_instance_to_bolt) | ranges::to<std::vector>();
  if (!std::ranges::empty(bolt_replicas)) {
    res.emplace_back(std::move(bolt_replicas), "READ");
  }

  auto const coord_instance_to_bolt = [](CoordinatorToCoordinatorConfig const &instance) {
    return instance.bolt_server.SocketAddress();
  };

  auto const &raft_log_coord_instances = GetCoordinatorInstances();
  auto bolt_coords =
      raft_log_coord_instances | ranges::views::transform(coord_instance_to_bolt) | ranges::to<std::vector>();

  res.emplace_back(std::move(bolt_coords), "ROUTE");

  return res;
}

}  // namespace memgraph::coordination
#endif
