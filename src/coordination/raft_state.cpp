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
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "coordination/raft_state.hpp"
#include "nuraft/logger_wrapper.hpp"
#include "utils/counter.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"

#include <spdlog/spdlog.h>
#include "json/json.hpp"

#include "nuraft/constants_log_durability.hpp"
#include "utils.hpp"

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
                     BecomeFollowerCb become_follower_cb, std::optional<CoordinationClusterChangeObserver> observer)
    : coordinator_port_(config.coordinator_port),
      coordinator_id_(config.coordinator_id),
      logger_(cs_new<Logger>(config.nuraft_log_file)),
      become_leader_cb_(std::move(become_leader_cb)),
      become_follower_cb_(std::move(become_follower_cb)) {
  auto const coordinator_state_manager_durability_dir = config.durability_dir / "network";
  memgraph::utils::EnsureDirOrDie(coordinator_state_manager_durability_dir);

  CoordinatorStateManagerConfig state_manager_config{config.coordinator_id,
                                                     config.coordinator_port,
                                                     config.bolt_port,
                                                     config.management_port,
                                                     coordinator_state_manager_durability_dir,
                                                     config.coordinator_hostname};
  auto logger_wrapper = LoggerWrapper(static_cast<Logger *>(logger_.get()));
  LogStoreDurability log_store_durability;

  if (config.use_durability) {
    auto const log_store_path = config.durability_dir / "logs";
    memgraph::utils::EnsureDirOrDie(log_store_path);
    auto const durability_store = std::make_shared<kvstore::KVStore>(log_store_path);

    log_store_durability.durability_store_ = durability_store;
    log_store_durability.stored_log_store_version_ = static_cast<LogStoreVersion>(
        GetOrSetDefaultVersion(*durability_store, kLogStoreVersion, static_cast<int>(kActiveVersion), logger_wrapper));
    state_manager_config.log_store_durability_ = log_store_durability;
  }

  state_machine_ = cs_new<CoordinatorStateMachine>(logger_wrapper, log_store_durability);
  state_manager_ = cs_new<CoordinatorStateManager>(state_manager_config, logger_wrapper, observer);

  auto const last_commit_index_snapshot = [this]() -> uint64_t {
    if (auto const last_snapshot = state_machine_->last_snapshot(); last_snapshot != nullptr) {
      return last_snapshot->get_last_log_idx();
    }
    return 0;
  }();  // iile

  auto log_store = state_manager_->load_log_store();

  if (!log_store) {
    return;
  }

  auto const last_committed_index_state_machine_{state_machine_->last_commit_index()};
  spdlog::trace("Last commited index from snapshot: {}, last commited index in state machine: {}",
                last_commit_index_snapshot, last_committed_index_state_machine_);
  auto *coordinator_log_store = static_cast<CoordinatorLogStore *>(log_store.get());
  auto log_entries =
      coordinator_log_store->GetAllEntriesRange(last_commit_index_snapshot, last_committed_index_state_machine_ + 1);

  for (auto const &entry : log_entries) {
    if (entry.second == nullptr) {
      spdlog::error("Log entry for id {} is nullptr", entry.first);
      continue;
    }
    spdlog::trace("Applying log entry from log store with index {}", entry.first);
    if (entry.second->get_val_type() == nuraft::log_val_type::conf) {
      auto cluster_config = state_manager_->load_config();
      state_machine_->commit_config(entry.first, cluster_config);
    } else {
      state_machine_->commit(entry.first, entry.second->get_buf());
    }
  }

  if (log_store_durability.stored_log_store_version_ != kActiveVersion) {
    log_store_durability.durability_store_->Put(kLogStoreVersion, std::to_string(static_cast<int>(kActiveVersion)));
  }
}

// Call to this function results in call to
// coordinator instance, make sure everything is initialized in coordinator instance
// prior to calling InitRaftServer. To be specific, this function
// will call `become_leader_cb_`
auto RaftState::InitRaftServer() -> void {
  asio_service::options asio_opts;
  asio_opts.thread_pool_size_ = 1;

  raft_params params;
  params.heart_beat_interval_ = 1000;
  params.election_timeout_lower_bound_ = 2000;
  params.election_timeout_upper_bound_ = 4000;
  params.reserved_log_items_ = 5;
  params.snapshot_distance_ = 5;
  params.client_req_timeout_ = 3000;
  params.return_method_ = raft_params::blocking;

  // leadership expiry needs to be above 0, otherwise leadership never expires
  // this is bug in nuraft code
  params.leadership_expiry_ = 2000;

  // https://github.com/eBay/NuRaft/blob/master/docs/custom_commit_policy.md#full-consensus-mode
  // we want to set coordinator to unhealthy as soon as it is down and doesn't respond
  auto limits = raft_server::get_raft_limits();
  // Limit is set to 2 because 2*params.heart_beat_interval_ == params.leadership_expiry_ which is 2000
  limits.response_limit_.store(2);
  raft_server::set_raft_limits(limits);

  raft_server::init_options init_opts;

  init_opts.start_server_in_constructor_ = false;
  init_opts.raft_callback_ = [this](cb_func::Type event_type, cb_func::Param *param) -> nuraft::CbReturnCode {
    if (event_type == cb_func::BecomeLeader) {
      spdlog::info("Node {} became leader", param->leaderId);
      become_leader_cb_();
    } else if (event_type == cb_func::BecomeFollower) {
      spdlog::trace("Got request to become follower");
      become_follower_cb_();
      spdlog::trace("Node {} became follower", param->myId);
    }
    return CbReturnCode::Ok;
  };

  asio_service_ = nuraft::cs_new<asio_service>(asio_opts, logger_);

  ptr<delayed_task_scheduler> scheduler = asio_service_;
  ptr<rpc_client_factory> rpc_cli_factory = asio_service_;

  nuraft::ptr<nuraft::state_mgr> casted_state_manager = state_manager_;
  nuraft::ptr<nuraft::state_machine> casted_state_machine = state_machine_;

  asio_listener_ = asio_service_->create_rpc_listener(coordinator_port_, logger_);
  if (!asio_listener_) {
    throw RaftServerStartException("Failed to create rpc listener on port {}", coordinator_port_);
  }

  auto *ctx = new nuraft::context(casted_state_manager, casted_state_machine, asio_listener_, logger_, rpc_cli_factory,
                                  scheduler, params);

  raft_server_ = nuraft::cs_new<raft_server>(ctx, init_opts);

  if (!raft_server_) {
    throw RaftServerStartException("Failed to allocate coordinator server on port {}", coordinator_port_);
  }

  auto const coord_endpoint = raft_server_->get_srv_config(static_cast<int>(coordinator_id_))->get_endpoint();

  spdlog::trace("Raft server allocated on {}", coord_endpoint);

  // If set to true, server won't be created and exception will be thrown.
  // By setting it to false, all coordinators are started as leaders.
  bool const skip_initial_election_timeout{false};
  raft_server_->start_server(skip_initial_election_timeout);
  spdlog::trace("Raft server started on {}", coord_endpoint);

  asio_listener_->listen(raft_server_);
  spdlog::trace("Asio listener active on {}", coord_endpoint);

  // If we don't get initialized in 2min, we throw an exception and abort coordinator initialization.
  // When the follower gets back, it waits for the leader to ping it.
  // In the meantime, the election timer will trigger and the follower will enter the pre-vote protocol which should
  // fail because the leader is actually alive. So even if rpc listener is created (on follower), the initialization
  // isn't complete until leader sends him append_entries_request.
  auto maybe_stop = utils::ResettableCounter<1200>();
  while (!maybe_stop()) {
    // Initialized is set to true after raft_callback_ is being executed (role as leader or follower)
    if (raft_server_->is_initialized()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (!raft_server_->is_initialized()) {
    throw RaftServerStartException("Waiting too long for raft server initialization on coordinator with endpoint {}",
                                   coord_endpoint);
  }
}

RaftState::~RaftState() {
  spdlog::trace("Shutting down RaftState for coordinator_{}", coordinator_id_);

  utils::OnScopeExit const reset_shared_ptrs{[this]() {
    state_machine_.reset();
    state_manager_.reset();
    logger_.reset();
  }};

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

auto RaftState::GetCoordinatorId() const -> uint32_t { return coordinator_id_; }

auto RaftState::GetCoordinatorToCoordinatorConfigs() const -> std::vector<CoordinatorToCoordinatorConfig> {
  return state_manager_->GetCoordinatorToCoordinatorConfigs();
}

auto RaftState::AddCoordinatorInstance(CoordinatorToCoordinatorConfig const &config) -> void {
  spdlog::trace("Adding coordinator instance {} start in RaftState for coordinator_{}", config.coordinator_id,
                coordinator_id_);
  auto const endpoint = config.coordinator_server.SocketAddress();  // non-resolved IP
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

auto RaftState::AppendClusterUpdate(std::vector<DataInstanceState> cluster_state, utils::UUID uuid) -> bool {
  auto new_log = CoordinatorStateMachine::SerializeUpdateClusterState(std::move(cluster_state), uuid);
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error("Failed to accept request for updating cluster state.");
    return false;
  }
  spdlog::trace("Request for updating cluster state accepted.");

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to update cluster state. Error code {}", int(res->get_result_code()));
    return false;
  }

  return true;
}

auto RaftState::MainExists() const -> bool { return state_machine_->MainExists(); }

auto RaftState::HasMainState(std::string_view instance_name) const -> bool {
  return state_machine_->HasMainState(instance_name);
}

auto RaftState::GetDataInstances() const -> std::vector<DataInstanceState> {
  return state_machine_->GetDataInstances();
}

auto RaftState::GetCurrentMainUUID() const -> utils::UUID { return state_machine_->GetCurrentMainUUID(); }

auto RaftState::IsCurrentMain(std::string_view instance_name) const -> bool {
  return state_machine_->IsCurrentMain(instance_name);
}

auto RaftState::TryGetCurrentMainName() const -> std::optional<std::string> {
  return state_machine_->TryGetCurrentMainName();
}

auto RaftState::GetRoutingTable() const -> RoutingTable {
  auto res = RoutingTable{};

  auto const repl_instance_to_bolt = [](auto &&instance) {
    return instance.config.BoltSocketAddress();  // non-resolved IP
  };

  auto const is_instance_main = [&](auto &&instance) { return IsCurrentMain(instance.config.instance_name); };

  auto const is_instance_replica = [&](auto &&instance) { return !IsCurrentMain(instance.config.instance_name); };

  auto const raft_log_data_instances = GetDataInstances();

  auto bolt_mains = raft_log_data_instances | ranges::views::filter(is_instance_main) |
                    ranges::views::transform(repl_instance_to_bolt) | ranges::to<std::vector>();
  MG_ASSERT(bolt_mains.size() <= 1, "There can be at most one main instance active!");

  if (!std::ranges::empty(bolt_mains)) {
    res.emplace_back(std::move(bolt_mains), "WRITE");
  }

  auto bolt_replicas = raft_log_data_instances | ranges::views::filter(is_instance_replica) |
                       ranges::views::transform(repl_instance_to_bolt) | ranges::to<std::vector>();
  if (!std::ranges::empty(bolt_replicas)) {
    res.emplace_back(std::move(bolt_replicas), "READ");
  }

  auto const coord_instance_to_bolt = [](CoordinatorToCoordinatorConfig const &instance) {
    return instance.bolt_server.SocketAddress();  // non-resolved IP
  };

  auto const &raft_log_coord_instances = GetCoordinatorInstances();
  auto bolt_coords =
      raft_log_coord_instances | ranges::views::transform(coord_instance_to_bolt) | ranges::to<std::vector>();

  res.emplace_back(std::move(bolt_coords), "ROUTE");

  return res;
}

auto RaftState::GetLeaderId() const -> uint32_t { return raft_server_->get_leader(); }

}  // namespace memgraph::coordination
#endif
