// Copyright 2026 Memgraph Ltd.
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
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "coordination/constants.hpp"
#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "coordination/logger_wrapper.hpp"
#include "coordination/raft_state.hpp"
#include "coordination/utils.hpp"
#include "utils/counter.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"

#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace {
constexpr std::string_view kStateMgrDurabilityPath = "network";
constexpr std::string_view kLogStoreDurabilityPath = "logs";

void LogRaftResultCode(nuraft::cmd_result_code const raft_code) {
  switch (raft_code) {
    using enum nuraft::cmd_result_code;
    case OK: {
      spdlog::info("Result is OK");
      break;
    }
    case CANCELLED: {
      spdlog::error("The request was cancelled");
      break;
    }
    case TIMEOUT: {
      spdlog::error("The request timeouted");
      break;
    }
    case NOT_LEADER: {
      spdlog::error("Request failed because the coordinator isn't the leader anymore");
      break;
    }
    case BAD_REQUEST: {
      spdlog::error("Bad request was sent");
      break;
    }
    case SERVER_ALREADY_EXISTS: {
      spdlog::error("Request failed because server already exists");
      break;
    }
    case CONFIG_CHANGING: {
      spdlog::error("Request failed because config is changing");
      break;
    }
    case SERVER_IS_JOINING: {
      spdlog::error("Request failed because server is joining");
      break;
    }
    case SERVER_NOT_FOUND: {
      spdlog::error("Request failed because server is not found");
      break;
    }
    case CANNOT_REMOVE_LEADER: {
      spdlog::error("Request failed because the leader cannot be removed");
      break;
    }
    case SERVER_IS_LEAVING: {
      spdlog::error("Request failed because server is leaving");
      break;
    }
    case TERM_MISMATCH: {
      spdlog::error("Request failed because of a term mismatch");
      break;
    }
    case RESULT_NOT_EXIST_YET: {
      spdlog::error("Request failed because the result doesn't exist yet");
      break;
    }
    case FAILED: {
      spdlog::error("Generic request failure");
      break;
    }
    default: {
      std::unreachable();
    }
  }
}

auto RaftResultToRemoveStatus(nuraft::cmd_result_code raft_code)
    -> memgraph::coordination::RemoveCoordinatorInstanceStatus {
  switch (raft_code) {
    using enum memgraph::coordination::RemoveCoordinatorInstanceStatus;
    using enum nuraft::cmd_result_code;
    case OK:
      return SUCCESS;
    case CANCELLED:
      return RAFT_CANCELLED;
    case TIMEOUT:
      return RAFT_TIMEOUT;
    case NOT_LEADER:
      return RAFT_NOT_LEADER;
    case BAD_REQUEST:
      return RAFT_BAD_REQUEST;
    case SERVER_ALREADY_EXISTS:
      return RAFT_SERVER_ALREADY_EXISTS;
    case CONFIG_CHANGING:
      return RAFT_CONFIG_CHANGING;
    case SERVER_IS_JOINING:
      return RAFT_SERVER_IS_JOINING;
    case SERVER_NOT_FOUND:
      return RAFT_SERVER_NOT_FOUND;
    case CANNOT_REMOVE_LEADER:
      return RAFT_CANNOT_REMOVE_LEADER;
    case SERVER_IS_LEAVING:
      return RAFT_SERVER_IS_LEAVING;
    case TERM_MISMATCH:
      return RAFT_TERM_MISMATCH;
    case RESULT_NOT_EXIST_YET:
      return RAFT_RESULT_NOT_EXIST_YET;
    case FAILED:
      return RAFT_FAILED;
    default:
      std::unreachable();
  }
}

auto RaftResultToAddStatus(nuraft::cmd_result_code raft_code) -> memgraph::coordination::AddCoordinatorInstanceStatus {
  switch (raft_code) {
    using enum memgraph::coordination::AddCoordinatorInstanceStatus;
    using enum nuraft::cmd_result_code;
    case OK:
      return SUCCESS;
    case CANCELLED:
      return RAFT_CANCELLED;
    case TIMEOUT:
      return RAFT_TIMEOUT;
    case NOT_LEADER:
      return RAFT_NOT_LEADER;
    case BAD_REQUEST:
      return RAFT_BAD_REQUEST;
    case SERVER_ALREADY_EXISTS:
      return RAFT_SERVER_ALREADY_EXISTS;
    case CONFIG_CHANGING:
      return RAFT_CONFIG_CHANGING;
    case SERVER_IS_JOINING:
      return RAFT_SERVER_IS_JOINING;
    case SERVER_NOT_FOUND:
      return RAFT_SERVER_NOT_FOUND;
    case CANNOT_REMOVE_LEADER:
      return RAFT_CANNOT_REMOVE_LEADER;
    case SERVER_IS_LEAVING:
      return RAFT_SERVER_IS_LEAVING;
    case TERM_MISMATCH:
      return RAFT_TERM_MISMATCH;
    case RESULT_NOT_EXIST_YET:
      return RAFT_RESULT_NOT_EXIST_YET;
    case FAILED:
      return RAFT_FAILED;
    default:
      std::unreachable();
  }
}

}  // namespace

namespace memgraph::coordination {
using nuraft::asio_service;
using nuraft::cb_func;
using nuraft::CbReturnCode;
using nuraft::raft_params;
using nuraft::raft_server;
using nuraft::srv_config;

RaftState::RaftState(CoordinatorInstanceInitConfig const &config, BecomeLeaderCb become_leader_cb,
                     BecomeFollowerCb become_follower_cb, std::optional<CoordinationClusterChangeObserver> observer)
    : coordinator_port_(config.coordinator_port),
      coordinator_id_(config.coordinator_id),
      logger_(std::make_shared<Logger>(config.nuraft_log_file)),
      become_leader_cb_(std::move(become_leader_cb)),
      become_follower_cb_(std::move(become_follower_cb)) {
  auto logger_wrapper = LoggerWrapper(static_cast<Logger *>(logger_.get()));
  auto const log_store_path = config.durability_dir / kLogStoreDurabilityPath;
  utils::EnsureDirOrDie(log_store_path);

  auto durability_store = std::make_shared<kvstore::KVStore>(log_store_path);
  auto const stored_version = static_cast<LogStoreVersion>(
      GetOrSetDefaultVersion(*durability_store, kLogStoreVersion, static_cast<int>(kActiveVersion), logger_wrapper));

  LogStoreDurability const log_store_durability{.durability_store_ = std::move(durability_store),
                                                .stored_log_store_version_ = stored_version};

  auto const state_manager_path = config.durability_dir / kStateMgrDurabilityPath;
  utils::EnsureDirOrDie(state_manager_path);
  CoordinatorStateManagerConfig const state_manager_config{.coordinator_id_ = config.coordinator_id,
                                                           .coordinator_port_ = config.coordinator_port,
                                                           .bolt_port_ = config.bolt_port,
                                                           .management_port_ = config.management_port,
                                                           .coordinator_hostname = config.coordinator_hostname,
                                                           .state_manager_durability_dir_ = state_manager_path,
                                                           .log_store_durability_ = log_store_durability};

  state_machine_ = std::make_shared<CoordinatorStateMachine>(logger_wrapper, log_store_durability);
  state_manager_ = std::make_shared<CoordinatorStateManager>(state_manager_config, logger_wrapper, observer);

  auto const last_commit_index_snapshot = [this]() -> uint64_t {
    if (auto const last_snapshot = state_machine_->last_snapshot(); last_snapshot != nullptr) {
      return last_snapshot->get_last_log_idx();
    }
    return 0;
  }();  // iile

  auto const last_committed_index_state_machine_{state_machine_->last_commit_index()};
  spdlog::trace("Last commited index from snapshot: {}, last commited index in state machine: {}",
                last_commit_index_snapshot,
                last_committed_index_state_machine_);

  auto log_store = state_manager_->load_log_store();
  if (!log_store) {
    return;
  }
  auto *coordinator_log_store = static_cast<CoordinatorLogStore *>(log_store.get());
  auto log_entries =
      coordinator_log_store->GetAllEntriesRange(last_commit_index_snapshot, last_committed_index_state_machine_ + 1);

  for (auto const &[log_id, log] : log_entries) {
    if (log == nullptr) {
      spdlog::error("Log entry for id {} is nullptr", log_id);
      continue;
    }
    spdlog::trace("Applying log entry from log store with index {}", log_id);
    if (log->get_val_type() == nuraft::log_val_type::conf) {
      auto cluster_config = state_manager_->load_config();
      state_machine_->commit_config(log_id, cluster_config);
    } else {
      state_machine_->commit(log_id, log->get_buf());
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

  asio_service_ = std::make_shared<asio_service>(asio_opts, logger_);

  std::shared_ptr<delayed_task_scheduler> scheduler = asio_service_;
  std::shared_ptr<rpc_client_factory> rpc_cli_factory = asio_service_;

  // For some reason context uses shared pointer reference, so we need to cast it
  std::shared_ptr<state_mgr> casted_state_manager = state_manager_;
  std::shared_ptr<state_machine> casted_state_machine = state_machine_;

  asio_listener_ = asio_service_->create_rpc_listener(coordinator_port_, logger_);
  if (!asio_listener_) {
    throw RaftServerStartException("Failed to create rpc listener on port {}", coordinator_port_);
  }

  auto *ctx = new context(
      casted_state_manager, casted_state_machine, asio_listener_, logger_, rpc_cli_factory, scheduler, params);

  raft_server_ = std::make_shared<raft_server>(ctx, init_opts);

  if (!raft_server_) {
    throw RaftServerStartException("Failed to allocate coordinator server on port {}", coordinator_port_);
  }

  spdlog::trace("Raft server allocated on port {}", coordinator_port_);

  // If set to true, server won't be created and exception will be thrown.
  // By setting it to false, all coordinators are started as leaders.
  bool constexpr skip_initial_election_timeout{false};
  raft_server_->start_server(skip_initial_election_timeout);
  spdlog::trace("Raft server started on port {}", coordinator_port_);

  asio_listener_->listen(raft_server_);
  spdlog::trace("Asio listener active on port {}", coordinator_port_);

  // If we don't get initialized in 2min, we throw an exception and abort coordinator initialization.
  // When the follower gets back, it waits for the leader to ping it.
  // In the meantime, the election timer will trigger and the follower will enter the pre-vote protocol which should
  // fail because the leader is actually alive. So even if rpc listener is created (on follower), the initialization
  // isn't complete until leader sends him append_entries_request.
  auto maybe_stop = utils::ResettableCounter{1200};
  while (!maybe_stop()) {
    // Initialized is set to true after raft_callback_ is being executed (role as leader or follower)
    if (raft_server_->is_initialized()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (!raft_server_->is_initialized()) {
    throw RaftServerStartException("Waiting too long for raft server initialization on coordinator with port {}",
                                   coordinator_port_);
  }
}

RaftState::~RaftState() {
  spdlog::trace("Shutting down RaftState for coordinator_{}", coordinator_id_);
  // Destruction order is critical:
  // 1. Shutdown raft_server first - it holds references to state_machine and state_manager
  //    and may be executing callbacks on its threads
  if (raft_server_) {
    raft_server_->shutdown();
    raft_server_.reset();
    spdlog::trace("Raft server closed");
  }

  // 2. Stop and shutdown asio_listener - it's listening for network connections
  if (asio_listener_) {
    asio_listener_->stop();
    asio_listener_->shutdown();
    asio_listener_.reset();
    spdlog::trace("Asio listener closed");
  }

  // 3. Stop asio_service and wait for workers to finish
  if (asio_service_) {
    asio_service_->stop();
    size_t count = 0;
    while (asio_service_->get_active_workers() != 0 && count < 500) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      count++;
    }
    if (asio_service_->get_active_workers() > 0) {
      spdlog::warn("Failed to shutdown raft server correctly for coordinator_{} in 5s", coordinator_id_);
    }
    asio_service_.reset();
    spdlog::trace("Asio service closed");
  }

  // 4. Reset state_machine and state_manager after raft_server is gone
  //    (raft_server holds shared_ptr to these, but we want to be explicit)
  state_machine_.reset();
  state_manager_.reset();

  // 5. Reset logger last - other components may have logged during their destruction
  logger_.reset();

  spdlog::trace("RaftState destruction complete for coordinator_{}", coordinator_id_);
}

auto RaftState::InstanceName() const -> std::string { return fmt::format("coordinator_{}", coordinator_id_); }

auto RaftState::GetMyCoordinatorId() const -> int32_t { return coordinator_id_; }

auto RaftState::GetMyBoltServer() const -> std::optional<std::string> { return GetBoltServer(coordinator_id_); }

auto RaftState::GetBoltServer(int32_t coordinator_id) const -> std::optional<std::string> {
  auto const coord_instances_context = GetCoordinatorInstancesContext();
  auto const target_coordinator = std::ranges::find_if(
      coord_instances_context, [coordinator_id](auto const &coordinator) { return coordinator.id == coordinator_id; });
  if (target_coordinator == coord_instances_context.end()) {
    return {};
  }

  return target_coordinator->bolt_server;
}

auto RaftState::RemoveCoordinatorInstance(int32_t coordinator_id) const -> RemoveCoordinatorInstanceStatus {
  if (const auto cmd_result = raft_server_->remove_srv(coordinator_id);
      cmd_result->get_result_code() != nuraft::cmd_result_code::OK) {
    LogRaftResultCode(cmd_result->get_result_code());
    return RaftResultToRemoveStatus(cmd_result->get_result_code());
  }

  spdlog::info("Request for removing coordinator {} from the cluster accepted", coordinator_id);
  // Waiting for server to join
  constexpr int max_tries{10};
  auto maybe_stop = utils::ResettableCounter(max_tries);
  std::chrono::milliseconds const waiting_period{200};
  bool removed{false};
  while (!maybe_stop()) {
    std::this_thread::sleep_for(waiting_period);
    if (const auto server_config = raft_server_->get_srv_config(coordinator_id); !server_config) {
      spdlog::info("Coordinator with id {} removed from the cluster", coordinator_id);
      removed = true;
      break;
    }
  }

  if (!removed) {
    spdlog::error(
        "Failed to remove coordinator {} from the cluster in {}ms", coordinator_id, max_tries * waiting_period);
    return RemoveCoordinatorInstanceStatus::LOCAL_TIMEOUT;
  }
  return RemoveCoordinatorInstanceStatus::SUCCESS;
}

auto RaftState::AddCoordinatorInstance(CoordinatorInstanceConfig const &config) const -> AddCoordinatorInstanceStatus {
  spdlog::trace(
      "Adding coordinator instance {} start in RaftState for coordinator_{}", config.coordinator_id, coordinator_id_);

  // If I am not adding myself, I need to use add_srv and rely on NuRaft...
  auto const coordinator_server = config.coordinator_server.SocketAddress();  // non-resolved IP
  auto const coord_instance_aux = CoordinatorInstanceAux{.id = config.coordinator_id,
                                                         .coordinator_server = coordinator_server,
                                                         .management_server = config.management_server.SocketAddress()};

  srv_config const srv_config_to_add(
      config.coordinator_id, 0, coordinator_server, nlohmann::json(coord_instance_aux).dump(), false);

  if (const auto cmd_result = raft_server_->add_srv(srv_config_to_add);
      cmd_result->get_result_code() != nuraft::cmd_result_code::OK) {
    LogRaftResultCode(cmd_result->get_result_code());
    return RaftResultToAddStatus(cmd_result->get_result_code());
  }

  spdlog::info("Request to add server {} to the cluster accepted", coordinator_server);

  // Waiting for server to join
  constexpr int max_tries{10};
  auto maybe_stop = utils::ResettableCounter(max_tries);
  std::chrono::milliseconds const waiting_period{200};
  while (!maybe_stop()) {
    std::this_thread::sleep_for(waiting_period);
    if (const auto server_config = raft_server_->get_srv_config(config.coordinator_id)) {
      spdlog::trace("Server with id {} added to cluster", config.coordinator_id);
      return AddCoordinatorInstanceStatus::SUCCESS;
    }
  }
  spdlog::error("Failed to add server {} to the cluster in {}ms", coordinator_server, max_tries * waiting_period);
  return AddCoordinatorInstanceStatus::LOCAL_TIMEOUT;
}

auto RaftState::CoordLastSuccRespMs(int32_t srv_id) const -> std::chrono::milliseconds {
  using std::chrono::duration_cast;
  using std::chrono::microseconds;
  using std::chrono::milliseconds;

  auto const peer_info = raft_server_->get_peer_info(srv_id);
  return duration_cast<milliseconds>(microseconds(peer_info.last_succ_resp_us_));
}

auto RaftState::GetLeaderCoordinatorData() const -> std::optional<LeaderCoordinatorData> {
  auto const leader_id = raft_server_->get_leader();

  auto const coordinator_contexts = GetCoordinatorInstancesContext();
  auto const leader_data = std::ranges::find_if(
      coordinator_contexts,
      [leader_id](CoordinatorInstanceContext const &coordinator) { return coordinator.id == leader_id; });
  if (leader_data == coordinator_contexts.end()) {
    spdlog::trace("Couldn't find data for the current leader.");
    return {};
  }
  return LeaderCoordinatorData{.id = leader_id, .bolt_server = leader_data->bolt_server};
}

auto RaftState::YieldLeadership() const -> void { raft_server_->yield_leadership(); }

auto RaftState::IsLeader() const -> bool { return raft_server_->is_leader(); }

auto RaftState::AppendClusterUpdate(CoordinatorClusterStateDelta const &delta_state) const -> bool {
  auto new_log = CoordinatorStateMachine::SerializeUpdateClusterState(delta_state);
  auto const res = raft_server_->append_entries({new_log});
  if (!res->get_accepted()) {
    spdlog::error("Failed to accept request for updating cluster state.");
    return false;
  }
  spdlog::trace("Request for updating cluster state accepted.");

  if (res->get_result_code() != nuraft::cmd_result_code::OK) {
    spdlog::error("Failed to update cluster state. Error code {}", static_cast<int>(res->get_result_code()));
    return false;
  }

  return true;
}

auto RaftState::MainExists() const -> bool { return state_machine_->MainExists(); }

auto RaftState::HasMainState(std::string_view instance_name) const -> bool {
  return state_machine_->HasMainState(instance_name);
}

auto RaftState::GetDataInstancesContext() const -> std::vector<DataInstanceContext> {
  return state_machine_->GetDataInstancesContext();
}

auto RaftState::GetCoordinatorInstancesContext() const -> std::vector<CoordinatorInstanceContext> {
  return state_machine_->GetCoordinatorInstancesContext();
}

auto RaftState::GetCoordinatorInstancesAux() const -> std::vector<CoordinatorInstanceAux> {
  return state_manager_->GetCoordinatorInstancesAux();
}

auto RaftState::GetMyCoordinatorInstanceAux() const -> CoordinatorInstanceAux {
  auto const coord_instances_aux = GetCoordinatorInstancesAux();
  auto const self_aux = std::ranges::find_if(
      coord_instances_aux,
      [coordinator_id = this->coordinator_id_](auto const &coordinator) { return coordinator_id == coordinator.id; });
  MG_ASSERT(self_aux != coord_instances_aux.end(),
            "Cannot find raft_server::aux for coordinator with id {}.",
            coordinator_id_);
  return *self_aux;
}

auto RaftState::GetCurrentMainUUID() const -> utils::UUID { return state_machine_->GetCurrentMainUUID(); }

auto RaftState::IsCurrentMain(std::string_view const instance_name) const -> bool {
  return state_machine_->IsCurrentMain(instance_name);
}

auto RaftState::TryGetCurrentMainName() const -> std::optional<std::string> {
  return state_machine_->TryGetCurrentMainName();
}

auto RaftState::GetRoutingTable(std::string_view const db_name,
                                std::map<std::string, std::map<std::string, int64_t>> const &replicas_lag) const
    -> RoutingTable {
  auto const is_instance_main = [&](auto const &instance) { return IsCurrentMain(instance.config.instance_name); };
  // Fetch data instances from raft log
  auto const raft_log_data_instances = GetDataInstancesContext();
  auto const coord_servers = GetCoordinatorInstancesContext();

  return CreateRoutingTable(raft_log_data_instances,
                            coord_servers,
                            is_instance_main,
                            GetEnabledReadsOnMain(),
                            GetMaxReplicaReadLag(),
                            db_name,
                            replicas_lag);
}

auto RaftState::GetLeaderId() const -> int32_t { return raft_server_->get_leader(); }

auto RaftState::GetEnabledReadsOnMain() const -> bool { return state_machine_->GetEnabledReadsOnMain(); }

auto RaftState::GetSyncFailoverOnly() const -> bool { return state_machine_->GetSyncFailoverOnly(); }

auto RaftState::GetMaxFailoverReplicaLag() const -> uint64_t { return state_machine_->GetMaxFailoverReplicaLag(); }

auto RaftState::GetMaxReplicaReadLag() const -> uint64_t { return state_machine_->GetMaxReplicaReadLag(); }

auto RaftState::GetDeltasBatchProgressSize() const -> uint64_t { return state_machine_->GetDeltasBatchProgressSize(); }

}  // namespace memgraph::coordination

// namespace memgraph::coordination
#endif
