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

#include <atomic>
#include <functional>
#include <memory>
#include <optional>

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_server.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/raft_state.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "coordination/replication_instance_client.hpp"
#include "coordination/replication_instance_connector.hpp"
#include "utils/resource_lock.hpp"
#include "utils/rw_lock.hpp"
#include "utils/thread_pool.hpp"

#include <list>
#include <range/v3/range/primitives.hpp>

namespace memgraph::coordination {

struct NewMainRes {
  std::string most_up_to_date_instance;
  std::string latest_epoch;
  uint64_t latest_commit_timestamp;
};
using InstanceNameDbHistories = std::pair<std::string, replication_coordination_glue::DatabaseHistories>;

class CoordinatorInstance {
 public:
  explicit CoordinatorInstance(CoordinatorInstanceInitConfig const &config);
  CoordinatorInstance(CoordinatorInstance const &) = delete;
  CoordinatorInstance &operator=(CoordinatorInstance const &) = delete;
  CoordinatorInstance(CoordinatorInstance &&) noexcept = delete;
  CoordinatorInstance &operator=(CoordinatorInstance &&) noexcept = delete;

  ~CoordinatorInstance();

  [[nodiscard]] auto RegisterReplicationInstance(CoordinatorToReplicaConfig const &config)
      -> RegisterInstanceCoordinatorStatus;
  [[nodiscard]] auto UnregisterReplicationInstance(std::string_view instance_name)
      -> UnregisterInstanceCoordinatorStatus;

  [[nodiscard]] auto SetReplicationInstanceToMain(std::string_view instance_name) -> SetInstanceToMainCoordinatorStatus;

  auto ShowInstances() const -> std::vector<InstanceStatus>;

  auto TryFailover() -> void;

  auto AddCoordinatorInstance(CoordinatorToCoordinatorConfig const &config) -> AddCoordinatorInstanceStatus;

  auto GetRoutingTable() const -> RoutingTable;

  static auto ChooseMostUpToDateInstance(std::span<InstanceNameDbHistories> histories) -> NewMainRes;

  auto HasMainState(std::string_view instance_name) const -> bool;

  auto HasReplicaState(std::string_view instance_name) const -> bool;

  auto GetLeaderCoordinatorData() const -> std::optional<CoordinatorToCoordinatorConfig>;

  auto DemoteInstanceToReplica(std::string_view instance_name) -> DemoteInstanceCoordinatorStatus;

  auto TryForceResetClusterState() -> ForceResetClusterStateStatus;

  auto ForceResetClusterState() -> ForceResetClusterStateStatus;

  void ShuttingDown();

 private:
  template <ranges::forward_range R>
  auto GetMostUpToDateInstanceFromHistories(R &&alive_instances) -> std::optional<std::string> {
    if (ranges::empty(alive_instances)) {
      return std::nullopt;
    }
    auto const get_ts = [](ReplicationInstanceConnector &replica) {
      spdlog::trace("Sending get instance timestamps to {}", replica.InstanceName());
      return replica.GetClient().SendGetInstanceTimestampsRpc();
    };

    auto maybe_instance_db_histories = alive_instances | ranges::views::transform(get_ts) | ranges::to<std::vector>();

    auto const ts_has_error = [](auto const &res) -> bool { return res.HasError(); };

    if (std::ranges::any_of(maybe_instance_db_histories, ts_has_error)) {
      spdlog::error("At least one instance which was alive didn't provide per database history.");
      return std::nullopt;
    }

    auto const ts_has_value = [](auto const &zipped) -> bool {
      auto &[replica, res] = zipped;
      return res.HasValue();
    };

    auto transform_to_pairs = ranges::views::transform([](auto const &zipped) {
      auto &[replica, res] = zipped;
      return std::make_pair(replica.InstanceName(), res.GetValue());
    });

    auto instance_db_histories = ranges::views::zip(alive_instances, maybe_instance_db_histories) |
                                 ranges::views::filter(ts_has_value) | transform_to_pairs | ranges::to<std::vector>();

    auto [most_up_to_date_instance, latest_epoch, latest_commit_timestamp] =
        ChooseMostUpToDateInstance(instance_db_histories);

    spdlog::trace("The most up to date instance is {} with epoch {} and {} latest commit timestamp",
                  most_up_to_date_instance, latest_epoch, latest_commit_timestamp);  // NOLINT

    return most_up_to_date_instance;
  }

  auto FindReplicationInstance(std::string_view replication_instance_name) -> ReplicationInstanceConnector &;

  void MainFailCallback(std::string_view);

  void MainSuccessCallback(std::string_view);

  void ReplicaSuccessCallback(std::string_view);

  void ReplicaFailCallback(std::string_view);

  void DemoteSuccessCallback(std::string_view repl_instance_name);

  void DemoteFailCallback(std::string_view repl_instance_name);

  auto ForceResetCluster_() -> ForceResetClusterStateStatus;

  auto GetBecomeLeaderCallback() -> std::function<void()>;
  auto GetBecomeFollowerCallback() -> std::function<void()>;

  HealthCheckClientCallback client_succ_cb_, client_fail_cb_;
  std::atomic<bool> is_leader_ready_{false};
  std::atomic<bool> is_shutting_down_{false};
  // NOTE: Must be std::list because we rely on pointer stability.
  // TODO(antoniofilipovic) do we still rely on pointer stability
  std::list<ReplicationInstanceConnector> repl_instances_;
  mutable utils::ResourceLock coord_instance_lock_{};

  std::unique_ptr<RaftState> raft_state_;

  // Thread pool must be destructed first, because there is option we are doing force reset in thread
  // while coordinator is destructed
  utils::ThreadPool thread_pool_{1};
};

}  // namespace memgraph::coordination
#endif
