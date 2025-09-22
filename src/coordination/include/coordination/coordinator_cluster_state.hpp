// Copyright 2025 Memgraph Ltd.
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

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_instance_context.hpp"
#include "coordination/data_instance_context.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/resource_lock.hpp"
#include "utils/uuid.hpp"

#include <libnuraft/nuraft.hxx>
#include <nlohmann/json_fwd.hpp>
#include <range/v3/view.hpp>

#include <string>

namespace memgraph::coordination {

using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::ptr;
using replication_coordination_glue::ReplicationRole;

struct CoordinatorClusterStateDelta {
  std::optional<std::vector<DataInstanceContext>> data_instances_;
  std::optional<std::vector<CoordinatorInstanceContext>> coordinator_instances_;
  std::optional<utils::UUID> current_main_uuid_;
  std::optional<bool> enabled_reads_on_main_;
  std::optional<bool> sync_failover_only_;
  std::optional<uint64_t> max_failover_replica_lag_;

  bool operator==(const CoordinatorClusterStateDelta &other) const = default;
};

// Represents the state of the cluster from the coordinator's perspective.
// Source of truth since it is modified only as the result of RAFT's commiting.
// Needs to be thread safe because the NuRaft's thread is committing and changing the state
// while possibly the user thread interacts with the API and asks for information.
class CoordinatorClusterState {
 public:
  CoordinatorClusterState() = default;

  CoordinatorClusterState(CoordinatorClusterState const &);
  CoordinatorClusterState &operator=(CoordinatorClusterState const &);

  CoordinatorClusterState(CoordinatorClusterState &&other) noexcept;
  CoordinatorClusterState &operator=(CoordinatorClusterState &&other) noexcept;
  ~CoordinatorClusterState() = default;

  auto MainExists() const -> bool;

  auto HasMainState(std::string_view instance_name) const -> bool;

  auto IsCurrentMain(std::string_view instance_name) const -> bool;

  auto DoAction(CoordinatorClusterStateDelta delta_state) -> void;

  auto Serialize(ptr<buffer> &data) const -> void;

  static auto Deserialize(buffer &data) -> CoordinatorClusterState;

  auto GetCoordinatorInstancesContext() const -> std::vector<CoordinatorInstanceContext>;

  auto GetDataInstancesContext() const -> std::vector<DataInstanceContext>;

  auto GetCurrentMainUUID() const -> utils::UUID;

  auto GetEnabledReadsOnMain() const -> bool;

  auto GetSyncFailoverOnly() const -> bool;

  auto GetMaxFailoverReplicaLag() const -> uint64_t;

  auto TryGetCurrentMainName() const -> std::optional<std::string>;

  // Setter function used on parsing data from json
  void SetCurrentMainUUID(utils::UUID);

  // Setter function used on parsing data from json
  void SetDataInstances(std::vector<DataInstanceContext>);

  // Setter function used on parsing data from json
  void SetCoordinatorInstances(std::vector<CoordinatorInstanceContext>);

  // Setter function used on parsing data from json
  void SetEnabledReadsOnMain(bool enabled_reads_on_main);

  void SetSyncFailoverOnly(bool sync_failover_only);

  void SetMaxFailoverLagOnReplica(uint64_t max_failover_replica_lag);

  friend bool operator==(const CoordinatorClusterState &lhs, const CoordinatorClusterState &rhs) {
    if (&lhs == &rhs) {
      return true;
    }
    std::scoped_lock const lock(lhs.app_lock_, rhs.app_lock_);

    return std::tie(lhs.data_instances_, lhs.coordinator_instances_, lhs.current_main_uuid_, lhs.enabled_reads_on_main_,
                    lhs.sync_failover_only_, lhs.max_failover_replica_lag_) ==
           std::tie(rhs.data_instances_, rhs.coordinator_instances_, rhs.current_main_uuid_, rhs.enabled_reads_on_main_,
                    rhs.sync_failover_only_, rhs.max_failover_replica_lag_);
  }

 private:
  std::vector<DataInstanceContext> data_instances_;
  std::vector<CoordinatorInstanceContext> coordinator_instances_;
  utils::UUID current_main_uuid_;
  // The option controls whether reads are enabled from the main instance. The default is set to false so we don't
  // overwhelm main with both read and write requests
  bool enabled_reads_on_main_{false};
  // The option controls whether it is allowed to failover only on sync and strict_sync replicas or async replicas
  // are also supported.
  bool sync_failover_only_{true};
  // The option controls what is the maximum lag allowed on any replica's database so it could be considered a valid
  // failover candidate
  uint64_t max_failover_replica_lag_{std::numeric_limits<uint64_t>::max()};
  mutable utils::ResourceLock app_lock_;
};

void to_json(nlohmann::json &j, CoordinatorClusterState const &state);
void from_json(nlohmann::json const &j, CoordinatorClusterState &instance_state);

}  // namespace memgraph::coordination
#endif
