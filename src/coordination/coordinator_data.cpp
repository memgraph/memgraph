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

#include "coordination/coordinator_data.hpp"

namespace memgraph::coordination {

namespace {

bool ReplicaWithNameExists(const std::list<CoordinatorClientInfo> &replicas, const CoordinatorClientConfig &config) {
  auto name_matches = [&instance_name = config.instance_name](auto const &replica) {
    return replica.InstanceName() == instance_name;
  };
  return std::ranges::any_of(replicas, name_matches);
};

bool ReplicaWithEndpointExists(const std::list<CoordinatorClientInfo> &replicas,
                               const CoordinatorClientConfig &config) {
  auto address_matches = [socket_address = fmt::format("{}:{}", config.ip_address, config.port)](auto const &replica) {
    return replica.SocketAddress() == socket_address;
  };
  return std::ranges::any_of(replicas, address_matches);
};

}  // namespace

CoordinatorData::CoordinatorData() {
  auto replica_find_client_info = [](CoordinatorData *coord_data,
                                     std::string_view instance_name) -> CoordinatorClientInfo & {
    std::shared_lock<utils::RWLock> lock{coord_data->coord_data_lock_};

    auto replica_client_info = std::ranges::find_if(
        coord_data->registered_replicas_info_,
        [instance_name](const CoordinatorClientInfo &replica) { return replica.InstanceName() == instance_name; });

    if (replica_client_info != coord_data->registered_replicas_info_.end()) {
      return *replica_client_info;
    }

    MG_ASSERT(coord_data->registered_main_info_->InstanceName() == instance_name,
              "Instance is neither a replica nor main...");
    return *coord_data->registered_main_info_;
  };

  replica_succ_cb_ = [replica_find_client_info](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto &client_info = replica_find_client_info(coord_data, instance_name);
    client_info.UpdateLastResponseTime();
  };

  replica_fail_cb_ = [replica_find_client_info](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto &client_info = replica_find_client_info(coord_data, instance_name);
    client_info.UpdateInstanceStatus();
  };

  auto get_main_client_info = [](CoordinatorData *coord_data,
                                 std::string_view instance_name) -> CoordinatorClientInfo & {
    MG_ASSERT(coord_data->registered_main_info_.has_value(), "Main info is not set, but callback is called");
    std::shared_lock<utils::RWLock> lock{coord_data->coord_data_lock_};

    // TODO When we will support restoration of main, we have to assert that the instance is main or replica, not at
    // this point....
    auto &registered_main_info = coord_data->registered_main_info_;
    MG_ASSERT(registered_main_info->InstanceName() == instance_name,
              "Callback called for wrong instance name: {}, expected: {}", instance_name,
              registered_main_info->InstanceName());
    return *registered_main_info;
  };

  main_succ_cb_ = [get_main_client_info](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto &registered_main_info = get_main_client_info(coord_data, instance_name);
    registered_main_info.UpdateLastResponseTime();
  };

  main_fail_cb_ = [this, get_main_client_info](CoordinatorData *coord_data, std::string_view instance_name) -> void {
    auto &registered_main_info = get_main_client_info(coord_data, instance_name);
    if (bool main_alive = registered_main_info.UpdateInstanceStatus(); !main_alive) {
      spdlog::warn("Main is not alive, starting failover");
      // switch (auto failover_status = DoFailover(); failover_status) {
      //   using enum DoFailoverStatus;
      //   case ALL_REPLICAS_DOWN:
      //     spdlog::warn("Failover aborted since all replicas are down!");
      //   case MAIN_ALIVE:
      //     spdlog::warn("Failover aborted since main is alive!");
      //   case CLUSTER_UNINITIALIZED:
      //     spdlog::warn("Failover aborted since cluster is uninitialized!");
      //   case SUCCESS:
      //     break;
      // }
    }
  };
}

auto CoordinatorData::DoFailover() -> DoFailoverStatus {
  using ReplicationClientInfo = CoordinatorClientConfig::ReplicationClientInfo;

  // std::lock_guard<utils::RWLock> lock{coord_data_lock_};

  if (!registered_main_info_.has_value()) {
    return DoFailoverStatus::CLUSTER_UNINITIALIZED;
  }

  if (registered_main_info_->IsAlive()) {
    return DoFailoverStatus::MAIN_ALIVE;
  }

  registered_main_->StopFrequentCheck();

  const auto chosen_replica_info = std::ranges::find_if(
      registered_replicas_info_, [](const CoordinatorClientInfo &client_info) { return client_info.IsAlive(); });
  if (chosen_replica_info == registered_replicas_info_.end()) {
    return DoFailoverStatus::ALL_REPLICAS_DOWN;
  }

  auto chosen_replica =
      std::ranges::find_if(registered_replicas_, [&chosen_replica_info](const CoordinatorClient &replica) {
        return replica.InstanceName() == chosen_replica_info->InstanceName();
      });
  MG_ASSERT(chosen_replica != registered_replicas_.end(), "Chosen replica {} not found in registered replicas",
            chosen_replica_info->InstanceName());
  chosen_replica->PauseFrequentCheck();

  std::vector<ReplicationClientInfo> repl_clients_info;
  repl_clients_info.reserve(registered_replicas_.size() - 1);
  std::ranges::for_each(registered_replicas_, [&chosen_replica, &repl_clients_info](const CoordinatorClient &replica) {
    if (replica != *chosen_replica) {
      repl_clients_info.emplace_back(replica.ReplicationClientInfo());
    }
  });

  if (!chosen_replica->SendPromoteReplicaToMainRpc(std::move(repl_clients_info))) {
    spdlog::error("Sent RPC message, but exception was caught, aborting Failover");
    // TODO: new status and rollback all changes that were done...
    MG_ASSERT(false, "RPC message failed");
  }

  registered_replicas_.erase(chosen_replica);
  registered_replicas_info_.erase(chosen_replica_info);

  registered_main_ = std::make_unique<CoordinatorClient>(this, chosen_replica->Config(), chosen_replica->SuccCallback(),
                                                         chosen_replica->FailCallback());
  registered_main_->ReplicationClientInfo().reset();
  registered_main_info_.emplace(*chosen_replica_info);
  registered_main_->StartFrequentCheck();

  return DoFailoverStatus::SUCCESS;
}

auto CoordinatorData::ShowMain() const -> std::optional<CoordinatorInstanceStatus> {
  if (!registered_main_info_.has_value()) {
    return std::nullopt;
  }
  return CoordinatorInstanceStatus{.instance_name = registered_main_info_->InstanceName(),
                                   .socket_address = registered_main_info_->SocketAddress(),
                                   .is_alive = registered_main_info_->IsAlive()};
};

auto CoordinatorData::ShowReplicas() const -> std::vector<CoordinatorInstanceStatus> {
  std::vector<CoordinatorInstanceStatus> instances_status;
  instances_status.reserve(registered_replicas_info_.size());

  std::ranges::transform(registered_replicas_info_, std::back_inserter(instances_status),
                         [](const CoordinatorClientInfo &coord_client_info) {
                           return CoordinatorInstanceStatus{.instance_name = coord_client_info.InstanceName(),
                                                            .socket_address = coord_client_info.SocketAddress(),
                                                            .is_alive = coord_client_info.IsAlive()};
                         });
  return instances_status;
}

auto CoordinatorData::RegisterMain(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  // TODO: (andi) test this
  if (ReplicaWithNameExists(registered_replicas_info_, config)) {
    return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
  }
  if (ReplicaWithEndpointExists(registered_replicas_info_, config)) {
    return RegisterMainReplicaCoordinatorStatus::ENDPOINT_EXISTS;
  }

  registered_main_ = std::make_unique<CoordinatorClient>(this, std::move(config), main_succ_cb_, main_fail_cb_);

  registered_main_info_.emplace(registered_main_->InstanceName(), registered_main_->SocketAddress());
  registered_main_->StartFrequentCheck();

  return RegisterMainReplicaCoordinatorStatus::SUCCESS;
}

auto CoordinatorData::RegisterReplica(CoordinatorClientConfig config) -> RegisterMainReplicaCoordinatorStatus {
  // TODO: (andi) Test it
  if (ReplicaWithNameExists(registered_replicas_info_, config)) {
    return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
  }

  if (registered_main_info_ && registered_main_info_->InstanceName() == config.instance_name) {
    return RegisterMainReplicaCoordinatorStatus::NAME_EXISTS;
  }

  if (ReplicaWithEndpointExists(registered_replicas_info_, config)) {
    return RegisterMainReplicaCoordinatorStatus::ENDPOINT_EXISTS;
  }

  auto *coord_client = &registered_replicas_.emplace_back(this, std::move(config), replica_succ_cb_, replica_fail_cb_);
  registered_replicas_info_.emplace_back(coord_client->InstanceName(), coord_client->SocketAddress());
  coord_client->StartFrequentCheck();

  return RegisterMainReplicaCoordinatorStatus::SUCCESS;
}

}  // namespace memgraph::coordination
#endif
