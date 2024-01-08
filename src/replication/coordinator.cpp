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

#include "replication/coordinator.hpp"

#include "flags/replication.hpp"

namespace memgraph::replication {

#ifdef MG_ENTERPRISE

CoordinatorState::CoordinatorState() {
  MG_ASSERT((FLAGS_coordinator && FLAGS_coordinator_server_port == 0) ||
                (!FLAGS_coordinator && FLAGS_coordinator_server_port > 0),
            "Instance must be a coordinator or have a registered coordinator server.");

  // MAIN or REPLICA instance
  if (FLAGS_coordinator_server_port) {
    auto const config = memgraph::replication::ReplicationServerConfig{
        .ip_address = memgraph::replication::kDefaultReplicationServerIp,
        .port = static_cast<uint16_t>(FLAGS_coordinator_server_port),
    };
    coordinator_server_ = std::make_unique<CoordinatorServer>(config);
  }
}

utils::BasicResult<RegisterReplicaError, CoordinatorClient *> CoordinatorState::RegisterReplica(
    const ReplicationClientConfig &config) {
  CoordinatorClient *client{nullptr};

  // TODO: (andi) Solve DRY by extracting
  auto name_check = [&config](auto const &replicas) {
    auto name_matches = [&name = config.name](auto const &replica) { return replica.name_ == name; };
    return std::any_of(replicas.begin(), replicas.end(), name_matches);
  };

  if (name_check(registered_replicas_)) {
    return RegisterReplicaError::NAME_EXISTS;
  }

  // endpoint check
  auto endpoint_check = [&](auto const &replicas) {
    auto endpoint_matches = [&config](auto const &replica) {
      const auto &ep = replica.rpc_client_.Endpoint();
      return ep.address == config.ip_address && ep.port == config.port;
    };
    return std::any_of(replicas.begin(), replicas.end(), endpoint_matches);
  };

  if (endpoint_check(registered_replicas_)) {
    return RegisterReplicaError::END_POINT_EXISTS;
  }

  // Maybe no need to return client if you can start replica client here
  client = &registered_replicas_.emplace_back(config);
  return client;
}

utils::BasicResult<RegisterReplicaError, CoordinatorClient *> CoordinatorState::RegisterMain(
    const ReplicationClientConfig &config) {
  // TODO: (andi) Solve DRY by extracting
  auto name_check = [&config](auto const &replicas) {
    auto name_matches = [&name = config.name](auto const &replica) { return replica.name_ == name; };
    return std::any_of(replicas.begin(), replicas.end(), name_matches);
  };

  if (name_check(registered_replicas_)) {
    return RegisterReplicaError::NAME_EXISTS;
  }

  // endpoint check
  auto endpoint_check = [&](auto const &replicas) {
    auto endpoint_matches = [&config](auto const &replica) {
      const auto &ep = replica.rpc_client_.Endpoint();
      return ep.address == config.ip_address && ep.port == config.port;
    };
    return std::any_of(replicas.begin(), replicas.end(), endpoint_matches);
  };

  if (endpoint_check(registered_replicas_)) {
    return RegisterReplicaError::END_POINT_EXISTS;
  }

  registered_main_ = std::make_unique<CoordinatorClient>(config);
  return registered_main_.get();
}

std::vector<CoordinatorEntityInfo> CoordinatorState::ShowReplicas() const {
  std::vector<CoordinatorEntityInfo> result;
  result.reserve(registered_replicas_.size());
  std::transform(registered_replicas_.begin(), registered_replicas_.end(), std::back_inserter(result),
                 [](const auto &replica) {
                   return CoordinatorEntityInfo{replica.name_, replica.rpc_client_.Endpoint()};
                 });
  return result;
}

std::optional<CoordinatorEntityInfo> CoordinatorState::ShowMain() const {
  if (registered_main_) {
    return CoordinatorEntityInfo{registered_main_->name_, registered_main_->rpc_client_.Endpoint()};
  }
  return std::nullopt;
}

#endif

}  // namespace memgraph::replication
