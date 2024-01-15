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

#include "replication/coordinator_config.hpp"
#include "replication/messages.hpp"
#include "rpc/client.hpp"
#include "utils/scheduler.hpp"
#include "utils/thread_pool.hpp"

#include <string_view>

namespace memgraph::replication {

class CoordinatorClient {
 public:
  explicit CoordinatorClient(const CoordinatorClientConfig &config);

  ~CoordinatorClient();

  CoordinatorClient(CoordinatorClient &other) = delete;
  CoordinatorClient &operator=(CoordinatorClient const &other) = delete;

  CoordinatorClient(CoordinatorClient &&) noexcept = delete;
  CoordinatorClient &operator=(CoordinatorClient &&) noexcept = delete;

  void StartFrequentCheck();
  void StopFrequentCheck();

  auto DoHealthCheck() const -> bool;
  auto SendFailoverRpc(std::vector<CoordinatorClientConfig::ReplicationClientInfo> replication_clients_info) const
      -> bool;

  auto InstanceName() const -> std::string_view;
  auto Endpoint() const -> io::network::Endpoint const &;
  auto Config() const -> CoordinatorClientConfig const &;
  auto ReplicationClientInfo() const -> CoordinatorClientConfig::ReplicationClientInfo const &;
  auto ReplicationClientInfo() -> std::optional<CoordinatorClientConfig::ReplicationClientInfo> &;

  friend bool operator==(CoordinatorClient const &first, CoordinatorClient const &second) {
    return first.config_ == second.config_;
  }

 private:
  utils::ThreadPool thread_pool_{1};
  utils::Scheduler replica_checker_;

  communication::ClientContext rpc_context_;
  mutable rpc::Client rpc_client_;
  CoordinatorClientConfig config_;
};
#endif

}  // namespace memgraph::replication
