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

#include "replication/config.hpp"
#include "replication/coordinator_config.hpp"
#include "replication/messages.hpp"
#include "rpc/client.hpp"
#include "utils/scheduler.hpp"
#include "utils/thread_pool.hpp"

#include <string_view>

namespace memgraph::replication {

#ifdef MG_ENTERPRISE
class CoordinatorClient {
 public:
  explicit CoordinatorClient(const CoordinatorClientConfig &config);

  ~CoordinatorClient();

  CoordinatorClient(CoordinatorClient &other) = delete;
  CoordinatorClient &operator=(CoordinatorClient const &other) = delete;

  CoordinatorClient(CoordinatorClient &&) noexcept = delete;
  CoordinatorClient &operator=(CoordinatorClient &&) noexcept = delete;

  void StartFrequentCheck();

  // TOODO: change method call signature
  bool DoHealthCheck() const;
  bool SendFailoverRpc(std::vector<CoordinatorClientConfig::ReplicationClientInfo> replication_clients_info) const;

  auto InstanceName() const -> std::string_view;
  auto Endpoint() const -> io::network::Endpoint const &;
  auto Config() const -> CoordinatorClientConfig const &;
  auto ReplicationClientInfo() const -> CoordinatorClientConfig::ReplicationClientInfo const &;

  friend bool operator==(CoordinatorClient const &first, CoordinatorClient const &second) {
    return first.config_ == second.config_;
  }

  // TODO: (andi) Do I need this?
  // This thread pool is used for background tasks so we don't
  // block the main storage thread
  // We use only 1 thread for 2 reasons:
  //  - background tasks ALWAYS contain some kind of RPC communication.
  //    We can't have multiple RPC communication from a same client
  //    because that's not logically valid (e.g. you cannot send a snapshot
  //    and WAL at a same time because WAL will arrive earlier and be applied
  //    before the snapshot which is not correct)
  //  - the implementation is simplified as we have a total control of what
  //    this pool is executing. Also, we can simply queue multiple tasks
  //    and be sure of the execution order.
  //    Not having mulitple possible threads in the same client allows us
  //    to ignore concurrency problems inside the client.
 private:
  utils::ThreadPool thread_pool_{1};
  utils::Scheduler replica_checker_;

  communication::ClientContext rpc_context_;
  mutable rpc::Client rpc_client_;
  CoordinatorClientConfig config_;
};
#endif

}  // namespace memgraph::replication
