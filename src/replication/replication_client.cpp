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

#include "replication/replication_client.hpp"

namespace memgraph::replication {

ReplicationClient::ReplicationClient(const ReplicationClientConfig &config)
    : name_{config.name},
      rpc_context_{communication::CreateClientContext(config.tls_config)},
      rpc_client_{config.repl_server_endpoint, &rpc_context_},
      replica_check_frequency_{config.replica_check_frequency},
      mode_{config.mode} {}

void ReplicationClient::Shutdown() const {
  // Abort must run first. It marks the client as aborted (so no task can open or reconnect a stream) and shuts down the
  // socket to break any in-flight RPC, releasing the rpc client lock. If we stopped the replica checker first, its
  // heartbeat callback could be blocked on that lock (held by an in-flight recovery RPC waiting on InProgressRes), and
  // Scheduler::Stop() would join a thread stuck mid-callback, deadlocking before we ever reach Abort.
  rpc_client_.Abort();
  replica_checker_.Stop();
  thread_pool_.ShutDown();
  rpc_client_.Shutdown();
}

ReplicationClient::~ReplicationClient() {
  auto const &endpoint = rpc_client_.Endpoint();
  try {
    spdlog::trace("Closing replication client on {}:{}.", endpoint.GetAddress(), endpoint.GetPort());
  } catch (...) {
    // Logging can throw. Not a big deal, just ignore.
  }
  Shutdown();
}

}  // namespace memgraph::replication
