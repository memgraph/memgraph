// Copyright 2023 Memgraph Ltd.
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
#include "replication/messages.hpp"
#include "rpc/client.hpp"
#include "utils/scheduler.hpp"
#include "utils/thread_pool.hpp"

#include <concepts>
#include <string_view>

namespace memgraph::replication {

template <typename F>
concept InvocableWithStringView = std::invocable<F, std::string_view>;

/// TODO: (andi) Consider adding some var type which would distinguish between MAIN and REPLICA checker
struct ReplicationClient {
  explicit ReplicationClient(const memgraph::replication::ReplicationClientConfig &config);

  ~ReplicationClient();
  ReplicationClient(ReplicationClient const &) = delete;
  ReplicationClient &operator=(ReplicationClient const &) = delete;
  ReplicationClient(ReplicationClient &&) noexcept = delete;
  ReplicationClient &operator=(ReplicationClient &&) noexcept = delete;

  template <InvocableWithStringView F>
  void StartFrequentCheck(F &&callback) {
    // Help the user to get the most accurate replica state possible.
    if (check_frequency_ > std::chrono::seconds(0)) {
      replica_checker_.Run("Replica Checker", check_frequency_, [this, cb = std::forward<F>(callback)] {
        try {
          bool success = false;
          {
            auto stream{rpc_client_.Stream<memgraph::replication::FrequentHeartbeatRpc>()};
            success = stream.AwaitResponse().success;
          }
          if (success) {
            cb(name_);
          }
        } catch (const rpc::RpcFailedException &) {
          // Nothing to do...wait for a reconnect
        }
      });
    }
  }

  std::string name_;
  communication::ClientContext rpc_context_;
  rpc::Client rpc_client_;
  std::chrono::seconds check_frequency_;

  std::optional<memgraph::replication::ReplicationMode> mode_{memgraph::replication::ReplicationMode::SYNC};
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
  utils::ThreadPool thread_pool_{1};

  utils::Scheduler replica_checker_;
};

}  // namespace memgraph::replication
