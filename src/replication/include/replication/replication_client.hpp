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

#include "replication/config.hpp"
#include "replication_coordination_glue/messages.hpp"
#include "rpc/client.hpp"
#include "utils/rw_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/synchronized.hpp"
#include "utils/thread_pool.hpp"

#include <concepts>

namespace memgraph::replication {
struct ReplicationClient;

template <typename F>
concept FrequentCheckCB = std::invocable<F, ReplicationClient &>;

struct ReplicationClient {
  explicit ReplicationClient(const ReplicationClientConfig &config);

  ~ReplicationClient();

  ReplicationClient(ReplicationClient const &) = delete;

  ReplicationClient &operator=(ReplicationClient const &) = delete;

  ReplicationClient(ReplicationClient &&) noexcept = delete;

  ReplicationClient &operator=(ReplicationClient &&) noexcept = delete;

  template <FrequentCheckCB FS, FrequentCheckCB FF>
  void StartFrequentCheck(FS &&callback, FF &&fail_callback) {
    // Help the user to get the most accurate replica state possible.
    if (replica_check_frequency_ > std::chrono::seconds(0)) {
      replica_checker_.SetInterval(replica_check_frequency_);
      replica_checker_.Run("Replica Checker",
                           [this, cb = std::forward<FS>(callback), fail_cb = std::forward<FF>(fail_callback),
                            failed_attempts = 0UL]() mutable {
                             constexpr auto kFailureAfterN = 3UL;
                             try {
                               {
                                 auto stream{rpc_client_.Stream<replication_coordination_glue::FrequentHeartbeatRpc>()};
                                 stream.AwaitResponse();
                               }
                               cb(*this);
                               failed_attempts = 0U;
                             } catch (const rpc::RpcFailedException &) {
                               // Nothing to do...wait for a reconnect
                               // NOTE: Here we are communicating with the instance connection.
                               //       We don't have access to the underlying client; so the only thing we can do it
                               //       tell the callback that this is a reconnection and to check the state
                               if (++failed_attempts == kFailureAfterN) {
                                 fail_cb(*this);
                               }
                             }
                           });
    }
  }

  //! \tparam RPC An rpc::RequestResponse
  //! \tparam Args the args type
  //! \param check predicate to check response is ok
  //! \param args arguments to forward to the rpc request
  //! \return If replica stream is completed or enqueued
  template <typename RPC, typename... Args>
  bool SteamAndFinalizeDelta(auto &&check, Args &&...args) {
    try {
      auto stream = rpc_client_.template Stream<RPC>(std::forward<Args>(args)...);
      auto task = [this, check = std::forward<decltype(check)>(check), stream = std::move(stream)]() mutable {
        if (stream.IsDefunct()) {
          state_.WithLock([](auto &state) { state = State::BEHIND; });
          return false;
        }
        try {
          if (check(stream.AwaitResponse())) {
            return true;
          }
        } catch (rpc::GenericRpcFailedException const &) {
          // swallow error, fallthrough to error handling
        }
        // This replica needs SYSTEM recovery
        state_.WithLock([](auto &state) { state = State::BEHIND; });
        return false;
      };

      if (mode_ == replication_coordination_glue::ReplicationMode::ASYNC) {
        thread_pool_.AddTask([task = utils::CopyMovableFunctionWrapper{std::move(task)}]() mutable { task(); });
        return true;
      }

      return task();
    } catch (rpc::GenericRpcFailedException const &) {
      // This replica needs SYSTEM recovery
      state_.WithLock([](auto &state) { state = State::BEHIND; });
      return false;
    }
  };

  void Shutdown();

  std::string name_;
  communication::ClientContext rpc_context_;
  rpc::Client rpc_client_;
  std::chrono::seconds replica_check_frequency_;
  // True only when we are migrating from V1 or V2 to V3 in replication durability
  // and we want to set replica to listen to main
  bool try_set_uuid{false};

  enum class State {
    BEHIND,
    READY,
    RECOVERY,
  };

  utils::Synchronized<State, utils::WritePrioritizedRWLock> state_{State::BEHIND};

  replication_coordination_glue::ReplicationMode mode_{replication_coordination_glue::ReplicationMode::SYNC};
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
  //    Not having multiple possible threads in the same client allows us
  //    to ignore concurrency problems inside the client.
  utils::ThreadPool thread_pool_{1};
  utils::Scheduler replica_checker_;
};
}  // namespace memgraph::replication
