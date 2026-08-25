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

#pragma once

#include "metrics/prometheus_metrics.hpp"
#include "metrics/scoped_histogram_timer.hpp"
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
  void StartFrequentCheck(FS &&success_callback, FF &&fail_callback) {
    // Help the user to get the most accurate replica state possible.
    if (replica_check_frequency_ > std::chrono::seconds(0)) {
      replica_checker_.SetInterval(replica_check_frequency_);
      replica_checker_.Run(
          "Replica Checker",
          [this,
           succ_cb = std::forward<FS>(success_callback),
           fail_cb = std::forward<FF>(fail_callback),
           failed_attempts = 0UL]() mutable {
            // Measure callbacks also to see how long it takes between scheduled runs
            metrics::ScopedHistogramTimer const timer{metrics::Metrics().global.frequent_heartbeat_rpc_seconds};
            try {
              {
                auto stream{rpc_client_.Stream<replication_coordination_glue::FrequentHeartbeatRpc>()};
                stream.SendAndWait();
              }
              succ_cb(*this);
              failed_attempts = 0U;
            } catch (const rpc::RpcFailedException &) {
              // Nothing to do...wait for a reconnect
              // NOTE: Here we are communicating with the instance connection.
              //       We don't have access to the underlying client; so the only thing we can do it
              //       tell the callback that this is a reconnection and to check the state
              if (constexpr auto kFailureAfterN = 3UL; ++failed_attempts == kFailureAfterN) {
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
  bool StreamAndFinalizeDelta(auto &&check, Args &&...args) {
    try {
      auto stream = rpc_client_.Stream<RPC>(std::forward<Args>(args)...);
      // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
      auto task = [this, check = std::forward<decltype(check)>(check), stream = std::move(stream)]() mutable {
        if (stream.IsDefunct()) {
          state_.WithLock([](auto &state) { state = State::BEHIND; });
          return false;
        }
        try {
          if (check(stream.SendAndWait())) {
            return true;
          }

        }
        // NOLINTNEXTLINE(bugprone-empty-catch)
        catch (rpc::RpcFailedException const &) {  // timeout exception not handled in a specific way
          // swallow error, fallthrough to error handling
        }
        // This replica needs SYSTEM recovery
        state_.WithLock([](auto &state) { state = State::BEHIND; });
        return false;
      };

      if (mode_ == replication_coordination_glue::ReplicationMode::ASYNC) {
        thread_pool_.AddTask(std::move(task));
        return true;
      }

      return task();
    } catch (rpc::RpcFailedException const &) {  // timeout exception not handled in a specific way
      // This replica needs SYSTEM recovery
      state_.WithLock([](auto &state) { state = State::BEHIND; });
      return false;
    }
  };

  // const because at the shutdown time (main thread) we need to take ReadLock() on repl state which requires constness
  // of functions being invoked
  void Shutdown() const;

  std::string name_;
  communication::ClientContext rpc_context_;
  // mutable because at the shutdown time (main thread) we need to take ReadLock() on repl state which requires
  // constness of functions being invoked
  mutable rpc::Client rpc_client_;
  std::chrono::seconds replica_check_frequency_;
  // True only when we are migrating from V1 or V2 to V3 in replication durability
  // and we want to set replica to listen to main
  bool try_set_uuid{false};

  enum class State : uint8_t {
    BEHIND,
    READY,
    RECOVERY,
  };

  utils::Synchronized<State, utils::WritePrioritizedRWLock> state_{State::BEHIND};

  replication_coordination_glue::ReplicationMode mode_{replication_coordination_glue::ReplicationMode::STRICT_SYNC};
  // Background tasks are split across two single-threaded pools by what they may block on, because the
  // commit thread awaits tasks on thread_pool_ while holding a database's engine_lock_:
  //
  //  - thread_pool_ runs the commit-path tasks (transaction encode/ship/decision, ASYNC finalize,
  //    system ASYNC tasks). Tasks here must NEVER block on any database's engine_lock_ — a task that
  //    did would deadlock the commit thread that both holds the lock and awaits the task.
  //  - maintenance_pool_ runs the replica state checks and recovery, which do block on engine locks
  //    (and on the RPC lock while a commit stream is open). They may only ever wait on the commit
  //    path, never the reverse.
  //
  // One thread per pool keeps execution order per task class deterministic. Exclusive use of the
  // underlying connection is NOT provided by these queues: the RPC client's stream lock is what
  // guarantees a single in-flight RPC per client (e.g. a recovery snapshot cannot interleave with a
  // commit's WAL stream), and the per-database replica state machine keeps recovery and commit
  // streaming out of each other's way.
  // Mutable because at the shutdown time (main thread) we need to take ReadLock() on repl state which
  // requires constness of functions being invoked.
  mutable utils::ThreadPool thread_pool_{1};
  mutable utils::ThreadPool maintenance_pool_{1};
  // mutable because at the shutdown time (main thread) we need to take ReadLock() on repl state which requires
  // constness of functions being invoked
  mutable utils::Scheduler replica_checker_;
};
}  // namespace memgraph::replication
