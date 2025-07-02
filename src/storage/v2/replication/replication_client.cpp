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

#include "replication/replication_client.hpp"

#include "flags/coord_flag_env_handler.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/recovery.hpp"
#include "storage/v2/storage.hpp"
#include "utils/event_histogram.hpp"
#include "utils/exceptions.hpp"
#include "utils/metrics_timer.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/uuid.hpp"
#include "utils/variant_helpers.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>

namespace {
template <typename>
[[maybe_unused]] inline constexpr bool always_false_v = false;

constexpr auto kHeartbeatRpcTimeout = std::chrono::milliseconds(5000);
constexpr auto kCommitRpcTimeout = std::chrono::milliseconds(50);

using memgraph::storage::replication::ReplicaState;
using namespace std::string_view_literals;

constexpr auto StateToString(ReplicaState const &replica_state) -> std::string_view {
  switch (replica_state) {
    case ReplicaState::MAYBE_BEHIND:
      return "MAYBE_BEHIND"sv;
    case ReplicaState::READY:
      return "READY"sv;
    case ReplicaState::REPLICATING:
      return "REPLICATING"sv;
    case ReplicaState::RECOVERY:
      return "RECOVERY"sv;
    case ReplicaState::DIVERGED_FROM_MAIN:
      return "DIVERGED_FROM_MAIN"sv;
    default:
      return "Unknown ReplicaState"sv;
  }
}
}  // namespace

namespace memgraph::metrics {
extern const Event HeartbeatRpc_us;
extern const Event PrepareCommitRpc_us;
extern const Event ReplicaStream_us;
extern const Event StartTxnReplication_us;
extern const Event FinalizeTxnReplication_us;
}  // namespace memgraph::metrics

namespace memgraph::storage {
ReplicationStorageClient::ReplicationStorageClient(::memgraph::replication::ReplicationClient &client,
                                                   utils::UUID const main_uuid)
    : client_{client}, main_uuid_(main_uuid) {}

void ReplicationStorageClient::UpdateReplicaState(Storage *main_storage, DatabaseAccessProtector db_acc) {
  auto &replStorageState = main_storage->repl_storage_state_;
  auto const &main_db_name = main_storage->name();

  // stream should be destroyed so that RPC lock is released before taking engine lock
  std::optional<replication::HeartbeatRes> const maybe_heartbeat_res =
      std::invoke([&]() -> std::optional<replication::HeartbeatRes> {
        utils::MetricsTimer const timer{metrics::HeartbeatRpc_us};

        // if ASYNC replica, try lock for 10s and if the lock cannot be obtained, skip this task
        // frequent heartbeat should reschedule the next one and should be OK. By this skipping, we prevent deadlock
        // from happening when there are old tasks in the queue which need to UpdateReplicaState and the newer commit
        // task. The deadlock would've occurred because in the commit we hold RPC lock all the time but the task cannot
        // get scheduled since UpdateReplicaState tasks cannot finish due to impossibility to get RPC lock
        if (client_.mode_ == replication_coordination_glue::ReplicationMode::ASYNC) {
          auto hb_stream = client_.rpc_client_.TryStream<replication::HeartbeatRpc>(
              std::optional{kHeartbeatRpcTimeout}, main_uuid_, main_storage->uuid(),
              replStorageState.last_durable_timestamp_.load(std::memory_order_acquire),
              std::string{replStorageState.epoch_.id()});

          std::optional<replication::HeartbeatRes> res;
          if (hb_stream.has_value()) {
            res.emplace(hb_stream->SendAndWait());
          }
          return res;
        }

        // If SYNC or STRICT_SYNC replica, block while waiting for RPC lock
        auto hb_stream = client_.rpc_client_.Stream<replication::HeartbeatRpc>(
            main_uuid_, main_storage->uuid(), replStorageState.last_durable_timestamp_.load(std::memory_order_acquire),
            std::string{replStorageState.epoch_.id()});
        return hb_stream.SendAndWait();
      });

  if (!maybe_heartbeat_res.has_value()) {
    spdlog::trace("Couldn't get RPC lock while trying to UpdateReplicaState");
    return;
  }

  auto const &heartbeat_res = *maybe_heartbeat_res;

  if (heartbeat_res.success) {
    last_known_ts_.store(heartbeat_res.current_commit_timestamp, std::memory_order_release);
  } else {
#ifdef MG_ENTERPRISE  // Multi-tenancy is only supported in enterprise
    // Replica is missing the current database
    client_.state_.WithLock([&](auto &state) {
      spdlog::debug("Replica '{}' can't respond or missing database '{}' - '{}'", client_.name_, main_db_name,
                    std::string{main_storage->uuid()});
      state = memgraph::replication::ReplicationClient::State::BEHIND;
    });
    return;
#endif
  }

  std::optional<uint64_t> branching_point;
  // different epoch id, replica was main
  // In case there is no epoch transfer, and MAIN doesn't hold all the epochs as it could have been down and miss it
  // we need then just to check commit timestamp
  if (heartbeat_res.epoch_id != replStorageState.epoch_.id() &&
      heartbeat_res.current_commit_timestamp != kTimestampInitialId) {
    spdlog::trace(
        "DB: {} Replica {}: Epoch id: {}, last_durable_timestamp: {}; Main: Epoch id: {}, last_durable_timestamp: {}",
        main_db_name, client_.name_, std::string(heartbeat_res.epoch_id), heartbeat_res.current_commit_timestamp,
        std::string(replStorageState.epoch_.id()),
        replStorageState.last_durable_timestamp_.load(std::memory_order_acquire));

    auto const &history = replStorageState.history;
    const auto epoch_info_iter = std::find_if(history.crbegin(), history.crend(), [&](const auto &main_epoch_info) {
      return main_epoch_info.first == heartbeat_res.epoch_id;
    });

    if (epoch_info_iter == history.crend()) {
      branching_point = 0;
      spdlog::trace("Couldn't find epoch {} in main for db {}, setting branching point to 0.",
                    std::string(heartbeat_res.epoch_id), main_db_name);
    } else if (epoch_info_iter->second <
               heartbeat_res
                   .current_commit_timestamp) {  // replica has larger commit ts associated with epoch than main
      spdlog::trace(
          "Found epoch {} on main for db {} with last_durable_timestamp {}, replica {} has last_durable_timestamp {}. "
          "Setting "
          "branching point to {}.",
          std::string(epoch_info_iter->first), main_db_name, epoch_info_iter->second, client_.name_,
          heartbeat_res.current_commit_timestamp, epoch_info_iter->second);
      branching_point = epoch_info_iter->second;
    } else {
      branching_point = std::nullopt;
      spdlog::trace(
          "Found continuous history between replica {} and main for db {}. Our commit timestamp for epoch {} was {}.",
          client_.name_, main_db_name, epoch_info_iter->first, epoch_info_iter->second);
    }
  }

  if (branching_point) {
    auto log_error = [replica_name = client_.name_]() {
      spdlog::error(
          "You cannot register Replica {} to this Main because at one point "
          "Replica {} acted as the Main instance. Both the Main and Replica {} "
          "now hold unique data. Please resolve data conflicts and start the "
          "replication on a clean instance.",
          replica_name, replica_name, replica_name);
    };
#ifdef MG_ENTERPRISE
    // when using enterprise replication print error when branching point is encountered
    if (!flags::CoordinationSetupInstance().IsDataInstanceManagedByCoordinator()) {
      replica_state_.WithLock([&](auto &state) {
        if (state != ReplicaState::DIVERGED_FROM_MAIN) {
          log_error();
          state = ReplicaState::DIVERGED_FROM_MAIN;
        }
      });
      return;
    }
    // When using HA, set the state to recovery and recover replica
    spdlog::debug("Found branching point on replica {} for db {}. Recovery will be executed with force reset option.",
                  client_.name_, main_db_name);
    replica_state_.WithLock([&](auto &state) {
      state = ReplicaState::RECOVERY;
      client_.thread_pool_.AddTask([main_storage, gk = std::move(db_acc), this] {
        this->RecoverReplica(/*replica_last_commit_ts*/ 0, main_storage,
                             true);  // needs force reset so we need to recover from 0.
      });
    });
#else
    // when using community replication print error when branching point is encountered and set the state to DIVERGED
    // FROM MAIN
    replica_state_.WithLock([&](auto &state) {
      if (state != ReplicaState::DIVERGED_FROM_MAIN) {
        log_error();
        state = ReplicaState::DIVERGED_FROM_MAIN;
      }
    });
#endif
    return;
  }

  // No branching point
  // Lock engine lock in order to read main_storage timestamp and synchronize with any active commits
  auto engine_lock = std::unique_lock{main_storage->engine_lock_};
  spdlog::trace("Current timestamp on replica {} for db {} is {}.", client_.name_, main_db_name,
                heartbeat_res.current_commit_timestamp);
  spdlog::trace("Current durable timestamp on main for db {} is {}", main_db_name,
                replStorageState.last_durable_timestamp_.load(std::memory_order_acquire));

  replica_state_.WithLock([&](auto &state) {
    // Recovered state didn't change in the meantime
    // ldt can be larger on replica due to snapshots
    if (heartbeat_res.current_commit_timestamp >=
        replStorageState.last_durable_timestamp_.load(std::memory_order_acquire)) {
      spdlog::debug("Replica {} up to date for db {}.", client_.name_, main_db_name);
      state = ReplicaState::READY;
    } else {
      spdlog::debug("Replica {} is behind for db {}.", client_.name_, main_db_name);
      state = ReplicaState::RECOVERY;
      client_.thread_pool_.AddTask([main_storage, current_commit_timestamp = heartbeat_res.current_commit_timestamp,
                                    gk = std::move(db_acc),
                                    this] { this->RecoverReplica(current_commit_timestamp, main_storage); });
    }
  });
}

TimestampInfo ReplicationStorageClient::GetTimestampInfo(Storage const *storage) const {
  TimestampInfo info;
  auto const main_timestamp = storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire);
  info.current_timestamp_of_replica = last_known_ts_.load(std::memory_order::acquire);
  info.current_number_of_timestamp_behind_main = info.current_timestamp_of_replica - main_timestamp;
  return info;
}

void ReplicationStorageClient::LogRpcFailure() const {
  spdlog::error(
      utils::MessageWithLink("Couldn't replicate data to {}.", client_.name_, "https://memgr.ph/replication"));
}

void ReplicationStorageClient::TryCheckReplicaStateAsync(Storage *main_storage, DatabaseAccessProtector db_acc) {
  client_.thread_pool_.AddTask([main_storage, db_acc = std::move(db_acc), this]() mutable {
    this->TryCheckReplicaStateSync(main_storage, std::move(db_acc));
  });
}

void ReplicationStorageClient::ForceRecoverReplica(Storage *main_storage, DatabaseAccessProtector db_acc) const {
  spdlog::debug("Force recoverying replica {} for db {}", client_.name_,
                static_cast<InMemoryStorage *>(main_storage)->name());
  replica_state_.WithLock([&](auto &state) {
    state = ReplicaState::RECOVERY;
    client_.thread_pool_.AddTask([main_storage, gk = std::move(db_acc), this] {
      this->RecoverReplica(/*replica_last_commit_ts*/ 0, main_storage,
                           true);  // needs force reset so we need to recover from 0.
    });
  });
}

void ReplicationStorageClient::TryCheckReplicaStateSync(Storage *main_storage, DatabaseAccessProtector db_acc) {
  try {
    UpdateReplicaState(main_storage, std::move(db_acc));
  } catch (const rpc::VersionMismatchRpcFailedException &) {
    replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
    spdlog::error(utils::MessageWithLink(
        "Failed to connect to replica {} at the endpoint {}. Because the replica "
        "deployed is not a compatible version.",
        client_.name_, client_.rpc_client_.Endpoint().SocketAddress(), "https://memgr.ph/replication"));
  } catch (const rpc::RpcFailedException &) {
    replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
    spdlog::error(utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}.", client_.name_,
                                         client_.rpc_client_.Endpoint().SocketAddress(),
                                         "https://memgr.ph/replication"));
  }
}

// If replica is in state RECOVERY -> skip
// If replica is REPLICATING old txn (ASYNC), set the state to MAYBE_BEHIND
// If replica is MAYBE_BEHIND, skip and asynchronously check the state of the replica
// If replica is DIVERGED_FROM_MAIN, skip
// If replica is READY, set it to replicating and create optional stream
//    If creating stream fails, set the state to MAYBE_BEHIND. RPC lock is taken.
auto ReplicationStorageClient::StartTransactionReplication(const uint64_t current_wal_seq_num, Storage *storage,
                                                           DatabaseAccessProtector db_acc,
                                                           bool const commit_immediately)
    -> std::optional<ReplicaStream> {
  utils::MetricsTimer const timer{metrics::StartTxnReplication_us};
  auto locked_state = replica_state_.Lock();
  spdlog::trace("Starting transaction replication for replica {} in state {}", client_.name_,
                StateToString(*locked_state));
  switch (*locked_state) {
    using enum ReplicaState;
    case RECOVERY: {
      spdlog::debug("Replica {} is behind MAIN instance", client_.name_);
      return std::nullopt;
    }
    case REPLICATING: {
      spdlog::debug("Replica {} missed a transaction", client_.name_);
      // We missed a transaction because we're still replicating
      // the previous transaction. We will go to MAYBE_BEHIND state so that frequent heartbeat enqueues the recovery
      // task to the queue.
      *locked_state = MAYBE_BEHIND;
      return std::nullopt;
    }
    case MAYBE_BEHIND: {
      spdlog::error(
          utils::MessageWithLink("Couldn't replicate data to {}.", client_.name_, "https://memgr.ph/replication"));
      TryCheckReplicaStateAsync(storage, std::move(db_acc));
      return std::nullopt;
    }
    case DIVERGED_FROM_MAIN: {
      spdlog::error(utils::MessageWithLink("Couldn't replicate data to {} since replica has diverged from main.",
                                           client_.name_, "https://memgr.ph/replication"));
      return std::nullopt;
    }
    case READY: {
      try {
        utils::MetricsTimer const replica_stream_timer{metrics::ReplicaStream_us};
        std::optional<rpc::Client::StreamHandler<replication::PrepareCommitRpc>> maybe_stream_handler;

        // Try to obtain RPC stream for ASYNC replica
        if (client_.mode_ == replication_coordination_glue::ReplicationMode::ASYNC) {
          maybe_stream_handler = client_.rpc_client_.TryStream<replication::PrepareCommitRpc>(
              std::optional{kCommitRpcTimeout}, main_uuid_, storage->uuid(),
              storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire), current_wal_seq_num,
              commit_immediately);
        } else {  // Block for SYNC and STRICT_SYNC replica
          maybe_stream_handler.emplace(client_.rpc_client_.Stream<replication::PrepareCommitRpc>(
              main_uuid_, storage->uuid(),
              storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire), current_wal_seq_num,
              commit_immediately));
        }

        if (!maybe_stream_handler.has_value()) {
          spdlog::trace("Couldn't obtain RPC lock for committing to ASYNC replica.");
          *locked_state = MAYBE_BEHIND;
          return std::nullopt;
        }

        *locked_state = REPLICATING;
        return ReplicaStream(storage, std::move(*maybe_stream_handler));
      } catch (const rpc::RpcFailedException &) {
        *locked_state = MAYBE_BEHIND;
        LogRpcFailure();
        return std::nullopt;
      }
    }
    default:
      LOG_FATAL("Unknown replica state when starting transaction replication.");
  }
}

// RPC lock released at the end of this function
// ReSharper disable once CppMemberFunctionMayBeConst
[[nodiscard]] bool ReplicationStorageClient::SendFinalizeCommitRpc(
    bool const decision, utils::UUID const &storage_uuid, DatabaseAccessProtector db_acc,
    uint64_t const durability_commit_timestamp, std::optional<ReplicaStream> &&replica_stream) noexcept {
  // Just skip instance which was down before voting
  if (!replica_stream.has_value()) {
    return true;
  }

  try {
    auto stream{client_.rpc_client_.UpgradeStream<replication::FinalizeCommitRpc>(
        std::move(replica_stream->GetStreamHandler()), decision, main_uuid_, storage_uuid,
        durability_commit_timestamp)};
    return stream.SendAndWait().success;
  } catch (const rpc::RpcFailedException &) {
    // Frequent heartbeat should trigger the recovery. Until then, commits on MAIN won't be allowed
    return false;
  }
}

// Only STRICT_SYNC replicas can call this function
bool ReplicationStorageClient::FinalizePrepareCommitPhase(DatabaseAccessProtector db_acc,
                                                          std::optional<ReplicaStream> &replica_stream,
                                                          uint64_t durability_commit_timestamp) const {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  utils::MetricsTimer const timer{metrics::FinalizeTxnReplication_us};
  auto const continue_finalize = replica_state_.WithLock([this, &replica_stream](auto &state) mutable {
    spdlog::trace("Finalizing 1st phase on replica {} in state {}", client_.name_, StateToString(state));

    if (state != ReplicaState::REPLICATING) {
      // Recovery finished between the txn start and txn finish.
      // Set the state to maybe behind and rely on heartbeat to check asynchronously the state
      if (state == ReplicaState::READY) {
        replica_stream.reset();
        state = ReplicaState::MAYBE_BEHIND;
      }
      return false;
    }
    return true;
  });

  if (!continue_finalize) {
    return false;
  }

  if (!replica_stream || replica_stream->IsDefunct()) {
    replica_state_.WithLock([&replica_stream](auto &state) {
      replica_stream.reset();
      state = ReplicaState::MAYBE_BEHIND;
    });
    LogRpcFailure();
    return false;
  }

  MG_ASSERT(replica_stream, "Missing stream for transaction deltas for replica {}", client_.name_);
  try {
    auto response = replica_stream->Finalize();
    // NOLINTNEXTLINE
    return replica_state_.WithLock(
        [this, response, db_acc = std::move(db_acc), durability_commit_timestamp](auto &state) mutable {
          // If we didn't receive successful response to PrepareCommit, or we got into MAYBE_BEHIND state since the
          // moment we started committing as ASYNC replica, we cannot set the ready state. We could have got into
          // MAYBE_BEHIND state if we missed next txn.
          if (state != ReplicaState::REPLICATING) {
            return false;
          }

          if (!response.success) {
            state = ReplicaState::MAYBE_BEHIND;
            return false;
          }

          last_known_ts_.store(durability_commit_timestamp, std::memory_order_release);
          state = ReplicaState::READY;
          return true;
        });
  } catch (const rpc::RpcFailedException &) {
    replica_state_.WithLock([&replica_stream](auto &state) {
      replica_stream.reset();
      state = ReplicaState::MAYBE_BEHIND;
    });
    LogRpcFailure();
    return false;
  }
}

bool ReplicationStorageClient::FinalizeTransactionReplication(DatabaseAccessProtector db_acc,
                                                              std::optional<ReplicaStream> &&replica_stream,
                                                              uint64_t durability_commit_timestamp) const {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  utils::MetricsTimer const timer{metrics::FinalizeTxnReplication_us};
  auto const continue_finalize = replica_state_.WithLock([this, &replica_stream](auto &state) mutable {
    spdlog::trace("Finalizing transaction on replica {} in state {}", client_.name_, StateToString(state));

    if (state != ReplicaState::REPLICATING) {
      // Recovery finished between the txn start and txn finish.
      // Set the state to maybe behind and rely on heartbeat to check asynchronously the state
      if (state == ReplicaState::READY) {
        replica_stream.reset();
        state = ReplicaState::MAYBE_BEHIND;
      }
      return false;
    }
    return true;
  });

  if (!continue_finalize) {
    return false;
  }

  if (!replica_stream || replica_stream->IsDefunct()) {
    replica_state_.WithLock([&replica_stream](auto &state) {
      replica_stream.reset();
      state = ReplicaState::MAYBE_BEHIND;
    });
    LogRpcFailure();
    return false;
  }

  auto task = [this, db_acc = std::move(db_acc), replica_stream_obj = std::move(replica_stream),
               durability_commit_timestamp]() mutable -> bool {
    MG_ASSERT(replica_stream_obj, "Missing stream for transaction deltas for replica {}", client_.name_);
    try {
      auto response = replica_stream_obj->Finalize();
      // NOLINTNEXTLINE
      return replica_state_.WithLock([this, response, db_acc = std::move(db_acc), &replica_stream_obj,
                                      durability_commit_timestamp](auto &state) mutable {
        replica_stream_obj.reset();
        // If we didn't receive successful response to AppendDeltas, or we got into MAYBE_BEHIND state since the
        // moment we started committing as ASYNC replica, we cannot set the ready state. We could have got into
        // MAYBE_BEHIND state if we missed next txn.
        if (state != ReplicaState::REPLICATING) {
          return false;
        }

        if (!response.success) {
          state = ReplicaState::MAYBE_BEHIND;
          return false;
        }

        last_known_ts_.store(durability_commit_timestamp, std::memory_order_release);
        state = ReplicaState::READY;
        return true;
      });
    } catch (const rpc::RpcFailedException &) {
      replica_state_.WithLock([&replica_stream_obj](auto &state) {
        replica_stream_obj.reset();
        state = ReplicaState::MAYBE_BEHIND;
      });
      LogRpcFailure();
      return false;
    }
  };

  if (client_.mode_ == replication_coordination_glue::ReplicationMode::ASYNC) {
    // When in ASYNC mode, we ignore the return value from task() and always return true
    client_.thread_pool_.AddTask([task = utils::CopyMovableFunctionWrapper{std::move(task)}]() mutable { task(); });
    return true;
  }

  // If we are in SYNC mode, we return the result of task().
  return task();
}

void ReplicationStorageClient::Start(Storage *storage, DatabaseAccessProtector db_acc) {
  spdlog::trace("Replication client started for database \"{}\"", storage->name());
  TryCheckReplicaStateSync(storage, std::move(db_acc));
}

// The function is finished by setting replica to READY state or by setting it to MAYBE_BEHIND state.
// The replica will be considered as READY if it gets fully recovered. If there are commits taking place while recovery
// is running, the replica will be again set to MAYBE_BEHIND state.
void ReplicationStorageClient::RecoverReplica(uint64_t replica_last_commit_ts, Storage *main_storage,
                                              bool const reset_needed) const {
  auto const &main_db_name = main_storage->name();

  if (main_storage->storage_mode_ != StorageMode::IN_MEMORY_TRANSACTIONAL) {
    throw utils::BasicException("Only InMemoryTransactional mode supports replication!");
  }

  // The recovery task could get executed at any point in the future hence some previous recovery task could have
  // already recovered replica.
  if (*replica_state_.Lock() != ReplicaState::RECOVERY) {
    spdlog::info("Replica {} is not in RECOVERY state anymore for db {}, ending the recovery task.", client_.name_,
                 main_db_name);
    return;
  }

  spdlog::debug("Starting replica {} recovery for db {}.", client_.name_, main_db_name);

  auto *main_mem_storage = static_cast<InMemoryStorage *>(main_storage);

  rpc::Client &rpcClient = client_.rpc_client_;
  bool recovery_failed{false};
  auto file_locker = main_mem_storage->file_retainer_.AddLocker();
  auto const maybe_steps = GetRecoverySteps(replica_last_commit_ts, &file_locker, main_mem_storage);
  if (!maybe_steps.has_value()) {
    spdlog::error(
        "Couldn't get recovery steps while trying to recover replica {} for db {}, setting the replica state to "
        "MAYBE_BEHIND",
        client_.name_, main_db_name);
    replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
    return;
  }
  auto const &steps = *maybe_steps;

  for (auto const &[step_index, recovery_step] : ranges::views::enumerate(steps)) {
    try {
      spdlog::trace("Replica: {}, db: {}. Recovering in step: {}. Current local replica commit: {}.", client_.name_,
                    main_db_name, step_index, replica_last_commit_ts);
      std::visit(
          utils::Overloaded{
              [this, &replica_last_commit_ts, main_mem_storage, &rpcClient, &recovery_failed, main_uuid = main_uuid_,
               &main_db_name, repl_mode = client_.mode_](RecoverySnapshot const &snapshot) {
                spdlog::debug("Sending the latest snapshot file {} to {} for db {}", snapshot, client_.name_,
                              main_db_name);
                // Loading snapshot on the replica side either passes cleanly or it doesn't pass at all. If it doesn't
                // pass, we won't update commit timestamp. Heartbeat should trigger recovering replica again.
                auto const maybe_response = TransferDurabilityFiles<replication::SnapshotRpc>(
                    snapshot, rpcClient, repl_mode, main_uuid, main_mem_storage->uuid());
                // Error happened on our side when trying to load snapshot file
                if (!maybe_response.has_value()) {
                  recovery_failed = true;
                  return;
                }

                if (auto const &response = *maybe_response; response.current_commit_timestamp.has_value()) {
                  replica_last_commit_ts = *(response.current_commit_timestamp);
                  spdlog::debug(
                      "Successful reply to the snapshot file {} received from {} for db {}. Current replica commit is "
                      "{}",
                      snapshot, client_.name_, main_db_name, replica_last_commit_ts);
                } else {
                  spdlog::debug(
                      "Unsuccessful reply to the snapshot file {} received from {} for db {}. Current replica commit "
                      "is {}",
                      snapshot, client_.name_, main_db_name, replica_last_commit_ts);
                  recovery_failed = true;
                }
              },
              [this, &replica_last_commit_ts, main_mem_storage, &rpcClient, &recovery_failed,
               do_reset = reset_needed && step_index == 0, main_uuid = main_uuid_, &main_db_name,
               repl_mode = client_.mode_](RecoveryWals const &wals) {
                spdlog::debug("Sending the latest wal files to {} for db {}.", client_.name_, main_db_name);

                if (wals.empty()) {
                  spdlog::trace("Wal files list is empty, nothing to send.");
                  return;
                }
                spdlog::debug("Sending WAL files to {} for db {}.", client_.name_, main_db_name);
                // We don't care about partial progress when loading WAL files. We are only interested if everything
                // passed so that possibly next step of recovering current wal can be executed

                auto const maybe_response = TransferDurabilityFiles<replication::WalFilesRpc>(
                    wals, rpcClient, repl_mode, wals.size(), main_uuid, main_mem_storage->uuid(), do_reset);
                if (!maybe_response.has_value()) {
                  recovery_failed = true;
                  return;
                }

                if (auto const &response = *maybe_response; response.current_commit_timestamp.has_value()) {
                  replica_last_commit_ts = *(response.current_commit_timestamp);
                  spdlog::debug(
                      "Successful reply to WAL files received from {} for db {}. Updating replica commit to {}",
                      client_.name_, main_db_name, replica_last_commit_ts);
                } else {
                  spdlog::debug(
                      "Unsuccessful reply to WAL files received from {} for db {}. Current replica commit is {}.",
                      client_.name_, main_db_name, replica_last_commit_ts);
                  recovery_failed = true;
                }
              },
              [this, &replica_last_commit_ts, main_mem_storage, &rpcClient, &recovery_failed,
               do_reset = reset_needed && step_index == 0, main_uuid = main_uuid_, &main_db_name,
               repl_mode = client_.mode_](RecoveryCurrentWal const &current_wal) {
                std::unique_lock transaction_guard(main_mem_storage->engine_lock_);
                if (main_mem_storage->wal_file_ &&
                    main_mem_storage->wal_file_->SequenceNumber() == current_wal.current_wal_seq_num) {
                  utils::OnScopeExit const on_exit(
                      [main_mem_storage]() { main_mem_storage->wal_file_->EnableFlushing(); });
                  main_mem_storage->wal_file_->DisableFlushing();
                  transaction_guard.unlock();
                  spdlog::debug("Sending current wal file to {} for db {}.", client_.name_, main_db_name);

                  auto const maybe_response = TransferDurabilityFiles<replication::CurrentWalRpc>(
                      main_mem_storage->wal_file_->Path(), rpcClient, repl_mode, main_uuid, main_mem_storage->uuid(),
                      do_reset);
                  // Error happened on our side when trying to load current WAL file
                  if (!maybe_response.has_value()) {
                    recovery_failed = true;
                    return;
                  }

                  if (auto const &response = *maybe_response; response.current_commit_timestamp.has_value()) {
                    replica_last_commit_ts = *(response.current_commit_timestamp);
                    spdlog::debug(
                        "Successful reply to the current WAL received from {} for db {}. Current replica commit is {}",
                        client_.name_, main_db_name, replica_last_commit_ts);
                  } else {
                    spdlog::debug(
                        "Unsuccessful reply to WAL files received from {} for db {}. Current replica commit is {}",
                        client_.name_, main_db_name, replica_last_commit_ts);
                    recovery_failed = true;
                  }
                } else {
                  spdlog::debug("Cannot recover using current wal file {} for db {}.", client_.name_, main_db_name);
                }
              },
              []<typename T>(T const &) { static_assert(always_false_v<T>, "Missing type from variant visitor"); },
          },
          recovery_step);
    } catch (const rpc::RpcFailedException &) {
      last_known_ts_.store(replica_last_commit_ts, std::memory_order_release);
      replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
      LogRpcFailure();
      return;
    }
    // If recovery failed, set the state to MAYBE_BEHIND because replica for sure didn't recover completely
    if (recovery_failed) {
      spdlog::debug("One of recovery steps failed, setting replica state to MAYBE_BEHIND");
      last_known_ts_.store(replica_last_commit_ts, std::memory_order_release);
      replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
      return;
    }
  }

  // Protect the exit from the recovery. Otherwise, FinalizeTransactionReplication()
  // could check that the replica state isn't replicating, this recovery sets the
  // replica state to ready. When the next txn starts, we are in state ready without
  // actually sending data to replica
  auto lock = std::lock_guard{main_storage->engine_lock_};
  const auto last_durable_timestamp =
      main_storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire);
  spdlog::info("Replica: {} DB: {} Timestamp: {}, Last main durable commit: {}", client_.name_, main_db_name,
               replica_last_commit_ts, last_durable_timestamp);
  last_known_ts_.store(replica_last_commit_ts, std::memory_order_release);

  // ldt can be larger on replica due to a snapshot
  if (last_durable_timestamp <= replica_last_commit_ts) {
    replica_state_.WithLock([name = client_.name_, &main_db_name](auto &val) {
      val = ReplicaState::READY;
      spdlog::info("Replica {} set to READY after recovery for db {}.", name, main_db_name);
    });
  } else {
    // Someone could've committed in the meantime, hence we set the state to MAYBE_BEHIND
    replica_state_.WithLock([name = client_.name_, &main_db_name](auto &val) {
      val = ReplicaState::MAYBE_BEHIND;
      spdlog::info("Replica {} set to MAYBE_BEHIND after recovery for db {}.", name, main_db_name);
    });
  }
}

////// ReplicaStream //////
ReplicaStream::ReplicaStream(Storage *storage, rpc::Client::StreamHandler<replication::PrepareCommitRpc> stream)
    : storage_{storage}, stream_(std::move(stream)) {
  replication::Encoder encoder{stream_.GetBuilder()};
  encoder.WriteString(storage->repl_storage_state_.epoch_.id());
}

void ReplicaStream::AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t const final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, storage_->name_id_mapper_.get(), storage_->config_.salient.items, delta, vertex,
              final_commit_timestamp);
}

auto ReplicaStream::AppendDelta(const Delta &delta, const Edge &edge, uint64_t const final_commit_timestamp) -> void {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, storage_->name_id_mapper_.get(), delta, edge, final_commit_timestamp);
}

void ReplicaStream::AppendTransactionEnd(uint64_t const final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeTransactionEnd(&encoder, final_commit_timestamp);
}

replication::PrepareCommitRes ReplicaStream::Finalize() {
  utils::MetricsTimer const timer{metrics::PrepareCommitRpc_us};
  return stream_.SendAndWaitProgress();
}
}  // namespace memgraph::storage
