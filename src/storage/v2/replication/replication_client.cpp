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
#include "utils/exceptions.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/uuid.hpp"
#include "utils/variant_helpers.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>

namespace {
template <typename>
[[maybe_unused]] inline constexpr bool always_false_v = false;

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

namespace memgraph::storage {
ReplicationStorageClient::ReplicationStorageClient(::memgraph::replication::ReplicationClient &client,
                                                   utils::UUID main_uuid)
    : client_{client}, main_uuid_(main_uuid) {}

void ReplicationStorageClient::UpdateReplicaState(Storage *storage, DatabaseAccessProtector db_acc) {
  auto &replStorageState = storage->repl_storage_state_;

  // stream should be destroyed so that RPC lock is released before taking engine lock
  replication::HeartbeatRes const heartbeat_res = std::invoke([&] {
    // stream should be destroyed so that RPC lock is released
    // before taking engine lock
    auto hb_stream = client_.rpc_client_.Stream<replication::HeartbeatRpc>(main_uuid_, storage->uuid(),
                                                                           replStorageState.last_durable_timestamp_,
                                                                           std::string{replStorageState.epoch_.id()});
    return hb_stream.AwaitResponse();
  });

  if (heartbeat_res.success) {
    last_known_ts_.store(heartbeat_res.current_commit_timestamp, std::memory_order_release);
  } else {
#ifdef MG_ENTERPRISE  // Multi-tenancy is only supported in enterprise
    // Replica is missing the current database
    client_.state_.WithLock([&](auto &state) {
      spdlog::debug("Replica '{}' can't respond or missing database '{}' - '{}'", client_.name_, storage->name(),
                    std::string{storage->uuid()});
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
        "Replica {}: Epoch id: {}, last_durable_timestamp: {}; Main: Epoch id: {}, last_durable_timestamp: {}",
        client_.name_, std::string(heartbeat_res.epoch_id), heartbeat_res.current_commit_timestamp,
        std::string(replStorageState.epoch_.id()), replStorageState.last_durable_timestamp_);

    auto const &history = replStorageState.history;
    const auto epoch_info_iter = std::find_if(history.crbegin(), history.crend(), [&](const auto &main_epoch_info) {
      return main_epoch_info.first == heartbeat_res.epoch_id;
    });

    if (epoch_info_iter == history.crend()) {
      branching_point = 0;
      spdlog::trace("Couldn't find epoch {} in main, setting branching point to 0.",
                    std::string(heartbeat_res.epoch_id));
    } else if (epoch_info_iter->second <
               heartbeat_res
                   .current_commit_timestamp) {  // replica has larger commit ts associated with epoch than main
      spdlog::trace(
          "Found epoch {} on main with last_durable_timestamp {}, replica {} has last_durable_timestamp {}. Setting "
          "branching point to {}.",
          std::string(epoch_info_iter->first), epoch_info_iter->second, client_.name_,
          heartbeat_res.current_commit_timestamp, epoch_info_iter->second);
      branching_point = epoch_info_iter->second;
    } else {
      branching_point = std::nullopt;
      spdlog::trace("Found continuous history between replica {} and main. Our commit timestamp for epoch {} was {}.",
                    client_.name_, epoch_info_iter->first, epoch_info_iter->second);
    }
  }
  if (branching_point) {
    auto replica_state = replica_state_.Lock();
    // Don't put additional task in the thread pool for force resetting if the previous didn't finish
    if (*replica_state == ReplicaState::DIVERGED_FROM_MAIN) {
      return;
    }
    *replica_state = ReplicaState::DIVERGED_FROM_MAIN;

    auto log_error = [client_name = client_.name_]() {
      spdlog::error(
          "You cannot register Replica {} to this Main because at one point "
          "Replica {} acted as the Main instance. Both the Main and Replica {} "
          "now hold unique data. Please resolve data conflicts and start the "
          "replication on a clean instance.",
          client_name, client_name, client_name);
    };
#ifdef MG_ENTERPRISE
    // if normal replication, not HA
    if (!flags::CoordinationSetupInstance().IsDataInstanceManagedByCoordinator()) {
      log_error();
      return;
    }
    client_.thread_pool_.AddTask([storage, gk = std::move(db_acc), this] {
      if (this->ForceResetStorage(storage)) {
        spdlog::info("Successfully reset storage of REPLICA {}.", client_.name_);
        return;
      }
      spdlog::error("You cannot register REPLICA {} to this MAIN because MAIN couldn't reset REPLICA's storage.",
                    client_.name_);
    });
#else
    log_error();
#endif
    return;
  }

  // Lock engine lock in order to read storage timestamp and synchronize with any active commits
  auto engine_lock = std::unique_lock{storage->engine_lock_};
  spdlog::trace("Current timestamp on replica {}: {}.", client_.name_, heartbeat_res.current_commit_timestamp);
  spdlog::trace("Current timestamp on main: {}. Current durable timestamp on main: {}", storage->timestamp_,
                replStorageState.last_durable_timestamp_.load());

  replica_state_.WithLock([&](auto &state) {
    // Recovered state didn't change in the meantime
    // ldt can be larger on replica due to snapshots
    if (heartbeat_res.current_commit_timestamp >=
        replStorageState.last_durable_timestamp_.load(std::memory_order_acquire)) {
      spdlog::debug("Replica '{}' up to date.", client_.name_);
      state = ReplicaState::READY;
    } else {
      spdlog::debug("Replica '{}' is behind.", client_.name_);
      state = ReplicaState::RECOVERY;
      client_.thread_pool_.AddTask([storage, current_commit_timestamp = heartbeat_res.current_commit_timestamp,
                                    gk = std::move(db_acc),
                                    this] { this->RecoverReplica(current_commit_timestamp, storage); });
    }
  });
}

TimestampInfo ReplicationStorageClient::GetTimestampInfo(Storage const *storage) const {
  TimestampInfo info;
  auto main_time_stamp = storage->repl_storage_state_.last_durable_timestamp_.load();
  info.current_timestamp_of_replica = last_known_ts_.load(std::memory_order::acquire);
  info.current_number_of_timestamp_behind_main = info.current_timestamp_of_replica - main_time_stamp;
  return info;
}

void ReplicationStorageClient::LogRpcFailure() const {
  spdlog::error(
      utils::MessageWithLink("Couldn't replicate data to {}.", client_.name_, "https://memgr.ph/replication"));
}

void ReplicationStorageClient::TryCheckReplicaStateAsync(Storage *storage, DatabaseAccessProtector db_acc) {
  client_.thread_pool_.AddTask([storage, db_acc = std::move(db_acc), this]() mutable {
    this->TryCheckReplicaStateSync(storage, std::move(db_acc));
  });
}

void ReplicationStorageClient::TryCheckReplicaStateSync(Storage *storage, DatabaseAccessProtector db_acc) {
  try {
    UpdateReplicaState(storage, std::move(db_acc));
  } catch (const rpc::VersionMismatchRpcFailedException &) {
    replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
    spdlog::error(
        utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}. Because the replica "
                               "deployed is not a compatible version.",
                               client_.name_, client_.rpc_client_.Endpoint(), "https://memgr.ph/replication"));
  } catch (const rpc::RpcFailedException &) {
    replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
    spdlog::error(utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}.", client_.name_,
                                         client_.rpc_client_.Endpoint(), "https://memgr.ph/replication"));
  }
}

// If replica is in state RECOVERY -> skip
// If replica is REPLICATING old txn (ASYNC), set the state to MAYBE_BEHIND
// If replica is MAYBE_BEHIND, skip and asynchronously check the state of the replica
// If replica is DIVERGED_FROM_MAIN, skip
// If replica is READY, set it to replicating and create optional stream
//    If creating stream fails, set the state to MAYBE_BEHIND. RPC lock is taken.
auto ReplicationStorageClient::StartTransactionReplication(const uint64_t current_wal_seq_num, Storage *storage,
                                                           DatabaseAccessProtector db_acc)
    -> std::optional<ReplicaStream> {
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
      auto replica_stream = std::optional<ReplicaStream>{};
      try {
        replica_stream.emplace(storage, client_.rpc_client_, current_wal_seq_num, main_uuid_);
        *locked_state = REPLICATING;
      } catch (const rpc::RpcFailedException &) {
        *locked_state = MAYBE_BEHIND;
        LogRpcFailure();
      }
      return replica_stream;
    }
    default:
      MG_ASSERT(false, "Unknown replica state when starting transaction replication.");
  }
}

bool ReplicationStorageClient::FinalizeTransactionReplication(DatabaseAccessProtector db_acc,
                                                              std::optional<ReplicaStream> &&replica_stream,
                                                              uint64_t timestamp) const {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  spdlog::trace("Finalizing transaction on replica {} in state {}", client_.name_,
                StateToString(*replica_state_.Lock()));
  if (State() != ReplicaState::REPLICATING) {
    spdlog::trace("Skipping finalizing transaction on replica {} because it's not replicating", client_.name_);
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
               timestamp]() mutable -> bool {
    MG_ASSERT(replica_stream_obj, "Missing stream for transaction deltas for replica {}", client_.name_);
    try {
      auto response = replica_stream_obj->Finalize();
      // NOLINTNEXTLINE
      return replica_state_.WithLock(
          [this, response, db_acc = std::move(db_acc), &replica_stream_obj, timestamp](auto &state) mutable {
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

            last_known_ts_.store(timestamp, std::memory_order_release);
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
void ReplicationStorageClient::RecoverReplica(uint64_t replica_commit, Storage *storage) const {
  if (storage->storage_mode_ != StorageMode::IN_MEMORY_TRANSACTIONAL) {
    throw utils::BasicException("Only InMemoryTransactional mode supports replication!");
  }

  // The recovery task could get executed at any point in the future hence some previous recovery task could have
  // already recovered replica.
  if (*replica_state_.Lock() != ReplicaState::RECOVERY) {
    spdlog::info("Replica {} is not in RECOVERY state anymore, ending the recovery task.", client_.name_);
    return;
  }

  spdlog::debug("Starting replica {} recovery ", client_.name_);

  auto *mem_storage = static_cast<InMemoryStorage *>(storage);

  rpc::Client &rpcClient = client_.rpc_client_;
  bool recovery_failed{false};
  auto file_locker = mem_storage->file_retainer_.AddLocker();
  auto const maybe_steps = GetRecoverySteps(replica_commit, &file_locker, mem_storage);
  if (!maybe_steps.has_value()) {
    spdlog::error(
        "Couldn't get recovery steps while trying to recover replica, setting the replica state to MAYBE_BEHIND");
    replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
    return;
  }
  auto const &steps = *maybe_steps;

  for (auto const &[step_index, recovery_step] : ranges::views::enumerate(steps)) {
    try {
      spdlog::trace("Recovering in step: {}. Current local replica commit: {}.", step_index, replica_commit);
      std::visit(
          utils::Overloaded{
              [this, &replica_commit, mem_storage, &rpcClient, &recovery_failed,
               main_uuid = main_uuid_](RecoverySnapshot const &snapshot) {
                spdlog::debug("Sending the latest snapshot file: {} to {}", snapshot, client_.name_);
                // Loading snapshot on the replica side either passes cleanly or it doesn't pass at all. If it doesn't
                // pass, we won't update commit timestamp. Heartbeat should trigger recovering replica again.
                if (auto const response = TransferSnapshot(main_uuid, mem_storage->uuid(), rpcClient, snapshot);
                    response.current_commit_timestamp) {
                  replica_commit = *response.current_commit_timestamp;
                  spdlog::debug(
                      "Successful reply to the snapshot file {} received from {}. Current replica commit is {}",
                      snapshot, client_.name_, replica_commit);
                } else {
                  spdlog::debug(
                      "Unsuccessful reply to the snapshot file {} received from {}. Current replica commit is {}",
                      snapshot, client_.name_, replica_commit);
                  recovery_failed = true;
                }
              },
              [this, &replica_commit, mem_storage, &rpcClient, &recovery_failed,
               main_uuid = main_uuid_](RecoveryWals const &wals) {
                spdlog::debug("Sending the latest wal files to {}", client_.name_);

                if (wals.empty()) {
                  spdlog::trace("Wal files list is empty, nothing to send");
                  return;
                }
                spdlog::debug("Sending WAL files to {}.", client_.name_);
                // We don't care about partial progress when loading WAL files. We are only interested if everything
                // passed so that possibly next step of recovering current wal can be executed
                if (auto const wal_files_res = TransferWalFiles(main_uuid, mem_storage->uuid(), rpcClient, wals);
                    wal_files_res.current_commit_timestamp) {
                  replica_commit = *wal_files_res.current_commit_timestamp;
                  spdlog::debug("Successful reply to WAL files received from {}. Updating replica commit to {}",
                                client_.name_, replica_commit);
                } else {
                  spdlog::debug("Unsuccessful reply to WAL files received from {}. Current replica commit is {}.",
                                client_.name_, replica_commit);
                  recovery_failed = true;
                }
              },
              [this, &replica_commit, mem_storage, &rpcClient, &recovery_failed,
               main_uuid = main_uuid_](RecoveryCurrentWal const &current_wal) {
                std::unique_lock transaction_guard(mem_storage->engine_lock_);
                if (mem_storage->wal_file_ &&
                    mem_storage->wal_file_->SequenceNumber() == current_wal.current_wal_seq_num) {
                  utils::OnScopeExit const on_exit([mem_storage]() { mem_storage->wal_file_->EnableFlushing(); });
                  mem_storage->wal_file_->DisableFlushing();
                  transaction_guard.unlock();
                  spdlog::debug("Sending current wal file to {}", client_.name_);
                  if (auto const response =
                          ReplicateCurrentWal(main_uuid, mem_storage, rpcClient, *mem_storage->wal_file_);
                      response.current_commit_timestamp) {
                    replica_commit = *response.current_commit_timestamp;
                    spdlog::debug("Successful reply to the current WAL received from {}. Current replica commit is {}",
                                  client_.name_, replica_commit);
                  } else {
                    spdlog::debug("Unsuccessful reply to WAL files received from {}. Current replica commit is {}",
                                  client_.name_, replica_commit);
                    recovery_failed = true;
                  }
                } else {
                  spdlog::debug("Cannot recover using current wal file {}", client_.name_);
                }
              },
              []<typename T>(T const &) { static_assert(always_false_v<T>, "Missing type from variant visitor"); },
          },
          recovery_step);
    } catch (const rpc::RpcFailedException &) {
      last_known_ts_.store(replica_commit, std::memory_order_release);
      replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
      LogRpcFailure();
      return;
    }
    // If recovery failed, set the state to MAYBE_BEHIND because replica for sure didn't recover completely
    if (recovery_failed) {
      spdlog::debug("One of recovery steps failed, setting replica state to MAYBE_BEHIND");
      last_known_ts_.store(replica_commit, std::memory_order_release);
      replica_state_.WithLock([](auto &val) { val = ReplicaState::MAYBE_BEHIND; });
      return;
    }
  }

  const auto last_durable_timestamp = storage->repl_storage_state_.last_durable_timestamp_.load();
  spdlog::info("Replica {} timestamp: {}, Last commit: {}", client_.name_, replica_commit, last_durable_timestamp);
  last_known_ts_.store(replica_commit, std::memory_order_release);
  // ldt can be larger on replica due to a snapshot
  if (last_durable_timestamp <= replica_commit) {
    replica_state_.WithLock([name = client_.name_](auto &val) {
      val = ReplicaState::READY;
      spdlog::info("Replica {} set to READY after recovery.", name);
    });
  } else {
    // Someone could've committed in the meantime, hence we set the state to MAYBE_BEHIND
    replica_state_.WithLock([name = client_.name_](auto &val) {
      val = ReplicaState::MAYBE_BEHIND;
      spdlog::info("Replica {} set to MAYBE_BEHIND after recovery.", name);
    });
  }
}

bool ReplicationStorageClient::ForceResetStorage(Storage *storage) const {
  // We set the state to MAYBE_BEHIND even if force reset failed. On the next ping, the branching point will be observed
  // again and force reset will be tried once again.
  utils::OnScopeExit set_to_maybe_behind{
      [this]() { replica_state_.WithLock([](auto &state) { state = ReplicaState::MAYBE_BEHIND; }); }};
  try {
    auto stream{client_.rpc_client_.Stream<replication::ForceResetStorageRpc>(main_uuid_, storage->uuid())};
    auto res = stream.AwaitResponse().success;
    if (res) {
      last_known_ts_.store(0, std::memory_order_release);
    }
    return res;
  } catch (const rpc::RpcFailedException &) {
    spdlog::error(
        utils::MessageWithLink("Couldn't ForceReset data to {}.", client_.name_, "https://memgr.ph/replication"));
    return false;
  }
}

////// ReplicaStream //////
ReplicaStream::ReplicaStream(Storage *storage, rpc::Client &rpc_client, const uint64_t current_wal_seq_num,
                             utils::UUID main_uuid)
    : storage_{storage},
      stream_(rpc_client.Stream<replication::AppendDeltasRpc>(
          main_uuid, storage->uuid(), storage->repl_storage_state_.last_durable_timestamp_.load(),
          current_wal_seq_num)),
      main_uuid_(main_uuid) {
  replication::Encoder encoder{stream_.GetBuilder()};
  encoder.WriteString(storage->repl_storage_state_.epoch_.id());
}

void ReplicaStream::AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, storage_->name_id_mapper_.get(), storage_->config_.salient.items, delta, vertex,
              final_commit_timestamp);
}

void ReplicaStream::AppendDelta(const Delta &delta, const Edge &edge, uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, storage_->name_id_mapper_.get(), delta, edge, final_commit_timestamp);
}

void ReplicaStream::AppendTransactionEnd(uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeTransactionEnd(&encoder, final_commit_timestamp);
}

replication::AppendDeltasRes ReplicaStream::Finalize() { return stream_.AwaitResponseWhileInProgress(); }
}  // namespace memgraph::storage
