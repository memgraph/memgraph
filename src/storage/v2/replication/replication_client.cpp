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

namespace {
template <typename>
[[maybe_unused]] inline constexpr bool always_false_v = false;
}  // namespace

namespace memgraph::storage {

ReplicationStorageClient::ReplicationStorageClient(::memgraph::replication::ReplicationClient &client,
                                                   utils::UUID main_uuid)
    : client_{client}, main_uuid_(main_uuid) {}

void ReplicationStorageClient::UpdateReplicaState(Storage *storage, DatabaseAccessProtector db_acc) {
  uint64_t current_commit_timestamp{kTimestampInitialId};

  auto &replStorageState = storage->repl_storage_state_;

  auto hb_stream{client_.rpc_client_.Stream<replication::HeartbeatRpc>(main_uuid_, storage->uuid(),
                                                                       replStorageState.last_durable_timestamp_,
                                                                       std::string{replStorageState.epoch_.id()})};
  const auto replica = hb_stream.AwaitResponse();

#ifdef MG_ENTERPRISE       // Multi-tenancy is only supported in enterprise
  if (!replica.success) {  // Replica is missing the current database
    client_.state_.WithLock([&](auto &state) {
      spdlog::debug("Replica '{}' can't respond or missing database '{}' - '{}'", client_.name_, storage->name(),
                    std::string{storage->uuid()});
      state = memgraph::replication::ReplicationClient::State::BEHIND;
    });
    return;
  }
#endif

  std::optional<uint64_t> branching_point;
  // different epoch id, replica was main
  // In case there is no epoch transfer, and MAIN doesn't hold all the epochs as it could have been down and miss it
  // we need then just to check commit timestamp
  if (replica.epoch_id != replStorageState.epoch_.id() && replica.current_commit_timestamp != kTimestampInitialId) {
    spdlog::trace(
        "REPLICA: epoch UUID: {} and last_durable_timestamp: {}; MAIN: epoch UUID {} and last_durable_timestamp {}",
        std::string(replica.epoch_id), replica.current_commit_timestamp, std::string(replStorageState.epoch_.id()),
        replStorageState.last_durable_timestamp_);
    auto const &history = replStorageState.history;
    const auto epoch_info_iter = std::find_if(history.crbegin(), history.crend(), [&](const auto &main_epoch_info) {
      return main_epoch_info.first == replica.epoch_id;
    });
    // main didn't have that epoch, but why is here branching point
    if (epoch_info_iter == history.crend()) {
      spdlog::trace("Couldn't find epoch {} in MAIN, setting branching point", std::string(replica.epoch_id));
      branching_point = 0;
    } else if (epoch_info_iter->second < replica.current_commit_timestamp) {
      spdlog::trace("Found epoch {} on MAIN with last_durable_timestamp {}, REPLICA's last_durable_timestamp {}",
                    std::string(epoch_info_iter->first), epoch_info_iter->second, replica.current_commit_timestamp);
      branching_point = epoch_info_iter->second;
    } else {
      branching_point = std::nullopt;
      spdlog::trace("Found continuous history between replica {} and main.", client_.name_);
    }
  }
  if (branching_point) {
    auto replica_state = replica_state_.Lock();
    if (*replica_state == replication::ReplicaState::DIVERGED_FROM_MAIN) {
      return;
    }
    *replica_state = replication::ReplicaState::DIVERGED_FROM_MAIN;

    auto log_error = [client_name = client_.name_]() {
      spdlog::error(
          "You cannot register Replica {} to this Main because at one point "
          "Replica {} acted as the Main instance. Both the Main and Replica {} "
          "now hold unique data. Please resolve data conflicts and start the "
          "replication on a clean instance.",
          client_name, client_name, client_name);
    };
#ifdef MG_ENTERPRISE
    if (!memgraph::flags::CoordinationSetupInstance().IsDataInstanceManagedByCoordinator()) {
      log_error();
      return;
    }
    client_.thread_pool_.AddTask([storage, gk = std::move(db_acc), this] {
      const auto [success, timestamp] = this->ForceResetStorage(storage);
      if (success) {
        spdlog::info("Successfully reset storage of REPLICA {} to timestamp {}.", client_.name_, timestamp);
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

  current_commit_timestamp = replica.current_commit_timestamp;
  spdlog::trace("Current timestamp on replica {}: {}", client_.name_, current_commit_timestamp);
  spdlog::trace("Current timestamp on main: {}. Current durable timestamp on main: {}", storage->timestamp_,
                replStorageState.last_durable_timestamp_.load());

  replica_state_.WithLock([&](auto &state) {
    // ldt can be larger on replica due to snapshots
    if (current_commit_timestamp >= replStorageState.last_durable_timestamp_.load()) {
      spdlog::debug("Replica '{}' up to date", client_.name_);
      state = replication::ReplicaState::READY;
    } else {
      spdlog::debug("Replica '{}' is behind", client_.name_);
      state = replication::ReplicaState::RECOVERY;
      client_.thread_pool_.AddTask([storage, current_commit_timestamp, gk = std::move(db_acc), this] {
        this->RecoverReplica(current_commit_timestamp, storage);
      });
    }
  });
}

TimestampInfo ReplicationStorageClient::GetTimestampInfo(Storage const *storage) {
  TimestampInfo info;
  info.current_timestamp_of_replica = 0;
  info.current_number_of_timestamp_behind_main = 0;

  try {
    // Exclusive access to client
    auto stream{client_.rpc_client_.Stream<replication::TimestampRpc>(main_uuid_, storage->uuid())};
    const auto response = stream.AwaitResponse();
    const auto is_success = response.success;

    auto main_time_stamp = storage->repl_storage_state_.last_durable_timestamp_.load();
    info.current_timestamp_of_replica = response.current_commit_timestamp;
    info.current_number_of_timestamp_behind_main = response.current_commit_timestamp - main_time_stamp;

    if (!is_success || info.current_number_of_timestamp_behind_main != 0) {
      // Still under client lock, all good
      replica_state_.WithLock([](auto &val) { val = replication::ReplicaState::MAYBE_BEHIND; });
      LogRpcFailure();
    }
  } catch (const rpc::RpcFailedException &) {
    replica_state_.WithLock([](auto &val) { val = replication::ReplicaState::MAYBE_BEHIND; });
    LogRpcFailure();  // mutex already unlocked, if the new enqueued task dispatches immediately it probably
                      // won't block
  }

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
    replica_state_.WithLock([](auto &val) { val = replication::ReplicaState::MAYBE_BEHIND; });
    spdlog::error(
        utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}. Because the replica "
                               "deployed is not a compatible version.",
                               client_.name_, client_.rpc_client_.Endpoint(), "https://memgr.ph/replication"));
  } catch (const rpc::RpcFailedException &) {
    replica_state_.WithLock([](auto &val) { val = replication::ReplicaState::MAYBE_BEHIND; });
    spdlog::error(utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}.", client_.name_,
                                         client_.rpc_client_.Endpoint(), "https://memgr.ph/replication"));
  }
}

auto ReplicationStorageClient::StartTransactionReplication(const uint64_t current_wal_seq_num, Storage *storage,
                                                           DatabaseAccessProtector db_acc)
    -> std::optional<ReplicaStream> {
  auto locked_state = replica_state_.Lock();
  spdlog::trace("Starting transaction replication for replica {} in state {}", client_.name_,
                StateToString(*locked_state));
  switch (*locked_state) {
    using enum replication::ReplicaState;
    case RECOVERY:
      spdlog::debug("Replica {} is behind MAIN instance", client_.name_);
      return std::nullopt;
    case REPLICATING:
      spdlog::debug("Replica {} missed a transaction", client_.name_);
      // We missed a transaction because we're still replicating
      // the previous transaction so we need to go to RECOVERY
      // state to catch up with the missing transaction
      // We cannot queue the recovery process here because
      // an error can happen while we're replicating the previous
      // transaction after which the client should go to
      // INVALID state before starting the recovery process
      //
      // This is a signal to any async streams that are still finalizing to start recovery, since this commit will be
      // missed.
      *locked_state = RECOVERY;
      return std::nullopt;
    case MAYBE_BEHIND:
      spdlog::error(
          utils::MessageWithLink("Couldn't replicate data to {}.", client_.name_, "https://memgr.ph/replication"));
      TryCheckReplicaStateAsync(storage, std::move(db_acc));
      return std::nullopt;
    case DIVERGED_FROM_MAIN:
      spdlog::error(utils::MessageWithLink("Couldn't replicate data to {} since replica has diverged from main.",
                                           client_.name_, "https://memgr.ph/replication"));
      return std::nullopt;
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
  }
}

bool ReplicationStorageClient::FinalizeTransactionReplication(Storage *storage, DatabaseAccessProtector db_acc,
                                                              std::optional<ReplicaStream> &&replica_stream) {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  spdlog::trace("Finalizing transaction on replica {} in state {}", client_.name_,
                StateToString(*replica_state_.Lock()));
  if (State() != replication::ReplicaState::REPLICATING) {
    spdlog::trace("Skipping finalizing transaction on replica {} because it's not replicating", client_.name_);
    return false;
  }

  if (!replica_stream || replica_stream->IsDefunct()) {
    replica_state_.WithLock([&replica_stream](auto &state) {
      replica_stream.reset();
      state = replication::ReplicaState::MAYBE_BEHIND;
    });
    LogRpcFailure();
    return false;
  }

  auto task = [storage, db_acc = std::move(db_acc), this,
               replica_stream_obj = std::move(replica_stream)]() mutable -> bool {
    MG_ASSERT(replica_stream_obj, "Missing stream for transaction deltas for replica {}", client_.name_);
    try {
      auto response = replica_stream_obj->Finalize();
      // NOLINTNEXTLINE
      return replica_state_.WithLock(
          [storage, response, db_acc = std::move(db_acc), this, &replica_stream_obj](auto &state) mutable {
            replica_stream_obj.reset();
            // When async replica executes this part of the code, the state could've changes since the check
            // at the beginning of the function happened.
            if (!response.success || state == replication::ReplicaState::RECOVERY) {
              state = replication::ReplicaState::RECOVERY;
              // NOLINTNEXTLINE
              client_.thread_pool_.AddTask([storage, response, db_acc = std::move(db_acc), this] {
                this->RecoverReplica(response.current_commit_timestamp, storage);
              });
              return false;
            }
            state = replication::ReplicaState::READY;
            return true;
          });
    } catch (const rpc::RpcFailedException &) {
      replica_state_.WithLock([&replica_stream_obj](auto &state) {
        replica_stream_obj.reset();
        state = replication::ReplicaState::MAYBE_BEHIND;
      });
      LogRpcFailure();
      return false;
    }
  };

  if (client_.mode_ == replication_coordination_glue::ReplicationMode::ASYNC) {
    // When in ASYNC mode, we ignore the return value from task() and always return true
    client_.thread_pool_.AddTask(
        [task = utils::CopyMovableFunctionWrapper{std::move(task)}]() mutable { (void)task(); });
    return true;
  }

  // If we are in SYNC mode, we return the result of task().
  // If replica is in RECOVERY or stream wasn't correctly finalized, we return false
  // If replica is READY, we return true
  return task();
}

void ReplicationStorageClient::Start(Storage *storage, DatabaseAccessProtector db_acc) {
  spdlog::trace("Replication client started for database \"{}\"", storage->name());
  TryCheckReplicaStateSync(storage, std::move(db_acc));
}

void ReplicationStorageClient::RecoverReplica(uint64_t replica_commit, memgraph::storage::Storage *storage) {
  if (storage->storage_mode_ != StorageMode::IN_MEMORY_TRANSACTIONAL) {
    throw utils::BasicException("Only InMemoryTransactional mode supports replication!");
  }
  spdlog::debug("Starting replica {} recovery ", client_.name_);
  auto *mem_storage = static_cast<InMemoryStorage *>(storage);

  while (true) {
    auto file_locker = mem_storage->file_retainer_.AddLocker();

    const auto steps = GetRecoverySteps(replica_commit, &file_locker, mem_storage);
    int i = 0;
    for (const RecoveryStep &recovery_step : steps) {
      spdlog::trace("Recovering in step: {}", i++);
      try {
        rpc::Client &rpcClient = client_.rpc_client_;
        std::visit(
            utils::Overloaded{
                [this, &replica_commit, mem_storage, &rpcClient,
                 main_uuid = main_uuid_](RecoverySnapshot const &snapshot) {
                  spdlog::debug("Sending the latest snapshot file: {} to {}", snapshot, client_.name_);
                  auto response = TransferSnapshot(main_uuid, mem_storage->uuid(), rpcClient, snapshot);
                  replica_commit = response.current_commit_timestamp;
                },
                [this, &replica_commit, mem_storage, &rpcClient, main_uuid = main_uuid_](RecoveryWals const &wals) {
                  spdlog::debug("Sending the latest wal files to {}", client_.name_);
                  auto response = TransferWalFiles(main_uuid, mem_storage->uuid(), rpcClient, wals);
                  replica_commit = response.current_commit_timestamp;
                  spdlog::debug("Wal files successfully transferred to {}.", client_.name_);
                },
                [this, &replica_commit, mem_storage, &rpcClient,
                 main_uuid = main_uuid_](RecoveryCurrentWal const &current_wal) {
                  std::unique_lock transaction_guard(mem_storage->engine_lock_);
                  if (mem_storage->wal_file_ &&
                      mem_storage->wal_file_->SequenceNumber() == current_wal.current_wal_seq_num) {
                    utils::OnScopeExit on_exit([mem_storage]() { mem_storage->wal_file_->EnableFlushing(); });
                    mem_storage->wal_file_->DisableFlushing();
                    transaction_guard.unlock();
                    spdlog::debug("Sending current wal file to {}", client_.name_);
                    replica_commit = ReplicateCurrentWal(main_uuid, mem_storage, rpcClient, *mem_storage->wal_file_);
                    spdlog::debug("Replica commit after replicating current wal: {}", replica_commit);
                  } else {
                    spdlog::debug("Cannot recover using current wal file {}", client_.name_);
                  }
                },
                [](auto const &in) {
                  static_assert(always_false_v<decltype(in)>, "Missing type from variant visitor");
                },
            },
            recovery_step);
      } catch (const rpc::RpcFailedException &) {
        replica_state_.WithLock([](auto &val) { val = replication::ReplicaState::MAYBE_BEHIND; });
        LogRpcFailure();
        return;
      }
    }

    // To avoid the situation where we read a correct commit timestamp in
    // one thread, and after that another thread commits a different a
    // transaction and THEN we set the state to READY in the first thread,
    // we set this lock before checking the timestamp.
    // We will detect that the state is invalid during the next commit,
    // because replication::AppendDeltasRpc sends the last durable timestamp which
    // replica checks if it's the same last durable timestamp it received
    // and we will go to recovery.
    // By adding this lock, we can avoid that, and go to RECOVERY immediately.
    const auto last_durable_timestamp = storage->repl_storage_state_.last_durable_timestamp_.load();
    SPDLOG_INFO("Replica {} timestamp: {}, Last commit: {}", client_.name_, replica_commit, last_durable_timestamp);
    // ldt can be larger on replica due to a snapshot
    if (last_durable_timestamp <= replica_commit) {
      replica_state_.WithLock([name = client_.name_](auto &val) {
        spdlog::trace("Replica {} set to ready", name);
        val = replication::ReplicaState::READY;
      });
      return;
    }
  }
}

std::pair<bool, uint64_t> ReplicationStorageClient::ForceResetStorage(memgraph::storage::Storage *storage) {
  utils::OnScopeExit set_to_maybe_behind{
      [this]() { replica_state_.WithLock([](auto &state) { state = replication::ReplicaState::MAYBE_BEHIND; }); }};
  try {
    auto stream{client_.rpc_client_.Stream<replication::ForceResetStorageRpc>(main_uuid_, storage->uuid())};
    const auto res = stream.AwaitResponse();
    return std::pair{res.success, res.current_commit_timestamp};
  } catch (const rpc::RpcFailedException &) {
    spdlog::error(
        utils::MessageWithLink("Couldn't ForceReset data to {}.", client_.name_, "https://memgr.ph/replication"));
  }

  return {false, 0};
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

replication::AppendDeltasRes ReplicaStream::Finalize() { return stream_.AwaitResponse(); }

}  // namespace memgraph::storage
