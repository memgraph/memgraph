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
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/variant_helpers.hpp"

#include <algorithm>

namespace {
template <typename>
[[maybe_unused]] inline constexpr bool always_false_v = false;
}  // namespace

namespace memgraph::storage {

ReplicationStorageClient::ReplicationStorageClient(::memgraph::replication::ReplicationClient &client)
    : client_{client} {}

void ReplicationStorageClient::UpdateReplicaState(Storage *storage, DatabaseAccessProtector db_acc) {
  uint64_t current_commit_timestamp{kTimestampInitialId};

  auto &replStorageState = storage->repl_storage_state_;

  auto hb_stream{client_.rpc_client_.Stream<replication::HeartbeatRpc>(
      storage->uuid(), replStorageState.last_commit_timestamp_, std::string{replStorageState.epoch_.id()})};
  const auto replica = hb_stream.AwaitResponse();

#ifdef MG_ENTERPRISE       // Multi-tenancy is only supported in enterprise
  if (!replica.success) {  // Replica is missing the current database
    client_.state_.WithLock([&](auto &state) {
      spdlog::debug("Replica '{}' missing database '{}' - '{}'", client_.name_, storage->name(),
                    std::string{storage->uuid()});
      state = memgraph::replication::ReplicationClient::State::BEHIND;
    });
    return;
  }
#endif

  std::optional<uint64_t> branching_point;
  if (replica.epoch_id != replStorageState.epoch_.id() && replica.current_commit_timestamp != kTimestampInitialId) {
    auto const &history = replStorageState.history;
    const auto epoch_info_iter = std::find_if(history.crbegin(), history.crend(), [&](const auto &main_epoch_info) {
      return main_epoch_info.first == replica.epoch_id;
    });
    if (epoch_info_iter == history.crend()) {
      branching_point = 0;
    } else if (epoch_info_iter->second != replica.current_commit_timestamp) {
      branching_point = epoch_info_iter->second;
    }
  }
  if (branching_point) {
    spdlog::error(
        "You cannot register Replica {} to this Main because at one point "
        "Replica {} acted as the Main instance. Both the Main and Replica {} "
        "now hold unique data. Please resolve data conflicts and start the "
        "replication on a clean instance.",
        client_.name_, client_.name_, client_.name_);
    // State not updated, hence in MAYBE_BEHIND state
    return;
  }

  current_commit_timestamp = replica.current_commit_timestamp;
  spdlog::trace("Current timestamp on replica {}: {}", client_.name_, current_commit_timestamp);
  spdlog::trace("Current timestamp on main: {}", replStorageState.last_commit_timestamp_.load());
  replica_state_.WithLock([&](auto &state) {
    if (current_commit_timestamp == replStorageState.last_commit_timestamp_.load()) {
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
  info.current_number_of_timestamp_behind_master = 0;

  try {
    auto stream{client_.rpc_client_.Stream<replication::TimestampRpc>(storage->uuid())};
    const auto response = stream.AwaitResponse();
    const auto is_success = response.success;

    auto main_time_stamp = storage->repl_storage_state_.last_commit_timestamp_.load();
    info.current_timestamp_of_replica = response.current_commit_timestamp;
    info.current_number_of_timestamp_behind_master = response.current_commit_timestamp - main_time_stamp;

    if (!is_success || info.current_number_of_timestamp_behind_master != 0) {
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

void ReplicationStorageClient::LogRpcFailure() {
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

void ReplicationStorageClient::StartTransactionReplication(const uint64_t current_wal_seq_num, Storage *storage,
                                                           DatabaseAccessProtector db_acc) {
  auto locked_state = replica_state_.Lock();
  switch (*locked_state) {
    using enum replication::ReplicaState;
    case RECOVERY:
      spdlog::debug("Replica {} is behind MAIN instance", client_.name_);
      return;
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
      return;
    case MAYBE_BEHIND:
      spdlog::error(
          utils::MessageWithLink("Couldn't replicate data to {}.", client_.name_, "https://memgr.ph/replication"));
      TryCheckReplicaStateAsync(storage, std::move(db_acc));
      return;
    case READY:
      MG_ASSERT(!replica_stream_);
      try {
        replica_stream_.emplace(storage, client_.rpc_client_, current_wal_seq_num);
        *locked_state = REPLICATING;
      } catch (const rpc::RpcFailedException &) {
        *locked_state = MAYBE_BEHIND;
        LogRpcFailure();
      }
      return;
  }
}

bool ReplicationStorageClient::FinalizeTransactionReplication(Storage *storage, DatabaseAccessProtector db_acc) {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  if (State() != replication::ReplicaState::REPLICATING) {
    return false;
  }

  if (!replica_stream_ || replica_stream_->IsDefunct()) {
    replica_state_.WithLock([this](auto &state) {
      replica_stream_.reset();
      state = replication::ReplicaState::MAYBE_BEHIND;
    });
    LogRpcFailure();
    return false;
  }

  auto task = [storage, db_acc = std::move(db_acc), this]() mutable {
    MG_ASSERT(replica_stream_, "Missing stream for transaction deltas");
    try {
      auto response = replica_stream_->Finalize();
      return replica_state_.WithLock([storage, &response, db_acc = std::move(db_acc), this](auto &state) mutable {
        replica_stream_.reset();
        if (!response.success || state == replication::ReplicaState::RECOVERY) {
          state = replication::ReplicaState::RECOVERY;
          client_.thread_pool_.AddTask([storage, &response, db_acc = std::move(db_acc), this] {
            this->RecoverReplica(response.current_commit_timestamp, storage);
          });
          return false;
        }
        state = replication::ReplicaState::READY;
        return true;
      });
    } catch (const rpc::RpcFailedException &) {
      replica_state_.WithLock([this](auto &state) {
        replica_stream_.reset();
        state = replication::ReplicaState::MAYBE_BEHIND;
      });
      LogRpcFailure();
      return false;
    }
  };

  if (client_.mode_ == memgraph::replication::ReplicationMode::ASYNC) {
    client_.thread_pool_.AddTask([task = std::move(task)]() mutable { (void)task(); });
    return true;
  }

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
  spdlog::debug("Starting replica recovery");
  auto *mem_storage = static_cast<InMemoryStorage *>(storage);

  while (true) {
    auto file_locker = mem_storage->file_retainer_.AddLocker();

    const auto steps = GetRecoverySteps(replica_commit, &file_locker, mem_storage);
    int i = 0;
    for (const RecoveryStep &recovery_step : steps) {
      spdlog::trace("Recovering in step: {}", i++);
      try {
        rpc::Client &rpcClient = client_.rpc_client_;
        std::visit(utils::Overloaded{
                       [&replica_commit, mem_storage, &rpcClient](RecoverySnapshot const &snapshot) {
                         spdlog::debug("Sending the latest snapshot file: {}", snapshot);
                         auto response = TransferSnapshot(mem_storage->uuid(), rpcClient, snapshot);
                         replica_commit = response.current_commit_timestamp;
                       },
                       [&replica_commit, mem_storage, &rpcClient](RecoveryWals const &wals) {
                         spdlog::debug("Sending the latest wal files");
                         auto response = TransferWalFiles(mem_storage->uuid(), rpcClient, wals);
                         replica_commit = response.current_commit_timestamp;
                         spdlog::debug("Wal files successfully transferred.");
                       },
                       [&replica_commit, mem_storage, &rpcClient](RecoveryCurrentWal const &current_wal) {
                         std::unique_lock transaction_guard(mem_storage->engine_lock_);
                         if (mem_storage->wal_file_ &&
                             mem_storage->wal_file_->SequenceNumber() == current_wal.current_wal_seq_num) {
                           utils::OnScopeExit on_exit([mem_storage]() { mem_storage->wal_file_->EnableFlushing(); });
                           mem_storage->wal_file_->DisableFlushing();
                           transaction_guard.unlock();
                           spdlog::debug("Sending current wal file");
                           replica_commit = ReplicateCurrentWal(mem_storage, rpcClient, *mem_storage->wal_file_);
                         } else {
                           spdlog::debug("Cannot recover using current wal file");
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

    spdlog::trace("Current timestamp on replica: {}", replica_commit);
    // To avoid the situation where we read a correct commit timestamp in
    // one thread, and after that another thread commits a different a
    // transaction and THEN we set the state to READY in the first thread,
    // we set this lock before checking the timestamp.
    // We will detect that the state is invalid during the next commit,
    // because replication::AppendDeltasRpc sends the last commit timestamp which
    // replica checks if it's the same last commit timestamp it received
    // and we will go to recovery.
    // By adding this lock, we can avoid that, and go to RECOVERY immediately.
    const auto last_commit_timestamp = storage->repl_storage_state_.last_commit_timestamp_.load();
    SPDLOG_INFO("Replica timestamp: {}", replica_commit);
    SPDLOG_INFO("Last commit: {}", last_commit_timestamp);
    if (last_commit_timestamp == replica_commit) {
      replica_state_.WithLock([](auto &val) { val = replication::ReplicaState::READY; });
      return;
    }
  }
}

////// ReplicaStream //////
ReplicaStream::ReplicaStream(Storage *storage, rpc::Client &rpc_client, const uint64_t current_seq_num)
    : storage_{storage},
      stream_(rpc_client.Stream<replication::AppendDeltasRpc>(
          storage->uuid(), storage->repl_storage_state_.last_commit_timestamp_.load(), current_seq_num)) {
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

void ReplicaStream::AppendOperation(durability::StorageMetadataOperation operation, LabelId label,
                                    const std::set<PropertyId> &properties, const LabelIndexStats &stats,
                                    const LabelPropertyIndexStats &property_stats, uint64_t timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeOperation(&encoder, storage_->name_id_mapper_.get(), operation, label, properties, stats, property_stats,
                  timestamp);
}

replication::AppendDeltasRes ReplicaStream::Finalize() { return stream_.AwaitResponse(); }

}  // namespace memgraph::storage
