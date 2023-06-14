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

#include "storage/v2/replication/replication_client.hpp"

#include <algorithm>
#include <type_traits>

#include "storage/v2/durability/durability.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/file_locker.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"

namespace memgraph::storage {

namespace {
template <typename>
[[maybe_unused]] inline constexpr bool always_false_v = false;
}  // namespace

////// ReplicationClient //////
Storage::ReplicationClient::ReplicationClient(std::string name, Storage *storage, const io::network::Endpoint &endpoint,
                                              const replication::ReplicationMode mode,
                                              const replication::ReplicationClientConfig &config)
    : name_(std::move(name)), storage_(storage), mode_(mode) {
  if (config.ssl) {
    rpc_context_.emplace(config.ssl->key_file, config.ssl->cert_file);
  } else {
    rpc_context_.emplace();
  }

  rpc_client_.emplace(endpoint, &*rpc_context_);
  TryInitializeClientSync();

  // Help the user to get the most accurate replica state possible.
  if (config.replica_check_frequency > std::chrono::seconds(0)) {
    replica_checker_.Run("Replica Checker", config.replica_check_frequency, [&] { FrequentCheck(); });
  }
}

void Storage::ReplicationClient::TryInitializeClientAsync() {
  thread_pool_.AddTask([this] {
    rpc_client_->Abort();
    this->TryInitializeClientSync();
  });
}

void Storage::ReplicationClient::FrequentCheck() {
  const auto is_success = std::invoke([this]() {
    try {
      auto stream{rpc_client_->Stream<replication::FrequentHeartbeatRpc>()};
      const auto response = stream.AwaitResponse();
      return response.success;
    } catch (const rpc::RpcFailedException &) {
      return false;
    }
  });
  // States: READY, REPLICATING, RECOVERY, INVALID
  // If success && ready, replicating, recovery -> stay the same because something good is going on.
  // If success && INVALID -> [it's possible that replica came back to life] -> TryInitializeClient.
  // If fail -> [replica is not reachable at all] -> INVALID state.
  // NOTE: TryInitializeClient might return nothing if there is a branching point.
  // NOTE: The early return pattern simplified the code, but the behavior should be as explained.
  if (!is_success) {
    replica_state_.store(replication::ReplicaState::INVALID);
    return;
  }
  if (replica_state_.load() == replication::ReplicaState::INVALID) {
    TryInitializeClientAsync();
  }
}

/// @throws rpc::RpcFailedException
void Storage::ReplicationClient::InitializeClient() {
  uint64_t current_commit_timestamp{kTimestampInitialId};

  std::optional<std::string> epoch_id;
  {
    // epoch_id_ can be changed if we don't take this lock
    std::unique_lock engine_guard(storage_->engine_lock_);
    epoch_id.emplace(storage_->epoch_id_);
  }

  auto stream{rpc_client_->Stream<replication::HeartbeatRpc>(storage_->last_commit_timestamp_, std::move(*epoch_id))};

  const auto response = stream.AwaitResponse();
  std::optional<uint64_t> branching_point;
  if (response.epoch_id != storage_->epoch_id_ && response.current_commit_timestamp != kTimestampInitialId) {
    const auto &epoch_history = storage_->epoch_history_;
    const auto epoch_info_iter =
        std::find_if(epoch_history.crbegin(), epoch_history.crend(),
                     [&](const auto &epoch_info) { return epoch_info.first == response.epoch_id; });
    if (epoch_info_iter == epoch_history.crend()) {
      branching_point = 0;
    } else if (epoch_info_iter->second != response.current_commit_timestamp) {
      branching_point = epoch_info_iter->second;
    }
  }
  if (branching_point) {
    spdlog::error(
        "You cannot register Replica {} to this Main because at one point "
        "Replica {} acted as the Main instance. Both the Main and Replica {} "
        "now hold unique data. Please resolve data conflicts and start the "
        "replication on a clean instance.",
        name_, name_, name_);
    return;
  }

  current_commit_timestamp = response.current_commit_timestamp;
  spdlog::trace("Current timestamp on replica: {}", current_commit_timestamp);
  spdlog::trace("Current timestamp on main: {}", storage_->last_commit_timestamp_.load());
  if (current_commit_timestamp == storage_->last_commit_timestamp_.load()) {
    spdlog::debug("Replica '{}' up to date", name_);
    std::unique_lock client_guard{client_lock_};
    replica_state_.store(replication::ReplicaState::READY);
  } else {
    spdlog::debug("Replica '{}' is behind", name_);
    {
      std::unique_lock client_guard{client_lock_};
      replica_state_.store(replication::ReplicaState::RECOVERY);
    }
    thread_pool_.AddTask([=, this] { this->RecoverReplica(current_commit_timestamp); });
  }
}

void Storage::ReplicationClient::TryInitializeClientSync() {
  try {
    InitializeClient();
  } catch (const rpc::RpcFailedException &) {
    std::unique_lock client_guard{client_lock_};
    replica_state_.store(replication::ReplicaState::INVALID);
    spdlog::error(utils::MessageWithLink("Failed to connect to replica {} at the endpoint {}.", name_,
                                         rpc_client_->Endpoint(), "https://memgr.ph/replication"));
  }
}

void Storage::ReplicationClient::HandleRpcFailure() {
  spdlog::error(utils::MessageWithLink("Couldn't replicate data to {}.", name_, "https://memgr.ph/replication"));
  TryInitializeClientAsync();
}

replication::SnapshotRes Storage::ReplicationClient::TransferSnapshot(const std::filesystem::path &path) {
  auto stream{rpc_client_->Stream<replication::SnapshotRpc>()};
  replication::Encoder encoder(stream.GetBuilder());
  encoder.WriteFile(path);
  return stream.AwaitResponse();
}

replication::WalFilesRes Storage::ReplicationClient::TransferWalFiles(
    const std::vector<std::filesystem::path> &wal_files) {
  MG_ASSERT(!wal_files.empty(), "Wal files list is empty!");
  auto stream{rpc_client_->Stream<replication::WalFilesRpc>(wal_files.size())};
  replication::Encoder encoder(stream.GetBuilder());
  for (const auto &wal : wal_files) {
    spdlog::debug("Sending wal file: {}", wal);
    encoder.WriteFile(wal);
  }

  return stream.AwaitResponse();
}

void Storage::ReplicationClient::StartTransactionReplication(const uint64_t current_wal_seq_num) {
  std::unique_lock guard(client_lock_);
  const auto status = replica_state_.load();
  switch (status) {
    case replication::ReplicaState::RECOVERY:
      spdlog::debug("Replica {} is behind MAIN instance", name_);
      return;
    case replication::ReplicaState::REPLICATING:
      spdlog::debug("Replica {} missed a transaction", name_);
      // We missed a transaction because we're still replicating
      // the previous transaction so we need to go to RECOVERY
      // state to catch up with the missing transaction
      // We cannot queue the recovery process here because
      // an error can happen while we're replicating the previous
      // transaction after which the client should go to
      // INVALID state before starting the recovery process
      replica_state_.store(replication::ReplicaState::RECOVERY);
      return;
    case replication::ReplicaState::INVALID:
      HandleRpcFailure();
      return;
    case replication::ReplicaState::READY:
      MG_ASSERT(!replica_stream_);
      try {
        replica_stream_.emplace(ReplicaStream{this, storage_->last_commit_timestamp_.load(), current_wal_seq_num});
        replica_state_.store(replication::ReplicaState::REPLICATING);
      } catch (const rpc::RpcFailedException &) {
        replica_state_.store(replication::ReplicaState::INVALID);
        HandleRpcFailure();
      }
      return;
  }
}

void Storage::ReplicationClient::IfStreamingTransaction(const std::function<void(ReplicaStream &handler)> &callback) {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  if (replica_state_ != replication::ReplicaState::REPLICATING) {
    return;
  }

  try {
    callback(*replica_stream_);
  } catch (const rpc::RpcFailedException &) {
    {
      std::unique_lock client_guard{client_lock_};
      replica_state_.store(replication::ReplicaState::INVALID);
    }
    HandleRpcFailure();
  }
}

bool Storage::ReplicationClient::FinalizeTransactionReplication() {
  // We can only check the state because it guarantees to be only
  // valid during a single transaction replication (if the assumption
  // that this and other transaction replication functions can only be
  // called from a one thread stands)
  if (replica_state_ != replication::ReplicaState::REPLICATING) {
    return false;
  }

  if (mode_ == replication::ReplicationMode::ASYNC) {
    thread_pool_.AddTask([this] { static_cast<void>(this->FinalizeTransactionReplicationInternal()); });
    return true;
  } else {
    return FinalizeTransactionReplicationInternal();
  }
}

bool Storage::ReplicationClient::FinalizeTransactionReplicationInternal() {
  MG_ASSERT(replica_stream_, "Missing stream for transaction deltas");
  try {
    auto response = replica_stream_->Finalize();
    replica_stream_.reset();
    std::unique_lock client_guard(client_lock_);
    if (!response.success || replica_state_ == replication::ReplicaState::RECOVERY) {
      replica_state_.store(replication::ReplicaState::RECOVERY);
      thread_pool_.AddTask([&, this] { this->RecoverReplica(response.current_commit_timestamp); });
    } else {
      replica_state_.store(replication::ReplicaState::READY);
      return true;
    }
  } catch (const rpc::RpcFailedException &) {
    replica_stream_.reset();
    {
      std::unique_lock client_guard(client_lock_);
      replica_state_.store(replication::ReplicaState::INVALID);
    }
    HandleRpcFailure();
  }
  return false;
}

void Storage::ReplicationClient::RecoverReplica(uint64_t replica_commit) {
  while (true) {
    auto file_locker = storage_->file_retainer_.AddLocker();

    const auto steps = GetRecoverySteps(replica_commit, &file_locker);
    for (const auto &recovery_step : steps) {
      try {
        std::visit(
            [&, this]<typename T>(T &&arg) {
              using StepType = std::remove_cvref_t<T>;
              if constexpr (std::is_same_v<StepType, RecoverySnapshot>) {
                spdlog::debug("Sending the latest snapshot file: {}", arg);
                auto response = TransferSnapshot(arg);
                replica_commit = response.current_commit_timestamp;
              } else if constexpr (std::is_same_v<StepType, RecoveryWals>) {
                spdlog::debug("Sending the latest wal files");
                auto response = TransferWalFiles(arg);
                replica_commit = response.current_commit_timestamp;
              } else if constexpr (std::is_same_v<StepType, RecoveryCurrentWal>) {
                std::unique_lock transaction_guard(storage_->engine_lock_);
                if (storage_->wal_file_ && storage_->wal_file_->SequenceNumber() == arg.current_wal_seq_num) {
                  storage_->wal_file_->DisableFlushing();
                  transaction_guard.unlock();
                  spdlog::debug("Sending current wal file");
                  replica_commit = ReplicateCurrentWal();
                  storage_->wal_file_->EnableFlushing();
                }
              } else {
                static_assert(always_false_v<T>, "Missing type from variant visitor");
              }
            },
            recovery_step);
      } catch (const rpc::RpcFailedException &) {
        {
          std::unique_lock client_guard{client_lock_};
          replica_state_.store(replication::ReplicaState::INVALID);
        }
        HandleRpcFailure();
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
    std::unique_lock client_guard{client_lock_};
    SPDLOG_INFO("Replica timestamp: {}", replica_commit);
    SPDLOG_INFO("Last commit: {}", storage_->last_commit_timestamp_);
    if (storage_->last_commit_timestamp_.load() == replica_commit) {
      replica_state_.store(replication::ReplicaState::READY);
      return;
    }
  }
}

uint64_t Storage::ReplicationClient::ReplicateCurrentWal() {
  const auto &wal_file = storage_->wal_file_;
  auto stream = TransferCurrentWalFile();
  stream.AppendFilename(wal_file->Path().filename());
  utils::InputFile file;
  MG_ASSERT(file.Open(storage_->wal_file_->Path()), "Failed to open current WAL file!");
  const auto [buffer, buffer_size] = wal_file->CurrentFileBuffer();
  stream.AppendSize(file.GetSize() + buffer_size);
  stream.AppendFileData(&file);
  stream.AppendBufferData(buffer, buffer_size);
  auto response = stream.Finalize();
  return response.current_commit_timestamp;
}

/// This method tries to find the optimal path for recovering a single replica.
/// Based on the last commit transfered to replica it tries to update the
/// replica using durability files - WALs and Snapshots. WAL files are much
/// smaller in size as they contain only the Deltas (changes) made during the
/// transactions while Snapshots contain all the data. For that reason we prefer
/// WALs as much as possible. As the WAL file that is currently being updated
/// can change during the process we ignore it as much as possible. Also, it
/// uses the transaction lock so lokcing it can be really expensive. After we
/// fetch the list of finalized WALs, we try to find the longest chain of
/// sequential WALs, starting from the latest one, that will update the recovery
/// with the all missed updates. If the WAL chain cannot be created, replica is
/// behind by a lot, so we use the regular recovery process, we send the latest
/// snapshot and all the necessary WAL files, starting from the newest WAL that
/// contains a timestamp before the snapshot. If we registered the existence of
/// the current WAL, we add the sequence number we read from it to the recovery
/// process. After all the other steps are finished, if the current WAL contains
/// the same sequence number, it's the same WAL we read while fetching the
/// recovery steps, so we can safely send it to the replica.
/// We assume that the property of preserving at least 1 WAL before the snapshot
/// is satisfied as we extract the timestamp information from it.
std::vector<Storage::ReplicationClient::RecoveryStep> Storage::ReplicationClient::GetRecoverySteps(
    const uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker) {
  // First check if we can recover using the current wal file only
  // otherwise save the seq_num of the current wal file
  // This lock is also necessary to force the missed transaction to finish.
  std::optional<uint64_t> current_wal_seq_num;
  std::optional<uint64_t> current_wal_from_timestamp;
  if (std::unique_lock transtacion_guard(storage_->engine_lock_); storage_->wal_file_) {
    current_wal_seq_num.emplace(storage_->wal_file_->SequenceNumber());
    current_wal_from_timestamp.emplace(storage_->wal_file_->FromTimestamp());
  }

  auto locker_acc = file_locker->Access();
  auto wal_files = durability::GetWalFiles(storage_->wal_directory_, storage_->uuid_, current_wal_seq_num);
  MG_ASSERT(wal_files, "Wal files could not be loaded");

  auto snapshot_files = durability::GetSnapshotFiles(storage_->snapshot_directory_, storage_->uuid_);
  std::optional<durability::SnapshotDurabilityInfo> latest_snapshot;
  if (!snapshot_files.empty()) {
    std::sort(snapshot_files.begin(), snapshot_files.end());
    latest_snapshot.emplace(std::move(snapshot_files.back()));
  }

  std::vector<RecoveryStep> recovery_steps;

  // No finalized WAL files were found. This means the difference is contained
  // inside the current WAL or the snapshot.
  if (wal_files->empty()) {
    if (current_wal_from_timestamp && replica_commit >= *current_wal_from_timestamp) {
      MG_ASSERT(current_wal_seq_num);
      recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
      return recovery_steps;
    }

    // Without the finalized WAL containing the current timestamp of replica,
    // we cannot know if the difference is only in the current WAL or we need
    // to send the snapshot.
    if (latest_snapshot) {
      const auto lock_success = locker_acc.AddPath(latest_snapshot->path);
      MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistent path.");
      recovery_steps.emplace_back(std::in_place_type_t<RecoverySnapshot>{}, std::move(latest_snapshot->path));
    }
    // if there are no finalized WAL files, snapshot left the current WAL
    // as the WAL file containing a transaction before snapshot creation
    // so we can be sure that the current WAL is present
    MG_ASSERT(current_wal_seq_num);
    recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
    return recovery_steps;
  }

  // Find the longest chain of WALs for recovery.
  // The chain consists ONLY of sequential WALs.
  auto rwal_it = wal_files->rbegin();

  // if the last finalized WAL is before the replica commit
  // then we can recovery only from current WAL
  if (rwal_it->to_timestamp <= replica_commit) {
    MG_ASSERT(current_wal_seq_num);
    recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
    return recovery_steps;
  }

  uint64_t previous_seq_num{rwal_it->seq_num};
  for (; rwal_it != wal_files->rend(); ++rwal_it) {
    // If the difference between two consecutive wal files is not 0 or 1
    // we have a missing WAL in our chain
    if (previous_seq_num - rwal_it->seq_num > 1) {
      break;
    }

    // Find first WAL that contains up to replica commit, i.e. WAL
    // that is before the replica commit or contains the replica commit
    // as the last committed transaction OR we managed to find the first WAL
    // file.
    if (replica_commit >= rwal_it->from_timestamp || rwal_it->seq_num == 0) {
      if (replica_commit >= rwal_it->to_timestamp) {
        // We want the WAL after because the replica already contains all the
        // commits from this WAL
        --rwal_it;
      }
      std::vector<std::filesystem::path> wal_chain;
      auto distance_from_first = std::distance(rwal_it, wal_files->rend() - 1);
      // We have managed to create WAL chain
      // We need to lock these files and add them to the chain
      for (auto result_wal_it = wal_files->begin() + distance_from_first; result_wal_it != wal_files->end();
           ++result_wal_it) {
        const auto lock_success = locker_acc.AddPath(result_wal_it->path);
        MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistent path.");
        wal_chain.push_back(std::move(result_wal_it->path));
      }

      recovery_steps.emplace_back(std::in_place_type_t<RecoveryWals>{}, std::move(wal_chain));

      if (current_wal_seq_num) {
        recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
      }
      return recovery_steps;
    }

    previous_seq_num = rwal_it->seq_num;
  }

  MG_ASSERT(latest_snapshot, "Invalid durability state, missing snapshot");
  // We didn't manage to find a WAL chain, we need to send the latest snapshot
  // with its WALs
  const auto lock_success = locker_acc.AddPath(latest_snapshot->path);
  MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistent path.");
  recovery_steps.emplace_back(std::in_place_type_t<RecoverySnapshot>{}, std::move(latest_snapshot->path));

  std::vector<std::filesystem::path> recovery_wal_files;
  auto wal_it = wal_files->begin();
  for (; wal_it != wal_files->end(); ++wal_it) {
    // Assuming recovery process is correct the snashpot should
    // always retain a single WAL that contains a transaction
    // before its creation
    if (latest_snapshot->start_timestamp < wal_it->to_timestamp) {
      if (latest_snapshot->start_timestamp < wal_it->from_timestamp) {
        MG_ASSERT(wal_it != wal_files->begin(), "Invalid durability files state");
        --wal_it;
      }
      break;
    }
  }

  for (; wal_it != wal_files->end(); ++wal_it) {
    const auto lock_success = locker_acc.AddPath(wal_it->path);
    MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistent path.");
    recovery_wal_files.push_back(std::move(wal_it->path));
  }

  // We only have a WAL before the snapshot
  if (recovery_wal_files.empty()) {
    const auto lock_success = locker_acc.AddPath(wal_files->back().path);
    MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistent path.");
    recovery_wal_files.push_back(std::move(wal_files->back().path));
  }

  recovery_steps.emplace_back(std::in_place_type_t<RecoveryWals>{}, std::move(recovery_wal_files));

  if (current_wal_seq_num) {
    recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
  }

  return recovery_steps;
}

Storage::TimestampInfo Storage::ReplicationClient::GetTimestampInfo() {
  Storage::TimestampInfo info;
  info.current_timestamp_of_replica = 0;
  info.current_number_of_timestamp_behind_master = 0;

  try {
    auto stream{rpc_client_->Stream<replication::TimestampRpc>()};
    const auto response = stream.AwaitResponse();
    const auto is_success = response.success;
    if (!is_success) {
      replica_state_.store(replication::ReplicaState::INVALID);
      HandleRpcFailure();
    }
    auto main_time_stamp = storage_->last_commit_timestamp_.load();
    info.current_timestamp_of_replica = response.current_commit_timestamp;
    info.current_number_of_timestamp_behind_master = response.current_commit_timestamp - main_time_stamp;
  } catch (const rpc::RpcFailedException &) {
    {
      std::unique_lock client_guard(client_lock_);
      replica_state_.store(replication::ReplicaState::INVALID);
    }
    HandleRpcFailure();  // mutex already unlocked, if the new enqueued task dispatches immediately it probably won't
                         // block
  }

  return info;
}

////// ReplicaStream //////
Storage::ReplicationClient::ReplicaStream::ReplicaStream(ReplicationClient *self,
                                                         const uint64_t previous_commit_timestamp,
                                                         const uint64_t current_seq_num)
    : self_(self),
      stream_(self_->rpc_client_->Stream<replication::AppendDeltasRpc>(previous_commit_timestamp, current_seq_num)) {
  replication::Encoder encoder{stream_.GetBuilder()};
  encoder.WriteString(self_->storage_->epoch_id_);
}

void Storage::ReplicationClient::ReplicaStream::AppendDelta(const Delta &delta, const Vertex &vertex,
                                                            uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, &self_->storage_->name_id_mapper_, self_->storage_->config_.items, delta, vertex,
              final_commit_timestamp);
}

void Storage::ReplicationClient::ReplicaStream::AppendDelta(const Delta &delta, const Edge &edge,
                                                            uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, &self_->storage_->name_id_mapper_, delta, edge, final_commit_timestamp);
}

void Storage::ReplicationClient::ReplicaStream::AppendTransactionEnd(uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeTransactionEnd(&encoder, final_commit_timestamp);
}

void Storage::ReplicationClient::ReplicaStream::AppendOperation(durability::StorageGlobalOperation operation,
                                                                LabelId label, const std::set<PropertyId> &properties,
                                                                uint64_t timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeOperation(&encoder, &self_->storage_->name_id_mapper_, operation, label, properties, timestamp);
}

replication::AppendDeltasRes Storage::ReplicationClient::ReplicaStream::Finalize() { return stream_.AwaitResponse(); }

////// CurrentWalHandler //////
Storage::ReplicationClient::CurrentWalHandler::CurrentWalHandler(ReplicationClient *self)
    : self_(self), stream_(self_->rpc_client_->Stream<replication::CurrentWalRpc>()) {}

void Storage::ReplicationClient::CurrentWalHandler::AppendFilename(const std::string &filename) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteString(filename);
}

void Storage::ReplicationClient::CurrentWalHandler::AppendSize(const size_t size) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteUint(size);
}

void Storage::ReplicationClient::CurrentWalHandler::AppendFileData(utils::InputFile *file) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteFileData(file);
}

void Storage::ReplicationClient::CurrentWalHandler::AppendBufferData(const uint8_t *buffer, const size_t buffer_size) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteBuffer(buffer, buffer_size);
}

replication::CurrentWalRes Storage::ReplicationClient::CurrentWalHandler::Finalize() { return stream_.AwaitResponse(); }
}  // namespace memgraph::storage
