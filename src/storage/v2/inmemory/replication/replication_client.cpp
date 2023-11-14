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

#include "storage/v2/inmemory/replication/replication_client.hpp"

#include "storage/v2/durability/durability.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace memgraph::storage {

namespace {
template <typename>
[[maybe_unused]] inline constexpr bool always_false_v = false;
}  // namespace

// Handler for transfering the current WAL file whose data is
// contained in the internal buffer and the file.
class CurrentWalHandler {
 public:
  explicit CurrentWalHandler(Storage *storage, rpc::Client &rpc_client);
  void AppendFilename(const std::string &filename);

  void AppendSize(size_t size);

  void AppendFileData(utils::InputFile *file);

  void AppendBufferData(const uint8_t *buffer, size_t buffer_size);

  /// @throw rpc::RpcFailedException
  replication::CurrentWalRes Finalize();

 private:
  rpc::Client::StreamHandler<replication::CurrentWalRpc> stream_;
};

////// CurrentWalHandler //////
CurrentWalHandler::CurrentWalHandler(Storage *storage, rpc::Client &rpc_client)
    : stream_(rpc_client.Stream<replication::CurrentWalRpc>(storage->id())) {}

void CurrentWalHandler::AppendFilename(const std::string &filename) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteString(filename);
}

void CurrentWalHandler::AppendSize(const size_t size) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteUint(size);
}

void CurrentWalHandler::AppendFileData(utils::InputFile *file) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteFileData(file);
}

void CurrentWalHandler::AppendBufferData(const uint8_t *buffer, const size_t buffer_size) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteBuffer(buffer, buffer_size);
}

replication::CurrentWalRes CurrentWalHandler::Finalize() { return stream_.AwaitResponse(); }

////// ReplicationClient Helpers //////

replication::WalFilesRes TransferWalFiles(std::string db_name, rpc::Client &client,
                                          const std::vector<std::filesystem::path> &wal_files) {
  MG_ASSERT(!wal_files.empty(), "Wal files list is empty!");
  auto stream = client.Stream<replication::WalFilesRpc>(std::move(db_name), wal_files.size());
  replication::Encoder encoder(stream.GetBuilder());
  for (const auto &wal : wal_files) {
    spdlog::debug("Sending wal file: {}", wal);
    encoder.WriteFile(wal);
  }
  return stream.AwaitResponse();
}

replication::SnapshotRes TransferSnapshot(std::string db_name, rpc::Client &client, const std::filesystem::path &path) {
  auto stream = client.Stream<replication::SnapshotRpc>(std::move(db_name));
  replication::Encoder encoder(stream.GetBuilder());
  encoder.WriteFile(path);
  return stream.AwaitResponse();
}

uint64_t ReplicateCurrentWal(CurrentWalHandler &stream, durability::WalFile const &wal_file) {
  stream.AppendFilename(wal_file.Path().filename());
  utils::InputFile file;
  MG_ASSERT(file.Open(wal_file.Path()), "Failed to open current WAL file at {}!", wal_file.Path());
  const auto [buffer, buffer_size] = wal_file.CurrentFileBuffer();
  stream.AppendSize(file.GetSize() + buffer_size);
  stream.AppendFileData(&file);
  stream.AppendBufferData(buffer, buffer_size);
  auto response = stream.Finalize();
  return response.current_commit_timestamp;
}

////// ReplicationClient //////

InMemoryReplicationClient::InMemoryReplicationClient(ReplicationClient &client) : ReplicationStorageClient{client} {}

void InMemoryReplicationClient::RecoverReplica(uint64_t replica_commit, memgraph::storage::Storage *storage) {
  spdlog::debug("Starting replica recover");
  auto *mem_storage = static_cast<InMemoryStorage *>(storage);
  while (true) {
    auto file_locker = mem_storage->file_retainer_.AddLocker();

    const auto steps = GetRecoverySteps(replica_commit, &file_locker, mem_storage);
    int i = 0;
    for (const InMemoryReplicationClient::RecoveryStep &recovery_step : steps) {
      spdlog::trace("Recovering in step: {}", i++);
      try {
        rpc::Client &rpcClient = client_.rpc_client_;
        std::visit(
            [&]<typename T>(T &&arg) {
              using StepType = std::remove_cvref_t<T>;
              if constexpr (std::is_same_v<StepType, RecoverySnapshot>) {  // TODO: split into 3 overloads
                spdlog::debug("Sending the latest snapshot file: {}", arg);
                auto response = TransferSnapshot(storage->id(), rpcClient, arg);
                replica_commit = response.current_commit_timestamp;
              } else if constexpr (std::is_same_v<StepType, RecoveryWals>) {
                spdlog::debug("Sending the latest wal files");
                auto response = TransferWalFiles(storage->id(), rpcClient, arg);
                replica_commit = response.current_commit_timestamp;
                spdlog::debug("Wal files successfully transferred.");
              } else if constexpr (std::is_same_v<StepType, RecoveryCurrentWal>) {
                std::unique_lock transaction_guard(storage->engine_lock_);
                if (mem_storage->wal_file_ && mem_storage->wal_file_->SequenceNumber() == arg.current_wal_seq_num) {
                  mem_storage->wal_file_->DisableFlushing();
                  transaction_guard.unlock();
                  spdlog::debug("Sending current wal file");
                  auto streamHandler = CurrentWalHandler{storage, rpcClient};
                  replica_commit = ReplicateCurrentWal(streamHandler, *mem_storage->wal_file_);
                  mem_storage->wal_file_->EnableFlushing();
                } else {
                  spdlog::debug("Cannot recover using current wal file");
                }
              } else {
                static_assert(always_false_v<T>, "Missing type from variant visitor");
              }
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
      replica_state_.WithLock([&](auto &val) { val = replication::ReplicaState::READY; });
      return;
    }
  }
}

/// This method tries to find the optimal path for recoverying a single replica.
/// Based on the last commit transfered to replica it tries to update the
/// replica using durability files - WALs and Snapshots. WAL files are much
/// smaller in size as they contain only the Deltas (changes) made during the
/// transactions while Snapshots contain all the data. For that reason we prefer
/// WALs as much as possible. As the WAL file that is currently being updated
/// can change during the process we ignore it as much as possible. Also, it
/// uses the transaction lock so locking it can be really expensive. After we
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
std::vector<InMemoryReplicationClient::RecoveryStep> InMemoryReplicationClient::GetRecoverySteps(
    uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker, const InMemoryStorage *storage) {
  // First check if we can recover using the current wal file only
  // otherwise save the seq_num of the current wal file
  // This lock is also necessary to force the missed transaction to finish.
  std::optional<uint64_t> current_wal_seq_num;
  std::optional<uint64_t> current_wal_from_timestamp;
  if (std::unique_lock transtacion_guard(storage->engine_lock_); storage->wal_file_) {
    current_wal_seq_num.emplace(storage->wal_file_->SequenceNumber());
    current_wal_from_timestamp.emplace(storage->wal_file_->FromTimestamp());
  }

  auto locker_acc = file_locker->Access();
  auto wal_files = durability::GetWalFiles(storage->wal_directory_, storage->uuid_, current_wal_seq_num, &locker_acc);
  MG_ASSERT(wal_files, "Wal files could not be loaded");

  (void)locker_acc.AddPath(
      storage->snapshot_directory_);  // protect snapshots from getting deleted while we are reading them
  auto snapshot_files = durability::GetSnapshotFiles(storage->snapshot_directory_, storage->uuid_);
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
      MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistant path.");
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
    for (const auto &wal : *wal_files) {
      (void)locker_acc.RemovePath(wal.path);  // unlock so it can be deleted
    }
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
    // that is before the replica commit or conatins the replica commit
    // as the last committed transaction OR we managed to find the first WAL
    // file.
    if (replica_commit >= rwal_it->from_timestamp || rwal_it->seq_num == 0) {
      if (replica_commit >= rwal_it->to_timestamp) {
        // We want the WAL after because the replica already contains all the
        // commits from this WAL
        (void)locker_acc.RemovePath(rwal_it->path);  // unlock so it can be deleted
        --rwal_it;
      }
      std::vector<std::filesystem::path> wal_chain;
      auto distance_from_first = std::distance(rwal_it, wal_files->rend() - 1);
      // We have managed to create WAL chain
      // We need to lock these files and add them to the chain
      for (auto result_wal_it = wal_files->begin() + distance_from_first; result_wal_it != wal_files->end();
           ++result_wal_it) {
        const auto lock_success = locker_acc.AddPath(result_wal_it->path);
        MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistant path.");
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
  MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistant path.");
  recovery_steps.emplace_back(std::in_place_type_t<RecoverySnapshot>{}, std::move(latest_snapshot->path));

  (void)locker_acc.RemovePath(storage->snapshot_directory_);  // No need to protect the whole directory anymore

  std::vector<std::filesystem::path> recovery_wal_files;
  auto wal_it = wal_files->begin();
  for (; wal_it != wal_files->end(); ++wal_it) {
    // Assuming recovery process is correct the snapshot should
    // always retain a single WAL that contains a transaction
    // before its creation
    if (latest_snapshot->start_timestamp < wal_it->to_timestamp) {
      if (latest_snapshot->start_timestamp < wal_it->from_timestamp) {
        MG_ASSERT(wal_it != wal_files->begin(), "Invalid durability files state");
        --wal_it;
      } else {
        if (wal_it != wal_files->begin())
          (void)locker_acc.RemovePath((wal_it - 1)->path);  // unlock so the file can be deleted
      }
      break;
    }
    if (wal_it != wal_files->begin()) {
      (void)locker_acc.RemovePath((wal_it - 1)->path);  // unlock so the file can be deleted
    }
  }

  for (; wal_it != wal_files->end(); ++wal_it) {
    const auto lock_success = locker_acc.AddPath(wal_it->path);
    MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistant path.");
    recovery_wal_files.push_back(std::move(wal_it->path));
  }

  // We only have a WAL before the snapshot
  if (recovery_wal_files.empty()) {
    const auto lock_success = locker_acc.AddPath(wal_files->back().path);
    MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistant path.");
    recovery_wal_files.push_back(std::move(wal_files->back().path));
  }

  recovery_steps.emplace_back(std::in_place_type_t<RecoveryWals>{}, std::move(recovery_wal_files));

  if (current_wal_seq_num) {
    recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
  }

  return recovery_steps;
}

}  // namespace memgraph::storage
