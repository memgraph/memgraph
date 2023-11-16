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

#include "storage/v2/inmemory/replication/recovery.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::storage {

// Handler for transferring the current WAL file whose data is
// contained in the internal buffer and the file.
class InMemoryCurrentWalHandler {
 public:
  explicit InMemoryCurrentWalHandler(InMemoryStorage const *storage, rpc::Client &rpc_client);
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
InMemoryCurrentWalHandler::InMemoryCurrentWalHandler(InMemoryStorage const *storage, rpc::Client &rpc_client)
    : stream_(rpc_client.Stream<replication::CurrentWalRpc>(storage->id())) {}

void InMemoryCurrentWalHandler::AppendFilename(const std::string &filename) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteString(filename);
}

void InMemoryCurrentWalHandler::AppendSize(const size_t size) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteUint(size);
}

void InMemoryCurrentWalHandler::AppendFileData(utils::InputFile *file) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteFileData(file);
}

void InMemoryCurrentWalHandler::AppendBufferData(const uint8_t *buffer, const size_t buffer_size) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteBuffer(buffer, buffer_size);
}

replication::CurrentWalRes InMemoryCurrentWalHandler::Finalize() { return stream_.AwaitResponse(); }

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

uint64_t ReplicateCurrentWal(const InMemoryStorage *storage, rpc::Client &client, durability::WalFile const &wal_file) {
  InMemoryCurrentWalHandler stream{storage, client};
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
std::vector<RecoveryStep> GetRecoverySteps(uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker,
                                           const InMemoryStorage *storage) {
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
