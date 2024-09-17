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

#include "storage/v2/inmemory/replication/recovery.hpp"
#include <algorithm>
#include <cstdint>
#include <iterator>
#include <type_traits>
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/replication/recovery.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/uuid.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::storage {

// Handler for transferring the current WAL file whose data is
// contained in the internal buffer and the file.
class InMemoryCurrentWalHandler {
 public:
  explicit InMemoryCurrentWalHandler(const utils::UUID &main_uuid, InMemoryStorage const *storage,
                                     rpc::Client &rpc_client);
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
InMemoryCurrentWalHandler::InMemoryCurrentWalHandler(const utils::UUID &main_uuid, InMemoryStorage const *storage,
                                                     rpc::Client &rpc_client)
    : stream_(rpc_client.Stream<replication::CurrentWalRpc>(main_uuid, storage->uuid())) {}

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
replication::WalFilesRes TransferWalFiles(const utils::UUID &main_uuid, const utils::UUID &uuid, rpc::Client &client,
                                          const std::vector<std::filesystem::path> &wal_files) {
  MG_ASSERT(!wal_files.empty(), "Wal files list is empty!");
  auto stream = client.Stream<replication::WalFilesRpc>(main_uuid, uuid, wal_files.size());
  replication::Encoder encoder(stream.GetBuilder());
  for (const auto &wal : wal_files) {
    spdlog::debug("Sending wal file: {}", wal);
    encoder.WriteFile(wal);
  }
  return stream.AwaitResponse();
}

replication::SnapshotRes TransferSnapshot(const utils::UUID &main_uuid, const utils::UUID &uuid, rpc::Client &client,
                                          const std::filesystem::path &path) {
  auto stream = client.Stream<replication::SnapshotRpc>(main_uuid, uuid);
  replication::Encoder encoder(stream.GetBuilder());
  encoder.WriteFile(path);
  return stream.AwaitResponse();
}

uint64_t ReplicateCurrentWal(const utils::UUID &main_uuid, const InMemoryStorage *storage, rpc::Client &client,
                             durability::WalFile const &wal_file) {
  InMemoryCurrentWalHandler stream{main_uuid, storage, client};
  stream.AppendFilename(wal_file.Path().filename());
  utils::InputFile file;
  MG_ASSERT(file.Open(wal_file.Path()), "Failed to open current WAL file at {}!", wal_file.Path());
  stream.AppendSize(file.GetSize());
  stream.AppendFileData(&file);
  auto response = stream.Finalize();
  return response.current_commit_timestamp;
}

/// This method tries to find the optimal path for recovering a single replica.
/// Based on the last commit transferred to replica it tries to update the
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
  std::vector<RecoveryStep> recovery_steps;
  auto locker_acc = file_locker->Access();

  // First check if we can recover using the current wal file only
  // otherwise save the seq_num of the current wal file
  // This lock is also necessary to force the missed transaction to finish.
  std::optional<uint64_t> current_wal_seq_num;
  std::optional<uint64_t> current_wal_from_timestamp;
  uint64_t last_durable_timestamp{kTimestampInitialId};

  std::unique_lock transaction_guard(
      storage->engine_lock_);  // Hold the storage lock so the current wal file cannot be changed
  (void)locker_acc.AddPath(storage->recovery_.wal_directory_);  // Protect all WALs from being deleted

  if (storage->wal_file_) {
    last_durable_timestamp = storage->wal_file_->ToTimestamp();
    current_wal_seq_num.emplace(storage->wal_file_->SequenceNumber());
    current_wal_from_timestamp.emplace(storage->wal_file_->FromTimestamp());
    // No need to hold the lock since the current WAL is present and we can simply skip them
    transaction_guard.unlock();
  }

  // Read in finalized WAL files (excluding the current/active WAL)
  utils::OnScopeExit
      release_wal_dir(  // Each individually used file will be locked, so at the end, the dir can be released
          [&locker_acc, &wal_dir = storage->recovery_.wal_directory_]() { (void)locker_acc.RemovePath(wal_dir); });
  // Get WAL files, ordered by timestamp, from oldest to newest
  auto wal_files = durability::GetWalFiles(storage->recovery_.wal_directory_, storage->uuid_, current_wal_seq_num);
  MG_ASSERT(wal_files, "Wal files could not be loaded");
  if (transaction_guard.owns_lock())
    transaction_guard.unlock();  // In case we didn't have a current wal file, we can unlock only now since there is no
                                 // guarantee what we'll see after we add the wal file

  // Read in snapshot files
  (void)locker_acc.AddPath(storage->recovery_.snapshot_directory_);  // Protect all snapshots from being deleted
  utils::OnScopeExit
      release_snapshot_dir(  // Each individually used file will be locked, so at the end, the dir can be released
          [&locker_acc, &snapshot_dir = storage->recovery_.snapshot_directory_]() {
            (void)locker_acc.RemovePath(snapshot_dir);
          });
  auto snapshot_files = durability::GetSnapshotFiles(storage->recovery_.snapshot_directory_, storage->uuid_);
  std::optional<durability::SnapshotDurabilityInfo> latest_snapshot{};
  if (!snapshot_files.empty()) {
    latest_snapshot.emplace(std::move(snapshot_files.back()));
  }

  auto add_snapshot = [&]() {
    // Handle snapshot step
    const auto lock_success = locker_acc.AddPath(latest_snapshot->path);
    MG_ASSERT(!lock_success.HasError(), "Tried to lock a non-existent snapshot path.");
    recovery_steps.emplace_back(std::in_place_type_t<RecoverySnapshot>{}, std::move(latest_snapshot->path));
    last_durable_timestamp = std::max(last_durable_timestamp, latest_snapshot->start_timestamp);
  };

  // There is a WAL chain and the data is newer than what the replica has
  if (!wal_files->empty() && wal_files->back().to_timestamp > replica_commit) {
    // Check what part of the WAL chain is needed
    bool covered_by_wals = false;
    auto prev_seq{wal_files->back().seq_num};
    int64_t first_useful_wal = (int64_t)wal_files->size() - 1;
    // Going from newest to oldest
    for (; first_useful_wal >= 0; --first_useful_wal) {
      const auto &wal = wal_files.value()[first_useful_wal];
      if (prev_seq - wal.seq_num > 1) {
        // Broken chain, must have a snapshot that covers the missing commits
        // Useful chain start from the previous (newer) wal file
        ++first_useful_wal;
        break;
      }
      prev_seq = wal.seq_num;
      // Got to the oldest necessary WAL file
      if (wal.from_timestamp <= replica_commit + 1) {
        covered_by_wals = true;
        break;
      }
    }
    first_useful_wal = std::max<int64_t>(first_useful_wal, 0);  // -1 means the first useful one is at position 0
    // Finished the WAL chain, but still missing some data
    if (!covered_by_wals) {
      const auto &wal = wal_files.value()[first_useful_wal];
      // We might not need the snapshot if there is no additional information contained in it
      if (latest_snapshot) {
        MG_ASSERT(latest_snapshot->start_timestamp > wal.from_timestamp || wal.seq_num == 0,
                  "Replication steps incomplete; broken data chain.");
        if (latest_snapshot->start_timestamp > replica_commit) {
          // There is some data we need in the snapshot
          add_snapshot();
          // Go along the WAL chain and remove unecessary parts (going from oldest to newest WAL)
          for (; first_useful_wal < wal_files->size(); ++first_useful_wal) {
            const auto &wal = wal_files.value()[first_useful_wal];
            if (wal.to_timestamp > latest_snapshot->start_timestamp) {
              // Got to the first WAL file with data not contained in the snapshot
              break;
            }
          }
        }
      } else {
        MG_ASSERT(wal.seq_num == 0, "Replication steps incomplete; missing data.");
      }
    }
    // Copy and lock the chain part we need, from oldest to newest
    if (first_useful_wal < wal_files->size()) {
      RecoveryWals rw{};
      rw.reserve(wal_files->size() - first_useful_wal);
      for (; first_useful_wal < wal_files->size(); ++first_useful_wal) {
        auto &wal = wal_files.value()[first_useful_wal];
        const auto lock_success = locker_acc.AddPath(wal.path);
        MG_ASSERT(!lock_success.HasError(), "Tried to lock a nonexistant WAL path.");
        rw.emplace_back(std::move(wal.path));
      }
      recovery_steps.emplace_back(std::in_place_type_t<RecoveryWals>{}, std::move(rw));
    }

  } else {
    if (latest_snapshot && latest_snapshot->start_timestamp > replica_commit) {
      // There is some data we need in the snapshot
      add_snapshot();
    }
  }

  // If we have a current wal file we need to use it
  if (current_wal_seq_num) {
    // NOTE: File not handled directly, so no need to lock it
    recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
  }

  return recovery_steps;
}

}  // namespace memgraph::storage
