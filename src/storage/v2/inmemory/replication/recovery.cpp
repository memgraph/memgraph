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

#include "storage/v2/inmemory/replication/recovery.hpp"
#include <algorithm>
#include <cstdint>
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/replication/recovery.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage {

template <>
// NOLINTNEXTLINE
auto const RpcInfo<replication::WalFilesRpc>::timerLabel = metrics::WalFilesRpc_us;
template <>
// NOLINTNEXTLINE
auto const RpcInfo<replication::CurrentWalRpc>::timerLabel = metrics::CurrentWalRpc_us;
template <>
// NOLINTNEXTLINE
auto const RpcInfo<replication::SnapshotRpc>::timerLabel = metrics::SnapshotRpc_us;

/// This method tries to find the optimal path for recovering a single replica.
/// Based on the last commit transferred to replica it tries to update the
/// replica using durability files - WALs and Snapshots. WAL files are much
/// smaller as they contain only the Deltas (changes) made during the
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
std::optional<std::vector<RecoveryStep>> GetRecoverySteps(uint64_t replica_commit,
                                                          utils::FileRetainer::FileLocker *file_locker,
                                                          const InMemoryStorage *main_storage) {
  std::vector<RecoveryStep> recovery_steps;
  auto locker_acc = file_locker->Access();

  // First check if we can recover using the current wal file only
  // otherwise save the seq_num of the current wal file
  // This lock is also necessary to force the missed transaction to finish.
  std::optional<uint64_t> current_wal_seq_num;
  uint64_t last_durable_timestamp{kTimestampInitialId};

  std::unique_lock transaction_guard(
      main_storage->engine_lock_);  // Hold the main_storage lock so the current wal file cannot be changed
  (void)locker_acc.AddPath(main_storage->recovery_.wal_directory_);  // Protect all WALs from being deleted

  if (main_storage->wal_file_) {
    last_durable_timestamp = main_storage->wal_file_->ToTimestamp();
    current_wal_seq_num.emplace(main_storage->wal_file_->SequenceNumber());
    // No need to hold the lock since the current WAL is present
    transaction_guard.unlock();
  }

  // Read in finalized WAL files (excluding the current/active WAL)
  utils::OnScopeExit
      release_wal_dir(  // Each individually used file will be locked, so at the end, the dir can be released
          [&locker_acc, &wal_dir = main_storage->recovery_.wal_directory_]() { (void)locker_acc.RemovePath(wal_dir); });
  // Get WAL files, ordered by timestamp, from oldest to newest
  auto const maybe_wal_files =
      durability::GetWalFiles(main_storage->recovery_.wal_directory_, main_storage->name_id_mapper_.get(),
                              std::string{main_storage->uuid()}, current_wal_seq_num);

  if (!maybe_wal_files) {
    spdlog::error("WAL files couldn't be loaded while trying to find recovery steps.");
    return std::nullopt;
  }
  auto const &wal_files = *maybe_wal_files;

  if (transaction_guard.owns_lock()) {
    transaction_guard.unlock();  // In case we didn't have a current wal file, we can unlock only now since there is no
                                 // guarantee what we'll see after we add the wal file
  }

  // Read in snapshot files
  (void)locker_acc.AddPath(main_storage->recovery_.snapshot_directory_);  // Protect all snapshots from being deleted
  utils::OnScopeExit
      release_snapshot_dir(  // Each individually used file will be locked, so at the end, the dir can be released
          [&locker_acc, &snapshot_dir = main_storage->recovery_.snapshot_directory_]() {
            (void)locker_acc.RemovePath(snapshot_dir);
          });

  std::optional<durability::SnapshotDurabilityInfo> latest_snapshot{};
  {
    auto snapshot_files =
        durability::GetSnapshotFiles(main_storage->recovery_.snapshot_directory_, std::string{main_storage->uuid()});
    if (!snapshot_files.empty()) {
      latest_snapshot.emplace(std::move(snapshot_files.back()));
    }
  }

  auto const add_snapshot = [&]() -> bool {
    // Handle snapshot step
    if (const auto lock_success = locker_acc.AddPath(latest_snapshot->path); lock_success.HasError()) {
      spdlog::error("Tried to lock a non-existent snapshot path while obtaining recovery steps.");
      return false;
    }
    recovery_steps.emplace_back(std::in_place_type_t<RecoverySnapshot>{}, std::move(latest_snapshot->path));
    last_durable_timestamp = std::max(last_durable_timestamp, latest_snapshot->start_timestamp);
    return true;
  };

  // There is a WAL chain and the data is newer than what the replica has
  if (!wal_files.empty() && wal_files.back().to_timestamp > replica_commit) {
    // Check what part of the WAL chain is needed
    bool covered_by_wals = false;
    auto prev_seq{wal_files.back().seq_num};
    int64_t first_useful_wal = static_cast<int64_t>(wal_files.size()) - 1;
    // Going from newest to oldest
    for (; first_useful_wal >= 0; --first_useful_wal) {
      const auto &wal = wal_files[first_useful_wal];
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
      const auto &wal = wal_files[first_useful_wal];
      // We might not need the snapshot if there is no additional information contained in it
      if (latest_snapshot) {
        if (latest_snapshot->start_timestamp <= wal.from_timestamp && wal.seq_num != 0) {
          spdlog::error("Replication steps incomplete; broken data chain.");
          return std::nullopt;
        }
        if (latest_snapshot->start_timestamp > replica_commit) {
          // There is some data we need in the snapshot
          if (!add_snapshot()) {
            // If we failed to add snapshot, recovery cannot be done successfully
            return std::nullopt;
          }
          // Go along the WAL chain and remove necessary parts (going from oldest to newest WAL)
          for (; first_useful_wal < wal_files.size(); ++first_useful_wal) {
            if (const auto &local_wal = wal_files[first_useful_wal];
                local_wal.to_timestamp > latest_snapshot->start_timestamp) {
              // Got to the first WAL file with data not contained in the snapshot
              break;
            }
          }
        }
      } else {
        if (wal.seq_num != 0) {
          spdlog::error("Replication steps incomplete; missing data.");
          return std::nullopt;
        }
      }
    }
    // Copy and lock the chain part we need, from oldest to newest
    if (first_useful_wal < wal_files.size()) {
      RecoveryWals rw{};
      rw.reserve(wal_files.size() - first_useful_wal);
      for (; first_useful_wal < wal_files.size(); ++first_useful_wal) {
        auto const &wal = wal_files[first_useful_wal];
        if (const auto lock_success = locker_acc.AddPath(wal.path); lock_success.HasError()) {
          spdlog::error("Tried to lock a nonexistent WAL path.");
          return std::nullopt;
        }
        rw.emplace_back(std::move(wal.path));
      }
      recovery_steps.emplace_back(std::in_place_type_t<RecoveryWals>{}, std::move(rw));
    }

  } else {
    if (latest_snapshot && latest_snapshot->start_timestamp > replica_commit) {
      // There is some data we need in the snapshot
      if (!add_snapshot()) {
        // If we failed to add snapshot, recovery cannot be done successfully
        return std::nullopt;
      }
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
