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

#include "storage/v2/durability/durability.hpp"

#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include <algorithm>
#include <tuple>
#include <utility>
#include <vector>

#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"
#include "utils/event_histogram.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/timer.hpp"

namespace memgraph::metrics {
extern const Event SnapshotRecoveryLatency_us;
}  // namespace memgraph::metrics

namespace memgraph::storage::durability {

void VerifyStorageDirectoryOwnerAndProcessUserOrDie(const std::filesystem::path &storage_directory) {
  // Get the process user ID.
  auto process_euid = geteuid();

  // Get the data directory owner ID.
  struct stat statbuf;
  auto ret = stat(storage_directory.c_str(), &statbuf);
  if (ret != 0 && errno == ENOENT) {
    // The directory doesn't currently exist.
    return;
  }
  MG_ASSERT(ret == 0, "Couldn't get stat for '{}' because of: {} ({})", storage_directory, strerror(errno), errno);
  auto directory_owner = statbuf.st_uid;

  auto get_username = [](auto uid) {
    auto info = getpwuid(uid);
    if (!info) return std::to_string(uid);
    return std::string(info->pw_name);
  };

  auto user_process = get_username(process_euid);
  auto user_directory = get_username(directory_owner);
  MG_ASSERT(process_euid == directory_owner,
            "The process is running as user {}, but the data directory is "
            "owned by user {}. Please start the process as user {}!",
            user_process, user_directory, user_directory);
}

std::vector<SnapshotDurabilityInfo> GetSnapshotFiles(const std::filesystem::path &snapshot_directory,
                                                     const std::string_view uuid) {
  std::vector<SnapshotDurabilityInfo> snapshot_files;
  std::error_code error_code;
  if (utils::DirExists(snapshot_directory)) {
    for (const auto &item : std::filesystem::directory_iterator(snapshot_directory, error_code)) {
      if (!item.is_regular_file()) continue;
      try {
        auto info = ReadSnapshotInfo(item.path());
        if (uuid.empty() || info.uuid == uuid) {
          snapshot_files.emplace_back(item.path(), std::move(info.uuid), info.start_timestamp);
        }
      } catch (const RecoveryFailure &) {
        continue;
      }
    }
    MG_ASSERT(!error_code, "Couldn't recover data because an error occurred: {}!", error_code.message());
  }

  return snapshot_files;
}

std::optional<std::vector<WalDurabilityInfo>> GetWalFiles(const std::filesystem::path &wal_directory,
                                                          const std::string_view uuid,
                                                          const std::optional<size_t> current_seq_num) {
  if (!utils::DirExists(wal_directory)) return std::nullopt;

  std::vector<WalDurabilityInfo> wal_files;
  std::error_code error_code;
  for (const auto &item : std::filesystem::directory_iterator(wal_directory, error_code)) {
    if (!item.is_regular_file()) continue;
    try {
      auto info = ReadWalInfo(item.path());
      if ((uuid.empty() || info.uuid == uuid) && (!current_seq_num || info.seq_num < *current_seq_num))
        wal_files.emplace_back(info.seq_num, info.from_timestamp, info.to_timestamp, std::move(info.uuid),
                               std::move(info.epoch_id), item.path());
    } catch (const RecoveryFailure &e) {
      spdlog::warn("Failed to read {}", item.path());
      continue;
    }
  }
  MG_ASSERT(!error_code, "Couldn't recover data because an error occurred: {}!", error_code.message());

  std::sort(wal_files.begin(), wal_files.end());
  return std::move(wal_files);
}

// Function used to recover all discovered indices and constraints. The
// indices and constraints must be recovered after the data recovery is done
// to ensure that the indices and constraints are consistent at the end of the
// recovery process.
void RecoverIndicesAndConstraints(const RecoveredIndicesAndConstraints &indices_constraints, Indices *indices,
                                  Constraints *constraints, utils::SkipList<Vertex> *vertices,
                                  const std::optional<ParallelizedIndexCreationInfo> &parallel_exec_info) {
  spdlog::info("Recreating indices from metadata.");
  // Recover label indices.
  spdlog::info("Recreating {} label indices from metadata.", indices_constraints.indices.label.size());
  for (const auto &item : indices_constraints.indices.label) {
    if (!indices->label_index.CreateIndex(item, vertices->access(), parallel_exec_info))
      throw RecoveryFailure("The label index must be created here!");

    spdlog::info("A label index is recreated from metadata.");
  }
  spdlog::info("Label indices are recreated.");

  // Recover label+property indices.
  spdlog::info("Recreating {} label+property indices from metadata.",
               indices_constraints.indices.label_property.size());
  for (const auto &item : indices_constraints.indices.label_property) {
    if (!indices->label_property_index.CreateIndex(item.first, item.second, vertices->access()))
      throw RecoveryFailure("The label+property index must be created here!");
    spdlog::info("A label+property index is recreated from metadata.");
  }
  spdlog::info("Label+property indices are recreated.");
  spdlog::info("Indices are recreated.");

  spdlog::info("Recreating constraints from metadata.");
  // Recover existence constraints.
  spdlog::info("Recreating {} existence constraints from metadata.", indices_constraints.constraints.existence.size());
  for (const auto &item : indices_constraints.constraints.existence) {
    auto ret = CreateExistenceConstraint(constraints, item.first, item.second, vertices->access());
    if (ret.HasError() || !ret.GetValue()) throw RecoveryFailure("The existence constraint must be created here!");
    spdlog::info("A existence constraint is recreated from metadata.");
  }
  spdlog::info("Existence constraints are recreated from metadata.");

  // Recover unique constraints.
  spdlog::info("Recreating {} unique constraints from metadata.", indices_constraints.constraints.unique.size());
  for (const auto &item : indices_constraints.constraints.unique) {
    auto ret = constraints->unique_constraints.CreateConstraint(item.first, item.second, vertices->access());
    if (ret.HasError() || ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS)
      throw RecoveryFailure("The unique constraint must be created here!");
    spdlog::info("A unique constraint is recreated from metadata.");
  }
  spdlog::info("Unique constraints are recreated from metadata.");
  spdlog::info("Constraints are recreated from metadata.");
}

std::optional<RecoveryInfo> RecoverData(const std::filesystem::path &snapshot_directory,
                                        const std::filesystem::path &wal_directory, std::string *uuid,
                                        std::string *epoch_id,
                                        std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                        utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                        std::atomic<uint64_t> *edge_count, NameIdMapper *name_id_mapper,
                                        Indices *indices, Constraints *constraints, const Config &config,
                                        uint64_t *wal_seq_num) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  spdlog::info("Recovering persisted data using snapshot ({}) and WAL directory ({}).", snapshot_directory,
               wal_directory);
  if (!utils::DirExists(snapshot_directory) && !utils::DirExists(wal_directory)) {
    spdlog::warn(utils::MessageWithLink("Snapshot or WAL directory don't exist, there is nothing to recover.",
                                        "https://memgr.ph/durability"));
    return std::nullopt;
  }

  utils::Timer timer;

  auto snapshot_files = GetSnapshotFiles(snapshot_directory);

  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;
  std::optional<uint64_t> snapshot_timestamp;
  if (!snapshot_files.empty()) {
    spdlog::info("Try recovering from snapshot directory {}.", snapshot_directory);
    // Order the files by name
    std::sort(snapshot_files.begin(), snapshot_files.end());

    // UUID used for durability is the UUID of the last snapshot file.
    *uuid = snapshot_files.back().uuid;
    std::optional<RecoveredSnapshot> recovered_snapshot;
    for (auto it = snapshot_files.rbegin(); it != snapshot_files.rend(); ++it) {
      const auto &[path, file_uuid, _] = *it;
      if (file_uuid != *uuid) {
        spdlog::warn("The snapshot file {} isn't related to the latest snapshot file!", path);
        continue;
      }
      spdlog::info("Starting snapshot recovery from {}.", path);
      try {
        recovered_snapshot = LoadSnapshot(path, vertices, edges, epoch_history, name_id_mapper, edge_count, config);
        spdlog::info("Snapshot recovery successful!");
        break;
      } catch (const RecoveryFailure &e) {
        spdlog::warn("Couldn't recover snapshot from {} because of: {}.", path, e.what());
        continue;
      }
    }
    MG_ASSERT(recovered_snapshot,
              "The database is configured to recover on startup, but couldn't "
              "recover using any of the specified snapshots! Please inspect them "
              "and restart the database.");
    recovery_info = recovered_snapshot->recovery_info;
    indices_constraints = std::move(recovered_snapshot->indices_constraints);
    snapshot_timestamp = recovered_snapshot->snapshot_info.start_timestamp;
    *epoch_id = std::move(recovered_snapshot->snapshot_info.epoch_id);

    if (!utils::DirExists(wal_directory)) {
      const auto par_exec_info = config.durability.allow_parallel_index_creation
                                     ? std::make_optional(std::make_pair(recovery_info.vertex_batches,
                                                                         config.durability.recovery_thread_count))
                                     : std::nullopt;
      RecoverIndicesAndConstraints(indices_constraints, indices, constraints, vertices, par_exec_info);
      return recovered_snapshot->recovery_info;
    }
  } else {
    spdlog::info("No snapshot file was found, collecting information from WAL directory {}.", wal_directory);
    std::error_code error_code;
    if (!utils::DirExists(wal_directory)) return std::nullopt;
    // We use this smaller struct that contains only a subset of information
    // necessary for the rest of the recovery function.
    // Also, the struct is sorted primarily on the path it contains.
    struct WalFileInfo {
      explicit WalFileInfo(std::filesystem::path path, std::string uuid, std::string epoch_id)
          : path(std::move(path)), uuid(std::move(uuid)), epoch_id(std::move(epoch_id)) {}
      std::filesystem::path path;
      std::string uuid;
      std::string epoch_id;

      auto operator<=>(const WalFileInfo &) const = default;
    };
    std::vector<WalFileInfo> wal_files;
    for (const auto &item : std::filesystem::directory_iterator(wal_directory, error_code)) {
      if (!item.is_regular_file()) continue;
      try {
        auto info = ReadWalInfo(item.path());
        wal_files.emplace_back(item.path(), std::move(info.uuid), std::move(info.epoch_id));
      } catch (const RecoveryFailure &e) {
        continue;
      }
    }
    MG_ASSERT(!error_code, "Couldn't recover data because an error occurred: {}!", error_code.message());
    if (wal_files.empty()) {
      spdlog::warn(utils::MessageWithLink("No snapshot or WAL file found.", "https://memgr.ph/durability"));
      return std::nullopt;
    }
    std::sort(wal_files.begin(), wal_files.end());
    // UUID used for durability is the UUID of the last WAL file.
    // Same for the epoch id.
    *uuid = std::move(wal_files.back().uuid);
    *epoch_id = std::move(wal_files.back().epoch_id);
  }

  auto maybe_wal_files = GetWalFiles(wal_directory, *uuid);
  if (!maybe_wal_files) {
    spdlog::warn(
        utils::MessageWithLink("Couldn't get WAL file info from the WAL directory.", "https://memgr.ph/durability"));
    return std::nullopt;
  }

  // Array of all discovered WAL files, ordered by sequence number.
  auto &wal_files = *maybe_wal_files;

  // By this point we should have recovered from a snapshot, or we should have
  // found some WAL files to recover from in the above `else`. This is just a
  // sanity check to circumvent the following case: The database didn't recover
  // from a snapshot, the above `else` triggered to find the recovery UUID from
  // a WAL file. The above `else` has an early exit in case there are no WAL
  // files. Because we reached this point there must have been some WAL files
  // and we must have some WAL files after this second WAL directory iteration.
  MG_ASSERT(snapshot_timestamp || !wal_files.empty(),
            "The database didn't recover from a snapshot and didn't find any WAL "
            "files that match the last WAL file!");

  if (!wal_files.empty()) {
    spdlog::info("Checking WAL files.");
    {
      const auto &first_wal = wal_files[0];
      if (first_wal.seq_num != 0) {
        // We don't have all WAL files. We need to see whether we need them all.
        if (!snapshot_timestamp) {
          // We didn't recover from a snapshot and we must have all WAL files
          // starting from the first one (seq_num == 0) to be able to recover
          // data from them.
          LOG_FATAL(
              "There are missing prefix WAL files and data can't be "
              "recovered without them!");
        } else if (first_wal.from_timestamp >= *snapshot_timestamp) {
          // We recovered from a snapshot and we must have at least one WAL file
          // that has at least one delta that was created before the snapshot in order to
          // verify that nothing is missing from the beginning of the WAL chain.
          LOG_FATAL(
              "You must have at least one WAL file that contains at least one "
              "delta that was created before the snapshot file!");
        }
      }
    }
    std::optional<uint64_t> previous_seq_num;
    auto last_loaded_timestamp = snapshot_timestamp;
    spdlog::info("Trying to load WAL files.");
    for (auto &wal_file : wal_files) {
      if (previous_seq_num && (wal_file.seq_num - *previous_seq_num) > 1) {
        LOG_FATAL("You are missing a WAL file with the sequence number {}!", *previous_seq_num + 1);
      }
      previous_seq_num = wal_file.seq_num;

      if (wal_file.epoch_id != *epoch_id) {
        // This way we skip WALs finalized only because of role change.
        // We can also set the last timestamp to 0 if last loaded timestamp
        // is nullopt as this can only happen if the WAL file with seq = 0
        // does not contain any deltas and we didn't find any snapshots.
        if (last_loaded_timestamp) {
          epoch_history->emplace_back(wal_file.epoch_id, *last_loaded_timestamp);
        }
        *epoch_id = std::move(wal_file.epoch_id);
      }
      try {
        auto info = LoadWal(wal_file.path, &indices_constraints, last_loaded_timestamp, vertices, edges, name_id_mapper,
                            edge_count, config.items);
        recovery_info.next_vertex_id = std::max(recovery_info.next_vertex_id, info.next_vertex_id);
        recovery_info.next_edge_id = std::max(recovery_info.next_edge_id, info.next_edge_id);
        recovery_info.next_timestamp = std::max(recovery_info.next_timestamp, info.next_timestamp);

        recovery_info.last_commit_timestamp = info.last_commit_timestamp;
      } catch (const RecoveryFailure &e) {
        LOG_FATAL("Couldn't recover WAL deltas from {} because of: {}", wal_file.path, e.what());
      }

      if (recovery_info.next_timestamp != 0) {
        last_loaded_timestamp.emplace(recovery_info.next_timestamp - 1);
      }
    }
    // The sequence number needs to be recovered even though `LoadWal` didn't
    // load any deltas from that file.
    *wal_seq_num = *previous_seq_num + 1;

    spdlog::info("All necessary WAL files are loaded successfully.");
  }

  RecoverIndicesAndConstraints(indices_constraints, indices, constraints, vertices);

  memgraph::metrics::Measure(memgraph::metrics::SnapshotRecoveryLatency_us,
                             std::chrono::duration_cast<std::chrono::microseconds>(timer.Elapsed()).count());

  return recovery_info;
}

}  // namespace memgraph::storage::durability
