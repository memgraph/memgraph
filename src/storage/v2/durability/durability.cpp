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

#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include <algorithm>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "flags/all.hpp"
#include "gflags/gflags.h"
#include "replication/epoch.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "utils/event_histogram.hpp"
#include "utils/flag_validation.hpp"
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
      if (!utils::HasReadAccess(item.path())) {
        spdlog::warn(
            "Skipping snapshot file '{}' because it is not readable, check file ownership and read permissions!",
            item.path());
        continue;
      }
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

  std::sort(snapshot_files.begin(), snapshot_files.end());
  return snapshot_files;
}

std::optional<std::vector<WalDurabilityInfo>> GetWalFiles(const std::filesystem::path &wal_directory,
                                                          const std::string_view uuid,
                                                          const std::optional<size_t> current_seq_num) {
  if (!utils::DirExists(wal_directory)) return std::nullopt;

  std::vector<WalDurabilityInfo> wal_files;
  std::error_code error_code;
  // There could be multiple "current" WAL files, the "_current" tag just means that the previous session didn't
  // finalize. We cannot skip based on name, will be able to skip based on invalid data or sequence number, so the
  // actual current wal will be skipped
  for (const auto &item : std::filesystem::directory_iterator(wal_directory, error_code)) {
    if (!item.is_regular_file()) continue;
    try {
      auto info = ReadWalInfo(item.path());
      spdlog::trace("Getting wal file with following info: uuid: {}, epoch id: {}, from timestamp {}, to_timestamp {} ",
                    info.uuid, info.epoch_id, info.from_timestamp, info.to_timestamp);
      if ((uuid.empty() || info.uuid == uuid) && (!current_seq_num || info.seq_num < *current_seq_num)) {
        wal_files.emplace_back(info.seq_num, info.from_timestamp, info.to_timestamp, std::move(info.uuid),
                               std::move(info.epoch_id), item.path());
      }
    } catch (const RecoveryFailure &e) {
      spdlog::warn("Failed to read {}", item.path());
      continue;
    }
  }
  MG_ASSERT(!error_code, "Couldn't recover data because an error occurred: {}!", error_code.message());

  // Sort based on the sequence number, not the file name
  std::sort(wal_files.begin(), wal_files.end());
  return std::move(wal_files);
}

// Function used to recover all discovered indices and constraints. The
// indices and constraints must be recovered after the data recovery is done
// to ensure that the indices and constraints are consistent at the end of the
// recovery process.

void RecoverConstraints(const RecoveredIndicesAndConstraints::ConstraintsMetadata &constraints_metadata,
                        Constraints *constraints, utils::SkipList<Vertex> *vertices, NameIdMapper *name_id_mapper,
                        const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info) {
  RecoverExistenceConstraints(constraints_metadata, constraints, vertices, name_id_mapper, parallel_exec_info);
  RecoverUniqueConstraints(constraints_metadata, constraints, vertices, name_id_mapper, parallel_exec_info);
}

void RecoverIndicesAndStats(const RecoveredIndicesAndConstraints::IndicesMetadata &indices_metadata, Indices *indices,
                            utils::SkipList<Vertex> *vertices, NameIdMapper *name_id_mapper,
                            const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info) {
  spdlog::info("Recreating indices from metadata.");

  // Recover label indices.
  spdlog::info("Recreating {} label indices from metadata.", indices_metadata.label.size());
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(indices->label_index_.get());
  for (const auto &item : indices_metadata.label) {
    if (!mem_label_index->CreateIndex(item, vertices->access(), parallel_exec_info)) {
      throw RecoveryFailure("The label index must be created here!");
    }
    spdlog::info("Index on :{} is recreated from metadata", name_id_mapper->IdToName(item.AsUint()));
  }
  spdlog::info("Label indices are recreated.");

  spdlog::info("Recreating index statistics from metadata.");

  // Recover label indices statistics.
  spdlog::info("Recreating {} label index statistics from metadata.", indices_metadata.label_stats.size());
  for (const auto &item : indices_metadata.label_stats) {
    mem_label_index->SetIndexStats(item.first, item.second);
    spdlog::info("Statistics for index on :{} are recreated from metadata",
                 name_id_mapper->IdToName(item.first.AsUint()));
  }
  spdlog::info("Label indices statistics are recreated.");

  // Recover label+property indices.
  spdlog::info("Recreating {} label+property indices from metadata.", indices_metadata.label_property.size());
  auto *mem_label_property_index = static_cast<InMemoryLabelPropertyIndex *>(indices->label_property_index_.get());
  for (const auto &item : indices_metadata.label_property) {
    if (!mem_label_property_index->CreateIndex(item.first, item.second, vertices->access(), parallel_exec_info))
      throw RecoveryFailure("The label+property index must be created here!");
    spdlog::info("Index on :{}({}) is recreated from metadata", name_id_mapper->IdToName(item.first.AsUint()),
                 name_id_mapper->IdToName(item.second.AsUint()));
  }
  spdlog::info("Label+property indices are recreated.");

  // Recover label+property indices statistics.
  spdlog::info("Recreating {} label+property indices statistics from metadata.",
               indices_metadata.label_property_stats.size());
  for (const auto &item : indices_metadata.label_property_stats) {
    const auto label_id = item.first;
    const auto property_id = item.second.first;
    const auto &stats = item.second.second;
    mem_label_property_index->SetIndexStats({label_id, property_id}, stats);
    spdlog::info("Statistics for index on :{}({}) are recreated from metadata",
                 name_id_mapper->IdToName(label_id.AsUint()), name_id_mapper->IdToName(property_id.AsUint()));
  }
  spdlog::info("Label+property indices statistics are recreated.");

  // Recover edge-type indices.
  spdlog::info("Recreating {} edge-type indices from metadata.", indices_metadata.edge.size());
  auto *mem_edge_type_index = static_cast<InMemoryEdgeTypeIndex *>(indices->edge_type_index_.get());
  for (const auto &item : indices_metadata.edge) {
    if (!mem_edge_type_index->CreateIndex(item, vertices->access())) {
      throw RecoveryFailure("The edge-type index must be created here!");
    }
    spdlog::info("Index on :{} is recreated from metadata", name_id_mapper->IdToName(item.AsUint()));
  }
  spdlog::info("Edge-type indices are recreated.");

  spdlog::info("Indices are recreated.");
}

void RecoverExistenceConstraints(const RecoveredIndicesAndConstraints::ConstraintsMetadata &constraints_metadata,
                                 Constraints *constraints, utils::SkipList<Vertex> *vertices,
                                 NameIdMapper *name_id_mapper,
                                 const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info) {
  spdlog::info("Recreating {} existence constraints from metadata.", constraints_metadata.existence.size());
  for (const auto &[label, property] : constraints_metadata.existence) {
    if (constraints->existence_constraints_->ConstraintExists(label, property)) {
      throw RecoveryFailure("The existence constraint already exists!");
    }

    if (auto violation =
            ExistenceConstraints::ValidateVerticesOnConstraint(vertices->access(), label, property, parallel_exec_info);
        violation.has_value()) {
      throw RecoveryFailure("The existence constraint failed because it couldn't be validated!");
    }

    constraints->existence_constraints_->InsertConstraint(label, property);
    spdlog::info("Existence constraint on :{}({}) is recreated from metadata", name_id_mapper->IdToName(label.AsUint()),
                 name_id_mapper->IdToName(property.AsUint()));
  }
  spdlog::info("Existence constraints are recreated from metadata.");
}

void RecoverUniqueConstraints(const RecoveredIndicesAndConstraints::ConstraintsMetadata &constraints_metadata,
                              Constraints *constraints, utils::SkipList<Vertex> *vertices, NameIdMapper *name_id_mapper,
                              const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info) {
  spdlog::info("Recreating {} unique constraints from metadata.", constraints_metadata.unique.size());

  for (const auto &[label, properties] : constraints_metadata.unique) {
    auto *mem_unique_constraints = static_cast<InMemoryUniqueConstraints *>(constraints->unique_constraints_.get());
    auto ret = mem_unique_constraints->CreateConstraint(label, properties, vertices->access(), parallel_exec_info);
    if (ret.HasError() || ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS)
      throw RecoveryFailure("The unique constraint must be created here!");

    std::vector<std::string> property_names;
    property_names.reserve(properties.size());
    for (const auto &prop : properties) {
      property_names.emplace_back(name_id_mapper->IdToName(prop.AsUint()));
    }
    const auto property_names_joined = utils::Join(property_names, ",");
    spdlog::info("Unique constraint on :{}({}) is recreated from metadata", name_id_mapper->IdToName(label.AsUint()),
                 property_names_joined);
  }
  spdlog::info("Unique constraints are recreated from metadata.");
  spdlog::info("Constraints are recreated from metadata.");
}

std::optional<ParallelizedSchemaCreationInfo> GetParallelExecInfo(const RecoveryInfo &recovery_info,
                                                                  const Config &config) {
  return config.durability.allow_parallel_schema_creation
             ? std::make_optional(ParallelizedSchemaCreationInfo{recovery_info.vertex_batches,
                                                                 config.durability.recovery_thread_count})
             : std::nullopt;
}

std::optional<ParallelizedSchemaCreationInfo> GetParallelExecInfoIndices(const RecoveryInfo &recovery_info,
                                                                         const Config &config) {
  return config.durability.allow_parallel_schema_creation || config.durability.allow_parallel_index_creation
             ? std::make_optional(ParallelizedSchemaCreationInfo{recovery_info.vertex_batches,
                                                                 config.durability.recovery_thread_count})
             : std::nullopt;
}

std::optional<RecoveryInfo> Recovery::RecoverData(std::string *uuid, ReplicationStorageState &repl_storage_state,
                                                  utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                                  std::atomic<uint64_t> *edge_count, NameIdMapper *name_id_mapper,
                                                  Indices *indices, Constraints *constraints, const Config &config,
                                                  uint64_t *wal_seq_num) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  spdlog::info("Recovering persisted data using snapshot ({}) and WAL directory ({}).", snapshot_directory_,
               wal_directory_);
  if (!utils::DirExists(snapshot_directory_) && !utils::DirExists(wal_directory_)) {
    spdlog::warn(utils::MessageWithLink("Snapshot or WAL directory don't exist, there is nothing to recover.",
                                        "https://memgr.ph/durability"));
    return std::nullopt;
  }

  auto *const epoch_history = &repl_storage_state.history;
  utils::Timer timer;

  auto snapshot_files = GetSnapshotFiles(snapshot_directory_);

  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;
  std::optional<uint64_t> snapshot_timestamp;
  if (!snapshot_files.empty()) {
    spdlog::info("Try recovering from snapshot directory {}.", wal_directory_);

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
    repl_storage_state.epoch_.SetEpoch(std::move(recovered_snapshot->snapshot_info.epoch_id));

    if (!utils::DirExists(wal_directory_)) {
      RecoverIndicesAndStats(indices_constraints.indices, indices, vertices, name_id_mapper,
                             GetParallelExecInfoIndices(recovery_info, config));
      RecoverConstraints(indices_constraints.constraints, constraints, vertices, name_id_mapper,
                         GetParallelExecInfo(recovery_info, config));
      return recovered_snapshot->recovery_info;
    }
  } else {
    spdlog::info("No snapshot file was found, collecting information from WAL directory {}.", wal_directory_);
    std::error_code error_code;
    if (!utils::DirExists(wal_directory_)) return std::nullopt;
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
    for (const auto &item : std::filesystem::directory_iterator(wal_directory_, error_code)) {
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
    repl_storage_state.epoch_.SetEpoch(std::move(wal_files.back().epoch_id));
  }

  auto maybe_wal_files = GetWalFiles(wal_directory_, *uuid);
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

    if (last_loaded_timestamp) {
      epoch_history->emplace_back(repl_storage_state.epoch_.id(), *last_loaded_timestamp);
    }

    for (auto &wal_file : wal_files) {
      if (previous_seq_num && (wal_file.seq_num - *previous_seq_num) > 1) {
        LOG_FATAL("You are missing a WAL file with the sequence number {}!", *previous_seq_num + 1);
      }
      previous_seq_num = wal_file.seq_num;

      try {
        auto info = LoadWal(wal_file.path, &indices_constraints, last_loaded_timestamp, vertices, edges, name_id_mapper,
                            edge_count, config.salient.items);
        recovery_info.next_vertex_id = std::max(recovery_info.next_vertex_id, info.next_vertex_id);
        recovery_info.next_edge_id = std::max(recovery_info.next_edge_id, info.next_edge_id);
        recovery_info.next_timestamp = std::max(recovery_info.next_timestamp, info.next_timestamp);

        recovery_info.last_commit_timestamp = info.last_commit_timestamp;

        if (recovery_info.next_timestamp != 0) {
          last_loaded_timestamp.emplace(recovery_info.next_timestamp - 1);
        }

        auto last_loaded_timestamp_value = last_loaded_timestamp.value_or(0);
        if (epoch_history->empty() || epoch_history->back().first != wal_file.epoch_id) {
          // no history or new epoch, add it
          epoch_history->emplace_back(wal_file.epoch_id, last_loaded_timestamp_value);
          repl_storage_state.epoch_.SetEpoch(wal_file.epoch_id);
        } else if (epoch_history->back().second < last_loaded_timestamp_value) {
          // existing epoch, update with newer timestamp
          epoch_history->back().second = last_loaded_timestamp_value;
        }

      } catch (const RecoveryFailure &e) {
        LOG_FATAL("Couldn't recover WAL deltas from {} because of: {}", wal_file.path, e.what());
      }
    }
    // The sequence number needs to be recovered even though `LoadWal` didn't
    // load any deltas from that file.
    *wal_seq_num = *previous_seq_num + 1;

    spdlog::info("All necessary WAL files are loaded successfully.");
  }

  RecoverIndicesAndStats(indices_constraints.indices, indices, vertices, name_id_mapper,
                         GetParallelExecInfoIndices(recovery_info, config));
  RecoverConstraints(indices_constraints.constraints, constraints, vertices, name_id_mapper,
                     GetParallelExecInfo(recovery_info, config));

  memgraph::metrics::Measure(memgraph::metrics::SnapshotRecoveryLatency_us,
                             std::chrono::duration_cast<std::chrono::microseconds>(timer.Elapsed()).count());
  spdlog::trace("Set epoch id: {}  with commit timestamp {}", std::string(repl_storage_state.epoch_.id()),
                repl_storage_state.last_commit_timestamp_);

  std::for_each(repl_storage_state.history.begin(), repl_storage_state.history.end(), [](auto &history) {
    spdlog::trace("epoch id: {}  with commit timestamp {}", std::string(history.first), history.second);
  });
  return recovery_info;
}

}  // namespace memgraph::storage::durability
