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

#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include <algorithm>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "flags/all.hpp"
#include "gflags/gflags.h"
#include "replication/epoch.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/edge_property_index.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/edge_type_property_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "utils/event_histogram.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/timer.hpp"

namespace r = ranges;
namespace rv = r::views;

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

  // TODO: (andi) Inefficient to use I/O again, you already read infos.
  for (const auto &item : std::filesystem::directory_iterator(wal_directory, error_code)) {
    if (!item.is_regular_file()) {
      spdlog::trace("Non-regular file {} found in the wal directory. Skipping it.", item.path());
      continue;
    }
    try {
      auto info = ReadWalInfo(item.path());
      spdlog::trace(
          "Read wal file {} with following info: storage_uuid: {}, epoch id: {}, from timestamp {}, to_timestamp "
          "{}, "
          "sequence "
          "number {}.",
          item.path(), info.uuid, info.epoch_id, info.from_timestamp, info.to_timestamp, info.seq_num);
      if ((uuid.empty() || info.uuid == uuid) && (!current_seq_num || info.seq_num < *current_seq_num)) {
        wal_files.emplace_back(info.seq_num, info.from_timestamp, info.to_timestamp, std::move(info.uuid),
                               std::move(info.epoch_id), item.path());
        spdlog::trace("Wal file {} will be used.", item.path());
      } else {
        spdlog::trace("Wal file {} won't be used. UUID: {}. Info UUID: {}. Current seq num: {}. Info seq num: {}.",
                      item.path(), uuid, info.uuid, current_seq_num, info.seq_num);
      }
    } catch (const RecoveryFailure &e) {
      spdlog::warn("Failed to read WAL file {}.", item.path());
      continue;
    }
  }
  MG_ASSERT(!error_code, "Couldn't recover data because an error occurred: {}!", error_code.message());

  // Sort based on the sequence number, not the file name.
  std::sort(wal_files.begin(), wal_files.end());
  return std::move(wal_files);
}

// Function used to recover all discovered indices and constraints. The
// indices and constraints must be recovered after the data recovery is done
// to ensure that the indices and constraints are consistent at the end of the
// recovery process.

void RecoverConstraints(const RecoveredIndicesAndConstraints::ConstraintsMetadata &constraints_metadata,
                        Constraints *constraints, utils::SkipList<Vertex> *vertices, NameIdMapper *name_id_mapper,
                        const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info,
                        std::optional<SnapshotObserverInfo> const &snapshot_info) {
  RecoverExistenceConstraints(constraints_metadata, constraints, vertices, name_id_mapper, parallel_exec_info,
                              snapshot_info);
  RecoverUniqueConstraints(constraints_metadata, constraints, vertices, name_id_mapper, parallel_exec_info,
                           snapshot_info);
  RecoverTypeConstraints(constraints_metadata, constraints, vertices, parallel_exec_info, snapshot_info);
}

void RecoverIndicesAndStats(const RecoveredIndicesAndConstraints::IndicesMetadata &indices_metadata, Indices *indices,
                            utils::SkipList<Vertex> *vertices, NameIdMapper *name_id_mapper, bool properties_on_edges,
                            const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info,
                            const std::optional<std::filesystem::path> &storage_dir,
                            std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(indices->label_index_.get());
  // Recover label indices.
  {
    spdlog::info("Recreating {} label indices from metadata.", indices_metadata.label.size());
    for (const auto &item : indices_metadata.label) {
      if (!mem_label_index->CreateIndex(item, vertices->access(), parallel_exec_info, snapshot_info)) {
        throw RecoveryFailure("The label index must be created here!");
      }
      spdlog::info("Index on :{} is recreated from metadata", name_id_mapper->IdToName(item.AsUint()));
    }
    spdlog::info("Label indices are recreated.");
  }
  // Recover label indices statistics.
  {
    spdlog::info("Recreating {} label index statistics from metadata.", indices_metadata.label_stats.size());
    for (const auto &item : indices_metadata.label_stats) {
      mem_label_index->SetIndexStats(item.first, item.second);
      spdlog::info("Statistics for index on :{} are recreated from metadata",
                   name_id_mapper->IdToName(item.first.AsUint()));
    }
    spdlog::info("Label indices statistics are recreated.");
  }

  // Recover label+property indices.
  auto *mem_label_property_index = static_cast<InMemoryLabelPropertyIndex *>(indices->label_property_index_.get());
  {
    spdlog::info("Recreating {} label+property indices from metadata.", indices_metadata.label_properties.size());
    for (auto const &[label, properties] : indices_metadata.label_properties) {
      if (!mem_label_property_index->CreateIndex(label, properties, vertices->access(), parallel_exec_info,
                                                 snapshot_info))
        throw RecoveryFailure("The label+property index must be created here!");

      auto path_to_name = [&](const PropertyPath &path) {
        return path |
               rv::transform([&](const auto &property_id) { return name_id_mapper->IdToName(property_id.AsUint()); }) |
               rv::join('.') | r::to<std::string>;
      };
      auto const properties_str = utils::Join(properties | rv::transform(path_to_name), ", ");
      spdlog::info("Index on :{}({}) is recreated from metadata", name_id_mapper->IdToName(label.AsUint()),
                   properties_str);
    }
    spdlog::info("Label+property indices are recreated.");
  }

  // Recover label+property indices statistics.
  {
    spdlog::info("Recreating {} label+property indices statistics from metadata.",
                 indices_metadata.label_property_stats.size());
    for (const auto &item : indices_metadata.label_property_stats) {
      const auto label_id = item.first;
      const auto &property_ids = item.second.first;
      const auto &stats = item.second.second;
      mem_label_property_index->SetIndexStats(label_id, property_ids, stats);
      auto path_to_name = [&](const PropertyPath &path) {
        return path |
               rv::transform([&](const auto &property_id) { return name_id_mapper->IdToName(property_id.AsUint()); }) |
               rv::join('.') | r::to<std::string>;
      };
      auto const properties_str = utils::Join(property_ids | rv::transform(path_to_name), ", ");
      spdlog::info("Statistics for index on :{}({}) are recreated from metadata",
                   name_id_mapper->IdToName(label_id.AsUint()), properties_str);
    }
    spdlog::info("Label+property indices statistics are recreated.");
  }

  // Recover edge-type indices.
  {
    spdlog::info("Recreating {} edge-type indices from metadata.", indices_metadata.edge.size());
    auto *mem_edge_type_index = static_cast<InMemoryEdgeTypeIndex *>(indices->edge_type_index_.get());
    MG_ASSERT(indices_metadata.edge.empty() || properties_on_edges,
              "Trying to recover edge type indices while properties on edges are disabled.");

    for (const auto &item : indices_metadata.edge) {
      // TODO: parallel execution
      if (!mem_edge_type_index->CreateIndex(item, vertices->access(), snapshot_info)) {
        throw RecoveryFailure("The edge-type index must be created here!");
      }
      spdlog::info("Index on :{} is recreated from metadata", name_id_mapper->IdToName(item.AsUint()));
    }
    spdlog::info("Edge-type indices are recreated.");
  }

  // Recover edge-type + property indices.
  spdlog::info("Recreating {} edge-type indices from metadata.", indices_metadata.edge_type_property.size());
  MG_ASSERT(indices_metadata.edge_type_property.empty() || properties_on_edges,
            "Trying to recover edge type+property indices while properties on edges are disabled.");
  auto *mem_edge_type_property_index =
      static_cast<InMemoryEdgeTypePropertyIndex *>(indices->edge_type_property_index_.get());
  for (const auto &item : indices_metadata.edge_type_property) {
    // TODO: parallel execution
    if (!mem_edge_type_property_index->CreateIndex(item.first, item.second, vertices->access(), snapshot_info)) {
      throw RecoveryFailure("The edge-type property index must be created here!");
    }
    spdlog::info("Index on :{} + {} is recreated from metadata", name_id_mapper->IdToName(item.first.AsUint()),
                 name_id_mapper->IdToName(item.second.AsUint()));
  }
  spdlog::info("Edge-type + property indices are recreated.");

  // Recover global edge property indices.
  spdlog::info("Recreating {} global edge property indices from metadata.", indices_metadata.edge_property.size());
  MG_ASSERT(indices_metadata.edge_property.empty() || properties_on_edges,
            "Trying to recover global edge property indices while properties on edges are disabled.");
  auto *mem_edge_property_index = static_cast<InMemoryEdgePropertyIndex *>(indices->edge_property_index_.get());
  for (const auto &property : indices_metadata.edge_property) {
    // TODO: parallel execution
    if (!mem_edge_property_index->CreateIndex(property, vertices->access(), snapshot_info)) {
      throw RecoveryFailure("The global edge property index must be created here!");
    }
    spdlog::info("Edge index on property {} is recreated from metadata", name_id_mapper->IdToName(property.AsUint()));
  }
  spdlog::info("Global edge property indices are recreated.");

  // Text idx
  if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    // Recover text indices.
    spdlog::info("Recreating {} text indices from metadata.", indices_metadata.text_indices.size());
    auto &mem_text_index = indices->text_index_;
    for (const auto &[index_name, label] : indices_metadata.text_indices) {
      try {
        if (!storage_dir.has_value()) {
          throw RecoveryFailure("There must exist a storage directory in order to recover text indices!");
        }
        // TODO: parallel execution
        mem_text_index.RecoverIndex(index_name, label, vertices->access(), name_id_mapper, snapshot_info);
      } catch (...) {
        throw RecoveryFailure("The text index must be created here!");
      }
      spdlog::info("Text index {} on :{} is recreated from metadata", index_name,
                   name_id_mapper->IdToName(label.AsUint()));
    }
    spdlog::info("Text indices are recreated.");
  }

  // Point idx
  {
    spdlog::info("Recreating {} point indices statistics from metadata.", indices_metadata.point_label_property.size());
    for (const auto &[label, property] : indices_metadata.point_label_property) {
      // TODO: parallel execution
      if (!indices->point_index_.CreatePointIndex(label, property, vertices->access(), snapshot_info)) {
        throw RecoveryFailure("The point index must be created here!");
      }
      spdlog::info("Point index on :{}({}) is recreated from metadata", name_id_mapper->IdToName(label.AsUint()),
                   name_id_mapper->IdToName(property.AsUint()));
    }
    spdlog::info("Point indices are recreated.");
  }
  // Vector idx
  {
    spdlog::info("Recreating {} vector indices from metadata.", indices_metadata.vector_indices.size());
    auto vertices_acc = vertices->access();
    for (const auto &spec : indices_metadata.vector_indices) {
      if (!indices->vector_index_.CreateIndex(spec, vertices_acc, snapshot_info)) {
        throw RecoveryFailure("The vector index must be created here!");
      }
      spdlog::info("Vector index on :{}({}) is recreated from metadata", name_id_mapper->IdToName(spec.label.AsUint()),
                   name_id_mapper->IdToName(spec.property.AsUint()));
    }
    spdlog::info("Vector indices are recreated.");
  }

  spdlog::info("Indices are recreated.");
}

void RecoverExistenceConstraints(const RecoveredIndicesAndConstraints::ConstraintsMetadata &constraints_metadata,
                                 Constraints *constraints, utils::SkipList<Vertex> *vertices,
                                 NameIdMapper *name_id_mapper,
                                 const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info,
                                 std::optional<SnapshotObserverInfo> const &snapshot_info) {
  spdlog::info("Recreating {} existence constraints from metadata.", constraints_metadata.existence.size());
  for (const auto &[label, property] : constraints_metadata.existence) {
    if (constraints->existence_constraints_->ConstraintExists(label, property)) {
      throw RecoveryFailure("The existence constraint already exists!");
    }

    if (auto violation = ExistenceConstraints::ValidateVerticesOnConstraint(vertices->access(), label, property,
                                                                            parallel_exec_info, snapshot_info);
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
                              const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info,
                              std::optional<SnapshotObserverInfo> const &snapshot_info) {
  spdlog::info("Recreating {} unique constraints from metadata.", constraints_metadata.unique.size());

  for (const auto &[label, properties] : constraints_metadata.unique) {
    auto *mem_unique_constraints = static_cast<InMemoryUniqueConstraints *>(constraints->unique_constraints_.get());
    auto ret = mem_unique_constraints->CreateConstraint(label, properties, vertices->access(), parallel_exec_info,
                                                        snapshot_info);
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

void RecoverTypeConstraints(const RecoveredIndicesAndConstraints::ConstraintsMetadata &constraints_metadata,
                            Constraints *constraints, utils::SkipList<Vertex> *vertices,
                            const std::optional<ParallelizedSchemaCreationInfo> & /**/,
                            std::optional<SnapshotObserverInfo> const &snapshot_info) {
  // TODO: parallel recovery
  spdlog::info("Recreating {} type constraints from metadata.", constraints_metadata.type.size());
  for (const auto &[label, property, type] : constraints_metadata.type) {
    if (!constraints->type_constraints_->InsertConstraint(label, property, type)) {
      throw RecoveryFailure("The type constraint already exists!");
    }
  }

  if (constraints->HasTypeConstraints()) {
    if (auto violation = constraints->type_constraints_->ValidateVertices(vertices->access(), snapshot_info);
        violation.has_value()) {
      throw RecoveryFailure("Type constraint recovery failed because they couldn't be validated!");
    }
  }

  spdlog::info("Type constraints are recreated from metadata.");
}

void RecoverIndicesStatsAndConstraints(utils::SkipList<Vertex> *vertices, NameIdMapper *name_id_mapper,
                                       Indices *indices, Constraints *constraints, Config const &config,
                                       RecoveryInfo const &recovery_info,
                                       RecoveredIndicesAndConstraints const &indices_constraints,
                                       bool properties_on_edges,
                                       std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto storage_dir = std::optional<std::filesystem::path>{};
  if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    storage_dir = config.durability.storage_directory;
  }

  RecoverIndicesAndStats(indices_constraints.indices, indices, vertices, name_id_mapper, properties_on_edges,
                         GetParallelExecInfo(recovery_info, config), storage_dir, snapshot_info);
  RecoverConstraints(indices_constraints.constraints, constraints, vertices, name_id_mapper,
                     GetParallelExecInfo(recovery_info, config), snapshot_info);
}

std::optional<ParallelizedSchemaCreationInfo> GetParallelExecInfo(const RecoveryInfo &recovery_info,
                                                                  const Config &config) {
  return (config.durability.allow_parallel_schema_creation && recovery_info.vertex_batches.size() > 1)
             ? std::make_optional(ParallelizedSchemaCreationInfo{recovery_info.vertex_batches,
                                                                 config.durability.recovery_thread_count})
             : std::nullopt;
}

std::optional<RecoveryInfo> Recovery::RecoverData(
    utils::UUID &uuid, ReplicationStorageState &repl_storage_state, utils::SkipList<Vertex> *vertices,
    utils::SkipList<Edge> *edges, utils::SkipList<EdgeMetadata> *edges_metadata, std::atomic<uint64_t> *edge_count,
    NameIdMapper *name_id_mapper, Indices *indices, Constraints *constraints, Config const &config,
    uint64_t *wal_seq_num, EnumStore *enum_store, SharedSchemaTracking *schema_info,
    std::function<std::optional<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>>(Gid)> find_edge,
    std::string const &db_name) {
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
    uuid.set(snapshot_files.back().uuid);
    auto const last_snapshot_uuid_str = std::string{uuid};

    spdlog::trace("UUID of the last snapshot file: {}");
    std::optional<RecoveredSnapshot> recovered_snapshot;

    for (auto it = snapshot_files.rbegin(); it != snapshot_files.rend(); ++it) {
      const auto &[path, file_uuid, _] = *it;
      if (file_uuid != last_snapshot_uuid_str) {
        spdlog::warn("The snapshot file {} isn't related to the latest snapshot file!", path);
        continue;
      }
      spdlog::info("Starting snapshot recovery from {}.", path);
      try {
        recovered_snapshot = LoadSnapshot(path, vertices, edges, edges_metadata, epoch_history, name_id_mapper,
                                          edge_count, config, enum_store, schema_info);
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
    snapshot_timestamp = recovered_snapshot->snapshot_info.durable_timestamp;
    spdlog::trace("Recovered epoch {} for db {}", recovered_snapshot->snapshot_info.epoch_id, db_name);
    repl_storage_state.epoch_.SetEpoch(std::move(recovered_snapshot->snapshot_info.epoch_id));
    recovery_info.last_durable_timestamp = snapshot_timestamp;
  } else {
    // UUID couldn't be recovered from the snapshot; recovering it from WALs
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
      if (!item.is_regular_file()) {
        spdlog::trace("Non-regular WAL file {} found in the wal directory. Skipping it.", item.path());
        continue;
      }
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

    // sort by path
    std::sort(wal_files.begin(), wal_files.end());

    // UUID used for durability is the UUID of the last WAL file.
    // Same for the epoch id.
    uuid.set(wal_files.back().uuid);
    repl_storage_state.epoch_.SetEpoch(std::move(wal_files.back().epoch_id));
    spdlog::trace("UUID of the last WAL file: {}. Epoch id from the last WAL file: {}.", std::string{uuid},
                  repl_storage_state.epoch_.id());
  }

  if (const auto maybe_wal_files = GetWalFiles(wal_directory_, std::string{uuid});
      maybe_wal_files && !maybe_wal_files->empty()) {
    // Array of all discovered WAL files, ordered by sequence number.
    const auto &wal_files = *maybe_wal_files;

    spdlog::info("Checking WAL files.");
    r::for_each(wal_files,
                [](auto &&wal_file) { spdlog::trace("Wal file: {}. Seq num: {}.", wal_file.path, wal_file.seq_num); });
    {
      const auto &first_wal = wal_files[0];
      spdlog::trace("Checking 1st wal file: {}.", first_wal.path);
      if (first_wal.seq_num != 0) {
        spdlog::trace("1st wal file {} has sequence number {} which is != 0.", first_wal.path, first_wal.seq_num);
        // We don't have all WAL files. We need to see whether we need them all.
        if (!snapshot_timestamp) {
          // We didn't recover from a snapshot and we must have all WAL files
          // starting from the first one (seq_num == 0) to be able to recover
          // data from them.
          LOG_FATAL(
              "There are missing prefix WAL files and data can't be "
              "recovered without them!");
        } else if (first_wal.from_timestamp > *snapshot_timestamp) {
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

    for (const auto &wal_file : wal_files) {
      if (previous_seq_num && (wal_file.seq_num - *previous_seq_num) > 1) {
        LOG_FATAL("You are missing a WAL file with the sequence number {}!", *previous_seq_num + 1);
      }
      previous_seq_num = wal_file.seq_num;

      try {
        auto info = LoadWal(wal_file.path, &indices_constraints, last_loaded_timestamp, vertices, edges, name_id_mapper,
                            edge_count, config.salient.items, enum_store, schema_info, find_edge);
        // Update recovery info data only if WAL file was used and its deltas loaded
        if (info.has_value()) {
          recovery_info.next_vertex_id = std::max(recovery_info.next_vertex_id, info->next_vertex_id);
          recovery_info.next_edge_id = std::max(recovery_info.next_edge_id, info->next_edge_id);
          recovery_info.next_timestamp = std::max(recovery_info.next_timestamp, info->next_timestamp);
          recovery_info.last_durable_timestamp = info->last_durable_timestamp;
          if (info->last_durable_timestamp) {
            last_loaded_timestamp.emplace(info->last_durable_timestamp.value_or(0));
          }
        }

        if (epoch_history->empty() || epoch_history->back().first != wal_file.epoch_id) {
          // no history or new epoch, add it
          epoch_history->emplace_back(wal_file.epoch_id, *last_loaded_timestamp);
          repl_storage_state.epoch_.SetEpoch(wal_file.epoch_id);
          spdlog::trace("Set epoch to {} for db {}", wal_file.epoch_id, db_name);
        } else if (epoch_history->back().second < *last_loaded_timestamp) {
          // existing epoch, update with newer timestamp
          epoch_history->back().second = *last_loaded_timestamp;
        }

      } catch (const RecoveryFailure &e) {
        LOG_FATAL("Couldn't recover WAL deltas from {} because of: {}", wal_file.path, e.what());
      }
    }
    // The sequence number needs to be recovered even though `LoadWal` didn't
    // load any deltas from that file.
    *wal_seq_num = *previous_seq_num + 1;

    spdlog::info("All necessary WAL files are loaded successfully.");

    // Regenerate the vertex batches
    // TODO edges?
    size_t pos = 0;
    size_t batched = 0;
    recovery_info.vertex_batches.clear();
    auto v_acc = vertices->access();
    const auto size = v_acc.size();
    for (auto v_itr = v_acc.begin(); v_itr != v_acc.end(); ++v_itr, ++pos) {
      if (pos == batched) {
        const auto left = size - pos;
        if (left <= config.durability.items_per_batch) {
          recovery_info.vertex_batches.emplace_back(v_itr->gid, left);
          break;
        }
        recovery_info.vertex_batches.emplace_back(v_itr->gid, config.durability.items_per_batch);
        batched += config.durability.items_per_batch;
      }
    }
  }

  // Apply meta structures now after all graph data has been loaded
  RecoverIndicesStatsAndConstraints(vertices, name_id_mapper, indices, constraints, config, recovery_info,
                                    indices_constraints, config.salient.items.properties_on_edges);

  memgraph::metrics::Measure(memgraph::metrics::SnapshotRecoveryLatency_us,
                             std::chrono::duration_cast<std::chrono::microseconds>(timer.Elapsed()).count());
  spdlog::trace("Epoch id: {}. Last durable commit timestamp: {}.", std::string(repl_storage_state.epoch_.id()),
                repl_storage_state.last_durable_timestamp_);

  spdlog::trace("History with its epochs and attached commit timestamps.");
  r::for_each(repl_storage_state.history, [](auto &&history) {
    spdlog::trace("Epoch id: {}. Commit timestamp: {}.", std::string(history.first), history.second);
  });
  return recovery_info;
}

}  // namespace memgraph::storage::durability
