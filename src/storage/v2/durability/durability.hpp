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

#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <variant>

#include "replication/epoch.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/replication/replication_storage_state.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage::durability {

/// Verifies that the owner of the storage directory is the same user that
/// started the current process. If the verification fails, the process is
/// killed (`CHECK` failure).
void VerifyStorageDirectoryOwnerAndProcessUserOrDie(const std::filesystem::path &storage_directory);

// Used to capture the snapshot's data related to durability
struct SnapshotDurabilityInfo {
  explicit SnapshotDurabilityInfo(std::filesystem::path path, std::string uuid, const uint64_t start_timestamp)
      : path(std::move(path)), uuid(std::move(uuid)), start_timestamp(start_timestamp) {}

  std::filesystem::path path;
  std::string uuid;
  uint64_t start_timestamp;

  auto operator<=>(const SnapshotDurabilityInfo &) const = default;
};

/// Get list of snapshot files with their UUID.
/// @param snapshot_directory Directory containing the Snapshot files.
/// @param uuid UUID of the Snapshot files. If not empty, fetch only Snapshot
/// file with the specified UUID. Otherwise, fetch only Snapshot files in the
/// snapshot_directory.
/// @return List of snapshot files defined with its path and UUID.
std::vector<SnapshotDurabilityInfo> GetSnapshotFiles(const std::filesystem::path &snapshot_directory,
                                                     std::string_view uuid = "");

/// Used to capture a WAL's data related to durability
struct WalDurabilityInfo {
  explicit WalDurabilityInfo(const uint64_t seq_num, const uint64_t from_timestamp, const uint64_t to_timestamp,
                             std::string uuid, std::string epoch_id, std::filesystem::path path)
      : seq_num(seq_num),
        from_timestamp(from_timestamp),
        to_timestamp(to_timestamp),
        uuid(std::move(uuid)),
        epoch_id(std::move(epoch_id)),
        path(std::move(path)) {}

  uint64_t seq_num;
  uint64_t from_timestamp;
  uint64_t to_timestamp;
  std::string uuid;
  std::string epoch_id;
  std::filesystem::path path;

  auto operator<=>(const WalDurabilityInfo &) const = default;
};

/// Get list of WAL files ordered by the sequence number
/// @param wal_directory Directory containing the WAL files.
/// @param uuid UUID of the WAL files. If not empty, fetch only WAL files
/// with the specified UUID. Otherwise, fetch all WAL files in the
/// wal_directory.
/// @param current_seq_num Sequence number of the WAL file which is currently
/// being written. If specified, load only finalized WAL files, i.e. WAL files
/// with seq_num < current_seq_num.
/// @return List of WAL files. Each WAL file is defined with its sequence
/// number, from timestamp, to timestamp and path.
std::optional<std::vector<WalDurabilityInfo>> GetWalFiles(const std::filesystem::path &wal_directory,
                                                          std::string_view uuid = "",
                                                          std::optional<size_t> current_seq_num = {});

// Helper function used to recover all discovered indices. The
// indices must be recovered after the data recovery is done
// to ensure that the indices consistent at the end of the
// recovery process.
/// @throw RecoveryFailure
void RecoverIndicesAndStats(const RecoveredIndicesAndConstraints::IndicesMetadata &indices_metadata, Indices *indices,
                            utils::SkipList<Vertex> *vertices, NameIdMapper *name_id_mapper,
                            const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info = std::nullopt,
                            const std::optional<std::filesystem::path> &storage_dir = std::nullopt);

// Helper function used to recover all discovered constraints. The
// constraints must be recovered after the data recovery is done
// to ensure that the constraints are consistent at the end of the
// recovery process.
/// @throw RecoveryFailure
void RecoverConstraints(const RecoveredIndicesAndConstraints::ConstraintsMetadata &constraints_metadata,
                        Constraints *constraints, utils::SkipList<Vertex> *vertices, NameIdMapper *name_id_mapper,
                        const std::optional<ParallelizedSchemaCreationInfo> &parallel_exec_info = std::nullopt);

std::optional<ParallelizedSchemaCreationInfo> GetParallelExecInfo(const RecoveryInfo &recovery_info,
                                                                  const Config &config);

std::optional<ParallelizedSchemaCreationInfo> GetParallelExecInfoIndices(const RecoveryInfo &recovery_info,
                                                                         const Config &config);

void RecoverExistenceConstraints(const RecoveredIndicesAndConstraints::ConstraintsMetadata &, Constraints *,
                                 utils::SkipList<Vertex> *, NameIdMapper *,
                                 const std::optional<ParallelizedSchemaCreationInfo> &);

void RecoverUniqueConstraints(const RecoveredIndicesAndConstraints::ConstraintsMetadata &, Constraints *,
                              utils::SkipList<Vertex> *, NameIdMapper *,
                              const std::optional<ParallelizedSchemaCreationInfo> &);
struct Recovery {
 public:
  /// Recovers data either from a snapshot and/or WAL files.
  /// @throw RecoveryFailure
  /// @throw std::bad_alloc
  std::optional<RecoveryInfo> RecoverData(std::string *uuid, ReplicationStorageState &repl_storage_state,
                                          utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                          std::atomic<uint64_t> *edge_count, NameIdMapper *name_id_mapper,
                                          Indices *indices, Constraints *constraints, const Config &config,
                                          uint64_t *wal_seq_num);

  const std::filesystem::path snapshot_directory_;
  const std::filesystem::path wal_directory_;
};

}  // namespace memgraph::storage::durability
