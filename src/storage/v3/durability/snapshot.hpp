// Copyright 2022 Memgraph Ltd.
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

#include <cstdint>
#include <filesystem>
#include <string>

#include "storage/v3/config.hpp"
#include "storage/v3/constraints.hpp"
#include "storage/v3/durability/metadata.hpp"
#include "storage/v3/edge.hpp"
#include "storage/v3/indices.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"
#include "utils/file_locker.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage::v3::durability {

/// Structure used to hold information about a snapshot.
struct SnapshotInfo {
  uint64_t offset_edges;
  uint64_t offset_vertices;
  uint64_t offset_indices;
  uint64_t offset_constraints;
  uint64_t offset_mapper;
  uint64_t offset_epoch_history;
  uint64_t offset_metadata;

  std::string uuid;
  std::string epoch_id;
  uint64_t start_timestamp;
  uint64_t edges_count;
  uint64_t vertices_count;
};

/// Structure used to hold information about the snapshot that has been
/// recovered.
struct RecoveredSnapshot {
  SnapshotInfo snapshot_info;
  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;
};

/// Function used to read information about the snapshot file.
/// @throw RecoveryFailure
SnapshotInfo ReadSnapshotInfo(const std::filesystem::path &path);

/// Function used to load the snapshot data into the storage.
/// @throw RecoveryFailure
RecoveredSnapshot LoadSnapshot(const std::filesystem::path &path, VerticesSkipList *vertices,
                               utils::SkipList<Edge> *edges,
                               std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                               NameIdMapper *name_id_mapper, uint64_t *edge_count, Config::Items items);

/// Function used to create a snapshot using the given transaction.
void CreateSnapshot(Transaction *transaction, const std::filesystem::path &snapshot_directory,
                    const std::filesystem::path &wal_directory, uint64_t snapshot_retention_count,
                    VerticesSkipList *vertices, utils::SkipList<Edge> *edges, NameIdMapper *name_id_mapper,
                    Indices *indices, Constraints *constraints, Config::Items items,
                    const SchemaValidator &schema_validator, const std::string &uuid, std::string_view epoch_id,
                    const std::deque<std::pair<std::string, uint64_t>> &epoch_history,
                    utils::FileRetainer *file_retainer);

}  // namespace memgraph::storage::v3::durability
