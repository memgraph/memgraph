#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

namespace storage::durability {

/// Structure used to hold information about a snapshot.
struct SnapshotInfo {
  uint64_t offset_edges;
  uint64_t offset_vertices;
  uint64_t offset_indices;
  uint64_t offset_constraints;
  uint64_t offset_mapper;
  uint64_t offset_metadata;

  std::string uuid;
  uint64_t start_timestamp;
  uint64_t edges_count;
  uint64_t vertices_count;
};

/// Function used to read information about the snapshot file.
/// @throw RecoveryFailure
SnapshotInfo ReadSnapshotInfo(const std::filesystem::path &path);

}  // namespace storage::durability
