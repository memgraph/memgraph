#include "storage/v2/durability/snapshot.hpp"

#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"

namespace storage::durability {

// Snapshot format:
//
// 1) Magic string (non-encoded)
//
// 2) Snapshot version (non-encoded, little-endian)
//
// 3) Section offsets:
//     * offset to the first edge in the snapshot (`0` if properties on edges
//       are disabled)
//     * offset to the first vertex in the snapshot
//     * offset to the indices section
//     * offset to the constraints section
//     * offset to the mapper section
//     * offset to the metadata section
//
// 4) Encoded edges (if properties on edges are enabled); each edge is written
//    in the following format:
//     * gid
//     * properties
//
// 5) Encoded vertices; each vertex is written in the following format:
//     * gid
//     * labels
//     * properties
//     * in edges
//         * edge gid
//         * from vertex gid
//         * edge type
//     * out edges
//         * edge gid
//         * to vertex gid
//         * edge type
//
// 6) Indices
//     * label indices
//         * label
//     * label+property indices
//         * label
//         * property
//
// 7) Constraints
//     * existence constraints
//         * label
//         * property
//     * unique constraints (from version 13)
//         * label
//         * properties
//
// 8) Name to ID mapper data
//     * id to name mappings
//         * id
//         * name
//
// 9) Metadata
//     * storage UUID
//     * snapshot transaction start timestamp (required when recovering
//       from snapshot combined with WAL to determine what deltas need to be
//       applied)
//     * number of edges
//     * number of vertices
//
// IMPORTANT: When changing snapshot encoding/decoding bump the snapshot/WAL
// version in `version.hpp`.

// Function used to read information about the snapshot file.
SnapshotInfo ReadSnapshotInfo(const std::filesystem::path &path) {
  // Check magic and version.
  Decoder snapshot;
  auto version = snapshot.Initialize(path, kSnapshotMagic);
  if (!version)
    throw RecoveryFailure("Couldn't read snapshot magic and/or version!");
  if (!IsVersionSupported(*version))
    throw RecoveryFailure("Invalid snapshot version!");

  // Prepare return value.
  SnapshotInfo info;

  // Read offsets.
  {
    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_OFFSETS)
      throw RecoveryFailure("Invalid snapshot data!");

    auto snapshot_size = snapshot.GetSize();
    if (!snapshot_size)
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto read_offset = [&snapshot, snapshot_size] {
      auto maybe_offset = snapshot.ReadUint();
      if (!maybe_offset) throw RecoveryFailure("Invalid snapshot format!");
      auto offset = *maybe_offset;
      if (offset > *snapshot_size)
        throw RecoveryFailure("Invalid snapshot format!");
      return offset;
    };

    info.offset_edges = read_offset();
    info.offset_vertices = read_offset();
    info.offset_indices = read_offset();
    info.offset_constraints = read_offset();
    info.offset_mapper = read_offset();
    info.offset_metadata = read_offset();
  }

  // Read metadata.
  {
    if (!snapshot.SetPosition(info.offset_metadata))
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_METADATA)
      throw RecoveryFailure("Invalid snapshot data!");

    auto maybe_uuid = snapshot.ReadString();
    if (!maybe_uuid) throw RecoveryFailure("Invalid snapshot data!");
    info.uuid = std::move(*maybe_uuid);

    auto maybe_timestamp = snapshot.ReadUint();
    if (!maybe_timestamp) throw RecoveryFailure("Invalid snapshot data!");
    info.start_timestamp = *maybe_timestamp;

    auto maybe_edges = snapshot.ReadUint();
    if (!maybe_edges) throw RecoveryFailure("Invalid snapshot data!");
    info.edges_count = *maybe_edges;

    auto maybe_vertices = snapshot.ReadUint();
    if (!maybe_vertices) throw RecoveryFailure("Invalid snapshot data!");
    info.vertices_count = *maybe_vertices;
  }

  return info;
}

}  // namespace storage::durability
