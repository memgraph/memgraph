#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <variant>
#include <string>

#include "storage/v2/config.hpp"
#include "storage/v2/constraints.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"

namespace storage::durability {

/// Verifies that the owner of the storage directory is the same user that
/// started the current process. If the verification fails, the process is
/// killed (`CHECK` failure).
void VerifyStorageDirectoryOwnerAndProcessUserOrDie(
    const std::filesystem::path &storage_directory);

// Used to capture the snapshot's data related to durability 
struct SnapshotDurabilityInfo {
  SnapshotDurabilityInfo() = default;
  SnapshotDurabilityInfo(const std::filesystem::path _path,
                         const std::string _uuid,
                         const uint64_t _start_timestamp)
    : path(_path),
      uuid(_uuid),
      start_timestamp(_start_timestamp) {}

  SnapshotDurabilityInfo(const SnapshotDurabilityInfo&) = delete;
  SnapshotDurabilityInfo& operator=(const SnapshotDurabilityInfo&) = delete;

  SnapshotDurabilityInfo(SnapshotDurabilityInfo&&) = default;
  SnapshotDurabilityInfo& operator=(SnapshotDurabilityInfo&&) = default;

  ~SnapshotDurabilityInfo() = default;

  std::filesystem::path path;
  std::string uuid; 
  uint64_t start_timestamp;
};

/// Get list of snapshot files with their UUID.
/// @param snapshot_directory Directory containing the Snapshot files.
/// @param uuid UUID of the Snapshot files. If not empty, fetch only Snapshot
/// file with the specified UUID. Otherwise, fetch only Snapshot files in the
/// snapshot_directory.
/// @return List of snapshot files defined with its path and UUID.
std::vector<SnapshotDurabilityInfo> GetSnapshotFiles(
    const std::filesystem::path &snapshot_directory,
    std::string_view uuid = "");

/// Used to capture a WAL's data related to durability
struct WalDurabilityInfo {
  WalDurabilityInfo() = default;
  WalDurabilityInfo(const uint64_t _seq_num,
                    const uint64_t _from_timestamp,
                    const uint64_t _to_timestamp,
                    const std::string &_uuid,
                    const std::filesystem::path &_path)
    : seq_num(_seq_num), 
      from_timestamp(_from_timestamp),
      to_timestamp(_to_timestamp),
      uuid(_uuid),
      path(_path) {}

  WalDurabilityInfo(const WalDurabilityInfo&) = delete;
  WalDurabilityInfo& operator=(const WalDurabilityInfo&) = delete;

  WalDurabilityInfo(WalDurabilityInfo&&) = default;
  WalDurabilityInfo& operator=(WalDurabilityInfo&&) = default;

  ~WalDurabilityInfo() = default;

  uint64_t seq_num;
  uint64_t from_timestamp;
  uint64_t to_timestamp;
  std::string uuid;
  std::filesystem::path path;
};

using RecoveryFileDurabilityInfo = std::variant<SnapshotDurabilityInfo,
                                                WalDurabilityInfo>;

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
std::optional<std::vector<WalDurabilityInfo>>
GetWalFiles(const std::filesystem::path &wal_directory,
            std::string_view uuid = "",
            std::optional<size_t> current_seq_num = {});

// Helper function used to recover all discovered indices and constraints. The
// indices and constraints must be recovered after the data recovery is done
// to ensure that the indices and constraints are consistent at the end of the
// recovery process.
/// @throw RecoveryFailure
void RecoverIndicesAndConstraints(
    const RecoveredIndicesAndConstraints &indices_constraints, Indices *indices,
    Constraints *constraints, utils::SkipList<Vertex> *vertices);

/// Recovers data either from a snapshot and/or WAL files.
/// @throw RecoveryFailure
/// @throw std::bad_alloc
std::optional<RecoveryInfo> RecoverData(
    const std::filesystem::path &snapshot_directory,
    const std::filesystem::path &wal_directory, std::string *uuid,
    utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
    std::atomic<uint64_t> *edge_count, NameIdMapper *name_id_mapper,
    Indices *indices, Constraints *constraints, Config::Items items,
    uint64_t *wal_seq_num);

}  // namespace storage::durability
