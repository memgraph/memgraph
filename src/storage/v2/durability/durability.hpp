#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <optional>
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

/// Get list of snapshot files with their UUID
std::vector<std::pair<std::filesystem::path, std::string>> GetSnapshotFiles(
    const std::filesystem::path &snapshot_directory);

template <bool order_by_sequence = false>
auto GetWalFiles(const std::filesystem::path &wal_directory,
                 const std::string *uuid = nullptr) {
  using ReturnType = std::conditional_t<
      order_by_sequence,
      std::vector<
          std::tuple<uint64_t, uint64_t, uint64_t, std::filesystem::path>>,
      std::vector<std::pair<std::filesystem::path, std::string>>>;
  if (!utils::DirExists(wal_directory)) return std::optional<ReturnType>{};

  ReturnType wal_files;
  std::error_code error_code;
  for (const auto &item :
       std::filesystem::directory_iterator(wal_directory, error_code)) {
    if (!item.is_regular_file()) continue;
    try {
      auto info = ReadWalInfo(item.path());
      if (uuid && info.uuid != *uuid) continue;
      if constexpr (order_by_sequence) {
        wal_files.emplace_back(info.seq_num, info.from_timestamp,
                               info.to_timestamp, item.path());
      } else {
        wal_files.emplace_back(item.path(), info.uuid);
      }
    } catch (const RecoveryFailure &e) {
      continue;
    }
  }
  CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                     << error_code.message() << "!";
  std::sort(wal_files.begin(), wal_files.end());
  return std::optional<ReturnType>{std::move(wal_files)};
}

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
