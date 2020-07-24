#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include "storage/v2/config.hpp"
#include "storage/v2/constraints.hpp"
#include "storage/v2/durability/metadata.hpp"
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
