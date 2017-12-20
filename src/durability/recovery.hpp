#pragma once

#include <experimental/filesystem>
#include <unordered_map>

#include "database/graph_db.hpp"
#include "durability/hashed_file_reader.hpp"
#include "storage/vertex_accessor.hpp"

namespace fs = std::experimental::filesystem;

namespace durability {

/** Reads snapshot metadata from the end of the file without messing up the
 * hash. */
bool ReadSnapshotSummary(HashedFileReader &buffer, int64_t &vertex_count,
                         int64_t &edge_count, uint64_t &hash);

/**
 * Recovers database from durability. If recovering fails, false is returned
 * and db_accessor aborts transaction, else true is returned and transaction is
 * commited.
 *
 * @param durability_dir - Path to durability directory.
 * @param db - The database to recover into.
 * @return - If recovery was succesful.
 */
bool Recover(const std::experimental::filesystem::path &durability_dir,
             GraphDb &db);
}  // namespace durability
