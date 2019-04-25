#pragma once

#include <filesystem>
#include <optional>
#include <unordered_map>
#include <vector>

#include "durability/hashed_file_reader.hpp"
#include "durability/single_node_ha/state_delta.hpp"
#include "transactions/type.hpp"

namespace database {
class GraphDb;
};

namespace durability {

struct IndexRecoveryData {
  std::string label;
  std::string property;
  bool create;  // distinguish between creating and dropping index
  bool unique;  // used only when creating an index
};

/// Data structure for exchanging info between main recovery function and
/// snapshot recovery functions.
struct RecoveryData {
  // A collection into which the indexes should be added so they
  // can be rebuilt at the end of the recovery transaction.
  std::vector<IndexRecoveryData> indexes;
};

/// Reads snapshot metadata from the end of the file without messing up the
/// hash.
bool ReadSnapshotSummary(HashedFileReader &buffer, int64_t &vertex_count,
                         int64_t &edge_count, uint64_t &hash);

/**
 * Recovers database from the given snapshot. If recovering fails, false is
 * returned and db_accessor aborts transaction, else true is returned and
 * transaction is commited.
 *
 * @param db - The database to recover into.
 * @param recovery_data - Struct that will contain additional recovery data.
 * @param durability_dir - Path to durability directory.
 * @param snapshot_filename - Snapshot filename.
 * @return - recovery info
 */
bool RecoverSnapshot(database::GraphDb *db,
                     durability::RecoveryData *recovery_data,
                     const std::filesystem::path &durability_dir,
                     const std::string &snapshot_filename);

void RecoverIndexes(database::GraphDb *db,
                    const std::vector<IndexRecoveryData> &indexes);

}  // namespace durability
