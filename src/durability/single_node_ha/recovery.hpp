#pragma once

#include <experimental/filesystem>
#include <experimental/optional>
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
  tx::TransactionId snapshooter_tx_id{0};
  std::vector<tx::TransactionId> snapshooter_tx_snapshot;
  // A collection into which the indexes should be added so they
  // can be rebuilt at the end of the recovery transaction.
  std::vector<IndexRecoveryData> indexes;

  void Clear() {
    snapshooter_tx_id = 0;
    snapshooter_tx_snapshot.clear();
    indexes.clear();
  }
};

/// Reads snapshot metadata from the end of the file without messing up the
/// hash.
bool ReadSnapshotSummary(HashedFileReader &buffer, int64_t &vertex_count,
                         int64_t &edge_count, uint64_t &hash);

/**
 * Recovers database from the latest possible snapshot. If recovering fails,
 * false is returned and db_accessor aborts transaction, else true is returned
 * and transaction is commited.
 *
 * @param durability_dir - Path to durability directory.
 * @param db - The database to recover into.
 * @return - recovery info
 */
bool RecoverOnlySnapshot(
    const std::experimental::filesystem::path &durability_dir,
    database::GraphDb *db, durability::RecoveryData *recovery_data);

void RecoverIndexes(database::GraphDb *db,
                    const std::vector<IndexRecoveryData> &indexes);

}  // namespace durability
