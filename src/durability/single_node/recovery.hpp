#pragma once

#include <experimental/filesystem>
#include <experimental/optional>
#include <unordered_map>
#include <vector>

#include "durability/hashed_file_reader.hpp"
#include "durability/single_node/state_delta.hpp"
#include "transactions/type.hpp"

namespace database {
class GraphDb;
};

namespace durability {

/// Stores info on what was (or needs to be) recovered from durability.
struct RecoveryInfo {
  RecoveryInfo() {}
  RecoveryInfo(const int64_t durability_version,
               tx::TransactionId snapshot_tx_id,
               const std::vector<tx::TransactionId> &wal_recovered)
      : durability_version(durability_version),
        snapshot_tx_id(snapshot_tx_id),
        wal_recovered(wal_recovered) {}
  int64_t durability_version;
  tx::TransactionId snapshot_tx_id;
  std::vector<tx::TransactionId> wal_recovered;

  bool operator==(const RecoveryInfo &other) const {
    return durability_version == other.durability_version &&
           snapshot_tx_id == other.snapshot_tx_id &&
           wal_recovered == other.wal_recovered;
  }
  bool operator!=(const RecoveryInfo &other) const { return !(*this == other); }
};

struct IndexRecoveryData {
  std::string label;
  std::string property;
  bool unique;
};

// A data structure for exchanging info between main recovery function and
// snapshot and WAL recovery functions.
struct RecoveryData {
  tx::TransactionId snapshooter_tx_id{0};
  std::vector<tx::TransactionId> wal_tx_to_recover{};
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

/** Reads snapshot metadata from the end of the file without messing up the
 * hash. */
bool ReadSnapshotSummary(HashedFileReader &buffer, int64_t &vertex_count,
                         int64_t &edge_count, uint64_t &hash);

/**
 * Checks version consistency within the durability directory.
 *
 * @param durability_dir - Path to durability directory.
 * @return - True if snapshot and WAL versions are compatible with
 *  `        current memgraph binary.
 */
bool VersionConsistency(
    const std::experimental::filesystem::path &durability_dir);

/**
 * Checks whether the current memgraph binary (on a worker) is
 * version consistent with the cluster master.
 *
 * @param master_version - Version of the master.
 * @return - True if versions match.
 */
bool DistributedVersionConsistency(const int64_t master_version);

/**
 * Checks whether the durability directory contains snapshot
 * or write-ahead log file.
 *
 * @param durability_dir - Path to durability directory.
 * @return - True if durability directory contains either a snapshot
 *           or WAL file.
 */
bool ContainsDurabilityFiles(
    const std::experimental::filesystem::path &durabilty_dir);

/**
 * Backup snapshots and WAL files to a backup folder.
 *
 * @param durability_dir - Path to durability directory.
 */
void MoveToBackup(const std::experimental::filesystem::path &durability_dir);

/**
 * Recovers database from the latest possible snapshot. If recovering fails,
 * false is returned and db_accessor aborts transaction, else true is returned
 * and transaction is commited.
 *
 * @param durability_dir - Path to durability directory.
 * @param db - The database to recover into.
 * @param required_snapshot_tx_id - Only used on distributed worker. Indicates
 * what the master recovered. The same snapshot must be recovered on the
 * worker.
 * @return - recovery info
 */
RecoveryInfo RecoverOnlySnapshot(
    const std::experimental::filesystem::path &durability_dir,
    database::GraphDb *db, durability::RecoveryData *recovery_data,
    std::experimental::optional<tx::TransactionId> required_snapshot_tx_id);

/** Interface for accessing transactions during WAL recovery. */
class RecoveryTransactions {
 public:
  explicit RecoveryTransactions(database::GraphDb *db);

  void Begin(const tx::TransactionId &tx_id);

  void Abort(const tx::TransactionId &tx_id);

  void Commit(const tx::TransactionId &tx_id);

  void Apply(const database::StateDelta &delta);

 private:
  database::GraphDbAccessor *GetAccessor(const tx::TransactionId &tx_id);

  database::GraphDb *db_;
  std::unordered_map<tx::TransactionId,
                     std::unique_ptr<database::GraphDbAccessor>>
      accessors_;
};

void RecoverWal(const std::experimental::filesystem::path &durability_dir,
                database::GraphDb *db, RecoveryData *recovery_data,
                RecoveryTransactions *transactions);

void RecoverIndexes(database::GraphDb *db,
                    const std::vector<IndexRecoveryData> &indexes);

}  // namespace durability
