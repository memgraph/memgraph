#pragma once

#include <experimental/optional>
#include <unordered_map>

#include "database/graph_db.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/recovery.capnp.h"
#include "storage/vertex_accessor.hpp"
#include "transactions/type.hpp"

namespace durability {

/// Stores info on what was (or needs to be) recovered from durability.
struct RecoveryInfo {
  RecoveryInfo() {}
  RecoveryInfo(tx::TransactionId snapshot_tx_id,
               tx::TransactionId max_wal_tx_id)
      : snapshot_tx_id(snapshot_tx_id), max_wal_tx_id(max_wal_tx_id) {}
  tx::TransactionId snapshot_tx_id;
  tx::TransactionId max_wal_tx_id;

  bool operator==(const RecoveryInfo &other) const {
    return snapshot_tx_id == other.snapshot_tx_id &&
           max_wal_tx_id == other.max_wal_tx_id;
  }
  bool operator!=(const RecoveryInfo &other) const { return !(*this == other); }

  void Save(capnp::RecoveryInfo::Builder *builder) const {
    builder->setSnapshotTxId(snapshot_tx_id);
    builder->setMaxWalTxId(max_wal_tx_id);
  }

  void Load(const capnp::RecoveryInfo::Reader &reader) {
    snapshot_tx_id = reader.getSnapshotTxId();
    max_wal_tx_id = reader.getMaxWalTxId();
  }

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &snapshot_tx_id;
    ar &max_wal_tx_id;
  }
};

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
 * @param required_recovery_info - Only used on distributed worker. Indicates
 * what the master recovered. The same transactions must be recovered on the
 * worker.
 * @return - recovery info
 */
RecoveryInfo Recover(
    const std::experimental::filesystem::path &durability_dir,
    database::GraphDb &db,
    std::experimental::optional<RecoveryInfo> required_recovery_info);

}  // namespace durability
