#pragma once

#include <experimental/optional>
#include <unordered_map>

#include "durability/hashed_file_reader.hpp"
#include "durability/recovery.capnp.h"
#include "storage/vertex_accessor.hpp"
#include "transactions/type.hpp"
#include "utils/serialization.hpp"

namespace database {
class GraphDb;
};

namespace durability {

/// Stores info on what was (or needs to be) recovered from durability.
struct RecoveryInfo {
  RecoveryInfo() {}
  RecoveryInfo(tx::TransactionId snapshot_tx_id,
               const std::vector<tx::TransactionId> &wal_recovered)
      : snapshot_tx_id(snapshot_tx_id), wal_recovered(wal_recovered) {}
  tx::TransactionId snapshot_tx_id;
  std::vector<tx::TransactionId> wal_recovered;

  bool operator==(const RecoveryInfo &other) const {
    return snapshot_tx_id == other.snapshot_tx_id &&
           wal_recovered == other.wal_recovered;
  }
  bool operator!=(const RecoveryInfo &other) const { return !(*this == other); }

  void Save(capnp::RecoveryInfo::Builder *builder) const {
    builder->setSnapshotTxId(snapshot_tx_id);
    auto list_builder = builder->initWalRecovered(wal_recovered.size());
    utils::SaveVector(wal_recovered, &list_builder);
  }

  void Load(const capnp::RecoveryInfo::Reader &reader) {
    snapshot_tx_id = reader.getSnapshotTxId();
    auto list_reader = reader.getWalRecovered();
    utils::LoadVector(&wal_recovered, list_reader);
  }
};

// A data structure for exchanging info between main recovery function and
// snapshot and WAL recovery functions.
struct RecoveryData {
  tx::TransactionId snapshooter_tx_id{0};
  std::vector<tx::TransactionId> wal_tx_to_recover{};
  std::vector<tx::TransactionId> snapshooter_tx_snapshot;
  // A collection into which the indexes should be added so they
  // can be rebuilt at the end of the recovery transaction.
  std::vector<std::pair<std::string, std::string>> indexes;

  void Clear() {
    snapshooter_tx_id = 0;
    snapshooter_tx_snapshot.clear();
    indexes.clear();
  }

  void Save(capnp::RecoveryData::Builder *builder) const {
    builder->setSnapshooterTxId(snapshooter_tx_id);
    {
      auto list_builder = builder->initWalTxToRecover(wal_tx_to_recover.size());
      utils::SaveVector(wal_tx_to_recover, &list_builder);
    }
    {
      auto list_builder =
          builder->initSnapshooterTxSnapshot(snapshooter_tx_snapshot.size());
      utils::SaveVector(snapshooter_tx_snapshot, &list_builder);
    }
    {
      auto list_builder = builder->initIndexes(indexes.size());
      utils::SaveVector<utils::capnp::Pair<::capnp::Text, ::capnp::Text>,
                        std::pair<std::string, std::string>>(
          indexes, &list_builder, [](auto *builder, const auto value) {
            builder->setFirst(value.first);
            builder->setSecond(value.second);
          });
    }
  }

  void Load(const capnp::RecoveryData::Reader &reader) {
    snapshooter_tx_id = reader.getSnapshooterTxId();
    {
      auto list_reader = reader.getWalTxToRecover();
      utils::LoadVector(&wal_tx_to_recover, list_reader);
    }
    {
      auto list_reader = reader.getSnapshooterTxSnapshot();
      utils::LoadVector(&snapshooter_tx_snapshot, list_reader);
    }
    {
      auto list_reader = reader.getIndexes();
      utils::LoadVector<utils::capnp::Pair<::capnp::Text, ::capnp::Text>,
                        std::pair<std::string, std::string>>(
          &indexes, list_reader, [](const auto &reader) {
            return std::make_pair(reader.getFirst(), reader.getSecond());
          });
    }
  }
};

/** Reads snapshot metadata from the end of the file without messing up the
 * hash. */
bool ReadSnapshotSummary(HashedFileReader &buffer, int64_t &vertex_count,
                         int64_t &edge_count, uint64_t &hash);

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
    std::experimental::optional<tx::TransactionId> required_snapshot_tx_id,
    int worker_id);

/** Interface for accessing transactions during WAL recovery. */
class RecoveryTransactions {
 public:
  virtual ~RecoveryTransactions() {}

  virtual void Begin(const tx::TransactionId &) = 0;
  virtual void Abort(const tx::TransactionId &) = 0;
  virtual void Commit(const tx::TransactionId &) = 0;
  virtual void Apply(const database::StateDelta &) = 0;
};

void RecoverWalAndIndexes(const std::experimental::filesystem::path &dir,
                          database::GraphDb *db, RecoveryData *recovery_data,
                          RecoveryTransactions *transactions);

}  // namespace durability
