#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>

#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"

#include "storage/v2/config.hpp"
#include "storage/v2/constraints.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/scheduler.hpp"
#include "utils/skip_list.hpp"

namespace storage::durability {

/// Durability class that is used to provide full durability functionality to
/// the storage.
class Durability final {
 public:
  struct RecoveryInfo {
    uint64_t next_vertex_id{0};
    uint64_t next_edge_id{0};
    uint64_t next_timestamp{0};
  };

  struct RecoveredSnapshot {
    SnapshotInfo snapshot_info;
    RecoveryInfo recovery_info;
    RecoveredIndicesAndConstraints indices_constraints;
  };

  Durability(Config::Durability config, utils::SkipList<Vertex> *vertices,
             utils::SkipList<Edge> *edges, NameIdMapper *name_id_mapper,
             std::atomic<uint64_t> *edge_count, Indices *indices,
             Constraints *constraints, Config::Items items);

  std::optional<RecoveryInfo> Initialize(
      std::function<void(std::function<void(Transaction *)>)>
          execute_with_transaction);

  void Finalize();

  void AppendToWal(const Transaction &transaction,
                   uint64_t final_commit_timestamp);

  void AppendToWal(StorageGlobalOperation operation, LabelId label,
                   const std::set<PropertyId> &properties,
                   uint64_t final_commit_timestamp);

 private:
  void CreateSnapshot(Transaction *transaction);

  std::optional<RecoveryInfo> RecoverData();

  RecoveredSnapshot LoadSnapshot(const std::filesystem::path &path);

  RecoveryInfo LoadWal(const std::filesystem::path &path,
                       RecoveredIndicesAndConstraints *indices_constraints,
                       std::optional<uint64_t> snapshot_timestamp);

  bool InitializeWalFile();

  void FinalizeWalFile();

  Config::Durability config_;

  utils::SkipList<Vertex> *vertices_;
  utils::SkipList<Edge> *edges_;
  NameIdMapper *name_id_mapper_;
  std::atomic<uint64_t> *edge_count_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items items_;

  std::function<void(std::function<void(Transaction *)>)>
      execute_with_transaction_;

  std::filesystem::path storage_directory_;
  std::filesystem::path snapshot_directory_;
  std::filesystem::path wal_directory_;
  std::filesystem::path lock_file_path_;
  utils::OutputFile lock_file_handle_;

  utils::Scheduler snapshot_runner_;

  // UUID used to distinguish snapshots and to link snapshots to WALs
  std::string uuid_;
  // Sequence number used to keep track of the chain of WALs.
  uint64_t wal_seq_num_{0};

  std::optional<WalFile> wal_file_;
  uint64_t wal_unsynced_transactions_{0};
};

}  // namespace storage::durability
