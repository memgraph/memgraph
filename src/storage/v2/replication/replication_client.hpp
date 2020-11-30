#pragma once

#include <atomic>
#include <thread>
#include <variant>

#include "rpc/client.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "storage/v2/storage.hpp"
#include "utils/file.hpp"
#include "utils/file_locker.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/thread_pool.hpp"

namespace storage {

class Storage::ReplicationClient {
 public:
  ReplicationClient(std::string name, Storage *storage,
                    const io::network::Endpoint &endpoint, bool use_ssl,
                    ReplicationMode mode);

  // Handler used for transfering the current transaction.
  class ReplicaStream {
   private:
    friend class ReplicationClient;
    explicit ReplicaStream(ReplicationClient *self,
                           uint64_t previous_commit_timestamp,
                           uint64_t current_seq_num);

   public:
    /// @throw rpc::RpcFailedException
    void AppendDelta(const Delta &delta, const Vertex &vertex,
                     uint64_t final_commit_timestamp);

    /// @throw rpc::RpcFailedException
    void AppendDelta(const Delta &delta, const Edge &edge,
                     uint64_t final_commit_timestamp);

    /// @throw rpc::RpcFailedException
    void AppendTransactionEnd(uint64_t final_commit_timestamp);

    /// @throw rpc::RpcFailedException
    void AppendOperation(durability::StorageGlobalOperation operation,
                         LabelId label, const std::set<PropertyId> &properties,
                         uint64_t timestamp);

   private:
    /// @throw rpc::RpcFailedException
    AppendDeltasRes Finalize();

    ReplicationClient *self_;
    rpc::Client::StreamHandler<AppendDeltasRpc> stream_;
  };

  // Handler for transfering the current WAL file whose data is
  // contained in the internal buffer and the file.
  class CurrentWalHandler {
   private:
    friend class ReplicationClient;
    explicit CurrentWalHandler(ReplicationClient *self);

   public:
    void AppendFilename(const std::string &filename);

    void AppendSize(size_t size);

    void AppendFileData(utils::InputFile *file);

    void AppendBufferData(const uint8_t *buffer, size_t buffer_size);

    /// @throw rpc::RpcFailedException
    CurrentWalRes Finalize();

   private:
    ReplicationClient *self_;
    rpc::Client::StreamHandler<CurrentWalRpc> stream_;
  };

  bool StartTransactionReplication(uint64_t current_wal_seq_num);

  // Replication clients can be removed at any point
  // so to avoid any complexity of checking if the client was removed whenever
  // we want to send part of transaction and to avoid adding some GC logic this
  // function will run a callback if, after previously callling
  // StartTransactionReplication, stream is created.
  void IfStreamingTransaction(
      const std::function<void(ReplicaStream &handler)> &callback);

  void FinalizeTransactionReplication();

  // Transfer the snapshot file.
  // @param path Path of the snapshot file.
  SnapshotRes TransferSnapshot(const std::filesystem::path &path);

  // Transfer the timestamp of the snapshot if it's the only difference
  // between main and replica
  OnlySnapshotRes TransferOnlySnapshot(uint64_t snapshot_timestamp);

  CurrentWalHandler TransferCurrentWalFile() { return CurrentWalHandler{this}; }

  // Transfer the WAL files
  WalFilesRes TransferWalFiles(
      const std::vector<std::filesystem::path> &wal_files);

  const auto &Name() const { return name_; }

  auto State() const { return replica_state_.load(); }

 private:
  void FinalizeTransactionReplicationInternal();

  void RecoverReplica(uint64_t replica_commit);

  uint64_t ReplicateCurrentWal();

  using RecoveryWals = std::vector<std::filesystem::path>;
  struct RecoveryCurrentWal {
    uint64_t current_wal_seq_num;

    explicit RecoveryCurrentWal(const uint64_t current_wal_seq_num)
        : current_wal_seq_num(current_wal_seq_num) {}
  };
  using RecoverySnapshot = std::filesystem::path;
  struct RecoveryFinalSnapshot {
    uint64_t snapshot_timestamp;

    explicit RecoveryFinalSnapshot(const uint64_t snapshot_timestamp)
        : snapshot_timestamp(snapshot_timestamp) {}
  };
  using RecoveryStep = std::variant<RecoverySnapshot, RecoveryWals,
                                    RecoveryCurrentWal, RecoveryFinalSnapshot>;

  std::vector<RecoveryStep> GetRecoverySteps(
      uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker);

  void InitializeClient();

  std::string name_;

  Storage *storage_;

  communication::ClientContext rpc_context_;
  rpc::Client rpc_client_;

  std::optional<ReplicaStream> replica_stream_;
  ReplicationMode mode_{ReplicationMode::SYNC};

  utils::SpinLock client_lock_;
  utils::ThreadPool thread_pool_{1};
  std::atomic<ReplicaState> replica_state_{ReplicaState::INVALID};
};

}  // namespace storage
