#pragma once

#include <atomic>
#include <condition_variable>
#include <thread>

#include "rpc/client.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "utils/file.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/thread_pool.hpp"

namespace storage::replication {

enum class ReplicationMode : std::uint8_t { SYNC, ASYNC };

class ReplicationClient {
 public:
  ReplicationClient(std::string name, NameIdMapper *name_id_mapper,
                    Config::Items items, const io::network::Endpoint &endpoint,
                    bool use_ssl, ReplicationMode mode);

  // Handler used for transfering the current transaction.
  class TransactionHandler {
   private:
    friend class ReplicationClient;
    explicit TransactionHandler(ReplicationClient *self);

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
    void Finalize();

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
    void Finalize();

   private:
    ReplicationClient *self_;
    rpc::Client::StreamHandler<WalFilesRpc> stream_;
  };

  bool StartTransactionReplication();

  // Replication clients can be removed at any point
  // so to avoid any complexity of checking if the client was removed whenever
  // we want to send part of transaction and to avoid adding some GC logic this
  // function will run a callback if, after previously callling
  // StartTransactionReplication, stream is created.
  void IfStreamingTransaction(
      const std::function<void(TransactionHandler &handler)> &callback);

  void FinalizeTransactionReplication();

  // Transfer the snapshot file.
  // @param path Path of the snapshot file.
  void TransferSnapshot(const std::filesystem::path &path);

  CurrentWalHandler TransferCurrentWalFile() { return CurrentWalHandler{this}; }

  // Transfer the WAL files
  void TransferWalFiles(const std::vector<std::filesystem::path> &wal_files);

  const auto &Name() const { return name_; }

 private:
  void FinalizeTransactionReplicationInternal() {
    if (stream_) {
      stream_->Finalize();
      stream_.reset();
      in_progress_.clear();
    }
  }

  std::string name_;
  NameIdMapper *name_id_mapper_;
  Config::Items items_;
  communication::ClientContext rpc_context_;
  rpc::Client rpc_client_;

  // TODO (antonio2368): Give a better name.
  std::optional<TransactionHandler> stream_;
  ReplicationMode mode_{ReplicationMode::SYNC};

  std::mutex client_lock_;
  utils::ThreadPool thread_pool_{1};
  std::atomic_flag in_progress_ = ATOMIC_FLAG_INIT;
  std::atomic<bool> recovery_{false};
};

}  // namespace storage::replication
