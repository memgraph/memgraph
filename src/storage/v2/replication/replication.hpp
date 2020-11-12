#pragma once

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

namespace storage::replication {

class ReplicationClient {
 public:
  ReplicationClient(std::string name, NameIdMapper *name_id_mapper,
                    Config::Items items, const io::network::Endpoint &endpoint,
                    bool use_ssl);

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

    /// @throw rpc::RpcFailedException
    void Finalize();

   private:
    ReplicationClient *self_;
    rpc::Client::StreamHandler<AppendDeltasRpc> stream_;
  };

  TransactionHandler ReplicateTransaction() { return TransactionHandler(this); }

  // Transfer the snapshot file.
  // @param path Path of the snapshot file.
  void TransferSnapshot(const std::filesystem::path &path);

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

  CurrentWalHandler TransferCurrentWalFile() { return CurrentWalHandler{this}; }

  // Transfer the WAL files
  void TransferWalFiles(const std::vector<std::filesystem::path> &wal_files);

  const auto &Name() const { return name_; }

 private:
  std::string name_;
  NameIdMapper *name_id_mapper_;
  Config::Items items_;
  communication::ClientContext rpc_context_;
  rpc::Client rpc_client_;
};

}  // namespace storage::replication
