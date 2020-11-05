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
                    bool use_ssl)
      : name_(std::move(name)),
        name_id_mapper_(name_id_mapper),
        items_(items),
        rpc_context_(use_ssl),
        rpc_client_(endpoint, &rpc_context_) {}

  class TransactionHandler {
   private:
    friend class ReplicationClient;
    explicit TransactionHandler(ReplicationClient *self)
        : self_(self), stream_(self_->rpc_client_.Stream<AppendDeltasRpc>()) {}

   public:
    /// @throw rpc::RpcFailedException
    void AppendDelta(const Delta &delta, const Vertex &vertex,
                     uint64_t final_commit_timestamp) {
      Encoder encoder(stream_.GetBuilder());
      EncodeDelta(&encoder, self_->name_id_mapper_, self_->items_, delta,
                  vertex, final_commit_timestamp);
    }

    /// @throw rpc::RpcFailedException
    void AppendDelta(const Delta &delta, const Edge &edge,
                     uint64_t final_commit_timestamp) {
      Encoder encoder(stream_.GetBuilder());
      EncodeDelta(&encoder, self_->name_id_mapper_, delta, edge,
                  final_commit_timestamp);
    }

    /// @throw rpc::RpcFailedException
    void AppendTransactionEnd(uint64_t final_commit_timestamp) {
      Encoder encoder(stream_.GetBuilder());
      EncodeTransactionEnd(&encoder, final_commit_timestamp);
    }

    /// @throw rpc::RpcFailedException
    void AppendOperation(durability::StorageGlobalOperation operation,
                         LabelId label, const std::set<PropertyId> &properties,
                         uint64_t timestamp) {
      Encoder encoder(stream_.GetBuilder());
      EncodeOperation(&encoder, self_->name_id_mapper_, operation, label,
                      properties, timestamp);
    }

    /// @throw rpc::RpcFailedException
    void Finalize() { stream_.AwaitResponse(); }

   private:
    ReplicationClient *self_;
    rpc::Client::StreamHandler<AppendDeltasRpc> stream_;
  };

  class SnapshotHandler {
   private:
    friend class ReplicationClient;
    explicit SnapshotHandler(ReplicationClient *self)
        : self_(self), stream_(self_->rpc_client_.Stream<SnapshotRpc>()) {}

   public:
    void StreamSnapshot(const std::filesystem::path &path) {
      Encoder encoder(stream_.GetBuilder());
      encoder.WriteFile(path);
      stream_.AwaitResponse();
    }

   private:
    ReplicationClient *self_;
    rpc::Client::StreamHandler<SnapshotRpc> stream_;
  };

  class WalHandler {
   private:
    friend class ReplicationClient;
    explicit WalHandler(ReplicationClient *self)
        : self_(self), stream_(self_->rpc_client_.Stream<WalFilesRpc>()) {}

   public:
    void StreamWalFiles(const std::vector<std::filesystem::path> &wal_files) {
      CHECK(!wal_files.empty()) << "Wal files list is empty!";
      Encoder encoder(stream_.GetBuilder());
      encoder.WriteUint(wal_files.size());
      for (const auto &wal : wal_files) {
        encoder.WriteFile(wal);
      }

      stream_.AwaitResponse();
    }

   private:
    ReplicationClient *self_;
    rpc::Client::StreamHandler<WalFilesRpc> stream_;
  };

  TransactionHandler ReplicateTransaction() { return TransactionHandler(this); }
  SnapshotHandler TransferSnapshot() { return SnapshotHandler(this); }
  WalHandler TransferWalFiles() { return WalHandler(this); };

  const auto &Name() const { return name_; }

 private:
  std::string name_;
  NameIdMapper *name_id_mapper_;
  Config::Items items_;
  communication::ClientContext rpc_context_;
  rpc::Client rpc_client_;
};

}  // namespace storage::replication
