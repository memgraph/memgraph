#pragma once

#include "storage/v2/storage.hpp"

namespace storage {

class Storage::ReplicationServer {
 public:
  explicit ReplicationServer(Storage *storage, io::network::Endpoint endpoint);
  ReplicationServer(const ReplicationServer &) = delete;
  ReplicationServer(ReplicationServer &&) = delete;
  ReplicationServer &operator=(const ReplicationServer &) = delete;
  ReplicationServer &operator=(ReplicationServer &&) = delete;

  ~ReplicationServer() {
    if (rpc_server_) {
      rpc_server_->Shutdown();
      rpc_server_->AwaitShutdown();
    }
  }

 private:
  // RPC handlers
  void HeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder);
  void AppendDeltasHandler(slk::Reader *req_reader, slk::Builder *res_builder);
  void SnapshotHandler(slk::Reader *req_reader, slk::Builder *res_builder);
  void OnlySnapshotHandler(slk::Reader *req_reader, slk::Builder *res_builder);
  void WalFilesHandler(slk::Reader *req_reader, slk::Builder *res_builder);
  void CurrentWalHandler(slk::Reader *req_reader, slk::Builder *res_builder);

  std::pair<durability::WalInfo, std::filesystem::path> LoadWal(
      replication::Decoder *decoder,
      durability::RecoveredIndicesAndConstraints *indices_constraints);

  std::optional<communication::ServerContext> rpc_server_context_;
  std::optional<rpc::Server> rpc_server_;

  Storage *storage_;
};

}  // namespace storage
