#include "storage/v2/replication/replication.hpp"

namespace storage::replication {

////// ReplicationClient //////
ReplicationClient::ReplicationClient(std::string name,
                                     NameIdMapper *name_id_mapper,
                                     Config::Items items,
                                     const io::network::Endpoint &endpoint,
                                     bool use_ssl, const ReplicationMode mode)
    : name_(std::move(name)),
      name_id_mapper_(name_id_mapper),
      items_(items),
      rpc_context_(use_ssl),
      rpc_client_(endpoint, &rpc_context_),
      mode_(mode) {}

void ReplicationClient::TransferSnapshot(const std::filesystem::path &path) {
  auto stream{rpc_client_.Stream<SnapshotRpc>()};
  Encoder encoder(stream.GetBuilder());
  encoder.WriteFile(path);
  stream.AwaitResponse();
}

void ReplicationClient::TransferWalFiles(
    const std::vector<std::filesystem::path> &wal_files) {
  CHECK(!wal_files.empty()) << "Wal files list is empty!";
  auto stream{rpc_client_.Stream<WalFilesRpc>()};
  Encoder encoder(stream.GetBuilder());
  encoder.WriteUint(wal_files.size());
  for (const auto &wal : wal_files) {
    encoder.WriteFile(wal);
  }

  stream.AwaitResponse();
}

bool ReplicationClient::StartTransactionReplication() {
  if (recovery_.load()) {
    DLOG(INFO) << "Replica " << name_ << " is behind MAIN instance";
    return false;
  }

  if (in_progress_.test_and_set()) {
    recovery_.store(true);
    return false;
  }

  CHECK(!stream_);
  stream_.emplace(TransactionHandler{this});
  return true;
}

void ReplicationClient::IfStreamingTransaction(
    const std::function<void(TransactionHandler &handler)> &callback) {
  if (stream_) {
    callback(*stream_);
  }
}

void ReplicationClient::FinalizeTransactionReplication() {
  if (mode_ == ReplicationMode::ASYNC) {
    thread_pool_.AddTask(
        [this] { this->FinalizeTransactionReplicationInternal(); });
  } else {
    FinalizeTransactionReplicationInternal();
  }
}

////// TransactionHandler //////
ReplicationClient::TransactionHandler::TransactionHandler(
    ReplicationClient *self)
    : self_(self), stream_(self_->rpc_client_.Stream<AppendDeltasRpc>()) {}

void ReplicationClient::TransactionHandler::AppendDelta(
    const Delta &delta, const Vertex &vertex, uint64_t final_commit_timestamp) {
  Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, self_->name_id_mapper_, self_->items_, delta, vertex,
              final_commit_timestamp);
}

void ReplicationClient::TransactionHandler::AppendDelta(
    const Delta &delta, const Edge &edge, uint64_t final_commit_timestamp) {
  Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, self_->name_id_mapper_, delta, edge,
              final_commit_timestamp);
}

void ReplicationClient::TransactionHandler::AppendTransactionEnd(
    uint64_t final_commit_timestamp) {
  Encoder encoder(stream_.GetBuilder());
  EncodeTransactionEnd(&encoder, final_commit_timestamp);
}

void ReplicationClient::TransactionHandler::AppendOperation(
    durability::StorageGlobalOperation operation, LabelId label,
    const std::set<PropertyId> &properties, uint64_t timestamp) {
  Encoder encoder(stream_.GetBuilder());
  EncodeOperation(&encoder, self_->name_id_mapper_, operation, label,
                  properties, timestamp);
}

void ReplicationClient::TransactionHandler::Finalize() {
  stream_.AwaitResponse();
}

////// CurrentWalHandler //////
ReplicationClient::CurrentWalHandler::CurrentWalHandler(ReplicationClient *self)
    : self_(self), stream_(self_->rpc_client_.Stream<WalFilesRpc>()) {
  Encoder encoder(stream_.GetBuilder());
  encoder.WriteUint(1);
}

void ReplicationClient::CurrentWalHandler::AppendFilename(
    const std::string &filename) {
  Encoder encoder(stream_.GetBuilder());
  encoder.WriteString(filename);
}

void ReplicationClient::CurrentWalHandler::AppendSize(const size_t size) {
  Encoder encoder(stream_.GetBuilder());
  encoder.WriteUint(size);
}

void ReplicationClient::CurrentWalHandler::AppendFileData(
    utils::InputFile *file) {
  Encoder encoder(stream_.GetBuilder());
  encoder.WriteFileData(file);
}

void ReplicationClient::CurrentWalHandler::AppendBufferData(
    const uint8_t *buffer, const size_t buffer_size) {
  Encoder encoder(stream_.GetBuilder());
  encoder.WriteBuffer(buffer, buffer_size);
}

void ReplicationClient::CurrentWalHandler::Finalize() {
  stream_.AwaitResponse();
}
}  // namespace storage::replication
