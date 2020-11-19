#include "storage/v2/replication/replication.hpp"

#include "storage/v2/durability/durability.hpp"
#include "utils/file_locker.hpp"

namespace storage::replication {

////// ReplicationClient //////
ReplicationClient::ReplicationClient(
    std::string name, const std::atomic<uint64_t> &last_commit_timestamp,
    NameIdMapper *name_id_mapper, Config::Items items,
    utils::FileRetainer *file_retainer,
    const std::filesystem::path &snapshot_directory,
    const std::filesystem::path &wal_directory, const std::string_view uuid,
    std::optional<durability::WalFile> *wal_file_ptr, uint64_t *wal_seq_num,
    utils::SpinLock *transaction_engine_lock,
    const io::network::Endpoint &endpoint, bool use_ssl,
    const ReplicationMode mode)
    : name_(std::move(name)),
      last_commit_timestamp_{last_commit_timestamp},
      name_id_mapper_(name_id_mapper),
      items_(items),
      file_retainer_(file_retainer),
      snapshot_directory_(snapshot_directory),
      wal_directory_(wal_directory),
      uuid_(uuid),
      wal_file_ptr_(wal_file_ptr),
      wal_seq_num_(wal_seq_num),
      transaction_engine_lock_(transaction_engine_lock),
      rpc_context_(use_ssl),
      rpc_client_(endpoint, &rpc_context_),
      mode_(mode) {
  uint64_t current_commit_timestamp{kTimestampInitialId};
  {
    auto stream{rpc_client_.Stream<HeartbeatRpc>()};
    const auto response = stream.AwaitResponse();
    current_commit_timestamp = response.current_commit_timestamp;
  }
  DLOG(INFO) << "CURRENT TIMESTAMP: " << current_commit_timestamp;
  DLOG(INFO) << "CURRENT MAIN TIMESTAMP: " << last_commit_timestamp.load();
  if (current_commit_timestamp == last_commit_timestamp.load()) {
    DLOG(INFO) << "REPLICA UP TO DATE";
    replica_state_.store(ReplicaState::READY);
  } else {
    DLOG(INFO) << "REPLICA IS BEHIND";
    replica_state_.store(ReplicaState::RECOVERY);

    auto &wal_file = *wal_file_ptr_;

    // Recovery
    auto recovery_locker = file_retainer_->AddLocker();
    std::optional<std::filesystem::path> snapshot_file;
    std::vector<std::filesystem::path> wal_files;
    std::optional<uint64_t> latest_seq_num;
    {
      auto recovery_locker_acc = recovery_locker.Access();
      // For now we assume we need to send the latest snapshot
      auto snapshot_files =
          durability::GetSnapshotFiles(snapshot_directory_, uuid_);
      if (!snapshot_files.empty()) {
        std::sort(snapshot_files.begin(), snapshot_files.end());
        // TODO (antonio2368): Send the last snapshot for now
        // check if additional logic is necessary
        // Also, prevent the deletion of the snapshot file and the required
        // WALs
        snapshot_file.emplace(std::move(snapshot_files.back().first));
        recovery_locker_acc.AddFile(*snapshot_file);
      }

      {
        // So the wal_file_ isn't overwritten
        std::unique_lock engine_guard(*transaction_engine_lock_);
        if (wal_file) {
          // For now let's disable flushing until we send the current WAL
          wal_file->DisableFlushing();
          latest_seq_num = wal_file->SequenceNumber();
        }
      }

      auto maybe_wal_files =
          durability::GetWalFiles(wal_directory_, uuid_, latest_seq_num);
      CHECK(maybe_wal_files) << "Failed to find WAL files";
      auto &wal_files_with_seq = *maybe_wal_files;

      std::optional<uint64_t> previous_seq_num;
      for (const auto &[seq_num, from_timestamp, to_timestamp, path] :
           wal_files_with_seq) {
        if (previous_seq_num && *previous_seq_num + 1 != seq_num) {
          LOG(FATAL) << "You are missing a WAL file with the sequence number "
                     << *previous_seq_num + 1 << "!";
        }
        previous_seq_num = seq_num;
        wal_files.push_back(path);
        recovery_locker_acc.AddFile(path);
      }
    }

    if (snapshot_file) {
      DLOG(INFO) << "Sending the latest snapshot file: " << *snapshot_file;
      auto response = TransferSnapshot(*snapshot_file);
      DLOG(INFO) << "CURRENT TIMESTAMP ON REPLICA: "
                 << response.current_commit_timestamp;
    }

    if (!wal_files.empty()) {
      DLOG(INFO) << "Sending the latest wal files";
      auto response = TransferWalFiles(wal_files);
      DLOG(INFO) << "CURRENT TIMESTAMP ON REPLICA: "
                 << response.current_commit_timestamp;
    }

    // We have a current WAL
    if (latest_seq_num) {
      auto stream = TransferCurrentWalFile();
      stream.AppendFilename(wal_file->Path().filename());
      utils::InputFile file;
      CHECK(file.Open(wal_file->Path())) << "Failed to open current WAL file!";
      const auto [buffer, buffer_size] = wal_file->CurrentFileBuffer();
      stream.AppendSize(file.GetSize() + buffer_size);
      stream.AppendFileData(&file);
      stream.AppendBufferData(buffer, buffer_size);
      auto response = stream.Finalize();
      wal_file->EnableFlushing();
      DLOG(INFO) << "CURRENT TIMESTAMP ON REPLICA: "
                 << response.current_commit_timestamp;
    }

    replica_state_.store(ReplicaState::READY);
  }
}

SnapshotRes ReplicationClient::TransferSnapshot(
    const std::filesystem::path &path) {
  auto stream{rpc_client_.Stream<SnapshotRpc>()};
  Encoder encoder(stream.GetBuilder());
  encoder.WriteFile(path);
  return stream.AwaitResponse();
}

WalFilesRes ReplicationClient::TransferWalFiles(
    const std::vector<std::filesystem::path> &wal_files) {
  CHECK(!wal_files.empty()) << "Wal files list is empty!";
  auto stream{rpc_client_.Stream<WalFilesRpc>(wal_files.size())};
  Encoder encoder(stream.GetBuilder());
  for (const auto &wal : wal_files) {
    DLOG(INFO) << "Sending wal file: " << wal;
    encoder.WriteFile(wal);
  }

  return stream.AwaitResponse();
}

bool ReplicationClient::StartTransactionReplication() {
  std::unique_lock guard(client_lock_);
  const auto status = replica_state_.load();
  switch (status) {
    case ReplicaState::RECOVERY:
      DLOG(INFO) << "Replica " << name_ << " is behind MAIN instance";
      return false;
    case ReplicaState::REPLICATING:
      DLOG(INFO) << "Replica missed a transaction, going to recovery";
      replica_state_.store(ReplicaState::RECOVERY);
      return false;
    case ReplicaState::READY:
      CHECK(!replica_stream_);
      replica_stream_.emplace(
          ReplicaStream{this, last_commit_timestamp_.load(), *wal_seq_num_});
      replica_state_.store(ReplicaState::REPLICATING);
      return true;
  }
}

void ReplicationClient::IfStreamingTransaction(
    const std::function<void(ReplicaStream &handler)> &callback) {
  if (replica_stream_) {
    callback(*replica_stream_);
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

void ReplicationClient::FinalizeTransactionReplicationInternal() {
  if (replica_stream_) {
    replica_stream_->Finalize();
    replica_stream_.reset();
  }

  std::unique_lock guard(client_lock_);
  if (replica_state_.load() == ReplicaState::REPLICATING) {
    replica_state_.store(ReplicaState::READY);
  }
}
////// ReplicaStream //////
ReplicationClient::ReplicaStream::ReplicaStream(
    ReplicationClient *self, const uint64_t previous_commit_timestamp,
    const uint64_t current_seq_num)
    : self_(self),
      stream_(self_->rpc_client_.Stream<AppendDeltasRpc>(
          previous_commit_timestamp, current_seq_num)) {}

void ReplicationClient::ReplicaStream::AppendDelta(
    const Delta &delta, const Vertex &vertex, uint64_t final_commit_timestamp) {
  Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, self_->name_id_mapper_, self_->items_, delta, vertex,
              final_commit_timestamp);
}

void ReplicationClient::ReplicaStream::AppendDelta(
    const Delta &delta, const Edge &edge, uint64_t final_commit_timestamp) {
  Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, self_->name_id_mapper_, delta, edge,
              final_commit_timestamp);
}

void ReplicationClient::ReplicaStream::AppendTransactionEnd(
    uint64_t final_commit_timestamp) {
  Encoder encoder(stream_.GetBuilder());
  EncodeTransactionEnd(&encoder, final_commit_timestamp);
}

void ReplicationClient::ReplicaStream::AppendOperation(
    durability::StorageGlobalOperation operation, LabelId label,
    const std::set<PropertyId> &properties, uint64_t timestamp) {
  Encoder encoder(stream_.GetBuilder());
  EncodeOperation(&encoder, self_->name_id_mapper_, operation, label,
                  properties, timestamp);
}

void ReplicationClient::ReplicaStream::Finalize() { stream_.AwaitResponse(); }

////// CurrentWalHandler //////
ReplicationClient::CurrentWalHandler::CurrentWalHandler(ReplicationClient *self)
    : self_(self), stream_(self_->rpc_client_.Stream<CurrentWalRpc>()) {}

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

CurrentWalRes ReplicationClient::CurrentWalHandler::Finalize() {
  return stream_.AwaitResponse();
}
}  // namespace storage::replication
