#include <iterator>
#include <list>
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
  std::unique_lock guard(client_lock_);
  const auto status = replica_state_.load();
  switch (status) {
    case ReplicaState::RECOVERY:
      DLOG(INFO) << "Replica " << name_ << " is behind MAIN instance";
      return false;
    case ReplicaState::REPLICATING:
      replica_state_.store(ReplicaState::RECOVERY);
      return false;
    case ReplicaState::READY:
      CHECK(!replica_stream_);
      replica_stream_.emplace(ReplicaStream{this});
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
ReplicationClient::ReplicaStream::ReplicaStream(ReplicationClient *self)
    : self_(self), stream_(self_->rpc_client_.Stream<AppendDeltasRpc>()) {}

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

uint64_t GetRecoveryFileTimestamp(
    const durability::RecoveryFileDurabilityInfo &recovery_file) {
  if (std::holds_alternative<durability::SnapshotDurabilityInfo>(
          recovery_file)) {
    return std::get<durability::SnapshotDurabilityInfo>(recovery_file)
        .start_timestamp;
  } else {
    return std::get<durability::WalDurabilityInfo>(recovery_file)
        .from_timestamp;
  }
}

bool IsSnapshot(const durability::RecoveryFileDurabilityInfo &rf) {
  return std::holds_alternative<durability::SnapshotDurabilityInfo>(rf);
}

bool IsWal(const durability::RecoveryFileDurabilityInfo &rf) {
  return std::holds_alternative<durability::WalDurabilityInfo>(rf);
}

bool RecoveryTimestampInWal(const uint64_t rec_ts,
                            const durability::WalDurabilityInfo &wal_file) {
  return (rec_ts >= wal_file.from_timestamp) &&
         (rec_ts <= wal_file.to_timestamp);
}

std::optional<ReplicationClient::RecoveryFiles> OptimalRecoveryFiles(
    const uint64_t replicas_last_processed_timestamp,
    ReplicationClient::RecoveryFiles &recovery_files) {
  // ASSUMPTION: recovery files must be sorted according to their timestamps
  // (snapshot - start_timestamp; WAL - from_timestamp)

  // Find latest snapshot; it will be used to check whether the WAL chain can
  // reach it. If there aren't any snapshots, we are forced to return a list
  // of WALs. If the latest snapshot exists, but we can't reach it from the
  // recovery file that's being currently sent (i.e. there's a gap in the
  // sequence number between neighboring WAL files larger than 1), we return
  // the latest snapshot, and any WAL created after it.
  if (recovery_files.empty()) return std::nullopt;
  durability::RecoveryFileDurabilityInfo latest_snapshot;
  for (auto rf = recovery_files.rbegin(); rf != recovery_files.rend(); ++rf) {
    if (IsSnapshot(*rf)) {
      latest_snapshot = std::get<durability::SnapshotDurabilityInfo>(*rf);
      break;
    }
  }

  bool current_recovery_file_reached = false;
  bool latest_snapshot_reached = false;
  ReplicationClient::RecoveryFiles optimal_recovery_files;
  for (auto rf_it = recovery_files.begin(); rf_it != recovery_files.end();
       ++rf_it) {
    if (current_recovery_file_reached) {
      if (IsSnapshot(*rf_it)) continue;  // skip snapshots (prefer WALs)
      optimal_recovery_files.emplace_back(*rf_it);
    }
    // is the replica's latest committed tx in the snapshot?
    if (IsSnapshot(*rf_it)) {
      if ((GetRecoveryFileTimestamp(*rf_it) <=
           replicas_last_processed_timestamp) &&
          (GetRecoveryFileTimestamp(*std::next(rf_it, 1)) >
           replicas_last_processed_timestamp)) {
        current_recovery_file_reached = true;
      }
    } else {
      if (RecoveryTimestampInWal(
              replicas_last_processed_timestamp,
              std::get<durability::WalDurabilityInfo>(*rf_it))) {
        current_recovery_file_reached = true;
      }
    }
  }
  return std::nullopt;
}

ReplicationClient::RecoveryFiles RecoveryFilesSortedByTimeStamp(
    std::vector<durability::SnapshotDurabilityInfo> &snapshot_files,
    std::optional<std::vector<durability::WalDurabilityInfo>> &wal_files) {
  ReplicationClient::RecoveryFiles recovery_files;
  if (!snapshot_files.empty()) {
    for (auto &sf : snapshot_files) {
      recovery_files.emplace_back(std::move(sf));
    }
  }
  if (wal_files) {
    for (auto &wal : *wal_files) {
      recovery_files.emplace_back(std::move(wal));
    }
  }

  auto by_timestamp = [](auto &recovery_file_a, auto &recovery_file_b) {
    return GetRecoveryFileTimestamp(recovery_file_a) <
           GetRecoveryFileTimestamp(recovery_file_b);
  };

  std::sort(recovery_files.begin(), recovery_files.end(), by_timestamp);
  return recovery_files;
};

std::optional<ReplicationClient::RecoveryFiles> GetOptimalRecoveryFiles(
    const uint64_t replicas_last_processed_timestamp,
    const std::filesystem::path &snapshot_dir,
    const std::filesystem::path &wal_dir, const std::string_view uuid) {
  auto snapshot_files = durability::GetSnapshotFiles(snapshot_dir, uuid);
  auto wal_files = durability::GetWalFiles(wal_dir, uuid);

  auto recovery_files =
      RecoveryFilesSortedByTimeStamp(snapshot_files, wal_files);

  auto optimal_recovery_files =
      OptimalRecoveryFiles(replicas_last_processed_timestamp, recovery_files);

  return optimal_recovery_files;
};

}  // namespace storage::replication
