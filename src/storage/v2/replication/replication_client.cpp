#include "storage/v2/replication/replication_client.hpp"

#include <algorithm>
#include <type_traits>

#include "storage/v2/durability/durability.hpp"
#include "utils/file_locker.hpp"

namespace storage {

namespace {
template <typename>
[[maybe_unused]] inline constexpr bool always_false_v = false;
}  // namespace

////// ReplicationClient //////
Storage::ReplicationClient::ReplicationClient(
    std::string name, Storage *storage, const io::network::Endpoint &endpoint,
    bool use_ssl, const ReplicationMode mode)
    : name_(std::move(name)),
      storage(storage),
      rpc_context_(use_ssl),
      rpc_client_(endpoint, &rpc_context_),
      mode_(mode) {
  InitializeClient();
}

void Storage::ReplicationClient::InitializeClient() {
  uint64_t current_commit_timestamp{kTimestampInitialId};
  auto stream{rpc_client_.Stream<HeartbeatRpc>()};
  replication::Encoder encoder{stream.GetBuilder()};
  const auto response = stream.AwaitResponse();
  current_commit_timestamp = response.current_commit_timestamp;
  DLOG(INFO) << "CURRENT TIMESTAMP: " << current_commit_timestamp;
  DLOG(INFO) << "CURRENT MAIN TIMESTAMP: "
             << storage->last_commit_timestamp_.load();
  if (current_commit_timestamp == storage->last_commit_timestamp_.load()) {
    DLOG(INFO) << "REPLICA UP TO DATE";
    replica_state_.store(ReplicaState::READY);
  } else {
    DLOG(INFO) << "REPLICA IS BEHIND";
    replica_state_.store(ReplicaState::RECOVERY);
    thread_pool_.AddTask(
        [=, this] { this->RecoverReplica(current_commit_timestamp); });
  }
}

SnapshotRes Storage::ReplicationClient::TransferSnapshot(
    const std::filesystem::path &path) {
  auto stream{rpc_client_.Stream<SnapshotRpc>()};
  replication::Encoder encoder(stream.GetBuilder());
  encoder.WriteFile(path);
  return stream.AwaitResponse();
}

WalFilesRes Storage::ReplicationClient::TransferWalFiles(
    const std::vector<std::filesystem::path> &wal_files) {
  CHECK(!wal_files.empty()) << "Wal files list is empty!";
  auto stream{rpc_client_.Stream<WalFilesRpc>(wal_files.size())};
  replication::Encoder encoder(stream.GetBuilder());
  for (const auto &wal : wal_files) {
    DLOG(INFO) << "Sending wal file: " << wal;
    encoder.WriteFile(wal);
  }

  return stream.AwaitResponse();
}

OnlySnapshotRes Storage::ReplicationClient::TransferOnlySnapshot(
    const uint64_t snapshot_timestamp) {
  auto stream{rpc_client_.Stream<OnlySnapshotRpc>(snapshot_timestamp)};
  return stream.AwaitResponse();
}

bool Storage::ReplicationClient::StartTransactionReplication(
    const uint64_t current_wal_seq_num) {
  std::unique_lock guard(client_lock_);
  const auto status = replica_state_.load();
  switch (status) {
    case ReplicaState::RECOVERY:
      DLOG(INFO) << "Replica " << name_ << " is behind MAIN instance";
      return false;
    case ReplicaState::REPLICATING:
      DLOG(INFO) << "Replica missed a transaction, going to recovery";
      replica_state_.store(ReplicaState::RECOVERY);
      // If it's in replicating state, it should have been up to date with all
      // the commits until now so the replica should contain the
      // last_commit_timestamp
      thread_pool_.AddTask([=, this] {
        this->RecoverReplica(storage->last_commit_timestamp_.load());
      });
      return false;
    case ReplicaState::READY:
      CHECK(!replica_stream_);
      try {
        replica_stream_.emplace(ReplicaStream{
            this, storage->last_commit_timestamp_.load(), current_wal_seq_num});
      } catch (const rpc::RpcFailedException &) {
        LOG(ERROR) << "Couldn't replicate data to " << name_;
        thread_pool_.AddTask([this] {
          rpc_client_.Abort();
          InitializeClient();
        });
      }
      replica_state_.store(ReplicaState::REPLICATING);
      return true;
  }
}

void Storage::ReplicationClient::IfStreamingTransaction(
    const std::function<void(ReplicaStream &handler)> &callback) {
  if (replica_stream_) {
    try {
      callback(*replica_stream_);
    } catch (const rpc::RpcFailedException &) {
      LOG(ERROR) << "Couldn't replicate data to " << name_;
      thread_pool_.AddTask([this] {
        rpc_client_.Abort();
        InitializeClient();
      });
    }
  }
}

void Storage::ReplicationClient::FinalizeTransactionReplication() {
  if (mode_ == ReplicationMode::ASYNC) {
    thread_pool_.AddTask(
        [this] { this->FinalizeTransactionReplicationInternal(); });
  } else {
    FinalizeTransactionReplicationInternal();
  }
}

void Storage::ReplicationClient::FinalizeTransactionReplicationInternal() {
  if (replica_stream_) {
    try {
      auto response = replica_stream_->Finalize();
      if (!response.success) {
        replica_state_.store(ReplicaState::RECOVERY);
        thread_pool_.AddTask([&, this] {
          this->RecoverReplica(response.current_commit_timestamp);
        });
      }
    } catch (const rpc::RpcFailedException &) {
      LOG(ERROR) << "Couldn't replicate data to " << name_;
      thread_pool_.AddTask([this] {
        rpc_client_.Abort();
        InitializeClient();
      });
    }
    replica_stream_.reset();
  }

  std::unique_lock guard(client_lock_);
  if (replica_state_.load() == ReplicaState::REPLICATING) {
    replica_state_.store(ReplicaState::READY);
  }
}

void Storage::ReplicationClient::RecoverReplica(uint64_t replica_commit) {
  while (true) {
    auto file_locker = storage->file_retainer_.AddLocker();

    const auto steps = GetRecoverySteps(replica_commit, &file_locker);
    for (const auto &recovery_step : steps) {
      std::visit(
          [&, this]<typename T>(T &&arg) {
            using StepType = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<StepType, RecoverySnapshot>) {
              DLOG(INFO) << "Sending the latest snapshot file: " << arg;
              auto response = TransferSnapshot(arg);
              replica_commit = response.current_commit_timestamp;
              DLOG(INFO) << "CURRENT TIMESTAMP ON REPLICA: " << replica_commit;
            } else if constexpr (std::is_same_v<StepType, RecoveryWals>) {
              DLOG(INFO) << "Sending the latest wal files";
              auto response = TransferWalFiles(arg);
              replica_commit = response.current_commit_timestamp;
              DLOG(INFO) << "CURRENT TIMESTAMP ON REPLICA: " << replica_commit;
            } else if constexpr (std::is_same_v<StepType, RecoveryCurrentWal>) {
              std::unique_lock transaction_guard(storage->engine_lock_);
              if (storage->wal_file_ && storage->wal_file_->SequenceNumber() ==
                                            arg.current_wal_seq_num) {
                storage->wal_file_->DisableFlushing();
                transaction_guard.unlock();
                DLOG(INFO) << "Sending current wal file";
                replica_commit = ReplicateCurrentWal();
                DLOG(INFO) << "CURRENT TIMESTAMP ON REPLICA: "
                           << replica_commit;
                storage->wal_file_->EnableFlushing();
              }
            } else if constexpr (std::is_same_v<StepType,
                                                RecoveryFinalSnapshot>) {
              DLOG(INFO) << "Snapshot timestamp is the latest";
              auto response = TransferOnlySnapshot(arg.snapshot_timestamp);
              if (response.success) {
                replica_commit = response.current_commit_timestamp;
              }
            } else {
              static_assert(always_false_v<T>,
                            "Missing type from variant visitor");
            }
          },
          recovery_step);
    }

    if (storage->last_commit_timestamp_.load() == replica_commit) {
      replica_state_.store(ReplicaState::READY);
      return;
    }
  }
}

uint64_t Storage::ReplicationClient::ReplicateCurrentWal() {
  auto stream = TransferCurrentWalFile();
  stream.AppendFilename(storage->wal_file_->Path().filename());
  utils::InputFile file;
  CHECK(file.Open(storage->wal_file_->Path()))
      << "Failed to open current WAL file!";
  const auto [buffer, buffer_size] = storage->wal_file_->CurrentFileBuffer();
  stream.AppendSize(file.GetSize() + buffer_size);
  stream.AppendFileData(&file);
  stream.AppendBufferData(buffer, buffer_size);
  auto response = stream.Finalize();
  return response.current_commit_timestamp;
}

/// This method tries to find the optimal path for recoverying a single replica.
/// Based on the last commit transfered to replica it tries to update the
/// replica using durability files - WALs and Snapshots. WAL files are much
/// smaller in size as they contain only the Deltas (changes) made during the
/// transactions while Snapshots contain all the data. For that reason we prefer
/// WALs as much as possible. As the WAL file that is currently being updated
/// can change during the process we ignore it as much as possible. Also, it
/// uses the transaction lock so lokcing it can be really expensive. After we
/// fetch the list of finalized WALs, we try to find the longest chain of
/// sequential WALs, starting from the latest one, that will update the recovery
/// with the all missed updates. If the WAL chain cannot be created, replica is
/// behind by a lot, so we use the regular recovery process, we send the latest
/// snapshot and all the necessary WAL files, starting from the newest WAL that
/// contains a timestamp before the snapshot. If we registered the existence of
/// the current WAL, we add the sequence number we read from it to the recovery
/// process. After all the other steps are finished, if the current WAL contains
/// the same sequence number, it's the same WAL we read while fetching the
/// recovery steps, so we can safely send it to the replica. There's also one
/// edge case, if MAIN instance restarted and the snapshot contained the last
/// change (creation of that snapshot) the latest timestamp is contained in it.
/// As no changes were made to the data, we only need to send the timestamp of
/// the snapshot so replica can set its last timestamp to that value.
std::vector<Storage::ReplicationClient::RecoveryStep>
Storage::ReplicationClient::GetRecoverySteps(
    const uint64_t replica_commit,
    utils::FileRetainer::FileLocker *file_locker) {
  // First check if we can recover using the current wal file only
  // otherwise save the seq_num of the current wal file
  // This lock is also necessary to force the missed transaction to finish.
  std::optional<uint64_t> current_wal_seq_num;
  if (std::unique_lock transtacion_guard(storage->engine_lock_);
      storage->wal_file_) {
    current_wal_seq_num.emplace(storage->wal_file_->SequenceNumber());
  }

  auto locker_acc = file_locker->Access();
  auto wal_files = durability::GetWalFiles(storage->wal_directory_,
                                           storage->uuid_, current_wal_seq_num);
  CHECK(wal_files) << "Wal files could not be loaded";

  auto snapshot_files = durability::GetSnapshotFiles(
      storage->snapshot_directory_, storage->uuid_);
  std::optional<durability::SnapshotDurabilityInfo> latest_snapshot;
  if (!snapshot_files.empty()) {
    std::sort(snapshot_files.begin(), snapshot_files.end());
    latest_snapshot.emplace(std::move(snapshot_files.back()));
  }

  std::vector<RecoveryStep> recovery_steps;

  // No finalized WAL files were found. This means the difference is contained
  // inside the current WAL or the snapshot was loaded back without any WALs
  // after.
  if (wal_files->empty()) {
    if (current_wal_seq_num) {
      recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
    } else {
      CHECK(latest_snapshot);
      locker_acc.AddFile(latest_snapshot->path);
      recovery_steps.emplace_back(
          RecoveryFinalSnapshot{latest_snapshot->start_timestamp});
    }
    return recovery_steps;
  }

  // Find the longest chain of WALs for recovery.
  // The chain consists ONLY of sequential WALs.
  auto rwal_it = wal_files->rbegin();

  // if the last finalized WAL is before the replica commit
  // then we can recovery only from current WAL or from snapshot
  // if the main just recovered
  if (rwal_it->to_timestamp <= replica_commit) {
    if (current_wal_seq_num) {
      recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
    } else {
      CHECK(latest_snapshot);
      locker_acc.AddFile(latest_snapshot->path);
      recovery_steps.emplace_back(
          RecoveryFinalSnapshot{latest_snapshot->start_timestamp});
    }
    return recovery_steps;
  }

  ++rwal_it;
  uint64_t previous_seq_num{rwal_it->seq_num};
  for (; rwal_it != wal_files->rend(); ++rwal_it) {
    // If the difference between two consecutive wal files is not 0 or 1
    // we have a missing WAL in our chain
    if (previous_seq_num - rwal_it->seq_num > 1) {
      break;
    }

    // Find first WAL that contains up to replica commit, i.e. WAL
    // that is before the replica commit or conatins the replica commit
    // as the last committed transaction.
    if (replica_commit >= rwal_it->from_timestamp &&
        replica_commit >= rwal_it->to_timestamp) {
      // We want the WAL after because the replica already contains all the
      // commits from this WAL
      --rwal_it;
      std::vector<std::filesystem::path> wal_chain;
      auto distance_from_first = std::distance(rwal_it, wal_files->rend() - 1);
      // We have managed to create WAL chain
      // We need to lock these files and add them to the chain
      for (auto result_wal_it = wal_files->begin() + distance_from_first;
           result_wal_it != wal_files->end(); ++result_wal_it) {
        locker_acc.AddFile(result_wal_it->path);
        wal_chain.push_back(std::move(result_wal_it->path));
      }

      recovery_steps.emplace_back(std::in_place_type_t<RecoveryWals>{},
                                  std::move(wal_chain));

      if (current_wal_seq_num) {
        recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
      }
      return recovery_steps;
    }

    previous_seq_num = rwal_it->seq_num;
  }

  CHECK(latest_snapshot) << "Invalid durability state, missing snapshot";
  // We didn't manage to find a WAL chain, we need to send the latest snapshot
  // with its WALs
  locker_acc.AddFile(latest_snapshot->path);
  recovery_steps.emplace_back(std::in_place_type_t<RecoverySnapshot>{},
                              std::move(latest_snapshot->path));

  std::vector<std::filesystem::path> recovery_wal_files;
  auto wal_it = wal_files->begin();
  for (; wal_it != wal_files->end(); ++wal_it) {
    // Assuming recovery process is correct the snashpot should
    // always retain a single WAL that contains a transaction
    // before its creation
    if (latest_snapshot->start_timestamp < wal_it->to_timestamp) {
      if (latest_snapshot->start_timestamp < wal_it->from_timestamp) {
        CHECK(wal_it != wal_files->begin()) << "Invalid durability files state";
        --wal_it;
      }
      break;
    }
  }

  for (; wal_it != wal_files->end(); ++wal_it) {
    locker_acc.AddFile(wal_it->path);
    recovery_wal_files.push_back(std::move(wal_it->path));
  }

  // We only have a WAL before the snapshot
  if (recovery_wal_files.empty()) {
    locker_acc.AddFile(wal_files->back().path);
    recovery_wal_files.push_back(std::move(wal_files->back().path));
  }

  recovery_steps.emplace_back(std::in_place_type_t<RecoveryWals>{},
                              std::move(recovery_wal_files));

  if (current_wal_seq_num) {
    recovery_steps.emplace_back(RecoveryCurrentWal{*current_wal_seq_num});
  }

  return recovery_steps;
}

////// ReplicaStream //////
Storage::ReplicationClient::ReplicaStream::ReplicaStream(
    ReplicationClient *self, const uint64_t previous_commit_timestamp,
    const uint64_t current_seq_num)
    : self_(self),
      stream_(self_->rpc_client_.Stream<AppendDeltasRpc>(
          previous_commit_timestamp, current_seq_num)) {}

void Storage::ReplicationClient::ReplicaStream::AppendDelta(
    const Delta &delta, const Vertex &vertex, uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, &self_->storage->name_id_mapper_,
              self_->storage->config_.items, delta, vertex,
              final_commit_timestamp);
}

void Storage::ReplicationClient::ReplicaStream::AppendDelta(
    const Delta &delta, const Edge &edge, uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeDelta(&encoder, &self_->storage->name_id_mapper_, delta, edge,
              final_commit_timestamp);
}

void Storage::ReplicationClient::ReplicaStream::AppendTransactionEnd(
    uint64_t final_commit_timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeTransactionEnd(&encoder, final_commit_timestamp);
}

void Storage::ReplicationClient::ReplicaStream::AppendOperation(
    durability::StorageGlobalOperation operation, LabelId label,
    const std::set<PropertyId> &properties, uint64_t timestamp) {
  replication::Encoder encoder(stream_.GetBuilder());
  EncodeOperation(&encoder, &self_->storage->name_id_mapper_, operation, label,
                  properties, timestamp);
}

AppendDeltasRes Storage::ReplicationClient::ReplicaStream::Finalize() {
  return stream_.AwaitResponse();
}

////// CurrentWalHandler //////
Storage::ReplicationClient::CurrentWalHandler::CurrentWalHandler(
    ReplicationClient *self)
    : self_(self), stream_(self_->rpc_client_.Stream<CurrentWalRpc>()) {}

void Storage::ReplicationClient::CurrentWalHandler::AppendFilename(
    const std::string &filename) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteString(filename);
}

void Storage::ReplicationClient::CurrentWalHandler::AppendSize(
    const size_t size) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteUint(size);
}

void Storage::ReplicationClient::CurrentWalHandler::AppendFileData(
    utils::InputFile *file) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteFileData(file);
}

void Storage::ReplicationClient::CurrentWalHandler::AppendBufferData(
    const uint8_t *buffer, const size_t buffer_size) {
  replication::Encoder encoder(stream_.GetBuilder());
  encoder.WriteBuffer(buffer, buffer_size);
}

CurrentWalRes Storage::ReplicationClient::CurrentWalHandler::Finalize() {
  return stream_.AwaitResponse();
}
}  // namespace storage
