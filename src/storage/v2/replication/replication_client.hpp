// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <atomic>
#include <chrono>
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
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "storage/v2/storage.hpp"
#include "utils/file.hpp"
#include "utils/file_locker.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/thread_pool.hpp"

namespace memgraph::storage {

class Storage::ReplicationClient {
 public:
  ReplicationClient(std::string name, Storage *storage, const io::network::Endpoint &endpoint,
                    replication::ReplicationMode mode, const replication::ReplicationClientConfig &config = {});

  // Handler used for transfering the current transaction.
  class ReplicaStream {
   private:
    friend class ReplicationClient;
    explicit ReplicaStream(ReplicationClient *self, uint64_t previous_commit_timestamp, uint64_t current_seq_num);

   public:
    /// @throw rpc::RpcFailedException
    void AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t final_commit_timestamp);

    /// @throw rpc::RpcFailedException
    void AppendDelta(const Delta &delta, const Edge &edge, uint64_t final_commit_timestamp);

    /// @throw rpc::RpcFailedException
    void AppendTransactionEnd(uint64_t final_commit_timestamp);

    /// @throw rpc::RpcFailedException
    void AppendOperation(durability::StorageGlobalOperation operation, LabelId label,
                         const std::set<PropertyId> &properties, uint64_t timestamp);

   private:
    /// @throw rpc::RpcFailedException
    replication::AppendDeltasRes Finalize();

    ReplicationClient *self_;
    rpc::Client::StreamHandler<replication::AppendDeltasRpc> stream_;
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
    replication::CurrentWalRes Finalize();

   private:
    ReplicationClient *self_;
    rpc::Client::StreamHandler<replication::CurrentWalRpc> stream_;
  };

  void StartTransactionReplication(uint64_t current_wal_seq_num);

  // Replication clients can be removed at any point
  // so to avoid any complexity of checking if the client was removed whenever
  // we want to send part of transaction and to avoid adding some GC logic this
  // function will run a callback if, after previously calling
  // StartTransactionReplication, stream is created.
  void IfStreamingTransaction(const std::function<void(ReplicaStream &handler)> &callback);

  // Return whether the transaction could be finalized on the replication client or not.
  [[nodiscard]] bool FinalizeTransactionReplication();

  // Transfer the snapshot file.
  // @param path Path of the snapshot file.
  replication::SnapshotRes TransferSnapshot(const std::filesystem::path &path);

  CurrentWalHandler TransferCurrentWalFile() { return CurrentWalHandler{this}; }

  // Transfer the WAL files
  replication::WalFilesRes TransferWalFiles(const std::vector<std::filesystem::path> &wal_files);

  const auto &Name() const { return name_; }

  auto State() const { return replica_state_.load(); }

  auto Mode() const { return mode_; }

  const auto &Endpoint() const { return rpc_client_->Endpoint(); }

  Storage::TimestampInfo GetTimestampInfo();

 private:
  [[nodiscard]] bool FinalizeTransactionReplicationInternal();

  void RecoverReplica(uint64_t replica_commit);

  uint64_t ReplicateCurrentWal();

  using RecoveryWals = std::vector<std::filesystem::path>;
  struct RecoveryCurrentWal {
    uint64_t current_wal_seq_num;

    explicit RecoveryCurrentWal(const uint64_t current_wal_seq_num) : current_wal_seq_num(current_wal_seq_num) {}
  };
  using RecoverySnapshot = std::filesystem::path;
  using RecoveryStep = std::variant<RecoverySnapshot, RecoveryWals, RecoveryCurrentWal>;

  std::vector<RecoveryStep> GetRecoverySteps(uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker);

  void FrequentCheck();
  void InitializeClient();
  void TryInitializeClientSync();
  void TryInitializeClientAsync();
  void HandleRpcFailure();

  std::string name_;
  Storage *storage_;
  std::optional<communication::ClientContext> rpc_context_;
  std::optional<rpc::Client> rpc_client_;

  std::optional<ReplicaStream> replica_stream_;
  replication::ReplicationMode mode_{replication::ReplicationMode::SYNC};

  utils::SpinLock client_lock_;
  // This thread pool is used for background tasks so we don't
  // block the main storage thread
  // We use only 1 thread for 2 reasons:
  //  - background tasks ALWAYS contain some kind of RPC communication.
  //    We can't have multiple RPC communication from a same client
  //    because that's not logically valid (e.g. you cannot send a snapshot
  //    and WAL at a same time because WAL will arrive earlier and be applied
  //    before the snapshot which is not correct)
  //  - the implementation is simplified as we have a total control of what
  //    this pool is executing. Also, we can simply queue multiple tasks
  //    and be sure of the execution order.
  //    Not having multiple possible threads in the same client allows us
  //    to ignore concurrency problems inside the client.
  utils::ThreadPool thread_pool_{1};
  std::atomic<replication::ReplicaState> replica_state_{replication::ReplicaState::INVALID};

  utils::Scheduler replica_checker_;
};

}  // namespace memgraph::storage
