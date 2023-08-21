// Copyright 2023 Memgraph Ltd.
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

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/replication/replication_client.hpp"
namespace memgraph::storage {

class InMemoryReplicationClient : public ReplicationClient {
 public:
  InMemoryReplicationClient(InMemoryStorage *storage, std::string name, io::network::Endpoint endpoint,
                            replication::ReplicationMode mode, const replication::ReplicationClientConfig &config = {});
  void StartTransactionReplication(uint64_t current_wal_seq_num) override;
  // Replication clients can be removed at any point
  // so to avoid any complexity of checking if the client was removed whenever
  // we want to send part of transaction and to avoid adding some GC logic this
  // function will run a callback if, after previously callling
  // StartTransactionReplication, stream is created.
  void IfStreamingTransaction(const std::function<void(ReplicaStream &)> &callback) override;

  auto GetEpochId() const -> std::string const & override;

  auto GetStorage() -> Storage * override;

  void Start() override;

  // Return whether the transaction could be finalized on the replication client or not.
  [[nodiscard]] bool FinalizeTransactionReplication() override;
  TimestampInfo GetTimestampInfo() override;

 private:
  void TryInitializeClientAsync();
  void FrequentCheck();
  void InitializeClient();
  void TryInitializeClientSync();
  void HandleRpcFailure();
  [[nodiscard]] bool FinalizeTransactionReplicationInternal();
  void RecoverReplica(uint64_t replica_commit);
  uint64_t ReplicateCurrentWal();

  using RecoverySnapshot = std::filesystem::path;
  using RecoveryWals = std::vector<std::filesystem::path>;
  struct RecoveryCurrentWal {
    explicit RecoveryCurrentWal(const uint64_t current_wal_seq_num) : current_wal_seq_num(current_wal_seq_num) {}
    uint64_t current_wal_seq_num;
  };
  using RecoveryStep = std::variant<RecoverySnapshot, RecoveryWals, RecoveryCurrentWal>;
  std::vector<RecoveryStep> GetRecoverySteps(uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker);

  // Transfer the snapshot file.
  // @param path Path of the snapshot file.
  replication::SnapshotRes TransferSnapshot(const std::filesystem::path &path);

  // Transfer the WAL files
  replication::WalFilesRes TransferWalFiles(const std::vector<std::filesystem::path> &wal_files);

  InMemoryStorage *storage_;
};

}  // namespace memgraph::storage
