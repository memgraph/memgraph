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

#include "kvstore/kvstore.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/storage_global_operation.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/result.hpp"

/// REPLICATION ///
#include "replication/replication_epoch.hpp"
#include "replication/replication_state.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/global.hpp"
#include "storage/v2/replication/replication_persistence_helper.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"

namespace memgraph::storage {

class Storage;
class ReplicationServer;
class ReplicationClient;

enum class RegisterReplicaError : uint8_t { NAME_EXISTS, END_POINT_EXISTS, CONNECTION_FAILED, COULD_NOT_BE_PERSISTED };

struct ReplicationStorageState : memgraph::replication::ReplicationState {
  // TODO: This mirrors the logic in InMemoryConstructor; make it independent
  ReplicationStorageState(bool restore, std::filesystem::path durability_dir);

  // Generic API
  void Reset();

  bool SetMainReplicationRole(Storage *storage);  // Set the instance to MAIN
  // TODO: ReplicationServer/Client uses Storage* for RPC callbacks
  bool SetReplicaRole(const replication::ReplicationServerConfig &config,
                      Storage *storage);  // Sets the instance to REPLICA
  // Generic restoration
  void RestoreReplicationRole(Storage *storage);

  // MAIN actually doing the replication
  void AppendOperation(durability::StorageMetadataOperation operation, LabelId label,
                       const std::set<PropertyId> &properties, const LabelIndexStats &stats,
                       const LabelPropertyIndexStats &property_stats, uint64_t final_commit_timestamp);
  void InitializeTransaction(uint64_t seq_num);
  void AppendDelta(const Delta &delta, const Vertex &parent, uint64_t timestamp);
  void AppendDelta(const Delta &delta, const Edge &parent, uint64_t timestamp);
  bool FinalizeTransaction(uint64_t timestamp);

  // MAIN connecting to replicas
  utils::BasicResult<RegisterReplicaError> RegisterReplica(const replication::RegistrationMode registration_mode,
                                                           const replication::ReplicationClientConfig &config,
                                                           Storage *storage);
  bool UnregisterReplica(std::string_view name);

  // MAIN reconnecting to replicas
  void RestoreReplicas(Storage *storage);

  // MAIN getting info from replicas
  // TODO make into const (problem with SpinLock and WithReadLock)
  std::optional<replication::ReplicaState> GetReplicaState(std::string_view name);
  std::vector<ReplicaInfo> ReplicasInfo();

  // Questions:
  //    - storage durability <- databases/*name*/wal and snapshots (where this for epoch_id)
  //    - multi-tenant durability <- databases/.durability (there is a list of all active tenants)
  // History of the previous epoch ids.
  // Each value consists of the epoch id along the last commit belonging to that
  // epoch.
  std::deque<std::pair<std::string, uint64_t>> history;

  // TODO: actually durability
  std::atomic<uint64_t> last_commit_timestamp_{kTimestampInitialId};

  void AddEpochToHistory(std::string prev_epoch);
  void AddEpochToHistoryForce(std::string prev_epoch);

 private:
  bool TryPersistReplicaClient(const replication::ReplicationClientConfig &config);
  bool ShouldStoreAndRestoreReplicationState() const { return nullptr != durability_; }

  // NOTE: Server is not in MAIN it is in REPLICA
  std::unique_ptr<ReplicationServer> replication_server_{nullptr};

  // We create ReplicationClient using unique_ptr so we can move
  // newly created client into the vector.
  // We cannot move the client directly because it contains ThreadPool
  // which cannot be moved. Also, the move is necessary because
  // we don't want to create the client directly inside the vector
  // because that would require the lock on the list putting all
  // commits (they iterate list of clients) to halt.
  // This way we can initialize client in main thread which means
  // that we can immediately notify the user if the initialization
  // failed.
  using ReplicationClientList = utils::Synchronized<std::vector<std::unique_ptr<ReplicationClient>>, utils::SpinLock>;
  ReplicationClientList replication_clients_;

  std::unique_ptr<kvstore::KVStore> durability_;
};

}  // namespace memgraph::storage
