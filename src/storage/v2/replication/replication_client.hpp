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

#include "replication/config.hpp"
#include "replication/epoch.hpp"
#include "rpc/client.hpp"
#include "storage/v2/durability/storage_global_operation.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/global.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "utils/file_locker.hpp"
#include "utils/scheduler.hpp"
#include "utils/thread_pool.hpp"

#include <atomic>
#include <optional>
#include <set>
#include <string>
#include <variant>

namespace memgraph::storage {

struct Delta;
struct Vertex;
struct Edge;
class Storage;
class ReplicationClient;

// Handler used for transferring the current transaction.
class ReplicaStream {
 public:
  explicit ReplicaStream(Storage *storage, rpc::Client &rpc_client, const uint64_t current_seq_num);

  /// @throw rpc::RpcFailedException
  void AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t final_commit_timestamp);

  /// @throw rpc::RpcFailedException
  void AppendDelta(const Delta &delta, const Edge &edge, uint64_t final_commit_timestamp);

  /// @throw rpc::RpcFailedException
  void AppendTransactionEnd(uint64_t final_commit_timestamp);

  /// @throw rpc::RpcFailedException
  void AppendOperation(durability::StorageMetadataOperation operation, LabelId label,
                       const std::set<PropertyId> &properties, const LabelIndexStats &stats,
                       const LabelPropertyIndexStats &property_stats, uint64_t timestamp);

  /// @throw rpc::RpcFailedException
  replication::AppendDeltasRes Finalize();

  bool IsDefunct() const { return stream_.IsDefunct(); }

 private:
  Storage *storage_;
  rpc::Client::StreamHandler<replication::AppendDeltasRpc> stream_;
};

class ReplicationClient {
  friend class CurrentWalHandler;
  friend class ReplicaStream;

 public:
  ReplicationClient(const memgraph::replication::ReplicationClientConfig &config, uint64_t id);

  ReplicationClient(ReplicationClient const &) = delete;
  ReplicationClient &operator=(ReplicationClient const &) = delete;
  ReplicationClient(ReplicationClient &&) noexcept = delete;
  ReplicationClient &operator=(ReplicationClient &&) noexcept = delete;

  virtual ~ReplicationClient();

  auto Mode() const -> memgraph::replication::ReplicationMode { return mode_; }
  auto Name() const -> std::string const & { return name_; }
  auto Endpoint() const -> io::network::Endpoint const & { return rpc_client_.Endpoint(); }
  auto ID() const -> uint64_t { return id_; }
  auto GetTimestampInfo(Storage *storage, std::atomic<replication::ReplicaState> &replica_state) -> TimestampInfo;

  replication::ReplicaState Start(Storage *storage);
  void StartTransactionReplication(uint64_t const current_wal_seq_num, Storage *storage,
                                   std::atomic<replication::ReplicaState> &replica_state);
  // Replication clients can be removed at any point
  // so to avoid any complexity of checking if the client was removed whenever
  // we want to send part of transaction and to avoid adding some GC logic this
  // function will run a callback if, after previously callling
  // StartTransactionReplication, stream is created.
  void IfStreamingTransaction(const std::function<void(ReplicaStream &)> &callback, Storage *storage,
                              std::atomic<replication::ReplicaState> &replica_state);
  // Return whether the transaction could be finalized on the replication client or not.
  [[nodiscard]] bool FinalizeTransactionReplication(Storage *storage,
                                                    std::atomic<replication::ReplicaState> &replica_state);

  void StartFrequentCheck(Storage *storage, std::atomic<replication::ReplicaState> &replica_state);
  void StopFrequentCheck();

 protected:
  virtual void RecoverReplica(uint64_t replica_commit, memgraph::storage::Storage *storage) = 0;

  void InitializeReplicaState(Storage *storage, std::atomic<replication::ReplicaState> &replica_state);
  void HandleRpcFailure(Storage *storage);
  void TryInitializeClientAsync(Storage *storage);
  void TryInitializeClientSync(Storage *storage, std::atomic<replication::ReplicaState> &replica_state);
  void FrequentCheck(Storage *storage, std::atomic<replication::ReplicaState> &replica_state);

  uint64_t id_;
  std::string name_;
  communication::ClientContext rpc_context_;
  rpc::Client rpc_client_;
  std::chrono::seconds replica_check_frequency_;

  std::optional<ReplicaStream> replica_stream_;
  memgraph::replication::ReplicationMode mode_{memgraph::replication::ReplicationMode::SYNC};

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
  //    Not having mulitple possible threads in the same client allows us
  //    to ignore concurrency problems inside the client.
  utils::ThreadPool thread_pool_{1};

  utils::Scheduler replica_checker_;
};

}  // namespace memgraph::storage
