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

#include "rpc/client.hpp"
#include "storage/v2/durability/storage_global_operation.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/global.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/file.hpp"
#include "utils/scheduler.hpp"
#include "utils/thread_pool.hpp"

namespace memgraph::storage {

class Storage;

class ReplicationClient;

// Handler used for transfering the current transaction.
class ReplicaStream {
 public:
  explicit ReplicaStream(ReplicationClient *self, uint64_t previous_commit_timestamp, uint64_t current_seq_num);

  /// @throw rpc::RpcFailedException
  void AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t final_commit_timestamp);

  /// @throw rpc::RpcFailedException
  void AppendDelta(const Delta &delta, const Edge &edge, uint64_t final_commit_timestamp);

  /// @throw rpc::RpcFailedException
  void AppendTransactionEnd(uint64_t final_commit_timestamp);

  /// @throw rpc::RpcFailedException
  void AppendOperation(durability::StorageGlobalOperation operation, LabelId label,
                       const std::set<PropertyId> &properties, uint64_t timestamp);

  /// @throw rpc::RpcFailedException
  replication::AppendDeltasRes Finalize();

 private:
  ReplicationClient *self_;
  rpc::Client::StreamHandler<replication::AppendDeltasRpc> stream_;
};

class ReplicationClient {
  friend class CurrentWalHandler;
  friend class ReplicaStream;

 public:
  ReplicationClient(Storage *storage, std::string name, memgraph::io::network::Endpoint endpoint,
                    replication::ReplicationMode mode, const replication::ReplicationClientConfig &config);

  ReplicationClient(ReplicationClient const &) = delete;
  ReplicationClient &operator=(ReplicationClient const &) = delete;
  ReplicationClient(ReplicationClient &&) noexcept = delete;
  ReplicationClient &operator=(ReplicationClient &&) noexcept = delete;

  virtual ~ReplicationClient();

  virtual void Start() = 0;

  const auto &Name() const { return name_; }

  auto State() const { return replica_state_.load(); }

  auto Mode() const { return mode_; }

  auto Endpoint() const -> io::network::Endpoint const & { return rpc_client_.Endpoint(); }

  virtual void StartTransactionReplication(uint64_t current_wal_seq_num) = 0;
  virtual void IfStreamingTransaction(const std::function<void(ReplicaStream &)> &callback) = 0;
  virtual auto GetEpochId() const -> std::string const & = 0;  // TODO: make non-virtual once epoch is moved to storage
  virtual auto GetStorage() -> Storage * = 0;
  [[nodiscard]] virtual bool FinalizeTransactionReplication() = 0;
  virtual TimestampInfo GetTimestampInfo() = 0;

 protected:
  std::string name_;
  communication::ClientContext rpc_context_;
  rpc::Client rpc_client_;
  std::chrono::seconds replica_check_frequency_;

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
  //    Not having mulitple possible threads in the same client allows us
  //    to ignore concurrency problems inside the client.
  utils::ThreadPool thread_pool_{1};
  std::atomic<replication::ReplicaState> replica_state_{replication::ReplicaState::INVALID};

  utils::Scheduler replica_checker_;
  Storage *storage_;
};

}  // namespace memgraph::storage
