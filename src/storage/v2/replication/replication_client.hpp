// Copyright 2024 Memgraph Ltd.
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
#include "replication/messages.hpp"
#include "replication/replication_client.hpp"
#include "rpc/client.hpp"
#include "storage/v2/database_access.hpp"
#include "storage/v2/durability/storage_global_operation.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/global.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "utils/file_locker.hpp"
#include "utils/scheduler.hpp"
#include "utils/synchronized.hpp"
#include "utils/thread_pool.hpp"

#include <atomic>
#include <concepts>
#include <functional>
#include <optional>
#include <set>
#include <string>
#include <variant>

namespace memgraph::storage {

struct Delta;
struct Vertex;
struct Edge;
class Storage;
class ReplicationStorageClient;

// Handler used for transferring the current transaction.
class ReplicaStream {
 public:
  explicit ReplicaStream(Storage *storage, rpc::Client &rpc_client, uint64_t current_seq_num);

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

template <typename F>
concept InvocableWithStream = std::invocable<F, ReplicaStream &>;

// TODO Rename to something without the word "client"
class ReplicationStorageClient {
  friend class InMemoryCurrentWalHandler;
  friend class ReplicaStream;
  friend struct ::memgraph::replication::ReplicationClient;

 public:
  explicit ReplicationStorageClient(::memgraph::replication::ReplicationClient &client);

  ReplicationStorageClient(ReplicationStorageClient const &) = delete;
  ReplicationStorageClient &operator=(ReplicationStorageClient const &) = delete;
  ReplicationStorageClient(ReplicationStorageClient &&) noexcept = delete;
  ReplicationStorageClient &operator=(ReplicationStorageClient &&) noexcept = delete;

  ~ReplicationStorageClient() = default;

  // TODO Remove the client related functions
  auto Mode() const -> memgraph::replication::ReplicationMode { return client_.mode_; }
  auto Name() const -> std::string const & { return client_.name_; }
  auto Endpoint() const -> io::network::Endpoint const & { return client_.rpc_client_.Endpoint(); }

  auto State() const -> replication::ReplicaState { return replica_state_.WithLock(std::identity()); }
  auto GetTimestampInfo(Storage const *storage) -> TimestampInfo;

  /**
   * @brief Check the replica state
   *
   * @param storage pointer to the storage associated with the client
   * @param gk gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void Start(Storage *storage, DatabaseAccessProtector db_acc);

  /**
   * @brief Start a new transaction replication (open up a stream)
   *
   * @param current_wal_seq_num
   * @param storage pointer to the storage associated with the client
   * @param gk gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void StartTransactionReplication(uint64_t current_wal_seq_num, Storage *storage, DatabaseAccessProtector db_acc);

  // Replication clients can be removed at any point
  // so to avoid any complexity of checking if the client was removed whenever
  // we want to send part of transaction and to avoid adding some GC logic this
  // function will run a callback if, after previously calling
  // StartTransactionReplication, stream is created.
  template <InvocableWithStream F>
  void IfStreamingTransaction(F &&callback) {
    // We can only check the state because it guarantees to be only
    // valid during a single transaction replication (if the assumption
    // that this and other transaction replication functions can only be
    // called from a one thread stands)
    if (State() != replication::ReplicaState::REPLICATING) {
      return;
    }
    if (!replica_stream_ || replica_stream_->IsDefunct()) {
      replica_state_.WithLock([this](auto &state) {
        replica_stream_.reset();
        state = replication::ReplicaState::MAYBE_BEHIND;
      });
      LogRpcFailure();
      return;
    }
    try {
      callback(*replica_stream_);  // failure state what if not streaming (std::nullopt)
    } catch (const rpc::RpcFailedException &) {
      replica_state_.WithLock([](auto &state) { state = replication::ReplicaState::MAYBE_BEHIND; });
      LogRpcFailure();
      return;
    }
  }

  /**
   * @brief Return whether the transaction could be finalized on the replication client or not.
   *
   * @param storage pointer to the storage associated with the client
   * @param gk gatekeeper access that protects the database; std::any to have separation between dbms and storage
   * @return true
   * @return false
   */
  [[nodiscard]] bool FinalizeTransactionReplication(Storage *storage, DatabaseAccessProtector db_acc);

  /**
   * @brief Asynchronously try to check the replica state and start a recovery thread if necessary
   *
   * @param storage pointer to the storage associated with the client
   * @param gk gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void TryCheckReplicaStateAsync(Storage *storage, DatabaseAccessProtector db_acc);  // TODO Move back to private

  auto &Client() { return client_; }

 private:
  /**
   * @brief Get necessary recovery steps and execute them.
   *
   * @param replica_commit the commit up to which we should recover to
   * @param gk gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void RecoverReplica(uint64_t replica_commit, memgraph::storage::Storage *storage);

  /**
   * @brief Check replica state
   *
   * @param storage pointer to the storage associated with the client
   * @param gk gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void UpdateReplicaState(Storage *storage, DatabaseAccessProtector db_acc);

  void LogRpcFailure();

  /**
   * @brief Synchronously try to check the replica state and start a recovery thread if necessary
   *
   * @param storage pointer to the storage associated with the client
   * @param gk gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void TryCheckReplicaStateSync(Storage *storage, DatabaseAccessProtector db_acc);

  ::memgraph::replication::ReplicationClient &client_;
  // TODO Do not store the stream, make is a local variable
  std::optional<ReplicaStream>
      replica_stream_;  // Currently active stream (nullopt if not in use), note: a single stream per rpc client
  mutable utils::Synchronized<replication::ReplicaState, utils::SpinLock> replica_state_{
      replication::ReplicaState::MAYBE_BEHIND};
};

}  // namespace memgraph::storage
