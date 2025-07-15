// Copyright 2025 Memgraph Ltd.
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
#include "replication/replication_client.hpp"
#include "replication_coordination_glue/messages.hpp"
#include "rpc/client.hpp"
#include "storage/v2/database_access.hpp"
#include "storage/v2/durability/storage_global_operation.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/global.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

#include <concepts>
#include <optional>
#include <set>
#include <string>

namespace memgraph::storage {

struct Delta;
struct Vertex;
struct Edge;
class Storage;
class ReplicationStorageClient;

// Handler used for transferring the current transaction.
// You need to acquire the RPC lock before creating ReplicaStream object
class ReplicaStream {
 public:
  explicit ReplicaStream(Storage *storage, rpc::Client::StreamHandler<replication::PrepareCommitRpc> stream);
  ReplicaStream(ReplicaStream const &) = delete;
  ReplicaStream &operator=(ReplicaStream const &) = delete;
  ReplicaStream(ReplicaStream &&) = default;
  ReplicaStream &operator=(ReplicaStream &&) = default;
  ~ReplicaStream() = default;

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
  void AppendOperation(durability::StorageMetadataOperation operation, EdgeTypeId edge_type,
                       const std::set<PropertyId> &properties, uint64_t timestamp);

  /// @throw rpc::RpcFailedException
  replication::PrepareCommitRes Finalize();

  bool IsDefunct() const { return stream_.IsDefunct(); }

  auto encoder() -> replication::Encoder { return replication::Encoder{stream_.GetBuilder()}; }

  auto GetStreamHandler() -> rpc::Client::StreamHandler<replication::PrepareCommitRpc> && { return std::move(stream_); }

 private:
  Storage *storage_;
  rpc::Client::StreamHandler<replication::PrepareCommitRpc> stream_;
};

class ReplicaStreamExecutor {
 public:
  explicit ReplicaStreamExecutor(std::optional<ReplicaStream> stream) : stream_(std::move(stream)) {}
  void operator()() const {}

 private:
  std::optional<ReplicaStream> stream_;
};

template <typename F>
concept InvocableWithStream = std::invocable<F, ReplicaStream &>;

class ReplicationStorageClient {
  friend class InMemoryCurrentWalHandler;
  friend class ReplicaStream;
  friend struct ::memgraph::replication::ReplicationClient;

 public:
  explicit ReplicationStorageClient(::memgraph::replication::ReplicationClient &client, utils::UUID main_uuid);

  ReplicationStorageClient(ReplicationStorageClient const &) = delete;
  ReplicationStorageClient &operator=(ReplicationStorageClient const &) = delete;
  ReplicationStorageClient(ReplicationStorageClient &&) noexcept = delete;
  ReplicationStorageClient &operator=(ReplicationStorageClient &&) noexcept = delete;

  ~ReplicationStorageClient() = default;

  auto Mode() const -> replication_coordination_glue::ReplicationMode { return client_.mode_; }
  auto Name() const -> std::string const & { return client_.name_; }
  auto Endpoint() const -> io::network::Endpoint const & { return client_.rpc_client_.Endpoint(); }
  void AbortRpcClient() const { client_.rpc_client_.Abort(); }

  void SetMaybeBehind() {
    replica_state_.WithLock([](auto &val) { val = replication::ReplicaState::MAYBE_BEHIND; });
  }
  auto State() const -> replication::ReplicaState { return *replica_state_.Lock(); }

  auto GetTimestampInfo(Storage const *storage) const -> TimestampInfo;

  /**
   * @brief Check the replica state
   *
   * @param storage pointer to the storage associated with the client
   * @param db_acc gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void Start(Storage *storage, DatabaseAccessProtector db_acc);

  /**
   * @brief Start a new transaction replication (open up a stream)
   *
   * @param current_wal_seq_num
   * @param storage pointer to the storage associated with the client
   * @param db_acc gatekeeper access that protects the database; std::any to have separation between dbms and storage
   * @param commit_immediately If set to true, replicas will commit immediately upon finalizing txn replication instead
   * of waiting for MAIN's 2nd phase in 2PC protocol.
   */
  auto StartTransactionReplication(uint64_t current_wal_seq_num, Storage *storage, DatabaseAccessProtector db_acc,
                                   bool commit_immediately) -> std::optional<ReplicaStream>;

  // Replication clients can be removed at any point
  // so to avoid any complexity of checking if the client was removed whenever
  // we want to send part of transaction and to avoid adding some GC logic this
  // function will run a callback if, after previously calling
  // StartTransactionReplication, stream is created.
  template <InvocableWithStream F>
  void IfStreamingTransaction(F &&callback, std::optional<ReplicaStream> &replica_stream) {
    // We can only check the state because it guarantees to be only
    // valid during a single transaction replication (if the assumption
    // that this and other transaction replication functions can only be
    // called from a one thread stands)
    if (State() != replication::ReplicaState::REPLICATING) {
      return;
    }
    if (!replica_stream || replica_stream->IsDefunct()) {
      replica_state_.WithLock([&replica_stream](auto &state) {
        replica_stream.reset();
        state = replication::ReplicaState::MAYBE_BEHIND;
      });
      LogRpcFailure();
      return;
    }
    try {
      callback(*replica_stream);  // failure state what if not streaming (std::nullopt)
    } catch (const rpc::RpcFailedException &) {
      // We don't need to reset replica stream here, as it is destroyed when object goes out of scope
      // in FinalizeTransactionReplication function
      replica_state_.WithLock([&replica_stream](auto &state) {
        replica_stream.reset();
        state = replication::ReplicaState::MAYBE_BEHIND;
      });
      LogRpcFailure();
    }
  }

  /**
   * @brief Return whether the transaction could be finalized on the replication client or not.
   *
   * @param db_acc gatekeeper access that protects the database; std::any to have separation between dbms and storage
   * @param replica_stream replica stream to finalize the transaction on
   * @param durability_commit_timestamp
   * @return true
   * @return false
   */
  [[nodiscard]] bool FinalizePrepareCommitPhase(DatabaseAccessProtector db_acc,
                                                std::optional<ReplicaStream> &replica_stream,
                                                uint64_t durability_commit_timestamp) const;

  bool FinalizeTransactionReplication(DatabaseAccessProtector db_acc, std::optional<ReplicaStream> &&replica_stream,
                                      uint64_t durability_commit_timestamp) const;

  [[nodiscard]] bool SendFinalizeCommitRpc(bool decision, utils::UUID const &storage_uuid,
                                           DatabaseAccessProtector db_acc, uint64_t durability_commit_timestamp,
                                           std::optional<ReplicaStream> &&replica_stream) noexcept;

  /**
   * @brief Asynchronously try to check the replica state and start a recovery thread if necessary
   *
   * @param main_storage pointer to the storage associated with the client
   * @param db_acc gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void TryCheckReplicaStateAsync(Storage *main_storage, DatabaseAccessProtector db_acc);

  /**
   * @brief Force reset a replica.
   * @param main_storage pointer to the storage associated with the client
   * @param db_acc gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void ForceRecoverReplica(Storage *storage, DatabaseAccessProtector db_acc) const;

 private:
  /**
   * @brief Get necessary recovery steps and execute them.
   *
   * @param replica_last_commit_ts the commit up to which we should recover to
   * @param main_storage pointer to the storage associated with the client
   * @param reset_needed If true, replica needs to reset its storage when the 1st recovery step is sent.
   */
  void RecoverReplica(uint64_t replica_last_commit_ts, Storage *main_storage, bool reset_needed = false) const;

  /**
   * @brief Check replica state
   *
   * @param main_storage pointer to the storage associated with the client
   * @param db_acc gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void UpdateReplicaState(Storage *main_storage, DatabaseAccessProtector db_acc);

  /**
   * @brief Forcefully reset storage to as it is when started from scratch.
   *
   */
  void LogRpcFailure() const;

  /**
   * @brief Synchronously try to check the replica state and start a recovery thread if necessary
   *
   * @param main_storage pointer to the storage associated with the client
   * @param db_acc gatekeeper access that protects the database; std::any to have separation between dbms and storage
   */
  void TryCheckReplicaStateSync(Storage *main_storage, DatabaseAccessProtector db_acc);

  ::memgraph::replication::ReplicationClient &client_;
  mutable std::atomic<uint64_t> last_known_ts_{0};
  mutable utils::Synchronized<replication::ReplicaState, utils::SpinLock> replica_state_{
      replication::ReplicaState::MAYBE_BEHIND};

  const utils::UUID main_uuid_;
};

}  // namespace memgraph::storage
