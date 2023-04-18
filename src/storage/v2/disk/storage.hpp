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

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <shared_mutex>
#include <span>
#include <variant>

#include "io/network/endpoint.hpp"
#include "kvstore/kvstore.hpp"
#include "storage/v2/commit_log.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/constraints.hpp"
#include "storage/v2/disk/disk_edge.hpp"
#include "storage/v2/disk/vertex_accessor.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/exceptions.hpp"
#include "utils/file_locker.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/rw_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

/// REPLICATION ///
#include "rpc/server.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "storage/v2/storage_error.hpp"

/// ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>

namespace memgraph::storage {

class DiskStorage final : public Storage {
 public:
  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit DiskStorage();

  ~DiskStorage();

  class DiskAccessor final : public Storage::Accessor {
   private:
    friend class DiskStorage;

    explicit DiskAccessor(DiskStorage *storage, IsolationLevel isolation_level);

   public:
    DiskAccessor(const DiskAccessor &) = delete;
    DiskAccessor &operator=(const DiskAccessor &) = delete;
    DiskAccessor &operator=(DiskAccessor &&other) = delete;

    // NOTE: After the accessor is moved, all objects derived from it (accessors
    // and iterators) are *invalid*. You have to get all derived objects again.
    DiskAccessor(DiskAccessor &&other) noexcept;

    ~DiskAccessor() override;

    /// @throw std::alloc
    std::unique_ptr<VertexAccessor> CreateVertex() override;

    std::unique_ptr<VertexAccessor> FindVertex(Gid gid, View view) override;

    /// Utility method to load all vertices from the underlying KV storage.
    VerticesIterable Vertices(View view) override;

    /// Utility method to load all vertices from the underlying KV storage with label `label`.
    VerticesIterable Vertices(LabelId label, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) override;

    int64_t ApproximateVertexCount() const override {
      throw utils::NotYetImplemented("ApproximateVertexCount() is not implemented for DiskStorage.");
    }

    int64_t ApproximateVertexCount(LabelId label) const override {
      throw utils::NotYetImplemented("ApproximateVertexCount(label) is not implemented for DiskStorage.");
    }

    int64_t ApproximateVertexCount(LabelId label, PropertyId property) const override {
      throw utils::NotYetImplemented("ApproximateVertexCount(label, property) is not implemented for DiskStorage.");
    }

    int64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const override {
      throw utils::NotYetImplemented(
          "ApproximateVertexCount(label, property, value) is not implemented for DiskStorage.");
    }

    int64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                   const std::optional<utils::Bound<PropertyValue>> &lower,
                                   const std::optional<utils::Bound<PropertyValue>> &upper) const override {
      throw utils::NotYetImplemented(
          "ApproximateVertexCount(label, property, lower, upper) is not implemented for DiskStorage.");
    }

    std::optional<storage::IndexStats> GetIndexStats(const storage::LabelId &label,
                                                     const storage::PropertyId &property) const override {
      throw utils::NotYetImplemented("GetIndexStats() is not implemented for DiskStorage.");
    }

    std::vector<std::pair<LabelId, PropertyId>> ClearIndexStats() override {
      throw utils::NotYetImplemented("ClearIndexStats() is not implemented for DiskStorage.");
    }

    std::vector<std::pair<LabelId, PropertyId>> DeleteIndexStatsForLabels(
        const std::span<std::string> labels) override {
      throw utils::NotYetImplemented("DeleteIndexStatsForLabels(labels) is not implemented for DiskStorage.");
    }

    void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                       const IndexStats &stats) override {
      throw utils::NotYetImplemented("SetIndexStats(stats) is not implemented for DiskStorage.");
    }

    Result<std::unique_ptr<VertexAccessor>> DeleteVertex(VertexAccessor *vertex) override;

    Result<std::optional<std::pair<std::unique_ptr<VertexAccessor>, std::vector<std::unique_ptr<EdgeAccessor>>>>>
    DetachDeleteVertex(VertexAccessor *vertex) override;

    Result<std::unique_ptr<EdgeAccessor>> CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                     EdgeTypeId edge_type) override;

    Result<std::unique_ptr<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge) override;

    Result<std::vector<std::unique_ptr<EdgeAccessor>>> DeleteEdges(const auto &edge_accessors);

    const std::string &LabelToName(LabelId label) const override;
    const std::string &PropertyToName(PropertyId property) const override;
    const std::string &EdgeTypeToName(EdgeTypeId edge_type) const override;

    /// @throw std::bad_alloc if unable to insert a new mapping
    LabelId NameToLabel(std::string_view name) override;

    /// @throw std::bad_alloc if unable to insert a new mapping
    PropertyId NameToProperty(std::string_view name) override;

    /// @throw std::bad_alloc if unable to insert a new mapping
    EdgeTypeId NameToEdgeType(std::string_view name) override;

    bool LabelIndexExists(LabelId label) const override {
      throw utils::NotYetImplemented("LabelIndexExists() is not implemented for DiskStorage.");
    }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const override {
      throw utils::NotYetImplemented("LabelPropertyIndexExists() is not implemented for DiskStorage.");
    }

    IndicesInfo ListAllIndices() const override {
      throw utils::NotYetImplemented("ListAllIndices() is not implemented for DiskStorage.");
    }

    ConstraintsInfo ListAllConstraints() const override {
      throw utils::NotYetImplemented("ListAllConstraints() is not implemented for DiskStorage.");
    }

    void AdvanceCommand() override;

    utils::BasicResult<StorageDataManipulationError, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp = {}) override;

    /// @throw std::bad_alloc
    void Abort() override;

    void FinalizeTransaction() override;

    std::optional<uint64_t> GetTransactionId() const override;

   private:
    /// @throw std::bad_alloc
    std::unique_ptr<VertexAccessor> CreateVertex(storage::Gid gid);

    /// @throw std::bad_alloc
    Result<std::unique_ptr<EdgeAccessor>> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type,
                                                     storage::Gid gid);

    // (De)serialization utility methods

    /// Serialize types defined with STORAGE_DEFINE_ID_TYPE
    std::string SerializeIdType(const auto &id) const;

    /// Deserialize types defined with STORAGE_DEFINE_ID_TYPE
    static auto DeserializeIdType(const std::string &str);

    /// Serialize timestamp to string
    static std::string SerializeTimestamp(uint64_t ts);

    /// Serialize labels to string
    std::string SerializeLabels(const auto &&labels) const;

    /// Serialize vertex to string as a key in KV store
    /// label1, label2 | GID | commit_timestamp
    std::string SerializeVertex(const VertexAccessor *vertex_acc) const;

    /// Serialize edge to string as a key in KV store
    /// vertex_gid_1 | vertex_gid_2 | direction | edge_type | GID | commit_timestamp
    std::pair<std::string, std::string> SerializeEdge(EdgeAccessor *edge_acc) const;

    /// Deserializes vertex from the string key and stores it into the vertices_ cache.
    /// Properties are deserialized from the value.
    std::unique_ptr<VertexAccessor> DeserializeVertex(std::string_view key, std::string_view value);

    /// Deserializes edge from the string key and stores it into the edges_ cache.
    /// Properties are deserialized from the value.
    std::unique_ptr<EdgeAccessor> DeserializeEdge(std::string_view key, std::string_view value);

    /// Flushes vertices and edges to the disk with the commit timestamp.
    /// At the time of calling, the commit_timestamp_ must already exist.
    /// After this method, the vertex and edge caches are cleared.
    void FlushCache();

    DiskStorage *storage_;
    /// Accessor is tighly coupled with the transaction and we store read/write set per transaction. That's why objects
    /// need to be stored here.
    utils::SkipList<storage::Vertex> vertices_;
    utils::SkipList<storage::Edge> edges_;

    std::shared_lock<utils::RWLock> storage_guard_;
    Transaction transaction_;
    std::optional<uint64_t> commit_timestamp_;
    bool is_transaction_active_;
    Config::Items config_;
  };

  std::unique_ptr<Storage::Accessor> Access(std::optional<IsolationLevel> override_isolation_level) override {
    return std::unique_ptr<DiskAccessor>(new DiskAccessor{this, override_isolation_level.value_or(isolation_level_)});
  }

  const std::string &LabelToName(LabelId label) const override;
  const std::string &PropertyToName(PropertyId property) const override;
  const std::string &EdgeTypeToName(EdgeTypeId edge_type) const override;

  /// @throw std::bad_alloc if unable to insert a new mapping
  LabelId NameToLabel(std::string_view name) override;

  /// @throw std::bad_alloc if unable to insert a new mapping
  PropertyId NameToProperty(std::string_view name) override;

  /// @throw std::bad_alloc if unable to insert a new mapping
  EdgeTypeId NameToEdgeType(std::string_view name) override;

  /// Create an index.
  /// Returns void if the index has been created.
  /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
  /// * `IndexDefinitionError`: the index already exists.
  /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// @throw std::bad_alloc
  utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
      LabelId label, std::optional<uint64_t> desired_commit_timestamp) override;

  /// Create an index.
  /// Returns void if the index has been created.
  /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
  /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `IndexDefinitionError`: the index already exists.
  /// @throw std::bad_alloc
  utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  /// Drop an existing index.
  /// Returns void if the index has been dropped.
  /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
  /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `IndexDefinitionError`: the index does not exist.
  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, std::optional<uint64_t> desired_commit_timestamp) override;

  /// Drop an existing index.
  /// Returns void if the index has been dropped.
  /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
  /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `IndexDefinitionError`: the index does not exist.
  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  IndicesInfo ListAllIndices() const override;

  /// Returns void if the existence constraint has been created.
  /// Returns `StorageExistenceConstraintDefinitionError` if an error occures. Error can be:
  /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `ConstraintViolation`: there is already a vertex existing that would break this new constraint.
  /// * `ConstraintDefinitionError`: the constraint already exists.
  /// @throw std::bad_alloc
  /// @throw std::length_error
  utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  /// Drop an existing existence constraint.
  /// Returns void if the existence constraint has been dropped.
  /// Returns `StorageExistenceConstraintDroppingError` if an error occures. Error can be:
  /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `ConstraintDefinitionError`: the constraint did not exists.
  utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  /// Create an unique constraint.
  /// Returns `StorageUniqueConstraintDefinitionError` if an error occures. Error can be:
  /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `ConstraintViolation`: there are already vertices violating the constraint.
  /// Returns `UniqueConstraints::CreationStatus` otherwise. Value can be:
  /// * `SUCCESS` if the constraint was successfully created,
  /// * `ALREADY_EXISTS` if the constraint already existed,
  /// * `EMPTY_PROPERTIES` if the property set is empty, or
  /// * `PROPERTIES_SIZE_LIMIT_EXCEEDED` if the property set exceeds the limit of maximum number of properties.
  /// @throw std::bad_alloc
  utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus> CreateUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties, std::optional<uint64_t> desired_commit_timestamp) override;

  /// Removes an existing unique constraint.
  /// Returns `StorageUniqueConstraintDroppingError` if an error occures. Error can be:
  /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// Returns `UniqueConstraints::DeletionStatus` otherwise. Value can be:
  /// * `SUCCESS` if constraint was successfully removed,
  /// * `NOT_FOUND` if the specified constraint was not found,
  /// * `EMPTY_PROPERTIES` if the property set is empty, or
  /// * `PROPERTIES_SIZE_LIMIT_EXCEEDED` if the property set exceeds the limit of maximum number of properties.
  utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus> DropUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties, std::optional<uint64_t> desired_commit_timestamp) override;

  ConstraintsInfo ListAllConstraints() const override;

  StorageInfo GetInfo() const override;

  bool LockPath() override;
  bool UnlockPath() override;

  bool SetReplicaRole(io::network::Endpoint endpoint, const replication::ReplicationServerConfig &config) override;

  bool SetMainReplicationRole() override;

  /// @pre The instance should have a MAIN role
  /// @pre Timeout can only be set for SYNC replication
  utils::BasicResult<RegisterReplicaError, void> RegisterReplica(
      std::string name, io::network::Endpoint endpoint, replication::ReplicationMode replication_mode,
      replication::RegistrationMode registration_mode, const replication::ReplicationClientConfig &config) override;
  /// @pre The instance should have a MAIN role
  bool UnregisterReplica(const std::string &name) override;

  std::optional<replication::ReplicaState> GetReplicaState(std::string_view name) override;

  ReplicationRole GetReplicationRole() const override;

  std::vector<ReplicaInfo> ReplicasInfo() override;

  void FreeMemory() override;

  void SetIsolationLevel(IsolationLevel isolation_level) override;

  utils::BasicResult<CreateSnapshotError> CreateSnapshot() override;

 private:
  Transaction CreateTransaction(IsolationLevel isolation_level);

  /// The force parameter determines the behaviour of the garbage collector.
  /// If it's set to true, it will behave as a global operation, i.e. it can't
  /// be part of a transaction, and no other transaction can be active at the same time.
  /// This allows it to delete immediately vertices without worrying that some other
  /// transaction is possibly using it. If there are active transactions when this method
  /// is called with force set to true, it will fallback to the same method with the force
  /// set to false.
  /// If it's set to false, it will execute in parallel with other transactions, ensuring
  /// that no object in use can be deleted.
  /// @throw std::system_error
  /// @throw std::bad_alloc
  template <bool force>
  void CollectGarbage();

  bool InitializeWalFile();
  void FinalizeWalFile();

  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataManipulation(const Transaction &transaction, uint64_t final_commit_timestamp);
  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataDefinition(durability::StorageGlobalOperation operation, LabelId label,
                                               const std::set<PropertyId> &properties, uint64_t final_commit_timestamp);

  uint64_t commitTimestamp(std::optional<uint64_t> desired_commit_timestamp = {});

  void RestoreReplicas();

  bool ShouldStoreAndRestoreReplicas() const;

  // Main storage lock.
  //
  // Accessors take a shared lock when starting, so it is possible to block
  // creation of new accessors by taking a unique lock. This is used when doing
  // operations on storage that affect the global state, for example index
  // creation.
  mutable utils::RWLock main_lock_{utils::RWLock::Priority::WRITE};

  std::atomic<uint64_t> vertex_id_{0};
  std::atomic<uint64_t> edge_id_{0};
  // Even though the edge count is already kept in the `edges_` SkipList, the
  // list is used only when properties are enabled for edges. Because of that we
  // keep a separate count of edges that is always updated.
  std::atomic<uint64_t> edge_count_{0};

  NameIdMapper name_id_mapper_;

  Constraints constraints_;
  Indices indices_;

  // Transaction engine
  utils::SpinLock engine_lock_;
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};
  // TODO: This isn't really a commit log, it doesn't even care if a
  // transaction commited or aborted. We could probably combine this with
  // `timestamp_` in a sensible unit, something like TransactionClock or
  // whatever.
  std::optional<CommitLog> commit_log;

  utils::Synchronized<std::list<Transaction>, utils::SpinLock> committed_transactions_;
  IsolationLevel isolation_level_;

  Config config_;
  utils::Scheduler gc_runner_;
  std::mutex gc_lock_;

  // Undo buffers that were unlinked and now are waiting to be freed.
  utils::Synchronized<std::list<std::pair<uint64_t, std::list<Delta>>>, utils::SpinLock> garbage_undo_buffers_;

  // Vertices that are logically deleted but still have to be removed from
  // indices before removing them from the main storage.
  utils::Synchronized<std::list<Gid>, utils::SpinLock> deleted_vertices_;

  // Vertices that are logically deleted and removed from indices and now wait
  // to be removed from the main storage.
  std::list<std::pair<uint64_t, Gid>> garbage_vertices_;

  // Edges that are logically deleted and wait to be removed from the main
  // storage.
  utils::Synchronized<std::list<Gid>, utils::SpinLock> deleted_edges_;

  // Durability
  std::filesystem::path snapshot_directory_;
  std::filesystem::path wal_directory_;
  std::filesystem::path lock_file_path_;
  utils::OutputFile lock_file_handle_;
  std::unique_ptr<kvstore::KVStore> storage_;

  utils::Scheduler snapshot_runner_;
  utils::SpinLock snapshot_lock_;

  // UUID used to distinguish snapshots and to link snapshots to WALs
  std::string uuid_;
  // Sequence number used to keep track of the chain of WALs.
  uint64_t wal_seq_num_{0};

  // UUID to distinguish different main instance runs for replication process
  // on SAME storage.
  // Multiple instances can have same storage UUID and be MAIN at the same time.
  // We cannot compare commit timestamps of those instances if one of them
  // becomes the replica of the other so we use epoch_id_ as additional
  // discriminating property.
  // Example of this:
  // We have 2 instances of the same storage, S1 and S2.
  // S1 and S2 are MAIN and accept their own commits and write them to the WAL.
  // At the moment when S1 commited a transaction with timestamp 20, and S2
  // a different transaction with timestamp 15, we change S2's role to REPLICA
  // and register it on S1.
  // Without using the epoch_id, we don't know that S1 and S2 have completely
  // different transactions, we think that the S2 is behind only by 5 commits.
  std::string epoch_id_;
  // History of the previous epoch ids.
  // Each value consists of the epoch id along the last commit belonging to that
  // epoch.
  std::deque<std::pair<std::string, uint64_t>> epoch_history_;

  std::optional<durability::WalFile> wal_file_;
  uint64_t wal_unsynced_transactions_{0};

  utils::FileRetainer file_retainer_;

  // Global locker that is used for clients file locking
  utils::FileRetainer::FileLocker global_locker_;

  // Last commited timestamp
  std::atomic<uint64_t> last_commit_timestamp_{kTimestampInitialId};

  class ReplicationServer;
  std::unique_ptr<ReplicationServer> replication_server_{nullptr};

  class ReplicationClient;
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

  std::atomic<ReplicationRole> replication_role_{ReplicationRole::MAIN};

  rocksdb::Options options_;
  rocksdb::DB *db_;
  rocksdb::ColumnFamilyHandle *vertex_chandle = nullptr;
  rocksdb::ColumnFamilyHandle *edge_chandle = nullptr;
};

}  // namespace memgraph::storage
