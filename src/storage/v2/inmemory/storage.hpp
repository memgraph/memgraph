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

#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/storage.hpp"

/// REPLICATION ///
#include "rpc/server.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/replication_persistence_helper.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"

namespace memgraph::storage {

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

class InMemoryStorage final : public Storage {
 public:
  enum class RegisterReplicaError : uint8_t {
    NAME_EXISTS,
    END_POINT_EXISTS,
    CONNECTION_FAILED,
    COULD_NOT_BE_PERSISTED
  };

  struct TimestampInfo {
    uint64_t current_timestamp_of_replica;
    uint64_t current_number_of_timestamp_behind_master;
  };

  struct ReplicaInfo {
    std::string name;
    replication::ReplicationMode mode;
    io::network::Endpoint endpoint;
    replication::ReplicaState state;
    TimestampInfo timestamp_info;
  };

  enum class CreateSnapshotError : uint8_t {
    DisabledForReplica,
    DisabledForAnalyticsPeriodicCommit,
    ReachedMaxNumTries
  };

  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit InMemoryStorage(Config config = Config());

  InMemoryStorage(const InMemoryStorage &) = delete;
  InMemoryStorage(InMemoryStorage &&) = delete;
  InMemoryStorage &operator=(const InMemoryStorage &) = delete;
  InMemoryStorage &operator=(InMemoryStorage &&) = delete;

  ~InMemoryStorage() override;

  class InMemoryAccessor final : public Storage::Accessor {
   private:
    friend class InMemoryStorage;

    explicit InMemoryAccessor(InMemoryStorage *storage, IsolationLevel isolation_level, StorageMode storage_mode);

   public:
    InMemoryAccessor(const InMemoryAccessor &) = delete;
    InMemoryAccessor &operator=(const InMemoryAccessor &) = delete;
    InMemoryAccessor &operator=(InMemoryAccessor &&other) = delete;

    // NOTE: After the accessor is moved, all objects derived from it (accessors
    // and iterators) are *invalid*. You have to get all derived objects again.
    InMemoryAccessor(InMemoryAccessor &&other) noexcept;

    ~InMemoryAccessor() override;

    /// @throw std::bad_alloc
    VertexAccessor CreateVertex() override;

    std::optional<VertexAccessor> FindVertex(Gid gid, View view) override;

    VerticesIterable Vertices(View view) override {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      return VerticesIterable(AllVerticesIterable(mem_storage->vertices_.access(), &transaction_, view,
                                                  &mem_storage->indices_, &mem_storage->constraints_,
                                                  mem_storage->config_.items));
    }

    VerticesIterable Vertices(LabelId label, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) override;

    /// Return approximate number of all vertices in the database.
    /// Note that this is always an over-estimate and never an under-estimate.
    uint64_t ApproximateVertexCount() const override {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      return mem_storage->vertices_.size();
    }

    /// Return approximate number of vertices with the given label.
    /// Note that this is always an over-estimate and never an under-estimate.
    uint64_t ApproximateVertexCount(LabelId label) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_index_->ApproximateVertexCount(label);
    }

    /// Return approximate number of vertices with the given label and property.
    /// Note that this is always an over-estimate and never an under-estimate.
    uint64_t ApproximateVertexCount(LabelId label, PropertyId property) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index_->ApproximateVertexCount(label,
                                                                                                              property);
    }

    /// Return approximate number of vertices with the given label and the given
    /// value for the given property. Note that this is always an over-estimate
    /// and never an under-estimate.
    uint64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index_->ApproximateVertexCount(
          label, property, value);
    }

    /// Return approximate number of vertices with the given label and value for
    /// the given property in the range defined by provided upper and lower
    /// bounds.
    uint64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                    const std::optional<utils::Bound<PropertyValue>> &lower,
                                    const std::optional<utils::Bound<PropertyValue>> &upper) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index_->ApproximateVertexCount(
          label, property, lower, upper);
    }

    template <typename TResult, typename TIndex, typename TIndexKey>
    std::optional<TResult> GetIndexStatsForIndex(TIndex *index, TIndexKey &&key) const {
      return index->GetIndexStats(key);
    }

    std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const override {
      return GetIndexStatsForIndex<storage::LabelIndexStats>(
          static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get()), label);
    }

    std::optional<storage::LabelPropertyIndexStats> GetIndexStats(const storage::LabelId &label,
                                                                  const storage::PropertyId &property) const override {
      return GetIndexStatsForIndex<storage::LabelPropertyIndexStats>(
          static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get()),
          std::make_pair(label, property));
    }

    template <typename TIndex, typename TIndexKey, typename TIndexStats>
    void SetIndexStatsForIndex(TIndex *index, TIndexKey &&key, TIndexStats &stats) const {
      index->SetIndexStats(key, stats);
    }

    void SetIndexStats(const storage::LabelId &label, const LabelIndexStats &stats) override {
      SetIndexStatsForIndex(static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get()), label, stats);
    }

    void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                       const LabelPropertyIndexStats &stats) override {
      SetIndexStatsForIndex(static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get()),
                            std::make_pair(label, property), stats);
    }

    template <typename TResult, typename TIndex>
    std::vector<TResult> ClearIndexStatsForIndex(TIndex *index) const {
      return index->ClearIndexStats();
    }

    std::vector<LabelId> ClearLabelIndexStats() override {
      return ClearIndexStatsForIndex<LabelId>(static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get()));
    }

    std::vector<std::pair<LabelId, PropertyId>> ClearLabelPropertyIndexStats() override {
      return ClearIndexStatsForIndex<std::pair<LabelId, PropertyId>>(
          static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get()));
    }

    template <typename TResult, typename TIndex>
    std::vector<TResult> DeleteIndexStatsForIndex(TIndex *index, const std::span<std::string> labels) {
      std::vector<TResult> deleted_indexes;

      for (const auto &label : labels) {
        std::vector<TResult> loc_results = index->DeleteIndexStats(NameToLabel(label));
        deleted_indexes.insert(deleted_indexes.end(), std::make_move_iterator(loc_results.begin()),
                               std::make_move_iterator(loc_results.end()));
      }
      return deleted_indexes;
    }

    std::vector<std::pair<LabelId, PropertyId>> DeleteLabelPropertyIndexStats(
        const std::span<std::string> labels) override {
      return DeleteIndexStatsForIndex<std::pair<LabelId, PropertyId>>(
          static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get()), labels);
    }

    std::vector<LabelId> DeleteLabelIndexStats(const std::span<std::string> labels) override {
      return DeleteIndexStatsForIndex<LabelId>(static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get()),
                                               labels);
    }

    /// @return Accessor to the deleted vertex if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    Result<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex) override;

    /// @return Accessor to the deleted vertex and deleted edges if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachDeleteVertex(
        VertexAccessor *vertex) override;

    void PrefetchInEdges(const VertexAccessor &vertex_acc) override{};

    void PrefetchOutEdges(const VertexAccessor &vertex_acc) override{};

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) override;

    /// Accessor to the deleted edge if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    Result<std::optional<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge) override;

    bool LabelIndexExists(LabelId label) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_index_->IndexExists(label);
    }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index_->IndexExists(label, property);
    }

    IndicesInfo ListAllIndices() const override {
      const auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      return mem_storage->ListAllIndices();
    }

    ConstraintsInfo ListAllConstraints() const override {
      const auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      return mem_storage->ListAllConstraints();
    }

    /// Returns void if the transaction has been committed.
    /// Returns `StorageDataManipulationError` if an error occures. Error can be:
    /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `ConstraintViolation`: the changes made by this transaction violate an existence or unique constraint. In this
    /// case the transaction is automatically aborted.
    /// @throw std::bad_alloc
    // NOLINTNEXTLINE(google-default-arguments)
    utils::BasicResult<StorageDataManipulationError, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp = {}) override;

    /// @throw std::bad_alloc
    void Abort() override;

    void FinalizeTransaction() override;

   private:
    /// @throw std::bad_alloc
    VertexAccessor CreateVertex(storage::Gid gid);

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type, storage::Gid gid);

    Config::Items config_;
  };

  std::unique_ptr<Storage::Accessor> Access(std::optional<IsolationLevel> override_isolation_level) override {
    return std::unique_ptr<InMemoryAccessor>(
        new InMemoryAccessor{this, override_isolation_level.value_or(isolation_level_), storage_mode_});
  }

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

  bool SetReplicaRole(io::network::Endpoint endpoint, const replication::ReplicationServerConfig &config);

  bool SetMainReplicationRole();

  /// @pre The instance should have a MAIN role
  /// @pre Timeout can only be set for SYNC replication
  utils::BasicResult<RegisterReplicaError, void> RegisterReplica(std::string name, io::network::Endpoint endpoint,
                                                                 replication::ReplicationMode replication_mode,
                                                                 replication::RegistrationMode registration_mode,
                                                                 const replication::ReplicationClientConfig &config);
  /// @pre The instance should have a MAIN role
  bool UnregisterReplica(const std::string &name);

  std::optional<replication::ReplicaState> GetReplicaState(std::string_view name);

  replication::ReplicationRole GetReplicationRole() const;

  std::vector<ReplicaInfo> ReplicasInfo();

  void FreeMemory(std::unique_lock<utils::RWLock> main_guard) override;

  utils::FileRetainer::FileLockerAccessor::ret_type IsPathLocked();
  utils::FileRetainer::FileLockerAccessor::ret_type LockPath();
  utils::FileRetainer::FileLockerAccessor::ret_type UnlockPath();

  utils::BasicResult<CreateSnapshotError> CreateSnapshot(std::optional<bool> is_periodic);

  Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) override;

 private:
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
  void CollectGarbage(std::unique_lock<utils::RWLock> main_guard = {});

  bool InitializeWalFile();
  void FinalizeWalFile();

  StorageInfo GetInfo() const override;

  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataManipulation(const Transaction &transaction, uint64_t final_commit_timestamp);
  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataDefinition(durability::StorageGlobalOperation operation, LabelId label,
                                               const std::set<PropertyId> &properties, uint64_t final_commit_timestamp);

  uint64_t CommitTimestamp(std::optional<uint64_t> desired_commit_timestamp = {});

  void RestoreReplicas();

  void RestoreReplicationRole();

  bool ShouldStoreAndRestoreReplicationState() const;

  // Main object storage
  utils::SkipList<storage::Vertex> vertices_;
  utils::SkipList<storage::Edge> edges_;

  // Durability
  std::filesystem::path snapshot_directory_;
  std::filesystem::path lock_file_path_;
  utils::OutputFile lock_file_handle_;
  std::unique_ptr<kvstore::KVStore> storage_;
  std::filesystem::path wal_directory_;

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
  // At the moment when S1 committed a transaction with timestamp 20, and S2
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

  // TODO: This isn't really a commit log, it doesn't even care if a
  // transaction committed or aborted. We could probably combine this with
  // `timestamp_` in a sensible unit, something like TransactionClock or
  // whatever.
  std::optional<CommitLog> commit_log_;
  utils::Synchronized<std::list<Transaction>, utils::SpinLock> committed_transactions_;
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

  // Flags to inform CollectGarbage that it needs to do the more expensive full scans
  std::atomic<bool> gc_full_scan_vertices_delete_ = false;
  std::atomic<bool> gc_full_scan_edges_delete_ = false;

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

  std::atomic<replication::ReplicationRole> replication_role_{replication::ReplicationRole::MAIN};
};

}  // namespace memgraph::storage
