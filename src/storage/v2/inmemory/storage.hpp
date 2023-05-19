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

#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

class InMemoryStorage final : public Storage {
 public:
  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit InMemoryStorage(Config config = Config());

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
    int64_t ApproximateVertexCount() const override {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      return mem_storage->vertices_.size();
    }

    /// Return approximate number of vertices with the given label.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_index.ApproximateVertexCount(label);
    }

    /// Return approximate number of vertices with the given label and property.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index.ApproximateVertexCount(label,
                                                                                                            property);
    }

    /// Return approximate number of vertices with the given label and the given
    /// value for the given property. Note that this is always an over-estimate
    /// and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index.ApproximateVertexCount(
          label, property, value);
    }

    /// Return approximate number of vertices with the given label and value for
    /// the given property in the range defined by provided upper and lower
    /// bounds.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                   const std::optional<utils::Bound<PropertyValue>> &lower,
                                   const std::optional<utils::Bound<PropertyValue>> &upper) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index.ApproximateVertexCount(
          label, property, lower, upper);
    }

    std::optional<storage::IndexStats> GetIndexStats(const storage::LabelId &label,
                                                     const storage::PropertyId &property) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index.GetIndexStats(label, property);
    }

    std::vector<std::pair<LabelId, PropertyId>> ClearIndexStats() override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index.ClearIndexStats();
    }

    std::vector<std::pair<LabelId, PropertyId>> DeleteIndexStatsForLabels(
        const std::span<std::string> labels) override {
      std::vector<std::pair<LabelId, PropertyId>> deleted_indexes;
      std::for_each(labels.begin(), labels.end(), [this, &deleted_indexes](const auto &label_str) {
        std::vector<std::pair<LabelId, PropertyId>> loc_results =
            static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index.DeleteIndexStatsForLabel(
                NameToLabel(label_str));
        deleted_indexes.insert(deleted_indexes.end(), std::make_move_iterator(loc_results.begin()),
                               std::make_move_iterator(loc_results.end()));
      });
      return deleted_indexes;
    }

    void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                       const IndexStats &stats) override {
      static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index.SetIndexStats(label, property, stats);
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
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_index.IndexExists(label);
    }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index.IndexExists(label, property);
    }

    IndicesInfo ListAllIndices() const override {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      return {mem_storage->indices_.label_index.ListIndices(),
              mem_storage->indices_.label_property_index.ListIndices()};
    }

    ConstraintsInfo ListAllConstraints() const override {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      return {ListExistenceConstraints(mem_storage->constraints_),
              mem_storage->constraints_.unique_constraints.ListConstraints()};
    }

    /// Returns void if the transaction has been committed.
    /// Returns `StorageDataManipulationError` if an error occures. Error can be:
    /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `ConstraintViolation`: the changes made by this transaction violate an existence or unique constraint. In this
    /// case the transaction is automatically aborted.
    /// @throw std::bad_alloc
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

  utils::BasicResult<SetIsolationLevelError> SetIsolationLevel(IsolationLevel isolation_level) override;
  void SetStorageMode(StorageMode storage_mode) override;

  StorageMode GetStorageMode() override;

  utils::BasicResult<CreateSnapshotError> CreateSnapshot(std::optional<bool> is_periodic) override;

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
  void CollectGarbage();

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

  bool ShouldStoreAndRestoreReplicas() const;

  // Main object storage
  utils::SkipList<storage::Vertex> vertices_;
  utils::SkipList<storage::Edge> edges_;
  std::atomic<uint64_t> vertex_id_{0};
  std::atomic<uint64_t> edge_id_{0};

  // Specific per storage engine
  IsolationLevel isolation_level_;
  StorageMode storage_mode_;
  Constraints constraints_;
  Indices indices_;

  // TODO: This isn't really a commit log, it doesn't even care if a
  // transaction commited or aborted. We could probably combine this with
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
};

}  // namespace memgraph::storage
