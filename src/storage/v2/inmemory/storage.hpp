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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/storage.hpp"

/// REPLICATION ///
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/replication.hpp"
#include "storage/v2/replication/replication_persistence_helper.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/memory.hpp"
#include "utils/resource_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

class InMemoryStorage final : public Storage {
  friend class InMemoryReplicationServer;
  friend class InMemoryReplicationClient;

 public:
  enum class CreateSnapshotError : uint8_t {
    DisabledForReplica,
    DisabledForAnalyticsPeriodicCommit,
    ReachedMaxNumTries
  };

  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit InMemoryStorage(Config config = Config());
  InMemoryStorage(Config config, StorageMode storage_mode);

  InMemoryStorage(const InMemoryStorage &) = delete;
  InMemoryStorage(InMemoryStorage &&) = delete;
  InMemoryStorage &operator=(const InMemoryStorage &) = delete;
  InMemoryStorage &operator=(InMemoryStorage &&) = delete;

  ~InMemoryStorage() override;

  class InMemoryAccessor : public Storage::Accessor {
   private:
    friend class InMemoryStorage;

    explicit InMemoryAccessor(auto tag, InMemoryStorage *storage, IsolationLevel isolation_level,
                              StorageMode storage_mode);

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

    void SetIndexStats(const storage::LabelId &label, const LabelIndexStats &stats) override;

    void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                       const LabelPropertyIndexStats &stats) override;

    template <typename TResult, typename TIndex>
    TResult DeleteIndexStatsForIndex(TIndex *index, const storage::LabelId &label) {
      return index->DeleteIndexStats(label);
    }

    std::vector<std::pair<LabelId, PropertyId>> DeleteLabelPropertyIndexStats(const storage::LabelId &label) override;

    bool DeleteLabelIndexStats(const storage::LabelId &label) override;

    Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>> DetachDelete(
        std::vector<VertexAccessor *> nodes, std::vector<EdgeAccessor *> edges, bool detach) override;

    void PrefetchInEdges(const VertexAccessor &vertex_acc) override{};

    void PrefetchOutEdges(const VertexAccessor &vertex_acc) override{};

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) override;

    Result<EdgeAccessor> EdgeSetFrom(EdgeAccessor *edge, VertexAccessor *new_from) override;

    Result<EdgeAccessor> EdgeSetTo(EdgeAccessor *edge, VertexAccessor *new_to) override;

    bool LabelIndexExists(LabelId label) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_index_->IndexExists(label);
    }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const override {
      return static_cast<InMemoryStorage *>(storage_)->indices_.label_property_index_->IndexExists(label, property);
    }

    IndicesInfo ListAllIndices() const override;

    ConstraintsInfo ListAllConstraints() const override;

    /// Returns void if the transaction has been committed.
    /// Returns `StorageDataManipulationError` if an error occures. Error can be:
    /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `ConstraintViolation`: the changes made by this transaction violate an existence or unique constraint. In this
    /// case the transaction is automatically aborted.
    /// @throw std::bad_alloc
    // NOLINTNEXTLINE(google-default-arguments)
    utils::BasicResult<StorageManipulationError, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp = {}) override;

    /// @throw std::bad_alloc
    void Abort() override;

    void FinalizeTransaction() override;

    /// Create an index.
    /// Returns void if the index has been created.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `IndexDefinitionError`: the index already exists.
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// @throw std::bad_alloc
    utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(LabelId label) override;

    /// Create an index.
    /// Returns void if the index has been created.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index already exists.
    /// @throw std::bad_alloc
    utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(LabelId label, PropertyId property) override;

    /// Drop an existing index.
    /// Returns void if the index has been dropped.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index does not exist.
    utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label) override;

    /// Drop an existing index.
    /// Returns void if the index has been dropped.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index does not exist.
    utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label, PropertyId property) override;

    /// Returns void if the existence constraint has been created.
    /// Returns `StorageExistenceConstraintDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `ConstraintViolation`: there is already a vertex existing that would break this new constraint.
    /// * `ConstraintDefinitionError`: the constraint already exists.
    /// @throw std::bad_alloc
    /// @throw std::length_error
    utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
        LabelId label, PropertyId property) override;

    /// Drop an existing existence constraint.
    /// Returns void if the existence constraint has been dropped.
    /// Returns `StorageExistenceConstraintDroppingError` if an error occures. Error can be:
    /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `ConstraintDefinitionError`: the constraint did not exists.
    utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
        LabelId label, PropertyId property) override;

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
    utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
    CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) override;

    /// Removes an existing unique constraint.
    /// Returns `StorageUniqueConstraintDroppingError` if an error occures. Error can be:
    /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// Returns `UniqueConstraints::DeletionStatus` otherwise. Value can be:
    /// * `SUCCESS` if constraint was successfully removed,
    /// * `NOT_FOUND` if the specified constraint was not found,
    /// * `EMPTY_PROPERTIES` if the property set is empty, or
    /// * `PROPERTIES_SIZE_LIMIT_EXCEEDED` if the property set exceeds the limit of maximum number of properties.
    UniqueConstraints::DeletionStatus DropUniqueConstraint(LabelId label,
                                                           const std::set<PropertyId> &properties) override;

   protected:
    // TODO Better naming
    /// @throw std::bad_alloc
    VertexAccessor CreateVertexEx(storage::Gid gid);
    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdgeEx(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type, storage::Gid gid);

    Config::Items config_;
  };

  class ReplicationAccessor final : public InMemoryAccessor {
   public:
    explicit ReplicationAccessor(InMemoryAccessor &&inmem) : InMemoryAccessor(std::move(inmem)) {}

    /// @throw std::bad_alloc
    VertexAccessor CreateVertexEx(storage::Gid gid) { return InMemoryAccessor::CreateVertexEx(gid); }

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdgeEx(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type,
                                      storage::Gid gid) {
      return InMemoryAccessor::CreateEdgeEx(from, to, edge_type, gid);
    }

    const Transaction &GetTransaction() const { return transaction_; }
    Transaction &GetTransaction() { return transaction_; }
  };

  std::unique_ptr<Storage::Accessor> Access(std::optional<IsolationLevel> override_isolation_level) override;

  std::unique_ptr<Storage::Accessor> UniqueAccess(std::optional<IsolationLevel> override_isolation_level) override;

  void FreeMemory(std::unique_lock<utils::ResourceLock> main_guard) override;

  utils::FileRetainer::FileLockerAccessor::ret_type IsPathLocked();
  utils::FileRetainer::FileLockerAccessor::ret_type LockPath();
  utils::FileRetainer::FileLockerAccessor::ret_type UnlockPath();

  utils::BasicResult<CreateSnapshotError> CreateSnapshot(std::optional<bool> is_periodic);

  Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) override;

  auto CreateReplicationClient(replication::ReplicationClientConfig const &config)
      -> std::unique_ptr<ReplicationClient> override;

  auto CreateReplicationServer(const replication::ReplicationServerConfig &config)
      -> std::unique_ptr<ReplicationServer> override;

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
  void CollectGarbage(std::unique_lock<utils::ResourceLock> main_guard = {});

  bool InitializeWalFile();
  void FinalizeWalFile();

  StorageInfo GetInfo() const override;

  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataManipulation(const Transaction &transaction, uint64_t final_commit_timestamp);
  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                               uint64_t final_commit_timestamp);
  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                               const std::set<PropertyId> &properties, uint64_t final_commit_timestamp);
  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                               LabelIndexStats stats, uint64_t final_commit_timestamp);
  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                               const std::set<PropertyId> &properties,
                                               LabelPropertyIndexStats property_stats, uint64_t final_commit_timestamp);
  /// Return true in all cases excepted if any sync replicas have not sent confirmation.
  [[nodiscard]] bool AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                               const std::set<PropertyId> &properties, LabelIndexStats stats,
                                               LabelPropertyIndexStats property_stats, uint64_t final_commit_timestamp);

  uint64_t CommitTimestamp(std::optional<uint64_t> desired_commit_timestamp = {});

  void EstablishNewEpoch() override;

  // Main object storage
  utils::SkipList<storage::Vertex> vertices_;
  utils::SkipList<storage::Edge> edges_;

  // Durability
  std::filesystem::path snapshot_directory_;
  std::filesystem::path lock_file_path_;
  utils::OutputFile lock_file_handle_;
  std::filesystem::path wal_directory_;

  utils::Scheduler snapshot_runner_;
  utils::SpinLock snapshot_lock_;

  // UUID used to distinguish snapshots and to link snapshots to WALs
  std::string uuid_;
  // Sequence number used to keep track of the chain of WALs.
  uint64_t wal_seq_num_{0};

  std::optional<durability::WalFile> wal_file_;
  uint64_t wal_unsynced_transactions_{0};

  utils::FileRetainer file_retainer_;

  // Global locker that is used for clients file locking
  utils::FileRetainer::FileLocker global_locker_;

  // TODO: This isn't really a commit log, it doesn't even care if a
  // transaction commited or aborted. We could probably combine this with
  // `timestamp_` in a sensible unit, something like TransactionClock or
  // whatever.
  std::optional<CommitLog> commit_log_;

  utils::Synchronized<std::list<Transaction>, utils::SpinLock> committed_transactions_;
  utils::Scheduler gc_runner_;
  std::mutex gc_lock_;

  using BondPmrLd = Bond<utils::pmr::list<Delta>>;
  // Ownership of unlinked deltas is transfered to garabage_undo_buffers once transaction is commited
  utils::Synchronized<std::list<std::pair<uint64_t, BondPmrLd>>, utils::SpinLock> garbage_undo_buffers_;

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
};

}  // namespace memgraph::storage
