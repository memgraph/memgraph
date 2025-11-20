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

#include <cstdint>
#include <memory>
#include <utility>
#include "flags/run_time_configurable.hpp"
#include "storage/v2/commit_log.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/replication/recovery.hpp"
#include "storage/v2/inmemory/snapshot_info.hpp"
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/ttl.hpp"

/// REPLICATION ///

#include "storage/v2/delta_container.hpp"
#include "storage/v2/replication/replication_storage_state.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/observer.hpp"
#include "utils/resource_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::dbms {
class InMemoryReplicationHandlers;
}  // namespace memgraph::dbms

namespace memgraph::replication {
struct ReplicationHandler;
}  // namespace memgraph::replication

namespace memgraph::storage {

using EdgeInfo = std::optional<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>>;

struct IndexPerformanceTracker {
  void update(Delta::Action action) {
    switch (action) {
      using enum Delta::Action;
      case DELETE_DESERIALIZED_OBJECT:
      case DELETE_OBJECT:
      case RECREATE_OBJECT: {
        // can impact correctness, but does not matter for performance
        return;
      }
      case SET_PROPERTY: {
        // without following the deltas parents to the object we do not know which vertex/edge this delta is for
        impacts_vertex_indexes_ = true;
        impacts_edge_indexes_ = true;
        return;
      }
      case ADD_LABEL:
      case REMOVE_LABEL: {
        impacts_vertex_indexes_ = true;
        return;
      }
      case ADD_IN_EDGE:
      case ADD_OUT_EDGE:
      case REMOVE_IN_EDGE:
      case REMOVE_OUT_EDGE: {
        impacts_edge_indexes_ = true;
        return;
      }
    }
  }

  bool impacts_vertex_indexes() { return impacts_vertex_indexes_; }
  bool impacts_edge_indexes() { return impacts_edge_indexes_; }

 private:
  bool impacts_vertex_indexes_ = false;
  bool impacts_edge_indexes_ = false;
};

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

class InMemoryStorage final : public Storage {
  friend struct memgraph::replication::ReplicationHandler;
  friend class memgraph::dbms::InMemoryReplicationHandlers;
  friend class ReplicationStorageClient;

  friend std::optional<std::vector<RecoveryStep>> GetRecoverySteps(uint64_t replica_commit,
                                                                   utils::FileRetainer::FileLocker *file_locker,
                                                                   const InMemoryStorage *main_storage);

  friend std::optional<durability::SnapshotDurabilityInfo> GetLatestSnapshot(const InMemoryStorage *main_storage);

  friend class InMemoryLabelIndex;
  friend class InMemoryLabelPropertyIndex;
  friend class InMemoryEdgeTypeIndex;
  friend class InMemoryEdgeTypePropertyIndex;
  friend class InMemoryEdgePropertyIndex;

 public:
  using free_mem_fn = std::function<void(std::unique_lock<utils::ResourceLock>, bool)>;
  enum class CreateSnapshotError : uint8_t { ReachedMaxNumTries, AbortSnapshot, AlreadyRunning, NothingNewToWrite };
  enum class RecoverSnapshotError : uint8_t {
    DisabledForReplica,
    NonEmptyStorage,
    MissingFile,
    CopyFailure,
    BackupFailure,
  };

  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit InMemoryStorage(Config config = Config(), std::optional<free_mem_fn> free_mem_fn_override = std::nullopt,
                           PlanInvalidatorPtr invalidator = std::make_unique<PlanInvalidatorDefault>(),
                           std::function<storage::DatabaseProtectorPtr()> database_protector_factory = nullptr);

  InMemoryStorage(const InMemoryStorage &) = delete;
  InMemoryStorage(InMemoryStorage &&) = delete;
  InMemoryStorage &operator=(const InMemoryStorage &) = delete;
  InMemoryStorage &operator=(InMemoryStorage &&) = delete;

  ~InMemoryStorage() override;

  class InMemoryAccessor : public Storage::Accessor {
   private:
    friend class InMemoryStorage;

    explicit InMemoryAccessor(SharedAccess tag, InMemoryStorage *storage, IsolationLevel isolation_level,
                              StorageMode storage_mode, StorageAccessType rw_type,
                              std::optional<std::chrono::milliseconds> timeout = std::nullopt);
    explicit InMemoryAccessor(auto tag, InMemoryStorage *storage, IsolationLevel isolation_level,
                              StorageMode storage_mode,
                              std::optional<std::chrono::milliseconds> timeout = std::nullopt);

    std::optional<ConstraintViolation> ExistenceConstraintsViolation() const;

    std::optional<ConstraintViolation> UniqueConstraintsViolation() const;

    void CheckForFastDiscardOfDeltas();

    [[nodiscard]] bool HandleDurabilityAndReplicate(uint64_t durability_commit_timestamp,
                                                    TransactionReplication &replicating_txn,
                                                    CommitArgs const &commit_args);

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
      return VerticesIterable(AllVerticesIterable(mem_storage->vertices_.access(), storage_, &transaction_, view));
    }

    VerticesIterable Vertices(LabelId label, View view) override;

    VerticesIterable Vertices(LabelId label, std::span<storage::PropertyPath const> properties,
                              std::span<storage::PropertyValueRange const> property_ranges, View view) override;

    VerticesChunkedIterable ChunkedVertices(View view, size_t num_chunks) override;
    VerticesChunkedIterable ChunkedVertices(LabelId label, View view, size_t num_chunks) override;
    VerticesChunkedIterable ChunkedVertices(LabelId label, std::span<storage::PropertyPath const> properties,
                                            std::span<storage::PropertyValueRange const> property_ranges, View view,
                                            size_t num_chunks) override;

    std::optional<EdgeAccessor> FindEdge(Gid gid, View view) override;

    std::optional<EdgeAccessor> FindEdge(Gid edge_gid, Gid from_vertex_gid, View view) override;

    EdgesIterable Edges(EdgeTypeId edge_type, View view) override;

    EdgesIterable Edges(EdgeTypeId edge_type, PropertyId property, View view) override;

    EdgesIterable Edges(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value, View view) override;

    EdgesIterable Edges(EdgeTypeId edge_type, PropertyId property,
                        const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                        const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) override;

    EdgesIterable Edges(PropertyId property, View view) override;

    EdgesIterable Edges(PropertyId property, const PropertyValue &value, View view) override;

    EdgesIterable Edges(PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                        const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) override;

    EdgesChunkedIterable ChunkedEdges(EdgeTypeId edge_type, View view, size_t num_chunks) override;

    EdgesChunkedIterable ChunkedEdges(EdgeTypeId edge_type, PropertyId property, View view, size_t num_chunks) override;

    EdgesChunkedIterable ChunkedEdges(EdgeTypeId edge_type, PropertyId property,
                                      const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                      const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                      size_t num_chunks) override;

    EdgesChunkedIterable ChunkedEdges(PropertyId property, View view, size_t num_chunks) override;

    EdgesChunkedIterable ChunkedEdges(PropertyId property, const PropertyValue &value, View view,
                                      size_t num_chunks) override;

    EdgesChunkedIterable ChunkedEdges(PropertyId property,
                                      const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                      const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                      size_t num_chunks) override;

    /// Return approximate number of all vertices in the database.
    /// Note that this is always an over-estimate and never an under-estimate.
    uint64_t ApproximateVertexCount() const override {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      return mem_storage->vertices_.size();
    }

    /// Return approximate number of vertices with the given label.
    /// Note that this is always an over-estimate and never an under-estimate.
    uint64_t ApproximateVertexCount(LabelId label) const override {
      return transaction_.active_indices_.label_->ApproximateVertexCount(label);
    }

    /// Return approximate number of vertices with the given label and property.
    /// Note that this is always an over-estimate and never an under-estimate.
    uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties) const override {
      return transaction_.active_indices_.label_properties_->ApproximateVertexCount(label, properties);
    }

    /// Return approximate number of vertices with the given label and the given
    /// value for the given property. Note that this is always an over-estimate
    /// and never an under-estimate.
    uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                    std::span<PropertyValue const> values) const override {
      return transaction_.active_indices_.label_properties_->ApproximateVertexCount(label, properties, values);
    }

    /// Return approximate number of vertices with the given label and value for
    /// the given properties in the range defined by provided upper and lower
    /// bounds.
    uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                    std::span<PropertyValueRange const> bounds) const override {
      return transaction_.active_indices_.label_properties_->ApproximateVertexCount(label, properties, bounds);
    }

    uint64_t ApproximateEdgeCount() const override { return storage_->edge_count_.load(std::memory_order_acquire); }

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type) const override {
      return transaction_.active_indices_.edge_type_->ApproximateEdgeCount(edge_type);
    }

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property) const override {
      return transaction_.active_indices_.edge_type_properties_->ApproximateEdgeCount(edge_type, property);
    }

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                  const PropertyValue &value) const override {
      return transaction_.active_indices_.edge_type_properties_->ApproximateEdgeCount(edge_type, property, value);
    }

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                  const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override {
      return transaction_.active_indices_.edge_type_properties_->ApproximateEdgeCount(edge_type, property, lower,
                                                                                      upper);
    }

    uint64_t ApproximateEdgeCount(PropertyId property) const override {
      return transaction_.active_indices_.edge_property_->ApproximateEdgeCount(property);
    }

    uint64_t ApproximateEdgeCount(PropertyId property, const PropertyValue &value) const override {
      return transaction_.active_indices_.edge_property_->ApproximateEdgeCount(property, value);
    }

    uint64_t ApproximateEdgeCount(PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override {
      return transaction_.active_indices_.edge_property_->ApproximateEdgeCount(property, lower, upper);
    }

    std::optional<uint64_t> ApproximateVerticesPointCount(LabelId label, PropertyId property) const override {
      return storage_->indices_.point_index_.ApproximatePointCount(label, property);
    }

    std::optional<uint64_t> ApproximateVerticesVectorCount(LabelId label, PropertyId property) const override {
      return storage_->indices_.vector_index_.ApproximateNodesVectorCount(label, property);
    }

    std::optional<uint64_t> ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const override {
      return storage_->indices_.vector_edge_index_.ApproximateEdgesVectorCount(edge_type, property);
    }

    std::optional<uint64_t> ApproximateVerticesTextCount(std::string_view index_name) const override {
      return storage_->indices_.text_index_.ApproximateVerticesTextCount(index_name);
    }

    std::optional<uint64_t> ApproximateEdgesTextCount(std::string_view index_name) const override {
      return storage_->indices_.text_edge_index_.ApproximateEdgesTextCount(index_name);
    }

    std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const override {
      return static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get())->GetIndexStats(label);
    }

    auto GetIndexStats(const storage::LabelId &label, std::span<storage::PropertyPath const> properties) const
        -> std::optional<storage::LabelPropertyIndexStats> override {
      return static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get())
          ->GetIndexStats(std::pair(label, properties));
    }

    void SetIndexStats(const storage::LabelId &label, const LabelIndexStats &stats) override;

    void SetIndexStats(const storage::LabelId &label, std::span<storage::PropertyPath const> properties,
                       const LabelPropertyIndexStats &stats) override;

    std::vector<std::pair<LabelId, std::vector<PropertyPath>>> DeleteLabelPropertyIndexStats(
        const storage::LabelId &label) override;

    bool DeleteLabelIndexStats(const storage::LabelId &label) override;

    Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>> DetachDelete(
        std::vector<VertexAccessor *> nodes, std::vector<EdgeAccessor *> edges, bool detach) override;

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) override;

    std::optional<EdgeAccessor> FindEdge(Gid gid, View view, EdgeTypeId edge_type, VertexAccessor *from_vertex,
                                         VertexAccessor *to_vertex) override;

    bool LabelIndexReady(LabelId label) const override {
      return transaction_.active_indices_.label_->IndexReady(label);
    }

    bool LabelPropertyIndexExists(LabelId label, std::span<PropertyPath const> properties) const override {
      return transaction_.active_indices_.label_properties_->IndexExists(label, properties);
    }

    bool LabelPropertyIndexReady(LabelId label, std::span<PropertyPath const> properties) const override {
      return transaction_.active_indices_.label_properties_->IndexReady(label, properties);
    }

    bool EdgeTypeIndexReady(EdgeTypeId edge_type) const override {
      return transaction_.active_indices_.edge_type_->IndexReady(edge_type);
    }

    bool EdgeTypePropertyIndexReady(EdgeTypeId edge_type, PropertyId property) const override {
      return transaction_.active_indices_.edge_type_properties_->IndexReady(edge_type, property);
    }

    bool EdgePropertyIndexExists(PropertyId property) const override {
      return transaction_.active_indices_.edge_property_->IndexExists(property);
    }

    bool EdgePropertyIndexReady(PropertyId property) const override {
      return transaction_.active_indices_.edge_property_->IndexReady(property);
    }

    bool PointIndexExists(LabelId label, PropertyId property) const override;

    IndicesInfo ListAllIndices() const override;

    ConstraintsInfo ListAllConstraints() const override;

    void DropAllIndexes() override;

    void DropAllConstraints() override;

    // Represents the 1st phase of 2PC protocol.
    // If there is only a single MG instance, this method serves as commit method. The method itself calls
    // finalize commit method which will bump ldt, update commit ts etc.
    // @throw std::bad_alloc
    // NOLINTNEXTLINE(google-default-arguments)
    utils::BasicResult<StorageManipulationError, void> PrepareForCommitPhase(CommitArgs commit_args) override;

    utils::BasicResult<StorageManipulationError, void> PeriodicCommit(CommitArgs commit_args) override;

    void AbortAndResetCommitTs();

    // Represents the 2nd phase of the 2PC protocol
    // NOTE: Needs to be called while holding the engine lock
    // NOTE: If there is a single instance, PrepareForCommitPhase will call this method, you shouldn't call this method
    // independently of PrepareForCommitPhase.
    void FinalizeCommitPhase(uint64_t durability_commit_timestamp);

    /// @throw std::bad_alloc
    void Abort() override;

    void FinalizeTransaction() override;

    /// Create an index.
    /// Returns void if the index has been created.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `IndexDefinitionError`: the index already exists.
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// @throw std::bad_alloc
    utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
        LabelId label, CheckCancelFunction cancel_check = neverCancel) override;

    /// Create an index.
    /// Returns void if the index has been created.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index already exists.
    /// @throw std::bad_alloc
    utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
        LabelId label, PropertiesPaths properties, CheckCancelFunction cancel_check = neverCancel) override;

    /// Create an index.
    /// Returns void if the index has been created.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index already exists.
    /// @throw std::bad_alloc
    utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
        EdgeTypeId edge_type, CheckCancelFunction cancel_check = neverCancel) override;

    /// Create an index.
    /// Returns void if the index has been created.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index already exists.
    /// @throw std::bad_alloc
    utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
        EdgeTypeId edge_type, PropertyId property, CheckCancelFunction cancel_check = neverCancel) override;

    /// Create an index.
    /// Returns void if the index has been created.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index already exists.
    /// @throw std::bad_alloc
    utils::BasicResult<StorageIndexDefinitionError, void> CreateGlobalEdgeIndex(
        PropertyId property, CheckCancelFunction cancel_check = neverCancel) override;

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
    utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
        LabelId label, std::vector<storage::PropertyPath> &&properties) override;

    /// Drop an existing index.
    /// Returns void if the index has been dropped.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index does not exist.
    utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(EdgeTypeId edge_type) override;

    /// Drop an existing index.
    /// Returns void if the index has been dropped.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index does not exist.
    utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(EdgeTypeId edge_type, PropertyId property) override;

    /// Drop an existing index.
    /// Returns void if the index has been dropped.
    /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
    /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `IndexDefinitionError`: the index does not exist.
    utils::BasicResult<StorageIndexDefinitionError, void> DropGlobalEdgeIndex(PropertyId property) override;

    utils::BasicResult<StorageIndexDefinitionError, void> CreatePointIndex(storage::LabelId label,
                                                                           storage::PropertyId property) override;

    utils::BasicResult<StorageIndexDefinitionError, void> DropPointIndex(storage::LabelId label,
                                                                         storage::PropertyId property) override;

    utils::BasicResult<StorageIndexDefinitionError, void> CreateVectorIndex(VectorIndexSpec spec) override;

    std::optional<std::vector<uint64_t>> IsVertexInVectorIndex(Vertex *vertex, PropertyId property) override;

    utils::BasicResult<StorageIndexDefinitionError, void> DropVectorIndex(std::string_view index_name) override;

    utils::BasicResult<StorageIndexDefinitionError, void> CreateVectorEdgeIndex(VectorEdgeIndexSpec spec) override;

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

    /// Create type constraint,
    /// Returns error result if already exists, or if constraint is already violated
    utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateTypeConstraint(
        LabelId label, PropertyId property, TypeConstraintKind type) override;

    /// Drop type constraint,
    /// Returns error result if constraint does not exist.
    utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropTypeConstraint(
        LabelId label, PropertyId property, TypeConstraintKind type) override;

    void DropGraph() override;

    /// View is not needed because a new rtree gets created for each transaction and it is always
    /// using the latest version
    auto PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                       PropertyValue const &point_value, PropertyValue const &boundary_value,
                       PointDistanceCondition condition) -> PointIterable override;

    /// View is not needed because a new rtree gets created for each transaction and it is always
    /// using the latest version
    auto PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                       PropertyValue const &bottom_left, PropertyValue const &top_right, WithinBBoxCondition condition)
        -> PointIterable override;

    std::vector<std::tuple<VertexAccessor, double, double>> VectorIndexSearchOnNodes(
        const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) override;

    std::vector<std::tuple<EdgeAccessor, double, double>> VectorIndexSearchOnEdges(
        const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) override;

    std::vector<VectorIndexInfo> ListAllVectorIndices() const override;

    std::vector<VectorEdgeIndexInfo> ListAllVectorEdgeIndices() const override;

#ifdef MG_ENTERPRISE
    // TTL management methods
    void StartTtl(TTLReplicationArgs repl_args = {}) override {
      DMG_ASSERT(type() == UNIQUE, "TTL operations require unique access to the storage!");
      if (!storage_->ttl_.Config()) throw ttl::TtlException("TTL not configured!");
      // Only MAIN should be running the TTL worker, it does write operations which only MAIN should be doing
      if (repl_args.is_main) {
        storage_->ttl_.Resume();
      }
      transaction_.md_deltas.emplace_back(MetadataDelta::ttl_operation, durability::TtlOperationType::ENABLE,
                                          std::nullopt, std::nullopt, false);
    }

    void StopTtl() override {
      DMG_ASSERT(type() == UNIQUE, "TTL operations require unique access to the storage!");
      storage_->ttl_.Pause();
      transaction_.md_deltas.emplace_back(MetadataDelta::ttl_operation, durability::TtlOperationType::STOP,
                                          std::nullopt, std::nullopt, false);
    }

    void ConfigureTtl(const storage::ttl::TtlInfo &ttl_info, TTLReplicationArgs repl_args = {}) override {
      DMG_ASSERT(type() == UNIQUE, "TTL operations require unique access to the storage!");
      auto ttl_label = NameToLabel("TTL");
      auto ttl_property = NameToProperty("ttl");

      auto &ttl = storage_->ttl_;

      // If TTL is not enabled, create required indices and enable TTL
      if (!ttl.Enabled()) {
        if (repl_args.is_main) {
          // Only if MAIN, do we proactivly make indexes
          // REPLICA will recieve deltas from MAIN that will create the indexes
          if (GetCreationStorageMode() == StorageMode::IN_MEMORY_TRANSACTIONAL) {
            // Use non-blocking async indexer
            auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
            // Async index creation -> happens in separate transaction
            mem_storage->async_indexer_.Enqueue(ttl_label, std::vector<storage::PropertyPath>{{ttl_property}});
            if (ttl_info.should_run_edge_ttl) {
              mem_storage->async_indexer_.Enqueue(ttl_property);
            }
          } else {
            // Create index with unique access in same transaction
            (void)CreateIndex(ttl_label, std::vector<storage::PropertyPath>{{ttl_property}});
            // Create edge index if needed based on TTL configuration
            if (ttl_info.should_run_edge_ttl) {
              (void)CreateGlobalEdgeIndex(ttl_property);
            }
          }
        }
        ttl.Enable();
      }

      // Configure TTL
      if (!ttl.Running()) ttl.Configure(ttl_info.should_run_edge_ttl);
      ttl.SetInterval(ttl_info.period, ttl_info.start_time);
      transaction_.md_deltas.emplace_back(MetadataDelta::ttl_operation, durability::TtlOperationType::CONFIGURE,
                                          ttl_info.period, ttl_info.start_time, ttl_info.should_run_edge_ttl);
    }

    void DisableTtl(TTLReplicationArgs repl_args = {}) override {
      DMG_ASSERT(type() == UNIQUE, "TTL operations require unique access to the storage!");
      auto ttl_label = NameToLabel("TTL");
      auto ttl_property = NameToProperty("ttl");
      auto &ttl = storage_->ttl_;
      if (repl_args.is_main) {
        // Only if MAIN, do we proactivly drop indexes
        // REPLICA will recieve deltas from MAIN that will drop the indexes
        // Drop indices (silently fail if index already dropped )
        (void)DropIndex(ttl_label, std::vector<storage::PropertyPath>{{ttl_property}});
        // Check if edge TTL was enabled and drop edge index if needed (silently fail if index already dropped)
        if (ttl.Config().should_run_edge_ttl) {
          (void)DropGlobalEdgeIndex(ttl_property);
        }
      }

      ttl.Disable();

      transaction_.md_deltas.emplace_back(MetadataDelta::ttl_operation, durability::TtlOperationType::DISABLE,
                                          std::nullopt, std::nullopt, false);
    }

    storage::ttl::TtlInfo GetTtlConfig() const override { return storage_->ttl_.Config(); }
#endif

    void DowngradeToReadIfValid();

   protected:
    // TODO Better naming
    /// @throw std::bad_alloc
    std::optional<VertexAccessor> CreateVertexEx(storage::Gid gid);
    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdgeEx(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type, storage::Gid gid);

    /// During commit, in some cases you do not need to hand over deltas to GC
    /// in those cases this method is a light weight way to unlink and discard our deltas
    void FastDiscardOfDeltas(std::unique_lock<std::mutex> gc_guard);
    void GCRapidDeltaCleanup(std::list<Gid> &current_deleted_edges, std::list<Gid> &current_deleted_vertices,
                             IndexPerformanceTracker &impact_tracker);
    SalientConfig::Items config_;

    uint64_t commit_flag_wal_position_{0};
    bool needs_wal_update_{false};
  };

  using Storage::Access;
  std::unique_ptr<Accessor> Access(StorageAccessType rw_type, std::optional<IsolationLevel> override_isolation_level,
                                   std::optional<std::chrono::milliseconds> timeout) override;
  using Storage::UniqueAccess;
  std::unique_ptr<Accessor> UniqueAccess(std::optional<IsolationLevel> override_isolation_level,
                                         std::optional<std::chrono::milliseconds> timeout) override;
  using Storage::ReadOnlyAccess;
  std::unique_ptr<Accessor> ReadOnlyAccess(std::optional<IsolationLevel> override_isolation_level,
                                           std::optional<std::chrono::milliseconds> timeout) override;

  void FreeMemory(std::unique_lock<utils::ResourceLock> main_guard, bool periodic) override;

  utils::FileRetainer::FileLockerAccessor::ret_type IsPathLocked();
  utils::FileRetainer::FileLockerAccessor::ret_type LockPath();
  utils::FileRetainer::FileLockerAccessor::ret_type UnlockPath();

  utils::BasicResult<InMemoryStorage::CreateSnapshotError, std::filesystem::path> CreateSnapshot(bool force = false);

  utils::BasicResult<InMemoryStorage::RecoverSnapshotError> RecoverSnapshot(
      std::filesystem::path path, bool force,
      memgraph::replication_coordination_glue::ReplicationRole replication_role);

  std::vector<SnapshotFileInfo> ShowSnapshots();

  std::optional<SnapshotFileInfo> ShowNextSnapshot();

  void CreateSnapshotHandler(std::function<utils::BasicResult<InMemoryStorage::CreateSnapshotError>()> cb);

  Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) override;

  void SetStorageMode(StorageMode storage_mode);

  const durability::Recovery &GetRecovery() const noexcept { return recovery_; }

  auto GetAsyncIndexer() -> AsyncIndexer & { return async_indexer_; }

  bool IsAsyncIndexerIdle() const override { return async_indexer_.IsIdle(); }

  bool HasAsyncIndexerStopped() const override { return async_indexer_.HasThreadStopped(); }

  void StopAllBackgroundTasks() override {
    async_indexer_.Shutdown();
    Storage::StopAllBackgroundTasks();
  }

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
  void CollectGarbage(std::unique_lock<utils::ResourceLock> main_guard, bool periodic);

  bool InitializeWalFile(memgraph::replication::ReplicationEpoch &epoch);
  void FinalizeWalFile();

  StorageInfo GetBaseInfo() override;
  StorageInfo GetInfo() override;

  uint64_t GetCommitTimestamp();

  void PrepareForNewEpoch() override;

  void UpdateEdgesMetadataOnModification(Edge *edge, Vertex *from_vertex);

  EdgeInfo FindEdge(Gid gid);

  EdgeInfo FindEdge(Gid edge_gid, Gid from_vertex_gid);

  EdgeInfo FindEdgeFromMetadata(Gid gid, const Edge *edge_ptr);

  void Clear();

  // Main object storage
  utils::SkipList<Vertex> vertices_;
  utils::SkipList<Edge> edges_;
  utils::SkipList<EdgeMetadata> edges_metadata_;

  // Durability
  durability::Recovery recovery_;

  std::filesystem::path lock_file_path_;
  std::unique_ptr<utils::OutputFile> lock_file_handle_ = std::make_unique<utils::OutputFile>();

  utils::Scheduler snapshot_runner_;
  utils::ResourceLock snapshot_lock_;
  std::atomic_bool snapshot_running_{false};
  std::atomic_bool abort_snapshot_{false};

  std::shared_ptr<utils::Observer<utils::SchedulerInterval>> snapshot_periodic_observer_;

  // Sequence number used to keep track of the chain of WALs.
  uint64_t wal_seq_num_{0};

  std::unique_ptr<durability::WalFile> wal_file_;
  uint64_t wal_unsynced_transactions_{0};

  utils::FileRetainer file_retainer_;

  // Global locker that is used for clients file locking
  utils::FileRetainer::FileLocker global_locker_;

  // TODO: This isn't really a commit log, it doesn't even care if a
  // transaction commited or aborted. We could probably combine this with
  // `timestamp_` in a sensible unit, something like TransactionClock or
  // whatever.
  std::optional<CommitLog> commit_log_;

  utils::Scheduler gc_runner_;
  std::mutex gc_lock_;

  struct GCDeltas {
    GCDeltas(uint64_t mark_timestamp, delta_container deltas, std::unique_ptr<std::atomic<uint64_t>> commit_timestamp)
        : mark_timestamp_{mark_timestamp}, deltas_{std::move(deltas)}, commit_timestamp_{std::move(commit_timestamp)} {}

    GCDeltas(GCDeltas &&) = default;
    GCDeltas &operator=(GCDeltas &&) = default;

    uint64_t mark_timestamp_{};                                  //!< a timestamp no active transaction currently has
    delta_container deltas_;                                     //!< the deltas that need cleaning
    std::unique_ptr<std::atomic<uint64_t>> commit_timestamp_{};  //!< the timestamp the deltas are pointing at
  };

  // Ownership of linked deltas is transferred to committed_transactions_ once transaction is commited
  utils::Synchronized<std::list<GCDeltas>, utils::SpinLock> committed_transactions_{};

  // Ownership of unlinked deltas is transferred to garabage_undo_buffers once transaction is commited/aborted
  utils::Synchronized<std::list<GCDeltas>, utils::SpinLock> garbage_undo_buffers_{};

  // Vertices that are logically deleted but still have to be removed from
  // indices before removing them from the main storage.
  utils::Synchronized<std::list<Gid>, utils::SpinLock> deleted_vertices_;

  // Edges that are logically deleted and wait to be removed from the main
  // storage.
  utils::Synchronized<std::list<Gid>, utils::SpinLock> deleted_edges_;

  std::atomic<bool> gc_index_cleanup_vertex_performance_ = false;
  std::atomic<bool> gc_index_cleanup_edge_performance_ = false;

  // Flags to inform CollectGarbage that it needs to do the more expensive full scans
  std::atomic<bool> gc_full_scan_vertices_delete_ = false;
  std::atomic<bool> gc_full_scan_edges_delete_ = false;

  free_mem_fn free_memory_func_;

  // Moved the create snapshot to a user defined handler so we can remove the global replication state from the storage
  std::function<void()> create_snapshot_handler{};

  // Snapshot digest is the minimal meta info of a snapshot
  // Used to figure out if the current snapshot should be written or not
  struct SnapshotDigest {
    memgraph::replication::ReplicationEpoch epoch_;
    memgraph::storage::EpochHistory history_;
    memgraph::utils::UUID storage_uuid_;
    uint64_t last_durable_ts_;

    friend bool operator==(SnapshotDigest const &, SnapshotDigest const &) = default;
  };

  std::optional<SnapshotDigest> last_snapshot_digest_;

  AsyncIndexer async_indexer_;
};

class ReplicationAccessor final : public InMemoryStorage::InMemoryAccessor {
 public:
  ReplicationAccessor(ReplicationAccessor &&other) = default;
  ReplicationAccessor &operator=(ReplicationAccessor &&other) = delete;

  ReplicationAccessor(ReplicationAccessor const &) = delete;
  ReplicationAccessor &operator=(ReplicationAccessor const &) = delete;
  ~ReplicationAccessor() override = default;

  /// @throw std::bad_alloc
  std::optional<VertexAccessor> CreateVertexEx(storage::Gid gid) { return InMemoryAccessor::CreateVertexEx(gid); }

  /// @throw std::bad_alloc
  Result<EdgeAccessor> CreateEdgeEx(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type, storage::Gid gid) {
    return InMemoryAccessor::CreateEdgeEx(from, to, edge_type, gid);
  }

  auto GetCommitTimestamp() -> std::optional<uint64_t> & { return commit_timestamp_; }

  void ResetCommitTimestamp() { commit_timestamp_.reset(); }

  const Transaction &GetTransaction() const { return transaction_; }
  Transaction &GetTransaction() { return transaction_; }
};

static_assert(std::is_move_constructible_v<ReplicationAccessor>, "Replication accessor isn't move constructible");

struct SingleTxnDeltasProcessingResult {
  std::unique_ptr<ReplicationAccessor> commit_acc;
  uint64_t current_delta_idx;
  uint64_t num_txns_committed;
  uint32_t current_batch_counter;
};

}  // namespace memgraph::storage
