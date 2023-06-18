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

#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/storage.hpp"

#include <rocksdb/db.h>

namespace memgraph::storage {

class DiskStorage final : public Storage {
 public:
  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit DiskStorage(Config config = Config());

  DiskStorage(const DiskStorage &) = delete;
  DiskStorage(DiskStorage &&) = delete;
  DiskStorage &operator=(const DiskStorage &) = delete;
  DiskStorage &operator=(DiskStorage &&) = delete;

  ~DiskStorage() override;

  class DiskAccessor final : public Storage::Accessor {
   private:
    friend class DiskStorage;

    explicit DiskAccessor(DiskStorage *storage, IsolationLevel isolation_level, StorageMode storage_mode);

   public:
    DiskAccessor(const DiskAccessor &) = delete;
    DiskAccessor &operator=(const DiskAccessor &) = delete;
    DiskAccessor &operator=(DiskAccessor &&other) = delete;

    // NOTE: After the accessor is moved, all objects derived from it (accessors
    // and iterators) are *invalid*. You have to get all derived objects again.
    DiskAccessor(DiskAccessor &&other) noexcept;

    ~DiskAccessor() override;

    VertexAccessor CreateVertex() override;

    /// Checks whether the vertex with the given `gid` exists in the vertices_. If it does, it returns a
    /// VertexAccessor to it. If it doesn't, it fetches vertex from the RocksDB. If it doesn't exist in the RocksDB
    /// either, it returns nullptr. If the vertex is fetched from the RocksDB, it is inserted into the vertices_ and
    /// lru_vertices_. Check whether the vertex is in the memory cache (vertices_) is done in O(logK) where K is the
    /// size of the cache.
    std::optional<VertexAccessor> FindVertex(Gid gid, View view) override;

    /// Utility method to load all vertices from the underlying KV storage.
    VerticesIterable Vertices(View view) override;

    /// Utility method to load all vertices from the underlying KV storage with label `label`.
    VerticesIterable Vertices(LabelId label, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) override;

    uint64_t ApproximateVertexCount() const override;

    uint64_t ApproximateVertexCount(LabelId label) const override { return 10; }

    uint64_t ApproximateVertexCount(LabelId label, PropertyId property) const override { return 10; }

    uint64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const override {
      return 10;
    }

    uint64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                    const std::optional<utils::Bound<PropertyValue>> &lower,
                                    const std::optional<utils::Bound<PropertyValue>> &upper) const override {
      return 10;
    }

    std::optional<storage::IndexStats> GetIndexStats(const storage::LabelId &label,
                                                     const storage::PropertyId &property) const override {
      return static_cast<DiskStorage *>(storage_)->indices_.label_property_index_->GetIndexStats(label, property);
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

    /// Deletes vertex only from the cache if it was created in the same transaction.
    /// If the vertex was fetched from the RocksDB, it is deleted from the RocksDB.
    /// It is impossible that the object isn't in the cache because of generated query plan.
    Result<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex) override;

    Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachDeleteVertex(
        VertexAccessor *vertex) override;

    void PrefetchInEdges(const VertexAccessor &vertex_acc) override;

    void PrefetchOutEdges(const VertexAccessor &vertex_acc) override;

    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) override;

    Result<std::optional<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge) override;

    bool LabelIndexExists(LabelId label) const override {
      auto *disk_storage = static_cast<DiskStorage *>(storage_);
      return disk_storage->indices_.label_index_->IndexExists(label);
    }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const override {
      auto *disk_storage = static_cast<DiskStorage *>(storage_);
      return disk_storage->indices_.label_property_index_->IndexExists(label, property);
    }

    IndicesInfo ListAllIndices() const override {
      auto *disk_storage = static_cast<DiskStorage *>(storage_);
      return disk_storage->ListAllIndices();
    }

    ConstraintsInfo ListAllConstraints() const override {
      auto *disk_storage = static_cast<DiskStorage *>(storage_);
      return disk_storage->ListAllConstraints();
    }

    utils::BasicResult<StorageDataManipulationError, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp = {}) override;

    /// @throw std::bad_alloc
    /// Currently, it does everything the same as in-memory version.
    void Abort() override;

    /// Currently, it does everything the same as in-memory version.
    void FinalizeTransaction() override;

    void PrepareForNextIndexQuery() override {
      indexed_vertices_.clear();
      index_deltas_.clear();
    }

    std::optional<storage::VertexAccessor> LoadVertexToLabelIndexCache(const rocksdb::Slice &key,
                                                                       const rocksdb::Slice &value, Delta *index_delta);

    std::optional<storage::VertexAccessor> LoadVertexToMainMemoryCache(const rocksdb::Slice &key,
                                                                       const rocksdb::Slice &value);

    std::optional<storage::VertexAccessor> LoadVertexToLabelPropertyIndexCache(const rocksdb::Slice &key,
                                                                               const rocksdb::Slice &value);

    /// Deserializes edge from the string key and stores it into the edges_ cache.
    /// Properties are deserialized from the value.
    /// The method should be called only when the edge is not in the cache.
    std::optional<storage::EdgeAccessor> DeserializeEdge(const rocksdb::Slice &key, const rocksdb::Slice &value);

   private:
    /// TODO(andi): Consolidate this vertex creation methods and find from in-memory version where are they used.
    VertexAccessor CreateVertex(utils::SkipList<Vertex>::Accessor &accessor, storage::Gid gid,
                                const std::vector<LabelId> &label_ids, PropertyStore &&properties, Delta *delta);

    void PrefetchEdges(const auto &prefetch_edge_filter);

    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type, storage::Gid gid,
                                    std::string_view properties, const std::string &old_disk_key);

    /// Flushes vertices and edges to the disk with the commit timestamp.
    /// At the time of calling, the commit_timestamp_ must already exist.
    /// After this method, the vertex and edge caches are cleared.
    [[nodiscard]] utils::BasicResult<StorageDataManipulationError, void> CheckConstraintsAndFlushMainMemoryCache();

    bool WriteVertexToDisk(const Vertex &vertex);
    bool WriteEdgeToDisk(const EdgeRef edge, const std::string &serializedEdgeKey);
    bool DeleteVertexFromDisk(const std::string &vertex);
    bool DeleteEdgeFromDisk(const std::string &edge);

    // Main object storage
    utils::SkipList<storage::Vertex> vertices_;
    utils::SkipList<storage::Vertex> indexed_vertices_;

    /// We need them because query context for indexed reading is cleared after the query is done not after the
    /// transaction is done
    std::list<Delta> index_deltas_;
    utils::SkipList<storage::Edge> edges_;
    Config::Items config_;
    std::vector<std::string> edges_to_delete_;
    std::vector<std::pair<std::string, std::string>> vertices_to_delete_;
    rocksdb::Transaction *disk_transaction_;
  };

  std::unique_ptr<Storage::Accessor> Access(std::optional<IsolationLevel> override_isolation_level) override {
    auto isolation_level = override_isolation_level.value_or(isolation_level_);
    if (isolation_level != IsolationLevel::SNAPSHOT_ISOLATION) {
      throw utils::NotYetImplemented("Disk storage supports only SNAPSHOT isolation level.");
    }
    return std::unique_ptr<DiskAccessor>(new DiskAccessor{this, isolation_level, storage_mode_});
  }

  RocksDBStorage *GetRocksDBStorage() { return kvstore_.get(); }

  utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
      LabelId label, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  /// TODO: andi Abstract if possible
  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, std::optional<uint64_t> desired_commit_timestamp) override;

  /// TODO: andi Abstract if possible
  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  /// TODO: andi Abstract if possible
  utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus> CreateUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties, std::optional<uint64_t> desired_commit_timestamp) override;

  /// TODO: andi Abstract if possible
  utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus> DropUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<SetIsolationLevelError> SetIsolationLevel(IsolationLevel isolation_level) override;

  Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) override;

 private:
  [[nodiscard]] std::optional<ConstraintViolation> CheckExistingVerticesBeforeCreatingExistenceConstraint(
      LabelId label, PropertyId property) const;

  [[nodiscard]] utils::BasicResult<ConstraintViolation, std::vector<std::pair<std::string, std::string>>>
  CheckExistingVerticesBeforeCreatingUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) const;

  std::vector<std::pair<std::string, std::string>> SerializeVerticesForLabelIndex(LabelId label);

  std::vector<std::pair<std::string, std::string>> SerializeVerticesForLabelPropertyIndex(LabelId label,
                                                                                          PropertyId property);

  StorageInfo GetInfo() const override;

  uint64_t CommitTimestamp(std::optional<uint64_t> desired_commit_timestamp = {});

  /// TODO: andi Why not on abstract storage
  std::atomic<uint64_t> vertex_id_{0};
  std::atomic<uint64_t> edge_id_{0};

  std::unique_ptr<RocksDBStorage> kvstore_;
};

}  // namespace memgraph::storage
