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

#include "kvstore/kvstore.hpp"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/disk/edge_import_mode_cache.hpp"
#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/edge_import_mode.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "utils/rw_lock.hpp"

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <unordered_set>

namespace memgraph::storage {

class DiskStorage final : public Storage {
 public:
  explicit DiskStorage(Config config = Config());

  DiskStorage(const DiskStorage &) = delete;
  DiskStorage(DiskStorage &&) = delete;
  DiskStorage &operator=(const DiskStorage &) = delete;
  DiskStorage &operator=(DiskStorage &&) = delete;

  ~DiskStorage() override;

  class DiskAccessor final : public Storage::Accessor {
   private:
    friend class DiskStorage;

    explicit DiskAccessor(auto tag, DiskStorage *storage, IsolationLevel isolation_level, StorageMode storage_mode);

    /// TODO: const methods?
    void LoadVerticesToMainMemoryCache();

    void LoadVerticesFromMainStorageToEdgeImportCache();

    void HandleMainLoadingForEdgeImportCache();

    void LoadVerticesFromLabelIndexStorageToEdgeImportCache(LabelId label);

    void HandleLoadingLabelForEdgeImportCache(LabelId label);

    void LoadVerticesFromLabelPropertyIndexStorageToEdgeImportCache(LabelId label, PropertyId property);

    void HandleLoadingLabelPropertyForEdgeImportCache(LabelId label, PropertyId property);

    std::unordered_set<Gid> MergeVerticesFromMainCacheWithLabelIndexCache(LabelId label, View view,
                                                                          std::list<Delta> &index_deltas,
                                                                          utils::SkipList<Vertex> *indexed_vertices);

    void LoadVerticesFromDiskLabelIndex(LabelId label, const std::unordered_set<storage::Gid> &gids,
                                        std::list<Delta> &index_deltas, utils::SkipList<Vertex> *indexed_vertices);

    std::unordered_set<Gid> MergeVerticesFromMainCacheWithLabelPropertyIndexCache(
        LabelId label, PropertyId property, View view, std::list<Delta> &index_deltas,
        utils::SkipList<Vertex> *indexed_vertices, const auto &label_property_filter);

    void LoadVerticesFromDiskLabelPropertyIndex(LabelId label, PropertyId property,
                                                const std::unordered_set<storage::Gid> &gids,
                                                std::list<Delta> &index_deltas,
                                                utils::SkipList<Vertex> *indexed_vertices,
                                                const auto &label_property_filter);

    void LoadVerticesFromDiskLabelPropertyIndexWithPointValueLookup(LabelId label, PropertyId property,
                                                                    const std::unordered_set<storage::Gid> &gids,
                                                                    const PropertyValue &value,
                                                                    std::list<Delta> &index_deltas,
                                                                    utils::SkipList<Vertex> *indexed_vertices);

    std::unordered_set<Gid> MergeVerticesFromMainCacheWithLabelPropertyIndexCacheForIntervalSearch(
        LabelId label, PropertyId property, View view, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
        const std::optional<utils::Bound<PropertyValue>> &upper_bound, std::list<Delta> &index_deltas,
        utils::SkipList<Vertex> *indexed_vertices);

    void LoadVerticesFromDiskLabelPropertyIndexForIntervalSearch(
        LabelId label, PropertyId property, const std::unordered_set<storage::Gid> &gids,
        const std::optional<utils::Bound<PropertyValue>> &lower_bound,
        const std::optional<utils::Bound<PropertyValue>> &upper_bound, std::list<Delta> &index_deltas,
        utils::SkipList<Vertex> *indexed_vertices);

   public:
    DiskAccessor(const DiskAccessor &) = delete;
    DiskAccessor &operator=(const DiskAccessor &) = delete;
    DiskAccessor &operator=(DiskAccessor &&other) = delete;

    DiskAccessor(DiskAccessor &&other) noexcept;

    ~DiskAccessor() override;

    VertexAccessor CreateVertex() override;

    std::optional<VertexAccessor> FindVertex(Gid gid, View view) override;

    VerticesIterable Vertices(View view) override;

    VerticesIterable Vertices(LabelId label, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) override;

    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) override;

    uint64_t ApproximateVertexCount() const override;

    uint64_t ApproximateVertexCount(LabelId /*label*/) const override { return 10; }

    uint64_t ApproximateVertexCount(LabelId /*label*/, PropertyId /*property*/) const override { return 10; }

    uint64_t ApproximateVertexCount(LabelId /*label*/, PropertyId /*property*/,
                                    const PropertyValue & /*value*/) const override {
      return 10;
    }

    uint64_t ApproximateVertexCount(LabelId /*label*/, PropertyId /*property*/,
                                    const std::optional<utils::Bound<PropertyValue>> & /*lower*/,
                                    const std::optional<utils::Bound<PropertyValue>> & /*upper*/) const override {
      return 10;
    }

    std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId & /*label*/) const override {
      return {};
    }

    std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
        const storage::LabelId & /*label*/, const storage::PropertyId & /*property*/) const override {
      return {};
    }

    bool DeleteLabelIndexStats(const storage::LabelId & /*labels*/) override {
      throw utils::NotYetImplemented("DeleteIndexStatsForLabels(labels) is not implemented for DiskStorage.");
    }

    std::vector<std::pair<LabelId, PropertyId>> DeleteLabelPropertyIndexStats(
        const storage::LabelId & /*labels*/) override {
      throw utils::NotYetImplemented("DeleteIndexStatsForLabels(labels) is not implemented for DiskStorage.");
    }

    void SetIndexStats(const storage::LabelId & /*label*/, const LabelIndexStats & /*stats*/) override {
      throw utils::NotYetImplemented("SetIndexStats(stats) is not implemented for DiskStorage.");
    }

    void SetIndexStats(const storage::LabelId & /*label*/, const storage::PropertyId & /*property*/,
                       const LabelPropertyIndexStats & /*stats*/) override {
      throw utils::NotYetImplemented("SetIndexStats(stats) is not implemented for DiskStorage.");
    }

    Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>> DetachDelete(
        std::vector<VertexAccessor *> nodes, std::vector<EdgeAccessor *> edges, bool detach) override;

    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) override;

    Result<EdgeAccessor> EdgeSetFrom(EdgeAccessor *edge, VertexAccessor *new_from) override;

    Result<EdgeAccessor> EdgeSetTo(EdgeAccessor *edge, VertexAccessor *new_to) override;

    bool LabelIndexExists(LabelId label) const override {
      auto *disk_storage = static_cast<DiskStorage *>(storage_);
      return disk_storage->indices_.label_index_->IndexExists(label);
    }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const override {
      auto *disk_storage = static_cast<DiskStorage *>(storage_);
      return disk_storage->indices_.label_property_index_->IndexExists(label, property);
    }

    IndicesInfo ListAllIndices() const override;

    ConstraintsInfo ListAllConstraints() const override;

    // NOLINTNEXTLINE(google-default-arguments)
    utils::BasicResult<StorageManipulationError, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp = {}) override;

    void UpdateObjectsCountOnAbort();

    void Abort() override;

    void FinalizeTransaction() override;

    std::optional<storage::VertexAccessor> LoadVertexToLabelIndexCache(
        const std::string &key, const std::string &value, Delta *index_delta,
        utils::SkipList<storage::Vertex>::Accessor index_accessor);

    std::optional<storage::VertexAccessor> LoadVertexToLabelPropertyIndexCache(
        const std::string &key, const std::string &value, Delta *index_delta,
        utils::SkipList<storage::Vertex>::Accessor index_accessor);

    std::optional<storage::EdgeAccessor> DeserializeEdge(const rocksdb::Slice &key, const rocksdb::Slice &value,
                                                         const rocksdb::Slice &ts);

    utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(LabelId label) override;

    utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(LabelId label, PropertyId property) override;

    utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label) override;

    utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label, PropertyId property) override;

    utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
        LabelId label, PropertyId property) override;

    utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
        LabelId label, PropertyId property) override;

    utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
    CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) override;

    UniqueConstraints::DeletionStatus DropUniqueConstraint(LabelId label,
                                                           const std::set<PropertyId> &properties) override;
   private:
    /// Flushes vertices and edges to the disk with the commit timestamp.
    /// At the time of calling, the commit_timestamp_ must already exist.
    /// After this method, the vertex and edge caches are cleared.

    [[nodiscard]] utils::BasicResult<StorageManipulationError, void> FlushIndexCache();

    [[nodiscard]] utils::BasicResult<StorageManipulationError, void> FlushDeletedVertices();

    [[nodiscard]] utils::BasicResult<StorageManipulationError, void> FlushDeletedEdges();

    [[nodiscard]] utils::BasicResult<StorageManipulationError, void> FlushVertices(
        const auto &vertex_acc, std::vector<std::vector<PropertyValue>> &unique_storage);

    [[nodiscard]] utils::BasicResult<StorageManipulationError, void> FlushModifiedEdges(const auto &edge_acc);

    [[nodiscard]] utils::BasicResult<StorageManipulationError, void> ClearDanglingVertices();

    [[nodiscard]] utils::BasicResult<StorageManipulationError, void> CheckVertexConstraintsBeforeCommit(
        const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage) const;

    bool WriteVertexToVertexColumnFamily(const Vertex &vertex);
    bool WriteEdgeToEdgeColumnFamily(const std::string &serialized_edge_key, const std::string &serialized_edge_value);

    bool WriteEdgeToConnectivityIndex(const std::string &vertex_gid, const std::string &edge_gid,
                                      rocksdb::ColumnFamilyHandle *handle, std::string mode);

    bool DeleteVertexFromDisk(const std::string &vertex_gid, const std::string &vertex);

    bool DeleteEdgeFromEdgeColumnFamily(const std::string &edge_gid);
    bool DeleteEdgeFromDisk(const std::string &edge_gid, const std::string &src_vertex_gid,
                            const std::string &dst_vertex_gid);
    bool DeleteEdgeFromConnectivityIndex(const std::string &vertex_gid, const std::string &edge_gid,
                                         rocksdb::ColumnFamilyHandle *handle, std::string mode);
  };

  std::unique_ptr<Storage::Accessor> Access(std::optional<IsolationLevel> override_isolation_level) override {
    auto isolation_level = override_isolation_level.value_or(isolation_level_);
    if (isolation_level != IsolationLevel::SNAPSHOT_ISOLATION) {
      throw utils::NotYetImplemented("Disk storage supports only SNAPSHOT isolation level.");
    }
    return std::unique_ptr<DiskAccessor>(new DiskAccessor{this, isolation_level, storage_mode_});
  }

  /// TODO: (andi) Methods working with rocksdb are scattered around DiskStorage and DiskStorage::DiskAccessor
  /// Two options:
  /// 1. move everything under DiskStorage level
  /// 2. propagate DiskStorage::DiskAccessor to vertex and edge accessor.
  /// Out of scope of this PR
  VertexAccessor CreateVertexFromDisk(Transaction *transaction, utils::SkipList<Vertex>::Accessor &accessor,
                                      storage::Gid gid, std::vector<LabelId> label_ids, PropertyStore properties,
                                      Delta *delta);

  std::optional<storage::VertexAccessor> LoadVertexToMainMemoryCache(Transaction *transaction, const std::string &key,
                                                                     const std::string &value, std::string &&ts);

  /// TODO: (andi) I don't think View is necessary
  std::optional<VertexAccessor> FindVertex(Gid gid, Transaction *transaction, View view);

  std::optional<EdgeAccessor> CreateEdgeFromDisk(const VertexAccessor *from, const VertexAccessor *to,
                                                 Transaction *transaction, EdgeTypeId edge_type, storage::Gid gid,
                                                 std::string_view properties, const std::string &old_disk_key,
                                                 std::string &&ts);

  /// TODO: (andi) Maybe const
  std::vector<EdgeAccessor> OutEdges(const VertexAccessor *src_vertex,
                                     const std::vector<EdgeTypeId> &possible_edge_types,
                                     const VertexAccessor *destination, Transaction *transaction, View view);

  /// TODO: (andi) Maybe const
  std::vector<EdgeAccessor> InEdges(const VertexAccessor *dst_vertex,
                                    const std::vector<EdgeTypeId> &possible_edge_types, const VertexAccessor *source,
                                    Transaction *transaction, View view);

  RocksDBStorage *GetRocksDBStorage() const { return kvstore_.get(); }

  std::unique_ptr<Storage::Accessor> Access(std::optional<IsolationLevel> override_isolation_level) override;

  std::unique_ptr<Storage::Accessor> UniqueAccess(std::optional<IsolationLevel> override_isolation_level) override;

  RocksDBStorage *GetRocksDBStorage() const { return kvstore_.get(); }

  Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) override;

  void SetEdgeImportMode(EdgeImportMode edge_import_status);

  EdgeImportMode GetEdgeImportMode() const;

 private:
  void LoadIndexInfoIfExists() const;

  /// TODO (andi): Maybe good to separate these methods and durability kvstore into a separate class
  bool PersistLabelIndexCreation(LabelId label) const;

  bool PersistLabelIndexDeletion(LabelId label) const;

  void LoadLabelIndexInfoIfExists() const;

  bool PersistLabelPropertyIndexAndExistenceConstraintCreation(LabelId label, PropertyId property,
                                                               const char *key) const;

  bool PersistLabelPropertyIndexAndExistenceConstraintDeletion(LabelId label, PropertyId property,
                                                               const char *key) const;

  void LoadLabelPropertyIndexInfoIfExists() const;

  void LoadConstraintsInfoIfExists() const;

  void LoadExistenceConstraintInfoIfExists() const;

  bool PersistUniqueConstraintCreation(LabelId label, const std::set<PropertyId> &properties) const;

  bool PersistUniqueConstraintDeletion(LabelId label, const std::set<PropertyId> &properties) const;

  void LoadUniqueConstraintInfoIfExists() const;

  uint64_t GetDiskSpaceUsage() const;

  void LoadTimestampIfExists();

  void LoadVertexAndEdgeCountIfExists();

  [[nodiscard]] std::optional<ConstraintViolation> CheckExistingVerticesBeforeCreatingExistenceConstraint(
      LabelId label, PropertyId property) const;

  [[nodiscard]] utils::BasicResult<ConstraintViolation, std::vector<std::pair<std::string, std::string>>>
  CheckExistingVerticesBeforeCreatingUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) const;

  std::vector<std::pair<std::string, std::string>> SerializeVerticesForLabelIndex(LabelId label);

  std::vector<std::pair<std::string, std::string>> SerializeVerticesForLabelPropertyIndex(LabelId label,
                                                                                          PropertyId property);

  StorageInfo GetInfo() const override;

  void FreeMemory(std::unique_lock<utils::ResourceLock> /*lock*/) override {}

  void EstablishNewEpoch() override { throw utils::BasicException("Disk storage mode does not support replication."); }

  uint64_t CommitTimestamp(std::optional<uint64_t> desired_commit_timestamp = {});

  EdgeImportMode edge_import_status_{EdgeImportMode::INACTIVE};
  std::unique_ptr<EdgeImportModeCache> edge_import_mode_cache_{nullptr};

  auto CreateReplicationClient(replication::ReplicationClientConfig const &config)
      -> std::unique_ptr<ReplicationClient> override {
    throw utils::BasicException("Disk storage mode does not support replication.");
  }

  auto CreateReplicationServer(const replication::ReplicationServerConfig &config)
      -> std::unique_ptr<ReplicationServer> override {
    throw utils::BasicException("Disk storage mode does not support replication.");
  }

 private:
  std::unique_ptr<RocksDBStorage> kvstore_;
  std::unique_ptr<kvstore::KVStore> durability_kvstore_;

  std::atomic<uint64_t> vertex_count_{0};
};

}  // namespace memgraph::storage
