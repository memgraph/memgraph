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
#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/storage.hpp"
#include "utils/rw_lock.hpp"

#include <rocksdb/db.h>

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

    explicit DiskAccessor(DiskStorage *storage, IsolationLevel isolation_level, StorageMode storage_mode);

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

    std::vector<LabelId> ClearLabelIndexStats() override {
      throw utils::NotYetImplemented("ClearIndexStats() is not implemented for DiskStorage.");
    }

    std::vector<std::pair<LabelId, PropertyId>> ClearLabelPropertyIndexStats() override {
      throw utils::NotYetImplemented("ClearIndexStats() is not implemented for DiskStorage.");
    }

    std::vector<LabelId> DeleteLabelIndexStats(std::span<std::string> /*labels*/) override {
      throw utils::NotYetImplemented("DeleteIndexStatsForLabels(labels) is not implemented for DiskStorage.");
    }

    std::vector<std::pair<LabelId, PropertyId>> DeleteLabelPropertyIndexStats(
        const std::span<std::string> /*labels*/) override {
      throw utils::NotYetImplemented("DeleteIndexStatsForLabels(labels) is not implemented for DiskStorage.");
    }

    void SetIndexStats(const storage::LabelId & /*label*/, const LabelIndexStats & /*stats*/) override {
      throw utils::NotYetImplemented("SetIndexStats(stats) is not implemented for DiskStorage.");
    }

    void SetIndexStats(const storage::LabelId & /*label*/, const storage::PropertyId & /*property*/,
                       const LabelPropertyIndexStats & /*stats*/) override {
      throw utils::NotYetImplemented("SetIndexStats(stats) is not implemented for DiskStorage.");
    }

    /// TODO: It is just marked as deleted but the memory isn't reclaimed because of the in-memory storage
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

    // NOLINTNEXTLINE(google-default-arguments)
    utils::BasicResult<StorageDataManipulationError, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp = {}) override;

    void Abort() override;

    void FinalizeTransaction() override;

    std::optional<storage::VertexAccessor> LoadVertexToLabelIndexCache(
        const rocksdb::Slice &key, const rocksdb::Slice &value, Delta *index_delta,
        utils::SkipList<storage::Vertex>::Accessor index_accessor);

    std::optional<storage::VertexAccessor> LoadVertexToMainMemoryCache(const rocksdb::Slice &key,
                                                                       const rocksdb::Slice &value);

    std::optional<storage::VertexAccessor> LoadVertexToLabelPropertyIndexCache(
        const rocksdb::Slice &key, const rocksdb::Slice &value, Delta *index_delta,
        utils::SkipList<storage::Vertex>::Accessor index_accessor);

    std::optional<storage::EdgeAccessor> DeserializeEdge(const rocksdb::Slice &key, const rocksdb::Slice &value);

   private:
    VertexAccessor CreateVertex(utils::SkipList<Vertex>::Accessor &accessor, storage::Gid gid,
                                const std::vector<LabelId> &label_ids, PropertyStore &&properties, Delta *delta);

    void PrefetchEdges(const auto &prefetch_edge_filter);

    Result<EdgeAccessor> CreateEdge(const VertexAccessor *from, const VertexAccessor *to, EdgeTypeId edge_type,
                                    storage::Gid gid, std::string_view properties, const std::string &old_disk_key);

    /// Flushes vertices and edges to the disk with the commit timestamp.
    /// At the time of calling, the commit_timestamp_ must already exist.
    /// After this method, the vertex and edge caches are cleared.
    [[nodiscard]] utils::BasicResult<StorageDataManipulationError, void> FlushMainMemoryCache();

    [[nodiscard]] utils::BasicResult<StorageDataManipulationError, void> FlushIndexCache();

    [[nodiscard]] utils::BasicResult<StorageDataManipulationError, void> CheckVertexConstraintsBeforeCommit(
        const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage) const;

    bool WriteVertexToDisk(const Vertex &vertex);
    bool WriteEdgeToDisk(EdgeRef edge, const std::string &serializedEdgeKey);
    bool DeleteVertexFromDisk(const std::string &vertex);
    bool DeleteEdgeFromDisk(const std::string &edge);

    /// Main storage
    utils::SkipList<storage::Vertex> vertices_;
    std::vector<std::unique_ptr<utils::SkipList<storage::Vertex>>> index_storage_;

    /// We need them because query context for indexed reading is cleared after the query is done not after the
    /// transaction is done
    std::vector<std::list<Delta>> index_deltas_storage_;
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

  RocksDBStorage *GetRocksDBStorage() const { return kvstore_.get(); }

  utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
      LabelId label, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus> CreateUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties, std::optional<uint64_t> desired_commit_timestamp) override;

  utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus> DropUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties, std::optional<uint64_t> desired_commit_timestamp) override;

  Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) override;

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

  [[nodiscard]] std::optional<ConstraintViolation> CheckExistingVerticesBeforeCreatingExistenceConstraint(
      LabelId label, PropertyId property) const;

  [[nodiscard]] utils::BasicResult<ConstraintViolation, std::vector<std::pair<std::string, std::string>>>
  CheckExistingVerticesBeforeCreatingUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) const;

  std::vector<std::pair<std::string, std::string>> SerializeVerticesForLabelIndex(LabelId label);

  std::vector<std::pair<std::string, std::string>> SerializeVerticesForLabelPropertyIndex(LabelId label,
                                                                                          PropertyId property);

  StorageInfo GetInfo() const override;

  void FreeMemory(std::unique_lock<utils::RWLock> /*lock*/) override {}

  uint64_t CommitTimestamp(std::optional<uint64_t> desired_commit_timestamp = {});

  std::unique_ptr<RocksDBStorage> kvstore_;
  std::unique_ptr<kvstore::KVStore> durability_kvstore_;
};

}  // namespace memgraph::storage
