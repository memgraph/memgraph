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

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>

#include "mg_procedure.h"
#include "query/exceptions.hpp"
#include "storage/v2/commit_log.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/database_access.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edges_iterable.hpp"
#include "storage/v2/enum_store.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/replication_storage_state.hpp"
#include "storage/v2/replication/replication_transaction.hpp"
#include "storage/v2/schema_info.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "utils/compressor.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_gauge.hpp"
#include "utils/event_histogram.hpp"
#include "utils/exceptions.hpp"
#include "utils/resource_lock.hpp"
#include "utils/synchronized_metadata_store.hpp"
#include "utils/timer.hpp"
#include "utils/uuid.hpp"

namespace memgraph::metrics {
extern const Event SnapshotCreationLatency_us;

extern const Event ActiveLabelIndices;
extern const Event ActiveLabelPropertyIndices;
extern const Event ActivePointIndices;
extern const Event ActiveTextIndices;
extern const Event ActiveVectorIndices;
}  // namespace memgraph::metrics

namespace memgraph::storage {

class SharedAccessTimeout : public utils::BasicException {
 public:
  SharedAccessTimeout()
      : utils::BasicException(
            "Cannot get shared access storage. Try stopping other queries that are running in parallel.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(SharedAccessTimeout)
};

class UniqueAccessTimeout : public utils::BasicException {
 public:
  UniqueAccessTimeout()
      : utils::BasicException(
            "Cannot get unique access to the storage. Try stopping other queries that are running in parallel.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(UniqueAccessTimeout)
};

class ReadOnlyAccessTimeout : public utils::BasicException {
 public:
  ReadOnlyAccessTimeout()
      : utils::BasicException(
            "Cannot get read only access to the storage. Try stopping other queries that are running in parallel.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ReadOnlyAccessTimeout)
};

struct Transaction;
class EdgeAccessor;

struct IndicesInfo {
  std::vector<LabelId> label;
  std::vector<std::pair<LabelId, std::vector<PropertyId>>> label_properties;
  std::vector<EdgeTypeId> edge_type;
  std::vector<std::pair<EdgeTypeId, PropertyId>> edge_type_property;
  std::vector<PropertyId> edge_property;
  std::vector<std::pair<std::string, LabelId>> text_indices;
  std::vector<std::pair<LabelId, PropertyId>> point_label_property;
  std::vector<VectorIndexSpec> vector_indices_spec;
};

struct ConstraintsInfo {
  std::vector<std::pair<LabelId, PropertyId>> existence;
  std::vector<std::pair<LabelId, std::set<PropertyId>>> unique;
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> type;
};

struct StorageInfo {
  uint64_t vertex_count;
  uint64_t edge_count;
  double average_degree;
  uint64_t memory_res;
  uint64_t peak_memory_res;
  uint64_t unreleased_delta_objects;
  uint64_t disk_usage;
  uint64_t label_indices;
  uint64_t label_property_indices;
  uint64_t text_indices;
  uint64_t vector_indices;
  uint64_t existence_constraints;
  uint64_t unique_constraints;
  StorageMode storage_mode;
  IsolationLevel isolation_level;
  bool durability_snapshot_enabled;
  bool durability_wal_enabled;
  bool property_store_compression_enabled;
  utils::CompressionLevel property_store_compression_level;
  uint64_t schema_vertex_count;
  uint64_t schema_edge_count;
};

struct EventInfo {
  std::string name;
  std::string type;
  std::string event_type;
  uint64_t value;
};

static inline nlohmann::json ToJson(const StorageInfo &info) {
  nlohmann::json res;

  res["edges"] = info.edge_count;
  res["vertices"] = info.vertex_count;
  res["memory"] = info.memory_res;
  res["disk"] = info.disk_usage;
  res["label_indices"] = info.label_indices;
  res["label_prop_indices"] = info.label_property_indices;
  res["text_indices"] = info.text_indices;
  res["vector_indices"] = info.vector_indices;
  res["existence_constraints"] = info.existence_constraints;
  res["unique_constraints"] = info.unique_constraints;
  res["storage_mode"] = storage::StorageModeToString(info.storage_mode);
  res["isolation_level"] = storage::IsolationLevelToString(info.isolation_level);
  res["durability"] = {{"snapshot_enabled", info.durability_snapshot_enabled},
                       {"WAL_enabled", info.durability_wal_enabled}};
  res["property_store_compression_enabled"] = info.property_store_compression_enabled;
  res["property_store_compression_level"] = utils::CompressionLevelToString(info.property_store_compression_level);
  res["schema_vertex_count"] = info.schema_vertex_count;
  res["schema_edge_count"] = info.schema_edge_count;
  return res;
}

struct EdgeInfoForDeletion {
  std::unordered_set<Gid> partial_src_edge_ids{};
  std::unordered_set<Gid> partial_dest_edge_ids{};
  std::unordered_set<Vertex *> partial_src_vertices{};
  std::unordered_set<Vertex *> partial_dest_vertices{};
};

struct CommitReplArgs {
  // REPLICA on recipt of Deltas will have a desired commit timestamp
  std::optional<uint64_t> desired_commit_timestamp = std::nullopt;

  bool is_main = true;

  bool IsMain() const { return is_main; }
};

class Storage {
  friend class ReplicationServer;
  friend class ReplicationStorageClient;

 public:
  Storage(Config config, StorageMode storage_mode);

  Storage(const Storage &) = delete;
  Storage(Storage &&) = delete;
  Storage &operator=(const Storage &) = delete;
  Storage &operator=(Storage &&) = delete;

  virtual ~Storage() = default;

  const std::string &name() const { return config_.salient.name; }

  auto uuid() const -> utils::UUID const & { return config_.salient.uuid; }

  auto uuid() -> utils::UUID & { return config_.salient.uuid; }

  class Accessor {
   public:
    static constexpr struct SharedAccess {
    } shared_access;
    static constexpr struct UniqueAccess {
    } unique_access;
    static constexpr struct ReadOnlyAccess {
    } read_only_access;

    enum Type { NO_ACCESS, UNIQUE, WRITE, READ, READ_ONLY };

    Accessor(SharedAccess /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode,
             Type rw_type = Type::WRITE, std::optional<std::chrono::milliseconds> timeout = std::nullopt);
    Accessor(UniqueAccess /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode,
             std::optional<std::chrono::milliseconds> timeout = std::nullopt);
    Accessor(ReadOnlyAccess /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode,
             std::optional<std::chrono::milliseconds> timeout = std::nullopt);
    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;
    Accessor &operator=(Accessor &&other) = delete;

    Accessor(Accessor &&other) noexcept;

    virtual ~Accessor() = default;

    Type type() const {
      if (unique_guard_.owns_lock()) {
        return UNIQUE;
      }
      if (storage_guard_.owns_lock()) {
        switch (storage_guard_.type()) {
          case utils::SharedResourceLockGuard::Type::WRITE:
            return WRITE;
          case utils::SharedResourceLockGuard::Type::READ:
            return READ;
          case utils::SharedResourceLockGuard::Type::READ_ONLY:
            return READ_ONLY;
        }
      }
      return NO_ACCESS;
    }

    virtual VertexAccessor CreateVertex() = 0;

    virtual std::optional<VertexAccessor> FindVertex(Gid gid, View view) = 0;

    virtual VerticesIterable Vertices(View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, std::span<storage::PropertyId const> properties,
                                      std::span<storage::PropertyValueRange const> property_ranges, View view) = 0;

    VerticesIterable Vertices(LabelId label, std::span<storage::PropertyId const> properties, View view) {
      return Vertices(label, properties, std::vector(properties.size(), storage::PropertyValueRange::IsNotNull()),
                      view);
    };

    virtual std::optional<EdgeAccessor> FindEdge(Gid gid, View view) = 0;

    virtual EdgesIterable Edges(EdgeTypeId edge_type, View view) = 0;

    virtual EdgesIterable Edges(EdgeTypeId edge_type, PropertyId property, View view) = 0;

    virtual EdgesIterable Edges(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value, View view) = 0;

    virtual EdgesIterable Edges(EdgeTypeId edge_type, PropertyId property,
                                const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) = 0;

    virtual EdgesIterable Edges(PropertyId property, View view) = 0;

    virtual EdgesIterable Edges(PropertyId property, const PropertyValue &value, View view) = 0;

    virtual EdgesIterable Edges(PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) = 0;

    virtual auto DeleteVertex(VertexAccessor *vertex) -> Result<std::optional<VertexAccessor>>;

    virtual auto DetachDeleteVertex(VertexAccessor *vertex)
        -> Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>>;

    virtual auto DetachDelete(std::vector<VertexAccessor *> nodes, std::vector<EdgeAccessor *> edges, bool detach)
        -> Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>>;

    virtual uint64_t ApproximateVertexCount() const = 0;

    virtual uint64_t ApproximateVertexCount(LabelId label) const = 0;

    virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties) const = 0;

    virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                            std::span<PropertyValue const> values) const = 0;

    virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyId const> properties,
                                            std::span<PropertyValueRange const> bounds) const = 0;

    virtual uint64_t ApproximateEdgeCount() const = 0;

    virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type) const = 0;

    virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property) const = 0;

    virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                          const PropertyValue &value) const = 0;

    virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                          const std::optional<utils::Bound<PropertyValue>> &lower,
                                          const std::optional<utils::Bound<PropertyValue>> &upper) const = 0;

    virtual uint64_t ApproximateEdgeCount(PropertyId property) const = 0;

    virtual uint64_t ApproximateEdgeCount(PropertyId property, const PropertyValue &value) const = 0;

    virtual uint64_t ApproximateEdgeCount(PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
                                          const std::optional<utils::Bound<PropertyValue>> &upper) const = 0;

    virtual std::optional<uint64_t> ApproximateVerticesPointCount(LabelId label, PropertyId property) const = 0;

    virtual std::optional<uint64_t> ApproximateVerticesVectorCount(LabelId label, PropertyId property) const = 0;

    virtual auto GetIndexStats(const storage::LabelId &label) const -> std::optional<storage::LabelIndexStats> = 0;

    virtual auto GetIndexStats(const storage::LabelId &label, std::span<storage::PropertyId const> properties) const
        -> std::optional<storage::LabelPropertyIndexStats> = 0;

    virtual void SetIndexStats(const storage::LabelId &label, const LabelIndexStats &stats) = 0;

    virtual void SetIndexStats(const storage::LabelId &label, std::span<storage::PropertyId const> property,
                               const LabelPropertyIndexStats &stats) = 0;

    virtual auto DeleteLabelPropertyIndexStats(const storage::LabelId &label)
        -> std::vector<std::pair<LabelId, std::vector<PropertyId>>> = 0;

    virtual bool DeleteLabelIndexStats(const storage::LabelId &label) = 0;

    virtual Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) = 0;

    virtual std::optional<EdgeAccessor> FindEdge(Gid gid, View view, EdgeTypeId edge_type, VertexAccessor *from_vertex,
                                                 VertexAccessor *to_vertex) = 0;

    virtual auto DeleteEdge(EdgeAccessor *edge) -> Result<std::optional<EdgeAccessor>>;

    virtual bool LabelIndexExists(LabelId label) const = 0;

    virtual bool LabelPropertyIndexExists(LabelId label, std::span<PropertyId const> properties) const = 0;

    auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels,
                                            std::span<PropertyId const> properties) const
        -> std::vector<LabelPropertiesIndicesInfo> {
      return storage_->indices_.label_property_index_->RelevantLabelPropertiesIndicesInfo(labels, properties);
    };

    virtual bool EdgeTypeIndexExists(EdgeTypeId edge_type) const = 0;

    virtual bool EdgeTypePropertyIndexExists(EdgeTypeId edge_type, PropertyId property) const = 0;

    virtual bool EdgePropertyIndexExists(PropertyId property) const = 0;

    bool TextIndexExists(const std::string &index_name) const {
      return storage_->indices_.text_index_.IndexExists(index_name);
    }

    void TextIndexAddVertex(const VertexAccessor &vertex) {
      storage_->indices_.text_index_.AddNode(vertex.vertex_, storage_->name_id_mapper_.get());
    }

    void TextIndexUpdateVertex(const VertexAccessor &vertex, const std::vector<LabelId> &removed_labels = {}) {
      storage_->indices_.text_index_.UpdateNode(vertex.vertex_, storage_->name_id_mapper_.get(), removed_labels);
    }

    std::vector<Gid> TextIndexSearch(const std::string &index_name, const std::string &search_query,
                                     text_search_mode search_mode) const {
      return storage_->indices_.text_index_.Search(index_name, search_query, search_mode);
    }

    std::string TextIndexAggregate(const std::string &index_name, const std::string &search_query,
                                   const std::string &aggregation_query) const {
      return storage_->indices_.text_index_.Aggregate(index_name, search_query, aggregation_query);
    }

    virtual bool PointIndexExists(LabelId label, PropertyId property) const = 0;

    virtual IndicesInfo ListAllIndices() const = 0;

    virtual ConstraintsInfo ListAllConstraints() const = 0;

    // NOLINTNEXTLINE(google-default-arguments)
    virtual utils::BasicResult<StorageManipulationError, void> Commit(CommitReplArgs reparg = {},
                                                                      DatabaseAccessProtector db_acc = {}) = 0;

    // NOLINTNEXTLINE(google-default-arguments)
    virtual utils::BasicResult<StorageManipulationError, void> PeriodicCommit(CommitReplArgs reparg = {},
                                                                              DatabaseAccessProtector db_acc = {}) = 0;

    virtual void Abort() = 0;

    virtual void FinalizeTransaction() = 0;

    std::optional<uint64_t> GetTransactionId() const;

    std::unique_ptr<utils::QueryMemoryTracker> &GetQueryMemoryTracker();

    void AdvanceCommand();

    const std::string &LabelToName(LabelId label) const { return storage_->LabelToName(label); }

    const std::string &PropertyToName(PropertyId property) const { return storage_->PropertyToName(property); }

    const std::string &EdgeTypeToName(EdgeTypeId edge_type) const { return storage_->EdgeTypeToName(edge_type); }

    LabelId NameToLabel(std::string_view name) { return storage_->NameToLabel(name); }

    PropertyId NameToProperty(std::string_view name) { return storage_->NameToProperty(name); }

    std::optional<PropertyId> NameToPropertyIfExists(std::string_view name) const {
      return storage_->NameToPropertyIfExists(name);
    }

    EdgeTypeId NameToEdgeType(std::string_view name) { return storage_->NameToEdgeType(name); }

    StorageMode GetCreationStorageMode() const noexcept;

    const std::string &id() const { return storage_->name(); }

    std::vector<LabelId> ListAllPossiblyPresentVertexLabels() const;

    std::vector<EdgeTypeId> ListAllPossiblyPresentEdgeTypes() const;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(LabelId label,
                                                                              bool unique_access_needed = true) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
        LabelId label, std::vector<storage::PropertyId> &&properties) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(EdgeTypeId edge_type,
                                                                              bool unique_access_needed = true) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(EdgeTypeId edge_type,
                                                                              PropertyId property) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateGlobalEdgeIndex(PropertyId property) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
        LabelId label, std::vector<storage::PropertyId> &&properties) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(EdgeTypeId edge_type) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(EdgeTypeId edge_type,
                                                                            PropertyId property) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> DropGlobalEdgeIndex(PropertyId property) = 0;

    virtual utils::BasicResult<storage::StorageIndexDefinitionError, void> CreatePointIndex(
        storage::LabelId label, storage::PropertyId property) = 0;

    virtual utils::BasicResult<storage::StorageIndexDefinitionError, void> DropPointIndex(
        storage::LabelId label, storage::PropertyId property) = 0;

    void CreateTextIndex(const std::string &index_name, LabelId label);

    void DropTextIndex(const std::string &index_name);

    virtual utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateVectorIndex(VectorIndexSpec spec) = 0;

    virtual utils::BasicResult<storage::StorageIndexDefinitionError, void> DropVectorIndex(
        std::string_view index_name) = 0;

    void TryInsertVertexIntoVectorIndex(const VertexAccessor &vertex);

    virtual utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
        LabelId label, PropertyId property) = 0;

    virtual utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
        LabelId label, PropertyId property) = 0;

    virtual utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
    CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) = 0;

    virtual UniqueConstraints::DeletionStatus DropUniqueConstraint(LabelId label,
                                                                   const std::set<PropertyId> &properties) = 0;

    virtual utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateTypeConstraint(
        LabelId label, PropertyId property, TypeConstraintKind type) = 0;

    virtual utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropTypeConstraint(
        LabelId label, PropertyId property, TypeConstraintKind type) = 0;

    virtual void DropGraph() = 0;

    auto GetTransaction() -> Transaction * { return std::addressof(transaction_); }

    auto GetEnumStoreUnique() -> EnumStore & {
      DMG_ASSERT(unique_guard_.owns_lock());
      return storage_->enum_store_;
    }
    auto GetEnumStoreShared() const -> EnumStore const & { return storage_->enum_store_; }

    auto CreateEnum(std::string_view name, std::span<std::string const> values)
        -> memgraph::utils::BasicResult<EnumStorageError, EnumTypeId> {
      auto res = storage_->enum_store_.RegisterEnum(name, values);
      if (res.HasValue()) {
        transaction_.md_deltas.emplace_back(MetadataDelta::enum_create, res.GetValue());
      }
      return res;
    }

    auto EnumAlterAdd(std::string_view name, std::string_view value)
        -> utils::BasicResult<storage::EnumStorageError, storage::Enum> {
      auto res = storage_->enum_store_.AddValue(name, value);
      if (res.HasValue()) {
        transaction_.md_deltas.emplace_back(MetadataDelta::enum_alter_add, res.GetValue());
      }
      return res;
    }

    auto EnumAlterUpdate(std::string_view name, std::string_view old_value, std::string_view new_value)
        -> utils::BasicResult<storage::EnumStorageError, storage::Enum> {
      auto res = storage_->enum_store_.UpdateValue(name, old_value, new_value);
      if (res.HasValue()) {
        transaction_.md_deltas.emplace_back(MetadataDelta::enum_alter_update, res.GetValue(), std::string{old_value});
      }
      return res;
    }

    auto ShowEnums() { return storage_->enum_store_.AllRegistered(); }

    auto GetEnumValue(std::string_view name, std::string_view value) const
        -> utils::BasicResult<EnumStorageError, Enum> {
      return storage_->enum_store_.ToEnum(name, value);
    }

    auto GetEnumValue(std::string_view enum_str) -> utils::BasicResult<EnumStorageError, Enum> {
      return storage_->enum_store_.ToEnum(enum_str);
    }

    virtual auto PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                               PropertyValue const &point_value, PropertyValue const &boundary_value,
                               PointDistanceCondition condition) -> PointIterable = 0;

    virtual auto PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                               PropertyValue const &bottom_left, PropertyValue const &top_right,
                               WithinBBoxCondition condition) -> PointIterable = 0;

    virtual std::vector<std::tuple<VertexAccessor, double, double>> VectorIndexSearch(
        const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) = 0;

    virtual std::vector<VectorIndexInfo> ListAllVectorIndices() const = 0;

    auto GetNameIdMapper() const -> NameIdMapper * { return storage_->name_id_mapper_.get(); }

   protected:
    Storage *storage_;
    utils::SharedResourceLockGuard storage_guard_;
    std::unique_lock<utils::ResourceLock> unique_guard_;  // TODO: Split the accessor into Shared/Unique
    /// IMPORTANT: transaction_ has to be constructed after the guards (so that destruction is in correct order)
    Transaction transaction_;
    std::optional<uint64_t> commit_timestamp_;
    bool is_transaction_active_;

    // Detach delete private methods
    Result<std::optional<std::unordered_set<Vertex *>>> PrepareDeletableNodes(
        const std::vector<VertexAccessor *> &vertices);
    EdgeInfoForDeletion PrepareDeletableEdges(const std::unordered_set<Vertex *> &vertices,
                                              const std::vector<EdgeAccessor *> &edges, bool detach) noexcept;
    Result<std::optional<std::vector<EdgeAccessor>>> ClearEdgesOnVertices(
        const std::unordered_set<Vertex *> &vertices, std::unordered_set<Gid> &deleted_edge_ids,
        std::optional<SchemaInfo::ModifyingAccessor> &schema_acc);
    Result<std::optional<std::vector<EdgeAccessor>>> DetachRemainingEdges(
        EdgeInfoForDeletion info, std::unordered_set<Gid> &partially_detached_edge_ids,
        std::optional<SchemaInfo::ModifyingAccessor> &schema_acc);
    Result<std::vector<VertexAccessor>> TryDeleteVertices(const std::unordered_set<Vertex *> &vertices,
                                                          std::optional<SchemaInfo::ModifyingAccessor> &schema_acc);
    void MarkEdgeAsDeleted(Edge *edge);

   private:
    StorageMode creation_storage_mode_;
  };

  const std::string &LabelToName(LabelId label) const { return name_id_mapper_->IdToName(label.AsUint()); }

  const std::string &PropertyToName(PropertyId property) const { return name_id_mapper_->IdToName(property.AsUint()); }

  const std::string &EdgeTypeToName(EdgeTypeId edge_type) const {
    return name_id_mapper_->IdToName(edge_type.AsUint());
  }

  LabelId NameToLabel(const std::string_view name) const { return LabelId::FromUint(name_id_mapper_->NameToId(name)); }

  PropertyId NameToProperty(const std::string_view name) const {
    return PropertyId::FromUint(name_id_mapper_->NameToId(name));
  }

  std::optional<PropertyId> NameToPropertyIfExists(std::string_view name) const {
    const auto id = name_id_mapper_->NameToIdIfExists(name);
    if (!id) {
      return std::nullopt;
    }
    return PropertyId::FromUint(*id);
  }

  EdgeTypeId NameToEdgeType(const std::string_view name) const {
    return EdgeTypeId::FromUint(name_id_mapper_->NameToId(name));
  }

  StorageMode GetStorageMode() const noexcept;

  virtual void FreeMemory(std::unique_lock<utils::ResourceLock> main_guard, bool periodic) = 0;

  void FreeMemory() {
    if (storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL) {
      FreeMemory(std::unique_lock{main_lock_}, false);
    } else {
      FreeMemory({}, false);
    }
  }

  virtual std::unique_ptr<Accessor> Access(Accessor::Type rw_type,
                                           std::optional<IsolationLevel> override_isolation_level,
                                           std::optional<std::chrono::milliseconds> timeout) = 0;
  std::unique_ptr<Accessor> Access(std::optional<IsolationLevel> override_isolation_level) {
    return Access(Accessor::Type::WRITE, override_isolation_level, std::nullopt);
  }
  std::unique_ptr<Accessor> Access(Accessor::Type rw_type) { return Access(rw_type, std::nullopt, std::nullopt); }
  std::unique_ptr<Accessor> Access() { return Access(Accessor::Type::WRITE, {}, std::nullopt); }

  virtual std::unique_ptr<Accessor> UniqueAccess(std::optional<IsolationLevel> override_isolation_level,
                                                 std::optional<std::chrono::milliseconds> timeout) = 0;
  std::unique_ptr<Accessor> UniqueAccess(std::optional<IsolationLevel> override_isolation_level) {
    return UniqueAccess(override_isolation_level, std::nullopt);
  }
  std::unique_ptr<Accessor> UniqueAccess() { return UniqueAccess({}, std::nullopt); }

  virtual std::unique_ptr<Accessor> ReadOnlyAccess(std::optional<IsolationLevel> override_isolation_level,
                                                   std::optional<std::chrono::milliseconds> timeout) = 0;
  std::unique_ptr<Accessor> ReadOnlyAccess(std::optional<IsolationLevel> override_isolation_level) {
    return ReadOnlyAccess(override_isolation_level, std::nullopt);
  }
  std::unique_ptr<Accessor> ReadOnlyAccess() { return ReadOnlyAccess({}, std::nullopt); }

  enum class SetIsolationLevelError : uint8_t { DisabledForAnalyticalMode };

  utils::BasicResult<SetIsolationLevelError> SetIsolationLevel(IsolationLevel isolation_level);
  IsolationLevel GetIsolationLevel() const noexcept;

  virtual StorageInfo GetBaseInfo() = 0;

  static std::vector<EventInfo> GetMetrics() noexcept;

  virtual StorageInfo GetInfo() = 0;

  virtual Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) = 0;

  virtual void PrepareForNewEpoch() = 0;

  auto GetReplicaState(std::string_view name) const -> std::optional<replication::ReplicaState> {
    return repl_storage_state_.GetReplicaState(name);
  }

  // TODO: make non-public
  ReplicationStorageState repl_storage_state_;

  // Main storage lock.
  // Accessors take a shared lock when starting, so it is possible to block
  // creation of new accessors by taking a unique lock. This is used when doing
  // operations on storage that affect the global state, for example index
  // creation.
  mutable utils::ResourceLock main_lock_;

  // Even though the edge count is already kept in the `edges_` SkipList, the
  // list is used only when properties are enabled for edges. Because of that we
  // keep a separate count of edges that is always updated. This counter is also used
  // for disk storage.
  std::atomic<uint64_t> edge_count_{0};

  std::unique_ptr<NameIdMapper> name_id_mapper_;
  Config config_;

  // Transaction engine
  mutable utils::SpinLock engine_lock_;
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};

  IsolationLevel isolation_level_;
  StorageMode storage_mode_;

  Indices indices_;
  Constraints constraints_;

  // Datastructures to provide fast retrieval of node-label and
  // edge-type related metadata.
  // Currently we should not remove any node-labels or edge-types even
  // if the set of given types are currently not present in the
  // database. This metadata is usually used by client side
  // applications that want to be aware of the kind of data that *may*
  // be present in the database.

  // TODO(gvolfing): check if this would be faster with flat_maps.
  utils::SynchronizedMetaDataStore<LabelId> stored_node_labels_;
  utils::SynchronizedMetaDataStore<EdgeTypeId> stored_edge_types_;

  // Maps that hold onto labels and edge-types that have to be created.
  // Used only if the label/edge-type auto-creation flags are enabled,
  // does not affect index creation otherwise. The counter in the maps
  // are needed because it is possible that two concurrent transactions
  // are introducing the same label/edge-type in which case we do only
  // build the index when the last transaction is commiting. The counter
  // is used trace if the currently commiting transaction is the last one
  // that would like to introduce a new index.
  utils::Synchronized<std::map<LabelId, uint32_t>, utils::SpinLock> labels_to_auto_index_;
  utils::Synchronized<std::map<EdgeTypeId, uint32_t>, utils::SpinLock> edge_types_to_auto_index_;

  std::atomic<uint64_t> vertex_id_{0};  // contains Vertex Gid that has not been used yet
  std::atomic<uint64_t> edge_id_{0};    // contains Edge Gid that has not been used yet

  // Mutable methods only safe if we have UniqueAccess to this storage
  EnumStore enum_store_;

  SchemaInfo schema_info_;
};

}  // namespace memgraph::storage
