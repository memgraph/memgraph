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

#include <chrono>
#include <semaphore>
#include <span>
#include <thread>

#include "io/network/endpoint.hpp"
#include "kvstore/kvstore.hpp"
#include "query/exceptions.hpp"
#include "replication/config.hpp"
#include "storage/v2/all_vertices_iterable.hpp"
#include "storage/v2/commit_log.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/replication/replication_server.hpp"
#include "storage/v2/replication/replication_storage_state.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_histogram.hpp"
#include "utils/resource_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/timer.hpp"
#include "utils/uuid.hpp"

namespace memgraph::metrics {
extern const Event SnapshotCreationLatency_us;

extern const Event ActiveLabelIndices;
extern const Event ActiveLabelPropertyIndices;
}  // namespace memgraph::metrics

namespace memgraph::storage {

struct Transaction;
class EdgeAccessor;

struct IndicesInfo {
  std::vector<LabelId> label;
  std::vector<std::pair<LabelId, PropertyId>> label_property;
};

struct ConstraintsInfo {
  std::vector<std::pair<LabelId, PropertyId>> existence;
  std::vector<std::pair<LabelId, std::set<PropertyId>>> unique;
};

struct StorageInfo {
  uint64_t vertex_count;
  uint64_t edge_count;
  double average_degree;
  uint64_t memory_usage;
  uint64_t disk_usage;
};

struct EdgeInfoForDeletion {
  std::unordered_set<Gid> partial_src_edge_ids{};
  std::unordered_set<Gid> partial_dest_edge_ids{};
  std::unordered_set<Vertex *> partial_src_vertices{};
  std::unordered_set<Vertex *> partial_dest_vertices{};
};

class Storage {
  friend class ReplicationServer;
  friend class ReplicationClient;

 public:
  Storage(Config config, StorageMode storage_mode);

  Storage(const Storage &) = delete;
  Storage(Storage &&) = delete;
  Storage &operator=(const Storage &) = delete;
  Storage &operator=(Storage &&) = delete;

  virtual ~Storage() = default;

  const std::string &id() const { return id_; }

  class Accessor {
   public:
    static constexpr struct SharedAccess {
    } shared_access;
    static constexpr struct UniqueAccess {
    } unique_access;

    Accessor(SharedAccess /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode);
    Accessor(UniqueAccess /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode);
    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;
    Accessor &operator=(Accessor &&other) = delete;

    Accessor(Accessor &&other) noexcept;

    virtual ~Accessor() {}

    virtual VertexAccessor CreateVertex() = 0;

    virtual std::optional<VertexAccessor> FindVertex(Gid gid, View view) = 0;

    virtual VerticesIterable Vertices(View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, PropertyId property, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, PropertyId property,
                                      const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                      const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) = 0;

    virtual Result<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex);

    virtual Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachDeleteVertex(
        VertexAccessor *vertex);

    virtual Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>> DetachDelete(
        std::vector<VertexAccessor *> nodes, std::vector<EdgeAccessor *> edges, bool detach);

    virtual uint64_t ApproximateVertexCount() const = 0;

    virtual uint64_t ApproximateVertexCount(LabelId label) const = 0;

    virtual uint64_t ApproximateVertexCount(LabelId label, PropertyId property) const = 0;

    virtual uint64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const = 0;

    virtual uint64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                            const std::optional<utils::Bound<PropertyValue>> &lower,
                                            const std::optional<utils::Bound<PropertyValue>> &upper) const = 0;

    virtual std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const = 0;

    virtual std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
        const storage::LabelId &label, const storage::PropertyId &property) const = 0;

    virtual void SetIndexStats(const storage::LabelId &label, const LabelIndexStats &stats) = 0;

    virtual void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                               const LabelPropertyIndexStats &stats) = 0;

    virtual std::vector<std::pair<LabelId, PropertyId>> DeleteLabelPropertyIndexStats(
        const storage::LabelId &label) = 0;

    virtual bool DeleteLabelIndexStats(const storage::LabelId &label) = 0;

    virtual Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) = 0;

    virtual Result<EdgeAccessor> EdgeSetFrom(EdgeAccessor *edge, VertexAccessor *new_from) = 0;

    virtual Result<EdgeAccessor> EdgeSetTo(EdgeAccessor *edge, VertexAccessor *new_to) = 0;

    virtual Result<std::optional<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge);

    virtual bool LabelIndexExists(LabelId label) const = 0;

    virtual bool LabelPropertyIndexExists(LabelId label, PropertyId property) const = 0;

    virtual IndicesInfo ListAllIndices() const = 0;

    virtual ConstraintsInfo ListAllConstraints() const = 0;

    // NOLINTNEXTLINE(google-default-arguments)
    virtual utils::BasicResult<StorageManipulationError, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp = {}) = 0;

    virtual void Abort() = 0;

    virtual void FinalizeTransaction() = 0;

    std::optional<uint64_t> GetTransactionId() const;

    void AdvanceCommand();

    const std::string &LabelToName(LabelId label) const { return storage_->LabelToName(label); }

    const std::string &PropertyToName(PropertyId property) const { return storage_->PropertyToName(property); }

    const std::string &EdgeTypeToName(EdgeTypeId edge_type) const { return storage_->EdgeTypeToName(edge_type); }

    LabelId NameToLabel(std::string_view name) { return storage_->NameToLabel(name); }

    PropertyId NameToProperty(std::string_view name) { return storage_->NameToProperty(name); }

    EdgeTypeId NameToEdgeType(std::string_view name) { return storage_->NameToEdgeType(name); }

    StorageMode GetCreationStorageMode() const;

    const std::string &id() const { return storage_->id(); }

    virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(LabelId label) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(LabelId label, PropertyId property) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label) = 0;

    virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label, PropertyId property) = 0;

    virtual utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
        LabelId label, PropertyId property) = 0;

    virtual utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
        LabelId label, PropertyId property) = 0;

    virtual utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
    CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) = 0;

    virtual UniqueConstraints::DeletionStatus DropUniqueConstraint(LabelId label,
                                                                   const std::set<PropertyId> &properties) = 0;

   protected:
    Storage *storage_;
    std::shared_lock<utils::ResourceLock> storage_guard_;
    std::unique_lock<utils::ResourceLock> unique_guard_;  // TODO: Split the accessor into Shared/Unique
    Transaction transaction_;
    std::optional<uint64_t> commit_timestamp_;
    bool is_transaction_active_;

    // Detach delete private methods
    Result<std::optional<std::unordered_set<Vertex *>>> PrepareDeletableNodes(
        const std::vector<VertexAccessor *> &vertices);
    EdgeInfoForDeletion PrepareDeletableEdges(const std::unordered_set<Vertex *> &vertices,
                                              const std::vector<EdgeAccessor *> &edges, bool detach) noexcept;
    Result<std::optional<std::vector<EdgeAccessor>>> ClearEdgesOnVertices(const std::unordered_set<Vertex *> &vertices,
                                                                          std::unordered_set<Gid> &deleted_edge_ids);
    Result<std::optional<std::vector<EdgeAccessor>>> DetachRemainingEdges(
        EdgeInfoForDeletion info, std::unordered_set<Gid> &partially_detached_edge_ids);
    Result<std::vector<VertexAccessor>> TryDeleteVertices(const std::unordered_set<Vertex *> &vertices);
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

  EdgeTypeId NameToEdgeType(const std::string_view name) const {
    return EdgeTypeId::FromUint(name_id_mapper_->NameToId(name));
  }

  void SetStorageMode(StorageMode storage_mode);

  StorageMode GetStorageMode() const;

  virtual void FreeMemory(std::unique_lock<utils::ResourceLock> main_guard) = 0;

  void FreeMemory() { FreeMemory({}); }

  virtual std::unique_ptr<Accessor> Access(std::optional<IsolationLevel> override_isolation_level) = 0;
  std::unique_ptr<Accessor> Access() { return Access(std::optional<IsolationLevel>{}); }

  virtual std::unique_ptr<Accessor> UniqueAccess(std::optional<IsolationLevel> override_isolation_level) = 0;
  std::unique_ptr<Accessor> UniqueAccess() { return UniqueAccess(std::optional<IsolationLevel>{}); }

  enum class SetIsolationLevelError : uint8_t { DisabledForAnalyticalMode };

  utils::BasicResult<SetIsolationLevelError> SetIsolationLevel(IsolationLevel isolation_level);
  IsolationLevel GetIsolationLevel() const noexcept;

  virtual StorageInfo GetInfo() const = 0;

  virtual Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) = 0;

  virtual void PrepareForNewEpoch(std::string prev_epoch) = 0;

  virtual auto CreateReplicationClient(const memgraph::replication::ReplicationClientConfig &config)
      -> std::unique_ptr<ReplicationClient> = 0;

  virtual auto CreateReplicationServer(const memgraph::replication::ReplicationServerConfig &config)
      -> std::unique_ptr<ReplicationServer> = 0;

  auto ReplicasInfo() const { return repl_storage_state_.ReplicasInfo(); }
  auto GetReplicaState(std::string_view name) const -> std::optional<replication::ReplicaState> {
    return repl_storage_state_.GetReplicaState(name);
  }

  // TODO: make non-public
  memgraph::replication::ReplicationState repl_state_;
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
  utils::SpinLock engine_lock_;
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};

  IsolationLevel isolation_level_;
  StorageMode storage_mode_;

  Indices indices_;
  Constraints constraints_;

  std::atomic<uint64_t> vertex_id_{0};
  std::atomic<uint64_t> edge_id_{0};
  const std::string id_;  //!< High-level assigned ID
};

}  // namespace memgraph::storage
