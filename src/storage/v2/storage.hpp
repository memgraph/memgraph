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
#include "storage/v2/all_vertices_iterable.hpp"
#include "storage/v2/commit_log.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/replication.hpp"
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/replication/replication_server.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_histogram.hpp"
#include "utils/rw_lock.hpp"
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

// For unique access to the storage, you need to lock the main_lock_.
// However, using unique_lock to lock it and then unlocking in another thread is UB.
// This broker is used to actually lock and unlock in a single thread, but dispatch
// locking requests.
// TODO: Look into this, there must a simpler way... Could we use a spin lock in storage?
class AccessBroker {
 public:
  class Grant {
   public:
    Grant() {}
    Grant(std::binary_semaphore *sem, std::atomic_int *waiting) : sem_(sem), waiting_(waiting) {
      ++(*waiting_);
      sem_->acquire();
      --(*waiting_);
    }
    ~Grant() {
      if (sem_) {
        sem_->release();
      }
    }
    Grant(Grant &&) = default;

    explicit operator bool() { return sem_ != nullptr; }

   private:
    std::binary_semaphore *sem_{};
    std::atomic_int *waiting_{};
  };

  explicit AccessBroker(utils::RWLock *mtx)
      : mtx_(mtx), l_(*mtx_, std::defer_lock), t_([&]() {
          bool ok = true;
          while (running_) {
            if (!ok) {
              sem_.acquire();
              ok = true;
            }
            if (l_.owns_lock()) {
              l_.unlock();
            } else if (waiting_) {
              l_.lock();
              ok = false;
              sem_.release();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
          }
        }) {}

  ~AccessBroker() {
    running_ = false;
    if (t_.joinable()) t_.join();
  }

  Grant UniqueAccess() { return {&sem_, &waiting_}; }

 private:
  utils::RWLock *mtx_;
  std::unique_lock<utils::RWLock> l_;
  std::atomic_int waiting_{0};
  std::atomic_bool running_{true};
  std::binary_semaphore sem_{0};
  std::thread t_;
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

    virtual std::vector<std::pair<LabelId, PropertyId>> ClearLabelPropertyIndexStats() = 0;

    virtual std::vector<LabelId> ClearLabelIndexStats() = 0;

    virtual std::vector<std::pair<LabelId, PropertyId>> DeleteLabelPropertyIndexStats(
        std::span<std::string> labels) = 0;

    virtual std::vector<LabelId> DeleteLabelIndexStats(std::span<std::string> labels) = 0;

    virtual void PrefetchInEdges(const VertexAccessor &vertex_acc) = 0;

    virtual void PrefetchOutEdges(const VertexAccessor &vertex_acc) = 0;

    virtual Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) = 0;

    virtual Result<EdgeAccessor> EdgeSetFrom(EdgeAccessor *edge, VertexAccessor *new_from) = 0;

    virtual Result<EdgeAccessor> EdgeSetTo(EdgeAccessor *edge, VertexAccessor *new_to) = 0;

    virtual Result<std::optional<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge);

    virtual bool LabelIndexExists(LabelId label) const = 0;

    virtual bool LabelPropertyIndexExists(LabelId label, PropertyId property) const = 0;

    virtual IndicesInfo ListAllIndices() const = 0;

    virtual ConstraintsInfo ListAllConstraints() const = 0;

    // NOLINTNEXTLINE(google-default-arguments)
    virtual utils::BasicResult<StorageDataManipulationError, void> Commit(
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

    // TODO: Better
    // bool UpgradeToUnique() {
    //   if (unique_guard_.owns_lock()) {
    //     return true;
    //   }
    //   if (transaction_.deltas.use().empty() && transaction_.md_deltas.empty()) {
    //     storage_guard_.unlock();
    //     unique_guard_.lock();
    //     return true;
    //   }
    //   return false;
    // }

   protected:
    Storage *storage_;
    std::shared_lock<utils::RWLock> storage_guard_;
    AccessBroker::Grant unique_grant_;
    std::unique_lock<utils::RWLock> unique_guard_;  // TODO: Split the accessor into Shared/Unique
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

  virtual void FreeMemory(std::unique_lock<utils::RWLock> main_guard) = 0;

  void FreeMemory() { FreeMemory({}); }

  virtual std::unique_ptr<Accessor> Access(std::optional<IsolationLevel> override_isolation_level) = 0;
  std::unique_ptr<Accessor> Access() { return Access(std::optional<IsolationLevel>{}); }

  virtual std::unique_ptr<Accessor> UniqueAccess(std::optional<IsolationLevel> override_isolation_level) = 0;
  std::unique_ptr<Accessor> UniqueAccess() { return UniqueAccess(std::optional<IsolationLevel>{}); }

  virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label) {
    return DropIndex(label, std::optional<uint64_t>{});
  }

  virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label, PropertyId property) {
    return DropIndex(label, property, std::optional<uint64_t>{});
  }

  IndicesInfo ListAllIndices() const;

  virtual utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) = 0;

  virtual utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) = 0;

  virtual utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
  CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                         std::optional<uint64_t> desired_commit_timestamp) = 0;

  virtual utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus>
  DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                       std::optional<uint64_t> desired_commit_timestamp) = 0;

  ConstraintsInfo ListAllConstraints() const;

  enum class SetIsolationLevelError : uint8_t { DisabledForAnalyticalMode };

  utils::BasicResult<SetIsolationLevelError> SetIsolationLevel(IsolationLevel isolation_level);
  IsolationLevel GetIsolationLevel() const noexcept;

  virtual StorageInfo GetInfo() const = 0;

  virtual Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) = 0;

  virtual void EstablishNewEpoch() = 0;

  virtual auto CreateReplicationClient(replication::ReplicationClientConfig const &config)
      -> std::unique_ptr<ReplicationClient> = 0;

  virtual auto CreateReplicationServer(const replication::ReplicationServerConfig &config)
      -> std::unique_ptr<ReplicationServer> = 0;

  /// REPLICATION
  bool SetReplicaRole(const replication::ReplicationServerConfig &config) {
    return replication_state_.SetReplicaRole(config, this);
  }
  bool SetMainReplicationRole() { return replication_state_.SetMainReplicationRole(this); }

  /// @pre The instance should have a MAIN role
  /// @pre Timeout can only be set for SYNC replication
  auto RegisterReplica(const replication::RegistrationMode registration_mode,
                       const replication::ReplicationClientConfig &config) {
    return replication_state_.RegisterReplica(registration_mode, config, this);
  }
  /// @pre The instance should have a MAIN role
  bool UnregisterReplica(const std::string &name) { return replication_state_.UnregisterReplica(name); }
  replication::ReplicationRole GetReplicationRole() const { return replication_state_.GetRole(); }
  auto ReplicasInfo() { return replication_state_.ReplicasInfo(); }
  std::optional<replication::ReplicaState> GetReplicaState(std::string_view name) {
    return replication_state_.GetReplicaState(name);
  }

 protected:
  void RestoreReplicas() { return replication_state_.RestoreReplicas(this); }
  void RestoreReplicationRole() { return replication_state_.RestoreReplicationRole(this); }

 public:
  // Main storage lock.
  // Accessors take a shared lock when starting, so it is possible to block
  // creation of new accessors by taking a unique lock. This is used when doing
  // operations on storage that affect the global state, for example index
  // creation.
  mutable utils::RWLock main_lock_{utils::RWLock::Priority::WRITE};

  mutable AccessBroker unique_access_;

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

 protected:
  ReplicationState replication_state_;
};

}  // namespace memgraph::storage
