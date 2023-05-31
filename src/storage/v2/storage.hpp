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

#include <span>

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
#include "storage/v2/storage_error.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage {

struct Transaction;
class EdgeAccessor;

enum class ReplicationRole : uint8_t { MAIN, REPLICA };

/// Generic access to different kinds of vertex iterations.
///
/// This class should be the primary type used by the client code to iterate
/// over vertices inside a Storage instance.

/// Structure used to return information about existing indices in the storage.
struct IndicesInfo {
  std::vector<LabelId> label;
  std::vector<std::pair<LabelId, PropertyId>> label_property;
};

/// Structure used to return information about existing constraints in the
/// storage.
struct ConstraintsInfo {
  std::vector<std::pair<LabelId, PropertyId>> existence;
  std::vector<std::pair<LabelId, std::set<PropertyId>>> unique;
};

/// Structure used to return information about the storage.
struct StorageInfo {
  uint64_t vertex_count;
  uint64_t edge_count;
  double average_degree;
  uint64_t memory_usage;
  uint64_t disk_usage;
};

class Storage {
 public:
  Storage(Config config, Constraints *constraints, StorageMode storage_mode);

  Storage(const Storage &) = delete;
  Storage(Storage &&) = delete;
  Storage &operator=(const Storage &) = delete;
  Storage &operator=(Storage &&) = delete;

  virtual ~Storage() {}

  class Accessor {
   public:
    Accessor(Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode);
    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;
    Accessor &operator=(Accessor &&other) = delete;

    // NOTE: After the accessor is moved, all objects derived from it (accessors
    // and iterators) are *invalid*. You have to get all derived objects again.
    Accessor(Accessor &&other) noexcept;

    virtual ~Accessor() {}

    /// @throw std::bad_alloc
    virtual VertexAccessor CreateVertex() = 0;

    virtual std::optional<VertexAccessor> FindVertex(Gid gid, View view) = 0;

    virtual VerticesIterable Vertices(View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, PropertyId property, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, PropertyId property,
                                      const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                      const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) = 0;

    /// Return approximate number of all vertices in the database.
    /// Note that this is always an over-estimate and never an under-estimate.
    virtual uint64_t ApproximateVertexCount() const = 0;

    /// Return approximate number of vertices with the given label.
    /// Note that this is always an over-estimate and never an under-estimate.
    virtual uint64_t ApproximateVertexCount(LabelId label) const = 0;

    /// Return approximate number of vertices with the given label and property.
    /// Note that this is always an over-estimate and never an under-estimate.
    virtual uint64_t ApproximateVertexCount(LabelId label, PropertyId property) const = 0;

    /// Return approximate number of vertices with the given label and the given
    /// value for the given property. Note that this is always an over-estimate
    /// and never an under-estimate.
    virtual uint64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const = 0;

    /// Return approximate number of vertices with the given label and value for
    /// the given property in the range defined by provided upper and lower
    /// bounds.
    virtual uint64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                            const std::optional<utils::Bound<PropertyValue>> &lower,
                                            const std::optional<utils::Bound<PropertyValue>> &upper) const = 0;

    virtual std::optional<storage::IndexStats> GetIndexStats(const storage::LabelId &label,
                                                             const storage::PropertyId &property) const = 0;

    virtual std::vector<std::pair<LabelId, PropertyId>> ClearIndexStats() = 0;

    virtual std::vector<std::pair<LabelId, PropertyId>> DeleteIndexStatsForLabels(
        const std::span<std::string> labels) = 0;

    virtual void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                               const IndexStats &stats) = 0;

    /// @return Accessor to the deleted vertex if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    virtual Result<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex) = 0;

    /// @return Accessor to the deleted vertex and deleted edges if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    virtual Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachDeleteVertex(
        VertexAccessor *vertex) = 0;

    virtual void PrefetchInEdges(const VertexAccessor &vertex_acc) = 0;

    virtual void PrefetchOutEdges(const VertexAccessor &vertex_acc) = 0;

    /// @throw std::bad_alloc
    virtual Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) = 0;

    /// Accessor to the deleted edge if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    virtual Result<std::optional<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge) = 0;

    virtual bool LabelIndexExists(LabelId label) const = 0;

    virtual bool LabelPropertyIndexExists(LabelId label, PropertyId property) const = 0;

    virtual IndicesInfo ListAllIndices() const = 0;

    virtual ConstraintsInfo ListAllConstraints() const = 0;

    /// Returns void if the transaction has been committed.
    /// Returns `StorageDataManipulationError` if an error occures. Error can be:
    /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
    /// * `ConstraintViolation`: the changes made by this transaction violate an existence or unique constraint. In this
    /// case the transaction is automatically aborted.
    /// @throw std::bad_alloc
    virtual utils::BasicResult<StorageDataManipulationError, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp = {}) = 0;

    /// @throw std::bad_alloc
    virtual void Abort() = 0;

    virtual void FinalizeTransaction() = 0;

    virtual void PrepareForNextQuery() = 0;

    std::optional<uint64_t> GetTransactionId() const;

    void AdvanceCommand();

    const std::string &LabelToName(LabelId label) const;

    const std::string &PropertyToName(PropertyId property) const;

    const std::string &EdgeTypeToName(EdgeTypeId edge_type) const;

    /// @throw std::bad_alloc if unable to insert a new mapping
    LabelId NameToLabel(std::string_view name);

    /// @throw std::bad_alloc if unable to insert a new mapping
    PropertyId NameToProperty(std::string_view name);

    /// @throw std::bad_alloc if unable to insert a new mapping
    EdgeTypeId NameToEdgeType(std::string_view name);

   protected:
    Storage *storage_;
    std::shared_lock<utils::RWLock> storage_guard_;
    Transaction transaction_;
    std::optional<uint64_t> commit_timestamp_;
    /// TODO: andi how can we remove usage of this?
    bool is_transaction_active_;
  };

  bool LockPath();
  bool UnlockPath();

  const std::string &LabelToName(LabelId label) const;

  const std::string &PropertyToName(PropertyId property) const;

  const std::string &EdgeTypeToName(EdgeTypeId edge_type) const;

  /// @throw std::bad_alloc if unable to insert a new mapping
  LabelId NameToLabel(std::string_view name);

  /// @throw std::bad_alloc if unable to insert a new mapping
  PropertyId NameToProperty(std::string_view name);

  /// @throw std::bad_alloc if unable to insert a new mapping
  EdgeTypeId NameToEdgeType(std::string_view name);

  virtual std::unique_ptr<Accessor> Access(std::optional<IsolationLevel> override_isolation_level) = 0;
  std::unique_ptr<Accessor> Access() { return Access(std::optional<IsolationLevel>{}); }

  /// Create an index.
  /// Returns void if the index has been created.
  /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
  /// * `IndexDefinitionError`: the index already exists.
  /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// @throw std::bad_alloc
  virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
      LabelId label, std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(LabelId label) {
    return CreateIndex(label, std::optional<uint64_t>{});
  }

  /// Create an index.
  /// Returns void if the index has been created.
  /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
  /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `IndexDefinitionError`: the index already exists.
  /// @throw std::bad_alloc
  virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(LabelId label, PropertyId property) {
    return CreateIndex(label, property, std::optional<uint64_t>{});
  }

  /// Drop an existing index.
  /// Returns void if the index has been dropped.
  /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
  /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `IndexDefinitionError`: the index does not exist.
  virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label) {
    return DropIndex(label, std::optional<uint64_t>{});
  }

  /// Drop an existing index.
  /// Returns void if the index has been dropped.
  /// Returns `StorageIndexDefinitionError` if an error occures. Error can be:
  /// * `ReplicationError`:  there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `IndexDefinitionError`: the index does not exist.
  virtual utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageIndexDefinitionError, void> DropIndex(LabelId label, PropertyId property) {
    return DropIndex(label, property, std::optional<uint64_t>{});
  }

  virtual IndicesInfo ListAllIndices() const = 0;

  /// Returns void if the existence constraint has been created.
  /// Returns `StorageExistenceConstraintDefinitionError` if an error occures. Error can be:
  /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `ConstraintViolation`: there is already a vertex existing that would break this new constraint.
  /// * `ConstraintDefinitionError`: the constraint already exists.
  /// @throw std::bad_alloc
  /// @throw std::length_error
  virtual utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(LabelId label,
                                                                                                PropertyId property) {
    return CreateExistenceConstraint(label, property, std::optional<uint64_t>{});
  }

  /// Drop an existing existence constraint.
  /// Returns void if the existence constraint has been dropped.
  /// Returns `StorageExistenceConstraintDroppingError` if an error occures. Error can be:
  /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// * `ConstraintDefinitionError`: the constraint did not exists.
  virtual utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(LabelId label,
                                                                                            PropertyId property) {
    return DropExistenceConstraint(label, property, std::optional<uint64_t>{});
  }

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
  virtual utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
  CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                         std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus> CreateUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties) {
    return CreateUniqueConstraint(label, properties, std::optional<uint64_t>{});
  }

  /// Removes an existing unique constraint.
  /// Returns `StorageUniqueConstraintDroppingError` if an error occures. Error can be:
  /// * `ReplicationError`: there is at least one SYNC replica that has not confirmed receiving the transaction.
  /// Returns `UniqueConstraints::DeletionStatus` otherwise. Value can be:
  /// * `SUCCESS` if constraint was successfully removed,
  /// * `NOT_FOUND` if the specified constraint was not found,
  /// * `EMPTY_PROPERTIES` if the property set is empty, or
  /// * `PROPERTIES_SIZE_LIMIT_EXCEEDED` if the property set exceeds the limit of maximum number of properties.
  virtual utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus>
  DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                       std::optional<uint64_t> desired_commit_timestamp) = 0;

  utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus> DropUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties) {
    return DropUniqueConstraint(label, properties, std::optional<uint64_t>{});
  }

  virtual ConstraintsInfo ListAllConstraints() const = 0;

  virtual bool SetReplicaRole(io::network::Endpoint endpoint, const replication::ReplicationServerConfig &config) = 0;

  bool SetReplicaRole(io::network::Endpoint endpoint) {
    return SetReplicaRole(endpoint, replication::ReplicationServerConfig{});
  }

  virtual bool SetMainReplicationRole() = 0;

  enum class RegisterReplicaError : uint8_t {
    NAME_EXISTS,
    END_POINT_EXISTS,
    CONNECTION_FAILED,
    COULD_NOT_BE_PERSISTED
  };

  /// @pre The instance should have a MAIN role
  /// @pre Timeout can only be set for SYNC replication
  virtual utils::BasicResult<RegisterReplicaError, void> RegisterReplica(
      std::string name, io::network::Endpoint endpoint, replication::ReplicationMode replication_mode,
      replication::RegistrationMode registration_mode, const replication::ReplicationClientConfig &config) = 0;

  utils::BasicResult<RegisterReplicaError, void> RegisterReplica(std::string name, io::network::Endpoint endpoint,
                                                                 replication::ReplicationMode replication_mode,
                                                                 replication::RegistrationMode registration_mode) {
    return RegisterReplica(name, endpoint, replication_mode, registration_mode, replication::ReplicationClientConfig{});
  }

  /// @pre The instance should have a MAIN role
  virtual bool UnregisterReplica(const std::string &name) = 0;

  virtual std::optional<replication::ReplicaState> GetReplicaState(std::string_view name) = 0;

  virtual ReplicationRole GetReplicationRole() const = 0;

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

  virtual std::vector<ReplicaInfo> ReplicasInfo() = 0;

  virtual void FreeMemory() = 0;

  enum class SetIsolationLevelError : uint8_t { DisabledForAnalyticalMode };

  virtual utils::BasicResult<SetIsolationLevelError> SetIsolationLevel(IsolationLevel isolation_level) = 0;

  virtual void SetStorageMode(StorageMode storage_mode) = 0;

  virtual StorageMode GetStorageMode() = 0;

  virtual StorageInfo GetInfo() const = 0;

  enum class CreateSnapshotError : uint8_t {
    DisabledForReplica,
    DisabledForAnalyticsPeriodicCommit,
    ReachedMaxNumTries
  };

  virtual utils::BasicResult<CreateSnapshotError> CreateSnapshot(std::optional<bool> is_periodic) = 0;

  virtual Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) = 0;

  // Main storage lock.
  // Accessors take a shared lock when starting, so it is possible to block
  // creation of new accessors by taking a unique lock. This is used when doing
  // operations on storage that affect the global state, for example index
  // creation.
  mutable utils::RWLock main_lock_{utils::RWLock::Priority::WRITE};

  // Even though the edge count is already kept in the `edges_` SkipList, the
  // list is used only when properties are enabled for edges. Because of that we
  // keep a separate count of edges that is always updated.
  std::atomic<uint64_t> edge_count_{0};

  NameIdMapper name_id_mapper_;
  Config config_;

  // Transaction engine
  utils::SpinLock engine_lock_;
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};

  // Durability
  std::filesystem::path snapshot_directory_;
  std::filesystem::path wal_directory_;
  std::filesystem::path lock_file_path_;
  utils::OutputFile lock_file_handle_;
  std::unique_ptr<kvstore::KVStore> storage_;

  Indices indices_;
  Constraints constraints_;

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
  // At the moment when S1 commited a transaction with timestamp 20, and S2
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

  // Last commited timestamp
  std::atomic<uint64_t> last_commit_timestamp_{kTimestampInitialId};
};

}  // namespace memgraph::storage
