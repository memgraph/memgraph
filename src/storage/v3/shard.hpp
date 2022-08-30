// Copyright 2022 Memgraph Ltd.
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

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <numeric>
#include <optional>
#include <shared_mutex>
#include <variant>
#include <vector>

#include "io/network/endpoint.hpp"
#include "kvstore/kvstore.hpp"
#include "storage/v3/commit_log.hpp"
#include "storage/v3/config.hpp"
#include "storage/v3/constraints.hpp"
#include "storage/v3/durability/metadata.hpp"
#include "storage/v3/durability/wal.hpp"
#include "storage/v3/edge.hpp"
#include "storage/v3/edge_accessor.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/indices.hpp"
#include "storage/v3/isolation_level.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/lexicographically_ordered_vertex.hpp"
#include "storage/v3/mvcc.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage/v3/vertices_skip_list.hpp"
#include "utils/exceptions.hpp"
#include "utils/file_locker.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/rw_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

/// REPLICATION ///
#include "rpc/server.hpp"
#include "storage/v3/replication/config.hpp"
#include "storage/v3/replication/enums.hpp"
#include "storage/v3/replication/rpc.hpp"
#include "storage/v3/replication/serialization.hpp"

namespace memgraph::storage::v3 {

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

/// Iterable for iterating through all vertices of a Storage.
///
/// An instance of this will be usually be wrapped inside VerticesIterable for
/// generic, public use.
class AllVerticesIterable final {
  VerticesSkipList::Accessor vertices_accessor_;
  Transaction *transaction_;
  View view_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
  const SchemaValidator *schema_validator_;
  const Schemas *schemas_;
  std::optional<VertexAccessor> vertex_;

 public:
  class Iterator final {
    AllVerticesIterable *self_;
    VerticesSkipList::Iterator it_;

   public:
    Iterator(AllVerticesIterable *self, VerticesSkipList::Iterator it);

    VertexAccessor operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const { return self_ == other.self_ && it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  AllVerticesIterable(VerticesSkipList::Accessor vertices_accessor, Transaction *transaction, View view,
                      Indices *indices, Constraints *constraints, Config::Items config,
                      const SchemaValidator &schema_validator)
      : vertices_accessor_(std::move(vertices_accessor)),
        transaction_(transaction),
        view_(view),
        indices_(indices),
        constraints_(constraints),
        config_(config),
        schema_validator_{&schema_validator} {}

  Iterator begin() { return {this, vertices_accessor_.begin()}; }
  Iterator end() { return {this, vertices_accessor_.end()}; }
};

/// Generic access to different kinds of vertex iterations.
///
/// This class should be the primary type used by the client code to iterate
/// over vertices inside a Storage instance.
class VerticesIterable final {
  enum class Type { ALL, BY_LABEL, BY_LABEL_PROPERTY };

  Type type_;
  union {
    AllVerticesIterable all_vertices_;
    LabelIndex::Iterable vertices_by_label_;
    LabelPropertyIndex::Iterable vertices_by_label_property_;
  };

 public:
  explicit VerticesIterable(AllVerticesIterable);
  explicit VerticesIterable(LabelIndex::Iterable);
  explicit VerticesIterable(LabelPropertyIndex::Iterable);

  VerticesIterable(const VerticesIterable &) = delete;
  VerticesIterable &operator=(const VerticesIterable &) = delete;

  VerticesIterable(VerticesIterable &&) noexcept;
  VerticesIterable &operator=(VerticesIterable &&) noexcept;

  ~VerticesIterable();

  class Iterator final {
    Type type_;
    union {
      AllVerticesIterable::Iterator all_it_;
      LabelIndex::Iterable::Iterator by_label_it_;
      LabelPropertyIndex::Iterable::Iterator by_label_property_it_;
    };

    void Destroy() noexcept;

   public:
    explicit Iterator(AllVerticesIterable::Iterator);
    explicit Iterator(LabelIndex::Iterable::Iterator);
    explicit Iterator(LabelPropertyIndex::Iterable::Iterator);

    Iterator(const Iterator &);
    Iterator &operator=(const Iterator &);

    Iterator(Iterator &&) noexcept;
    Iterator &operator=(Iterator &&) noexcept;

    ~Iterator();

    VertexAccessor operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const;
    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  Iterator begin();
  Iterator end();
};

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

/// Structure used to return information about existing schemas in the storage
struct SchemasInfo {
  Schemas::SchemasList schemas;
};

/// Structure used to return information about the storage.
struct StorageInfo {
  uint64_t vertex_count;
  uint64_t edge_count;
  double average_degree;
  uint64_t memory_usage;
  uint64_t disk_usage;
};

enum class ReplicationRole : uint8_t { MAIN, REPLICA };

class Shard final {
 public:
  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit Shard(const std::string &primary_label, PrimaryKey min_primary_key,
                 std::optional<PrimaryKey> max_primary_key, Config config = Config());

  Shard(const Shard &) = delete;
  Shard(Shard &&) noexcept = delete;
  Shard &operator=(const Shard &) = delete;
  Shard operator=(Shard &&) noexcept = delete;
  ~Shard();

  class Accessor final {
   private:
    friend class Shard;

    explicit Accessor(Shard *shard, IsolationLevel isolation_level);

   public:
    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;
    Accessor &operator=(Accessor &&other) = delete;

    // NOTE: After the accessor is moved, all objects derived from it (accessors
    // and iterators) are *invalid*. You have to get all derived objects again.
    Accessor(Accessor &&other) noexcept;

    ~Accessor();

    /// @throw std::bad_alloc
    ResultSchema<VertexAccessor> CreateVertexAndValidate(
        LabelId primary_label, const std::vector<LabelId> &labels,
        const std::vector<std::pair<PropertyId, PropertyValue>> &properties);

    std::optional<VertexAccessor> FindVertex(std::vector<PropertyValue> primary_key, View view);

    VerticesIterable Vertices(View view) {
      return VerticesIterable(AllVerticesIterable(shard_->vertices_.access(), &transaction_, view, &shard_->indices_,
                                                  &shard_->constraints_, shard_->config_.items,
                                                  shard_->schema_validator_));
    }

    VerticesIterable Vertices(LabelId label, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view);

    /// Return approximate number of all vertices in the database.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount() const { return static_cast<int64_t>(shard_->vertices_.size()); }

    /// Return approximate number of vertices with the given label.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label) const {
      return shard_->indices_.label_index.ApproximateVertexCount(label);
    }

    /// Return approximate number of vertices with the given label and property.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property) const {
      return shard_->indices_.label_property_index.ApproximateVertexCount(label, property);
    }

    /// Return approximate number of vertices with the given label and the given
    /// value for the given property. Note that this is always an over-estimate
    /// and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const {
      return shard_->indices_.label_property_index.ApproximateVertexCount(label, property, value);
    }

    /// Return approximate number of vertices with the given label and value for
    /// the given property in the range defined by provided upper and lower
    /// bounds.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                   const std::optional<utils::Bound<PropertyValue>> &lower,
                                   const std::optional<utils::Bound<PropertyValue>> &upper) const {
      return shard_->indices_.label_property_index.ApproximateVertexCount(label, property, lower, upper);
    }

    /// @return Accessor to the deleted vertex if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    Result<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex);

    /// @return Accessor to the deleted vertex and deleted edges if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachDeleteVertex(
        VertexAccessor *vertex);

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type);

    /// Accessor to the deleted edge if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    Result<std::optional<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge);

    const std::string &LabelToName(LabelId label) const;
    const std::string &PropertyToName(PropertyId property) const;
    const std::string &EdgeTypeToName(EdgeTypeId edge_type) const;

    /// @throw std::bad_alloc if unable to insert a new mapping
    LabelId NameToLabel(std::string_view name);

    /// @throw std::bad_alloc if unable to insert a new mapping
    PropertyId NameToProperty(std::string_view name);

    /// @throw std::bad_alloc if unable to insert a new mapping
    EdgeTypeId NameToEdgeType(std::string_view name);

    bool LabelIndexExists(LabelId label) const { return shard_->indices_.label_index.IndexExists(label); }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const {
      return shard_->indices_.label_property_index.IndexExists(label, property);
    }

    IndicesInfo ListAllIndices() const {
      return {shard_->indices_.label_index.ListIndices(), shard_->indices_.label_property_index.ListIndices()};
    }

    ConstraintsInfo ListAllConstraints() const {
      return {ListExistenceConstraints(shard_->constraints_),
              shard_->constraints_.unique_constraints.ListConstraints()};
    }

    const SchemaValidator &GetSchemaValidator() const;

    SchemasInfo ListAllSchemas() const { return {shard_->schemas_.ListSchemas()}; }

    void AdvanceCommand();

    /// Commit returns `ConstraintViolation` if the changes made by this
    /// transaction violate an existence or unique constraint. In that case the
    /// transaction is automatically aborted. Otherwise, void is returned.
    /// @throw std::bad_alloc
    utils::BasicResult<ConstraintViolation, void> Commit(std::optional<uint64_t> desired_commit_timestamp = {});

    /// @throw std::bad_alloc
    void Abort();

    void FinalizeTransaction();

   private:
    /// @throw std::bad_alloc
    VertexAccessor CreateVertex(Gid gid, LabelId primary_label);

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type, Gid gid);

    Shard *shard_;
    Transaction transaction_;
    std::optional<uint64_t> commit_timestamp_;
    bool is_transaction_active_;
    Config::Items config_;
  };

  Accessor Access(std::optional<IsolationLevel> override_isolation_level = {}) {
    return Accessor{this, override_isolation_level.value_or(isolation_level_)};
  }

  const std::string &LabelToName(LabelId label) const;
  const std::string &PropertyToName(PropertyId property) const;
  const std::string &EdgeTypeToName(EdgeTypeId edge_type) const;

  /// @throw std::bad_alloc if unable to insert a new mapping
  LabelId NameToLabel(std::string_view name);

  /// @throw std::bad_alloc if unable to insert a new mapping
  PropertyId NameToProperty(std::string_view name);

  /// @throw std::bad_alloc if unable to insert a new mapping
  EdgeTypeId NameToEdgeType(std::string_view name);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, std::optional<uint64_t> desired_commit_timestamp = {});

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp = {});

  bool DropIndex(LabelId label, std::optional<uint64_t> desired_commit_timestamp = {});

  bool DropIndex(LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp = {});

  IndicesInfo ListAllIndices() const;

  /// Creates an existence constraint. Returns true if the constraint was
  /// successfully added, false if it already exists and a `ConstraintViolation`
  /// if there is an existing vertex violating the constraint.
  ///
  /// @throw std::bad_alloc
  /// @throw std::length_error
  utils::BasicResult<ConstraintViolation, bool> CreateExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp = {});

  /// Removes an existence constraint. Returns true if the constraint was
  /// removed, and false if it doesn't exist.
  bool DropExistenceConstraint(LabelId label, PropertyId property,
                               std::optional<uint64_t> desired_commit_timestamp = {});

  /// Creates a unique constraint. In the case of two vertices violating the
  /// constraint, it returns `ConstraintViolation`. Otherwise returns a
  /// `UniqueConstraints::CreationStatus` enum with the following possibilities:
  ///     * `SUCCESS` if the constraint was successfully created,
  ///     * `ALREADY_EXISTS` if the constraint already existed,
  ///     * `EMPTY_PROPERTIES` if the property set is empty, or
  //      * `PROPERTIES_SIZE_LIMIT_EXCEEDED` if the property set exceeds the
  //        limit of maximum number of properties.
  ///
  /// @throw std::bad_alloc
  utils::BasicResult<ConstraintViolation, UniqueConstraints::CreationStatus> CreateUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties, std::optional<uint64_t> desired_commit_timestamp = {});

  /// Removes a unique constraint. Returns `UniqueConstraints::DeletionStatus`
  /// enum with the following possibilities:
  ///     * `SUCCESS` if constraint was successfully removed,
  ///     * `NOT_FOUND` if the specified constraint was not found,
  ///     * `EMPTY_PROPERTIES` if the property set is empty, or
  ///     * `PROPERTIES_SIZE_LIMIT_EXCEEDED` if the property set exceeds the
  //        limit of maximum number of properties.
  UniqueConstraints::DeletionStatus DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                                         std::optional<uint64_t> desired_commit_timestamp = {});

  ConstraintsInfo ListAllConstraints() const;

  SchemasInfo ListAllSchemas() const;

  const Schemas::Schema *GetSchema(LabelId primary_label) const;

  bool CreateSchema(LabelId primary_label, const std::vector<SchemaProperty> &schemas_types);

  bool DropSchema(LabelId primary_label);

  StorageInfo GetInfo() const;

  bool LockPath();
  bool UnlockPath();

  bool SetReplicaRole(io::network::Endpoint endpoint, const replication::ReplicationServerConfig &config = {});

  bool SetMainReplicationRole();

  enum class RegisterReplicaError : uint8_t {
    NAME_EXISTS,
    END_POINT_EXISTS,
    CONNECTION_FAILED,
    COULD_NOT_BE_PERSISTED
  };

  /// @pre The instance should have a MAIN role
  /// @pre Timeout can only be set for SYNC replication
  utils::BasicResult<RegisterReplicaError, void> RegisterReplica(
      std::string name, io::network::Endpoint endpoint, replication::ReplicationMode replication_mode,
      const replication::ReplicationClientConfig &config = {});
  /// @pre The instance should have a MAIN role
  bool UnregisterReplica(std::string_view name);

  std::optional<replication::ReplicaState> GetReplicaState(std::string_view name);

  ReplicationRole GetReplicationRole() const;

  struct ReplicaInfo {
    std::string name;
    replication::ReplicationMode mode;
    std::optional<double> timeout;
    io::network::Endpoint endpoint;
    replication::ReplicaState state;
  };

  std::vector<ReplicaInfo> ReplicasInfo();

  void FreeMemory();

  void SetIsolationLevel(IsolationLevel isolation_level);

  enum class CreateSnapshotError : uint8_t { DisabledForReplica };

  utils::BasicResult<CreateSnapshotError> CreateSnapshot();

 private:
  Transaction CreateTransaction(IsolationLevel isolation_level);

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

  void AppendToWal(const Transaction &transaction, uint64_t final_commit_timestamp);
  void AppendToWal(durability::StorageGlobalOperation operation, LabelId label, const std::set<PropertyId> &properties,
                   uint64_t final_commit_timestamp);

  uint64_t CommitTimestamp(std::optional<uint64_t> desired_commit_timestamp = {});

  // Main object storage
  NameIdMapper name_id_mapper_;
  LabelId primary_label_;
  PrimaryKey min_primary_key_;
  std::optional<PrimaryKey> max_primary_key_;
  VerticesSkipList vertices_;
  utils::SkipList<Edge> edges_;
  uint64_t edge_id_{0};
  // Even though the edge count is already kept in the `edges_` SkipList, the
  // list is used only when properties are enabled for edges. Because of that we
  // keep a separate count of edges that is always updated.
  uint64_t edge_count_{0};

  SchemaValidator schema_validator_;
  Constraints constraints_;
  Indices indices_;
  Schemas schemas_;

  // Transaction engine
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};
  // TODO: This isn't really a commit log, it doesn't even care if a
  // transaction commited or aborted. We could probably combine this with
  // `timestamp_` in a sensible unit, something like TransactionClock or
  // whatever.
  std::optional<CommitLog> commit_log_;

  std::list<Transaction> committed_transactions_;
  IsolationLevel isolation_level_;

  Config config_;

  // Undo buffers that were unlinked and now are waiting to be freed.
  std::list<std::pair<uint64_t, std::list<Delta>>> garbage_undo_buffers_;

  // Vertices that are logically deleted but still have to be removed from
  // indices before removing them from the main storage.
  std::list<PrimaryKey> deleted_vertices_;

  // Vertices that are logically deleted and removed from indices and now wait
  // to be removed from the main storage.
  std::list<std::pair<uint64_t, PrimaryKey>> garbage_vertices_;

  // Edges that are logically deleted and wait to be removed from the main
  // storage.
  std::list<Gid> deleted_edges_;

  // Durability
  std::filesystem::path snapshot_directory_;
  std::filesystem::path wal_directory_;
  std::filesystem::path lock_file_path_;
  utils::OutputFile lock_file_handle_;

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
  uint64_t last_commit_timestamp_{kTimestampInitialId};

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

  ReplicationRole replication_role_{ReplicationRole::MAIN};
};

}  // namespace memgraph::storage::v3
