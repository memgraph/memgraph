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

#include <set>
#include <span>

#include "io/network/endpoint.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"

#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"

namespace memgraph::storage {

struct Transaction;
class EdgeAccessor;

enum class ReplicationRole : uint8_t { MAIN, REPLICA };

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

/// Iterable for iterating through all vertices of a Storage.
///
/// An instance of this will be usually be wrapped inside VerticesIterable for
/// generic, public use.
class AllVerticesIterable final {
  utils::SkipList<Vertex>::Accessor vertices_accessor_;
  Transaction *transaction_;
  View view_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
  std::unique_ptr<VertexAccessor> vertex_;

 public:
  class Iterator final {
    AllVerticesIterable *self_;
    utils::SkipList<Vertex>::Iterator it_;

   public:
    Iterator(AllVerticesIterable *self, utils::SkipList<Vertex>::Iterator it);

    std::unique_ptr<VertexAccessor> operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const { return self_ == other.self_ && it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  AllVerticesIterable(utils::SkipList<Vertex>::Accessor vertices_accessor, Transaction *transaction, View view,
                      Indices *indices, Constraints *constraints, Config::Items config)
      : vertices_accessor_(std::move(vertices_accessor)),
        transaction_(transaction),
        view_(view),
        indices_(indices),
        constraints_(constraints),
        config_(config) {}

  Iterator begin() { return Iterator(this, vertices_accessor_.begin()); }
  Iterator end() { return Iterator(this, vertices_accessor_.end()); }
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

    std::unique_ptr<VertexAccessor> operator*();

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
  virtual ~Storage() {}
  class Accessor {
   public:
    Accessor() {}
    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;
    Accessor &operator=(Accessor &&other) = delete;

    // NOTE: After the accessor is moved, all objects derived from it (accessors
    // and iterators) are *invalid*. You have to get all derived objects again.
    Accessor(Accessor &&other) noexcept;

    virtual ~Accessor() {}

    /// @throw std::bad_alloc
    virtual std::unique_ptr<VertexAccessor> CreateVertex() = 0;

    virtual std::unique_ptr<VertexAccessor> FindVertex(Gid gid, View view) = 0;

    virtual VerticesIterable Vertices(View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, PropertyId property, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) = 0;

    virtual VerticesIterable Vertices(LabelId label, PropertyId property,
                                      const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                      const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) = 0;

    /// Return approximate number of all vertices in the database.
    /// Note that this is always an over-estimate and never an under-estimate.
    virtual int64_t ApproximateVertexCount() const = 0;

    /// Return approximate number of vertices with the given label.
    /// Note that this is always an over-estimate and never an under-estimate.
    virtual int64_t ApproximateVertexCount(LabelId label) const = 0;

    /// Return approximate number of vertices with the given label and property.
    /// Note that this is always an over-estimate and never an under-estimate.
    virtual int64_t ApproximateVertexCount(LabelId label, PropertyId property) const = 0;

    /// Return approximate number of vertices with the given label and the given
    /// value for the given property. Note that this is always an over-estimate
    /// and never an under-estimate.
    virtual int64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const = 0;

    /// Return approximate number of vertices with the given label and value for
    /// the given property in the range defined by provided upper and lower
    /// bounds.
    virtual int64_t ApproximateVertexCount(LabelId label, PropertyId property,
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
    // TODO: Think about moving from raw pointer to unique_ptr!
    virtual Result<std::unique_ptr<VertexAccessor>> DeleteVertex(VertexAccessor *vertex) = 0;

    /// @return Accessor to the deleted vertex and deleted edges if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    virtual Result<
        std::optional<std::pair<std::unique_ptr<VertexAccessor>, std::vector<std::unique_ptr<EdgeAccessor>>>>>
    DetachDeleteVertex(VertexAccessor *vertex) = 0;

    /// @throw std::bad_alloc
    virtual Result<std::unique_ptr<EdgeAccessor>> CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                             EdgeTypeId edge_type) = 0;

    /// Accessor to the deleted edge if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    virtual Result<std::unique_ptr<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge) = 0;

    virtual const std::string &LabelToName(LabelId label) const = 0;
    virtual const std::string &PropertyToName(PropertyId property) const = 0;
    virtual const std::string &EdgeTypeToName(EdgeTypeId edge_type) const = 0;

    /// @throw std::bad_alloc if unable to insert a new mapping
    virtual LabelId NameToLabel(std::string_view name) = 0;

    /// @throw std::bad_alloc if unable to insert a new mapping
    virtual PropertyId NameToProperty(std::string_view name) = 0;

    /// @throw std::bad_alloc if unable to insert a new mapping
    virtual EdgeTypeId NameToEdgeType(std::string_view name) = 0;

    virtual bool LabelIndexExists(LabelId label) const = 0;

    virtual bool LabelPropertyIndexExists(LabelId label, PropertyId property) const = 0;

    virtual IndicesInfo ListAllIndices() const = 0;

    virtual ConstraintsInfo ListAllConstraints() const = 0;

    virtual void AdvanceCommand() = 0;

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

    virtual std::optional<uint64_t> GetTransactionId() const = 0;
  };

  virtual std::unique_ptr<Accessor> Access(std::optional<IsolationLevel> override_isolation_level) = 0;
  std::unique_ptr<Accessor> Access() { return Access(std::optional<IsolationLevel>{}); }

  virtual const std::string &LabelToName(LabelId label) const = 0;
  virtual const std::string &PropertyToName(PropertyId property) const = 0;
  virtual const std::string &EdgeTypeToName(EdgeTypeId edge_type) const = 0;

  /// @throw std::bad_alloc if unable to insert a new mapping
  virtual LabelId NameToLabel(std::string_view name) = 0;

  /// @throw std::bad_alloc if unable to insert a new mapping
  virtual PropertyId NameToProperty(std::string_view name) = 0;

  /// @throw std::bad_alloc if unable to insert a new mapping
  virtual EdgeTypeId NameToEdgeType(std::string_view name) = 0;

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

  virtual StorageInfo GetInfo() const = 0;

  virtual bool LockPath() = 0;
  virtual bool UnlockPath() = 0;

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

  virtual void SetIsolationLevel(IsolationLevel isolation_level) = 0;

  enum class CreateSnapshotError : uint8_t { DisabledForReplica };

  virtual utils::BasicResult<CreateSnapshotError> CreateSnapshot() = 0;
};

}  // namespace memgraph::storage
