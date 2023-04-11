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

#include "storage/v2/config.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::storage {

class VerticesIterableImpl;
struct Transaction;
class EdgeAccessor;

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

/// Generic access to different kinds of vertex iterations.
///
/// This class should be the primary type used by the client code to iterate
/// over vertices inside a Storage instance.
class VerticesIterable final {
  enum class Type { ALL, BY_LABEL, BY_LABEL_PROPERTY };

  Type type_;
  VerticesIterableImpl *impl_;

 public:
  explicit VerticesIterable(VerticesIterableImpl *);

  VerticesIterable(const VerticesIterable &) = delete;
  VerticesIterable &operator=(const VerticesIterable &) = delete;

  VerticesIterable(VerticesIterable &&) noexcept;
  VerticesIterable &operator=(VerticesIterable &&) noexcept;

  ~VerticesIterable();

  class Iterator final {
    Type type_;
    VerticesIterable::Iterator *impl_;

    void Destroy() noexcept;

   public:
    explicit Iterator(VerticesIterable::Iterator *);

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

class Accessor {
 public:
  Accessor(const Accessor &) = delete;
  Accessor &operator=(const Accessor &) = delete;
  Accessor &operator=(Accessor &&other) = delete;

  // NOTE: After the accessor is moved, all objects derived from it (accessors
  // and iterators) are *invalid*. You have to get all derived objects again.
  Accessor(Accessor &&other) noexcept;

  virtual ~Accessor();

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
  virtual Result<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex) = 0;

  /// @return Accessor to the deleted vertex and deleted edges if a deletion took place, std::nullopt otherwise
  /// @throw std::bad_alloc
  virtual Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachDeleteVertex(
      VertexAccessor *vertex) = 0;

  /// @throw std::bad_alloc
  virtual Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) = 0;

  /// Accessor to the deleted edge if a deletion took place, std::nullopt otherwise
  /// @throw std::bad_alloc
  virtual Result<std::optional<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge) = 0;

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

}  // namespace memgraph::storage
