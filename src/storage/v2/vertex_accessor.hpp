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

#include <optional>

#include "storage/v2/vertex.hpp"

#include "storage/v2/config.hpp"
#include "storage/v2/constraints.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::storage {

class EdgeAccessor;
struct Indices;

class VertexAccessor {
 private:
  friend class Storage;

 public:
  VertexAccessor(Transaction *transaction, Config::Items config, bool for_deleted = false)
      : transaction_(transaction), config_(config), for_deleted_(for_deleted) {}

  VertexAccessor(const VertexAccessor &) = default;

  virtual ~VertexAccessor() {}

  static std::unique_ptr<VertexAccessor> Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                                Constraints *constraints, Config::Items config, View view);

  /// @return true if the object is visible from the current transaction
  virtual bool IsVisible(View view) const = 0;

  /// Add a label and return `true` if insertion took place.
  /// `false` is returned if the label already existed.
  /// @throw std::bad_alloc
  virtual Result<bool> AddLabel(LabelId label) = 0;

  /// Remove a label and return `true` if deletion took place.
  /// `false` is returned if the vertex did not have a label already.
  /// @throw std::bad_alloc
  virtual Result<bool> RemoveLabel(LabelId label) = 0;

  virtual Result<bool> HasLabel(LabelId label, View view) const = 0;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  virtual Result<std::vector<LabelId>> Labels(View view) const = 0;

  /// Set a property value and return the old value.
  /// @throw std::bad_alloc
  virtual Result<PropertyValue> SetProperty(PropertyId property, const PropertyValue &value) = 0;

  /// Set property values only if property store is empty. Returns `true` if successully set all values,
  /// `false` otherwise.
  /// @throw std::bad_alloc
  virtual Result<bool> InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) = 0;

  /// Remove all properties and return the values of the removed properties.
  /// @throw std::bad_alloc
  virtual Result<std::map<PropertyId, PropertyValue>> ClearProperties() = 0;

  /// @throw std::bad_alloc
  virtual Result<PropertyValue> GetProperty(PropertyId property, View view) const = 0;

  /// @throw std::bad_alloc
  virtual Result<std::map<PropertyId, PropertyValue>> Properties(View view) const = 0;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  virtual Result<std::vector<std::unique_ptr<EdgeAccessor>>> InEdges(View view,
                                                                     const std::vector<EdgeTypeId> &edge_types,
                                                                     const VertexAccessor *destination) const = 0;

  Result<std::vector<std::unique_ptr<EdgeAccessor>>> InEdges(View view,
                                                             const std::vector<EdgeTypeId> &edge_types) const;

  Result<std::vector<std::unique_ptr<EdgeAccessor>>> InEdges(View view) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  virtual Result<std::vector<std::unique_ptr<EdgeAccessor>>> OutEdges(View view,
                                                                      const std::vector<EdgeTypeId> &edge_types,
                                                                      const VertexAccessor *destination) const = 0;

  Result<std::vector<std::unique_ptr<EdgeAccessor>>> OutEdges(View view,
                                                              const std::vector<EdgeTypeId> &edge_types) const;
  Result<std::vector<std::unique_ptr<EdgeAccessor>>> OutEdges(View view) const;

  virtual Result<size_t> InDegree(View view) const = 0;

  virtual Result<size_t> OutDegree(View view) const = 0;

  virtual Gid Gid() const noexcept = 0;

  virtual std::unique_ptr<VertexAccessor> Copy() const = 0;

  virtual bool operator==(const VertexAccessor &other) const noexcept = 0;
  bool operator!=(const VertexAccessor &other) const noexcept { return !(*this == other); }

 protected:
  Transaction *transaction_;
  Config::Items config_;

  // if the accessor was created for a deleted vertex.
  // Accessor behaves differently for some methods based on this
  // flag.
  // E.g. If this field is set to true, GetProperty will return the property of the node
  // even though the node is deleted.
  // All the write operations, and operators used for traversal (e.g. InEdges) will still
  // return an error if it's called for a deleted vertex.
  bool for_deleted_{false};
};

}  // namespace memgraph::storage

namespace std {
template <>
struct hash<memgraph::storage::VertexAccessor> {
  size_t operator()(const memgraph::storage::VertexAccessor &v) const noexcept { return v.Gid().AsUint(); }
};
}  // namespace std
