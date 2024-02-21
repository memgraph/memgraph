// Copyright 2024 Memgraph Ltd.
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
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::storage {

class EdgeAccessor;
class Storage;
struct Constraints;
struct Indices;
struct EdgesVertexAccessorResult;
using edge_store = TcoVector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>>;

class VertexAccessor final {
 private:
  friend class Storage;

 public:
  VertexAccessor(Vertex *vertex, Storage *storage, Transaction *transaction, bool for_deleted = false)
      : vertex_(vertex), storage_(storage), transaction_(transaction), for_deleted_(for_deleted) {}

  static std::optional<VertexAccessor> Create(Vertex *vertex, Storage *storage, Transaction *transaction, View view);

  static bool IsVisible(Vertex const *vertex, Transaction const *transaction, View view);

  /// @return true if the object is visible from the current transaction
  bool IsVisible(View view) const;

  /// Add a label and return `true` if insertion took place.
  /// `false` is returned if the label already existed.
  /// @throw std::bad_alloc
  Result<bool> AddLabel(LabelId label);

  /// Remove a label and return `true` if deletion took place.
  /// `false` is returned if the vertex did not have a label already.
  /// @throw std::bad_alloc
  Result<bool> RemoveLabel(LabelId label);

  Result<bool> HasLabel(LabelId label, View view) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<LabelId>> Labels(View view) const;

  /// Set a property value and return the old value.
  /// @throw std::bad_alloc
  Result<PropertyValue> SetProperty(PropertyId property, const PropertyValue &value);

  /// Set property values only if property store is empty. Returns `true` if successully set all values,
  /// `false` otherwise.
  /// @throw std::bad_alloc
  Result<bool> InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties);

  Result<std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>>> UpdateProperties(
      std::map<storage::PropertyId, storage::PropertyValue> &properties) const;

  /// Remove all properties and return the values of the removed properties.
  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> ClearProperties();

  /// @throw std::bad_alloc
  Result<PropertyValue> GetProperty(PropertyId property, View view) const;

  /// Returns the size of the encoded vertex property in bytes.
  Result<uint64_t> GetPropertySize(PropertyId property, View view) const;

  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> Properties(View view) const;

  auto BuildResultOutEdges(edge_store const &out_edges) const;

  auto BuildResultInEdges(edge_store const &out_edges) const;

  auto BuildResultWithDisk(edge_store const &in_memory_edges, std::vector<EdgeAccessor> const &disk_edges, View view,
                           const std::string &mode) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<EdgesVertexAccessorResult> InEdges(View view, const std::vector<EdgeTypeId> &edge_types = {},
                                            const VertexAccessor *destination = nullptr) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<EdgesVertexAccessorResult> OutEdges(View view, const std::vector<EdgeTypeId> &edge_types = {},
                                             const VertexAccessor *destination = nullptr) const;

  Result<size_t> InDegree(View view) const;

  Result<size_t> OutDegree(View view) const;

  Gid Gid() const noexcept { return vertex_->gid; }

  bool operator==(const VertexAccessor &other) const noexcept {
    return vertex_ == other.vertex_ && transaction_ == other.transaction_;
  }
  bool operator!=(const VertexAccessor &other) const noexcept { return !(*this == other); }

  Vertex *vertex_;
  Storage *storage_;
  Transaction *transaction_;

  // if the accessor was created for a deleted vertex.
  // Accessor behaves differently for some methods based on this
  // flag.
  // E.g. If this field is set to true, GetProperty will return the property of the node
  // even though the node is deleted.
  // All the write operations, and operators used for traversal (e.g. InEdges) will still
  // return an error if it's called for a deleted vertex.
  bool for_deleted_{false};
};

static_assert(std::is_trivially_copyable_v<memgraph::storage::VertexAccessor>,
              "storage::VertexAccessor must be trivially copyable!");

struct EdgesVertexAccessorResult {
  std::vector<EdgeAccessor> edges;
  int64_t expanded_count;
};

}  // namespace memgraph::storage

namespace std {
template <>
struct hash<memgraph::storage::VertexAccessor> {
  size_t operator()(const memgraph::storage::VertexAccessor &v) const noexcept { return v.Gid().AsUint(); }
};
}  // namespace std
