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

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_ref.hpp"

#include "storage/v2/config.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::storage {

struct Vertex;
class VertexAccessor;
struct Indices;
struct Constraints;

class EdgeAccessor final {
 private:
  friend class Storage;

 public:
  EdgeAccessor(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex, Transaction *transaction,
               Indices *indices, Constraints *constraints, Config::Items config, bool for_deleted = false)
      : edge_(edge),
        edge_type_(edge_type),
        from_vertex_(from_vertex),
        to_vertex_(to_vertex),
        transaction_(transaction),
        indices_(indices),
        constraints_(constraints),
        config_(config),
        for_deleted_(for_deleted) {}

  /// @return true if the object is visible from the current transaction
  bool IsVisible(View view) const;

  VertexAccessor FromVertex() const;

  VertexAccessor ToVertex() const;

  EdgeTypeId EdgeType() const { return edge_type_; }

  /// Set a property value and return the old value.
  /// @throw std::bad_alloc
  Result<storage::PropertyValue> SetProperty(PropertyId property, const PropertyValue &value);

  /// Set property values only if property store is empty. Returns `true` if successfully set all values,
  /// `false` otherwise.
  /// @throw std::bad_alloc
  Result<bool> InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties);

  /// Remove all properties and return old values for each removed property.
  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> ClearProperties();

  /// @throw std::bad_alloc
  Result<PropertyValue> GetProperty(PropertyId property, View view) const;

  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> Properties(View view) const;

  Gid Gid() const noexcept {
    if (config_.properties_on_edges) {
      return edge_.ptr->gid;
    } else {
      return edge_.gid;
    }
  }

  bool IsCycle() const { return from_vertex_ == to_vertex_; }

  bool operator==(const EdgeAccessor &other) const noexcept {
    return edge_ == other.edge_ && transaction_ == other.transaction_;
  }
  bool operator!=(const EdgeAccessor &other) const noexcept { return !(*this == other); }

 private:
  EdgeRef edge_;
  EdgeTypeId edge_type_;
  Vertex *from_vertex_;
  Vertex *to_vertex_;
  Transaction *transaction_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;

  // if the accessor was created for a deleted edge.
  // Accessor behaves differently for some methods based on this
  // flag.
  // E.g. If this field is set to true, GetProperty will return the property of the edge
  // even though the edge is deleted.
  // All the write operations will still return an error if it's called for a deleted edge.
  bool for_deleted_{false};
};

}  // namespace memgraph::storage

namespace std {
template <>
struct hash<memgraph::storage::EdgeAccessor> {
  size_t operator()(const memgraph::storage::EdgeAccessor &e) const { return e.Gid().AsUint(); }
};
}  // namespace std
