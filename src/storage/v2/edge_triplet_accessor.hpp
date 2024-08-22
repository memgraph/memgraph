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

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/vertex_accessor.hpp"

#include "storage/v2/transaction.hpp"

namespace memgraph::storage {

class EdgeTripletAccessor final {
 public:
  EdgeTripletAccessor(EdgeAccessor edge, VertexAccessor from_vertex, VertexAccessor to_vertex)
      : edge_(edge), from_vertex_(from_vertex), to_vertex_(to_vertex) {}

  EdgeAccessor Edge() const { return edge_; }
  VertexAccessor FromVertex() const { return from_vertex_; }
  VertexAccessor ToVertex() const { return to_vertex_; }

  /// @throw std::bad_alloc
  Result<PropertyValue> GetProperty(PropertyId property, View view) const { return edge_.GetProperty(property, view); }

  /// Set a property value and return the old value.
  /// @throw std::bad_alloc
  Result<storage::PropertyValue> SetProperty(PropertyId property, const PropertyValue &value) {
    return edge_.SetProperty(property, value);
  }

  bool operator==(const EdgeTripletAccessor &other) const noexcept {
    return edge_ == other.edge_ && from_vertex_ == other.from_vertex_ && to_vertex_ == other.to_vertex_;
  }
  bool operator!=(const EdgeTripletAccessor &other) const noexcept { return !(*this == other); }

  EdgeAccessor edge_;
  VertexAccessor from_vertex_;
  VertexAccessor to_vertex_;
};

}  // namespace memgraph::storage

static_assert(std::is_trivially_copyable_v<memgraph::storage::EdgeTripletAccessor>,
              "storage::EdgeTripletAccessor must be trivially copyable!");

namespace std {
template <>
struct hash<memgraph::storage::EdgeTripletAccessor> {
  size_t operator()(const memgraph::storage::EdgeTripletAccessor &e) const { return e.edge_.Gid().AsUint(); }
};
}  // namespace std
