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

#include <memory>
#include <optional>

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_ref.hpp"

#include "storage/v2/config.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace memgraph::storage {

struct Transaction;

class EdgeAccessor {
 private:
  friend class Storage;

 public:
  EdgeAccessor(EdgeTypeId edge_type, Transaction *transaction, Config::Items config, bool for_deleted = false)
      : edge_type_(edge_type), transaction_(transaction), config_(config), for_deleted_(for_deleted) {}

  virtual ~EdgeAccessor() {}

  static std::unique_ptr<EdgeAccessor> Create(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex,
                                              Vertex *to_vertex, Transaction *transaction, Indices *indices,
                                              Constraints *constraints, Config::Items config, bool for_deleted = false);

  /// @return true if the object is visible from the current transaction
  virtual bool IsVisible(View view) const = 0;

  virtual std::unique_ptr<VertexAccessor> FromVertex() const = 0;

  virtual std::unique_ptr<VertexAccessor> ToVertex() const = 0;

  EdgeTypeId EdgeType() const { return edge_type_; }

  /// Set a property value and return the old value.
  /// @throw std::bad_alloc
  virtual Result<storage::PropertyValue> SetProperty(PropertyId property, const PropertyValue &value) = 0;

  /// Set property values only if property store is empty. Returns `true` if successully set all values,
  /// `false` otherwise.
  /// @throw std::bad_alloc
  virtual Result<bool> InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) = 0;

  /// Remove all properties and return old values for each removed property.
  /// @throw std::bad_alloc
  virtual Result<std::map<PropertyId, PropertyValue>> ClearProperties() = 0;

  /// @throw std::bad_alloc
  virtual Result<PropertyValue> GetProperty(PropertyId property, View view) const = 0;

  /// @throw std::bad_alloc
  virtual Result<std::map<PropertyId, PropertyValue>> Properties(View view) const = 0;

  virtual storage::Gid Gid() const noexcept = 0;

  virtual bool IsCycle() const = 0;

  virtual std::unique_ptr<EdgeAccessor> Copy() const = 0;

  virtual bool operator==(const EdgeAccessor &other) const noexcept = 0;
  bool operator!=(const EdgeAccessor &other) const noexcept { return !(*this == other); }

 protected:
  EdgeTypeId edge_type_;
  Transaction *transaction_;
  Config::Items config_;

  // if the accessor was created for a deleted edge.
  // Accessor behaves differently for some methods based on this
  // flag.
  // E.g. If this field is set to true, GetProperty will return the property of the edge
  // even though the edge is deleted.
  // All the write operations will still return an error if it's called for a deleted edge.
  bool for_deleted_{false};
};

bool operator==(const std::unique_ptr<EdgeAccessor> &ea1, const std::unique_ptr<EdgeAccessor> &ea2) noexcept;

}  // namespace memgraph::storage

namespace std {
template <>
struct hash<memgraph::storage::EdgeAccessor *> {
  size_t operator()(const memgraph::storage::EdgeAccessor *e) const { return e->Gid().AsUint(); }
};
}  // namespace std
