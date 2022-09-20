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

#include <optional>

#include "storage/v3/config.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"
#include "storage/v3/vertex_id.hpp"
#include "storage/v3/view.hpp"

namespace memgraph::storage::v3 {

class EdgeAccessor;
class Shard;
struct Indices;
struct Constraints;

class VertexAccessor final {
 private:
  friend class Shard;

 public:
  // Be careful when using VertexAccessor since it can be instantiated with
  // nullptr values
  VertexAccessor(Vertex *vertex, Transaction *transaction, Indices *indices, Constraints *constraints,
                 Config::Items config, const VertexValidator &vertex_validator, bool for_deleted = false)
      : vertex_(vertex),
        transaction_(transaction),
        indices_(indices),
        constraints_(constraints),
        config_(config),
        vertex_validator_{&vertex_validator},
        for_deleted_(for_deleted) {}

  static std::optional<VertexAccessor> Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                              Constraints *constraints, Config::Items config,
                                              const VertexValidator &vertex_validator, View view);

  /// @return true if the object is visible from the current transaction
  bool IsVisible(View view) const;

  /// Add a label and return `true` if insertion took place.
  /// `false` is returned if the label already existed, or SchemaViolation
  /// if adding the label has violated one of the schema constraints.
  /// @throw std::bad_alloc
  ResultSchema<bool> AddLabelAndValidate(LabelId label);

  /// Remove a label and return `true` if deletion took place.
  /// `false` is returned if the vertex did not have a label already. or SchemaViolation
  /// if adding the label has violated one of the schema constraints.
  /// @throw std::bad_alloc
  ResultSchema<bool> RemoveLabelAndValidate(LabelId label);

  Result<bool> HasLabel(LabelId label, View view) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<LabelId>> Labels(View view) const;

  Result<LabelId> PrimaryLabel(View view) const;

  Result<PrimaryKey> PrimaryKey(View view) const;

  Result<VertexId> Id(View view) const;

  /// Set a property value and return the old value or error.
  /// @throw std::bad_alloc
  ResultSchema<PropertyValue> SetPropertyAndValidate(PropertyId property, const PropertyValue &value);

  /// Remove all properties and return the values of the removed properties.
  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> ClearProperties();

  /// @throw std::bad_alloc
  Result<PropertyValue> GetProperty(PropertyId property, View view) const;

  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> Properties(View view) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<EdgeAccessor>> InEdges(View view, const std::vector<EdgeTypeId> &edge_types = {},
                                            const VertexId *destination_id = nullptr) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<EdgeAccessor>> OutEdges(View view, const std::vector<EdgeTypeId> &edge_types = {},
                                             const VertexId *destination_id = nullptr) const;

  Result<size_t> InDegree(View view) const;

  Result<size_t> OutDegree(View view) const;

  const SchemaValidator *GetSchemaValidator() const;

  bool operator==(const VertexAccessor &other) const noexcept {
    return vertex_ == other.vertex_ && transaction_ == other.transaction_;
  }
  bool operator!=(const VertexAccessor &other) const noexcept { return !(*this == other); }

 private:
  /// Add a label and return `true` if insertion took place.
  /// `false` is returned if the label already existed.
  /// @throw std::bad_alloc
  Result<bool> AddLabel(LabelId label);

  /// Remove a label and return `true` if deletion took place.
  /// `false` is returned if the vertex did not have a label already.
  /// @throw std::bad_alloc
  Result<bool> RemoveLabel(LabelId label);

  /// Set a property value and return the old value.
  /// @throw std::bad_alloc
  Result<PropertyValue> SetProperty(PropertyId property, const PropertyValue &value);

  Result<void> CheckVertexExistence(View view) const;

  Vertex *vertex_;
  Transaction *transaction_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
  const VertexValidator *vertex_validator_;

  // if the accessor was created for a deleted vertex.
  // Accessor behaves differently for some methods based on this
  // flag.
  // E.g. If this field is set to true, GetProperty will return the property of the node
  // even though the node is deleted.
  // All the write operations, and operators used for traversal (e.g. InEdges) will still
  // return an error if it's called for a deleted vertex.
  bool for_deleted_{false};
};

}  // namespace memgraph::storage::v3

namespace std {
template <>
struct hash<memgraph::storage::v3::VertexAccessor> {
  size_t operator()(const memgraph::storage::v3::VertexAccessor & /*v*/) const noexcept { return 0; }
};
}  // namespace std
