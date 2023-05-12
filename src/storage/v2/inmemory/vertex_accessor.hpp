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

#include "storage/v2/vertex.hpp"

#include "storage/v2/config.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::storage {

class EdgeAccessor;
class Storage;
struct Indices;
struct Constraints;

class InMemoryVertexAccessor final : public VertexAccessor {
 private:
  friend class InMemoryStorage;

 public:
  InMemoryVertexAccessor(Vertex *vertex, Transaction *transaction, Indices *indices, Constraints *constraints,
                         Config::Items config, bool for_deleted = false)
      : VertexAccessor(transaction, config, for_deleted),
        vertex_(vertex),
        indices_(indices),
        constraints_(constraints) {}

  static std::unique_ptr<InMemoryVertexAccessor> Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                                        Constraints *constraints, Config::Items config, View view);

  /// @return true if the object is visible from the current transaction
  bool IsVisible(View view) const override;

  /// Add a label and return `true` if insertion took place.
  /// `false` is returned if the label already existed.
  /// @throw std::bad_alloc
  Result<bool> AddLabel(LabelId label) override;

  /// Remove a label and return `true` if deletion took place.
  /// `false` is returned if the vertex did not have a label already.
  /// @throw std::bad_alloc
  Result<bool> RemoveLabel(LabelId label) override;

  Result<bool> HasLabel(LabelId label, View view) const override;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<LabelId>> Labels(View view) const override;

  /// Set a property value and return the old value.
  /// @throw std::bad_alloc
  Result<PropertyValue> SetProperty(PropertyId property, const PropertyValue &value) override;

  /// Set property values only if property store is empty. Returns `true` if successully set all values,
  /// `false` otherwise.
  /// @throw std::bad_alloc
  Result<bool> InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) override;

  /// Remove all properties and return the values of the removed properties.
  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> ClearProperties() override;

  /// @throw std::bad_alloc
  Result<PropertyValue> GetProperty(PropertyId property, View view) const override;

  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> Properties(View view) const override;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<std::unique_ptr<EdgeAccessor>>> InEdges(View view, const std::vector<EdgeTypeId> &edge_types,
                                                             const VertexAccessor *destination) const override;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<std::unique_ptr<EdgeAccessor>>> OutEdges(View view, const std::vector<EdgeTypeId> &edge_types,
                                                              const VertexAccessor *destination) const override;

  Result<size_t> InDegree(View view) const override;

  Result<size_t> OutDegree(View view) const override;

  storage::Gid Gid() const noexcept override { return vertex_->gid; }

  std::string PropertyStore() const override;

  void SetPropertyStore(std::string_view buffer) const override;

  std::unique_ptr<VertexAccessor> Copy() const override { return std::make_unique<InMemoryVertexAccessor>(*this); }

  bool operator==(const VertexAccessor &other) const noexcept override {
    const auto *otherVertex = dynamic_cast<const InMemoryVertexAccessor *>(&other);
    if (otherVertex == nullptr) return false;
    return vertex_ == otherVertex->vertex_ && transaction_ == otherVertex->transaction_;
  }

  bool operator!=(const VertexAccessor &other) const noexcept { return !(*this == other); }

 private:
  Vertex *vertex_;
  Indices *indices_;
  Constraints *constraints_;
};

}  // namespace memgraph::storage
