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

#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

// forward declarations
struct Vertex;
struct Transaction;
class PropertyValue;

/**

 */
struct ConstraintVerificationInfo final {
  ConstraintVerificationInfo();
  ~ConstraintVerificationInfo();

  // By design would be a mistake to copy the cache
  ConstraintVerificationInfo(ConstraintVerificationInfo const &) = delete;
  ConstraintVerificationInfo &operator=(ConstraintVerificationInfo const &) = delete;

  ConstraintVerificationInfo(ConstraintVerificationInfo &&) noexcept;
  ConstraintVerificationInfo &operator=(ConstraintVerificationInfo &&) noexcept;

  auto GetAddedLabels(Vertex const *vertex) const -> std::unordered_set<LabelId>;

  void AddLabel(Vertex const *vertex, LabelId label);

  auto GetAddedProperties(Vertex const *vertex) const -> std::unordered_set<PropertyId>;

  void AddProperty(Vertex const *vertex, PropertyId property);

  auto GetRemovedProperties(Vertex const *vertex) const -> std::unordered_set<PropertyId>;

  void RemoveProperty(Vertex const *vertex, PropertyId property);

  auto GetVerticesInfoForUniqueConstraintChecking() const
      -> std::unordered_map<Vertex const *, std::pair<std::unordered_set<LabelId>, std::unordered_set<PropertyId>>>;
  auto GetVerticesInfoForExistenceConstraintChecking() const
      -> std::unordered_map<Vertex const *, std::pair<std::unordered_set<LabelId>, std::unordered_set<PropertyId>>>;

  bool NeedsUniqueConstraintVerification() const;
  bool NeedsExistenceConstraintVerification() const;

 private:
  // Update unique constraints to check whether any vertex already has that value
  // Update existence constraints to check whether for that label the node has all the properties present
  std::unordered_map<Vertex const *, std::unordered_set<LabelId>> added_labels_;

  // Update unique constraints to check whether any vertex already has that property
  // No update to existence constraints because we only added a property
  std::unordered_map<Vertex const *, std::unordered_set<PropertyId>> added_properties_;

  // No update to unique constraints because uniqueness is preserved
  // Update existence constraints because it might be the referenced property of the constraint
  std::unordered_map<Vertex const *, std::unordered_set<PropertyId>> removed_properties_;
};
}  // namespace memgraph::storage
