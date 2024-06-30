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

#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

// forward declarations
struct Vertex;
struct Transaction;

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

  void AddedLabel(Vertex const *vertex);

  void AddedProperty(Vertex const *vertex);

  void RemovedProperty(Vertex const *vertex);

  auto GetVerticesForUniqueConstraintChecking() const -> std::unordered_set<Vertex const *>;
  auto GetVerticesForExistenceConstraintChecking() const -> std::unordered_set<Vertex const *>;

  bool NeedsUniqueConstraintVerification() const;
  bool NeedsExistenceConstraintVerification() const;

 private:
  // Update unique constraints to check whether any vertex already has that value
  // Update existence constraints to check whether for that label the node has all the properties present
  std::unordered_set<Vertex const *> added_labels_;

  // Update unique constraints to check whether any vertex already has that property
  // No update to existence constraints because we only added a property
  std::unordered_set<Vertex const *> added_properties_;

  // No update to unique constraints because uniqueness is preserved
  // Update existence constraints because it might be the referenced property of the constraint
  std::unordered_set<Vertex const *> removed_properties_;
};
}  // namespace memgraph::storage
