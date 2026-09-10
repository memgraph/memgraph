// Copyright 2026 Memgraph Ltd.
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

#include "storage/v2/interesting_ids.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

// forward declarations
struct Vertex;
struct Transaction;

/// The ids each kind of constraint is keyed on. Every field defaults to reporting everything, so
/// a kind left unfilled over-reports rather than losing a check. All are borrowed from the
/// constraint snapshot the transaction holds and may not outlive it.
struct ConstraintRelevance {
  InterestingProperties unique_properties{};
  InterestingLabels unique_labels{};
  InterestingProperties existence_properties{};
  InterestingLabels existence_labels{};
};

/// The objects a transaction's writes oblige it to re-check at commit, gathered as it writes. A
/// caller reports what it wrote and this decides whether the write can reach a constraint at all.
struct ConstraintVerificationInfo final {
  ConstraintVerificationInfo();
  explicit ConstraintVerificationInfo(ConstraintRelevance relevance);
  ~ConstraintVerificationInfo();

  // By design would be a mistake to copy the cache
  ConstraintVerificationInfo(ConstraintVerificationInfo const &) = delete;
  ConstraintVerificationInfo &operator=(ConstraintVerificationInfo const &) = delete;

  ConstraintVerificationInfo(ConstraintVerificationInfo &&) noexcept;
  ConstraintVerificationInfo &operator=(ConstraintVerificationInfo &&) noexcept;

  /// Ignored when no constraint of either kind is keyed on `label`: one that never mentions it
  /// cannot start applying to a vertex that gains it.
  void AddedLabel(LabelId label, Vertex const *vertex);

  /// Ignored when no unique constraint is keyed on `property`: a value under a property none of
  /// them mention cannot collide with anything they hold.
  void AddedProperty(PropertyId property, Vertex const *vertex);

  /// Ignored when no existence constraint is keyed on `property`: one that never asked for it
  /// cannot be left unmet by its absence.
  void RemovedProperty(PropertyId property, Vertex const *vertex);

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

  ConstraintRelevance relevance_{};
};
}  // namespace memgraph::storage
