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

#include "storage/v2/interesting_properties.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

// forward declarations
struct Vertex;
struct Transaction;

/// The objects a transaction's writes oblige it to re-check against the constraints, gathered as
/// it writes and read at commit.
///
/// A caller reports what it wrote and this decides whether the write can reach a constraint at
/// all, so the rule lives here rather than at each write site. Each kind of constraint narrows a
/// different report, so each gets its own set of properties: writing a value can only collide
/// with a unique constraint, and removing one can only leave an existence constraint unmet. The
/// sets are borrowed from the constraint snapshot the transaction holds, so they must not name a
/// set that does not outlive this.
struct ConstraintVerificationInfo final {
  ConstraintVerificationInfo();
  ConstraintVerificationInfo(InterestingProperties unique_constrained, InterestingProperties existence_constrained);
  ~ConstraintVerificationInfo();

  // By design would be a mistake to copy the cache
  ConstraintVerificationInfo(ConstraintVerificationInfo const &) = delete;
  ConstraintVerificationInfo &operator=(ConstraintVerificationInfo const &) = delete;

  ConstraintVerificationInfo(ConstraintVerificationInfo &&) noexcept;
  ConstraintVerificationInfo &operator=(ConstraintVerificationInfo &&) noexcept;

  void AddedLabel(Vertex const *vertex);

  /// A value was written to `property`. Ignored when no unique constraint is keyed on it: a value
  /// under a property none of them mention cannot collide with anything they hold.
  void AddedProperty(PropertyId property, Vertex const *vertex);

  /// The value under `property` is gone. Ignored when no existence constraint is keyed on it: a
  /// constraint that never asked for that property cannot be left unmet by its absence.
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

  InterestingProperties unique_constrained_{};
  InterestingProperties existence_constrained_{};
};
}  // namespace memgraph::storage
