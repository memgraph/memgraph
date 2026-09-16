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

#include <boost/container_hash/hash.hpp>
#include <set>
#include <unordered_set>

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

/// A vertex together with an id a write on it named. Which id it was decides which constraints
/// that write can have changed, and so which of them are owed anything at commit.
template <typename TId>
struct VertexWrite {
  Vertex const *vertex;
  TId id;

  friend bool operator==(VertexWrite const &, VertexWrite const &) = default;

  struct Hash {
    std::size_t operator()(VertexWrite const &write) const noexcept {
      std::size_t seed = 0;
      boost::hash_combine(seed, write.vertex);
      boost::hash_combine(seed, write.id.AsUint());
      return seed;
    }
  };
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

  /// Whether a write in this transaction could have changed what a unique constraint keyed on
  /// `label` and `properties` holds for `vertex`: the label arriving on the vertex, or one of the
  /// properties being set on it.
  bool CouldHaveChangedUniqueKey(Vertex const *vertex, LabelId label, std::set<PropertyId> const &properties) const;

  /// Forgets what has been reported, keeping which ids are worth reporting. A transaction that
  /// commits more than once owes each commit what was written since the one before it, and a
  /// vertex still named by a commit that wrote nothing it is keyed on is one whose constraints
  /// gain an entry that commit never armed a sweep for.
  void Clear();

 private:
  // Update unique constraints to check whether any vertex already has that value
  // Update existence constraints to check whether for that label the node has all the properties present
  std::unordered_set<VertexWrite<LabelId>, VertexWrite<LabelId>::Hash> added_labels_;

  // Update unique constraints to check whether any vertex already has that property
  // No update to existence constraints because we only added a property
  std::unordered_set<VertexWrite<PropertyId>, VertexWrite<PropertyId>::Hash> added_properties_;

  // No update to unique constraints because uniqueness is preserved
  // Update existence constraints because it might be the referenced property of the constraint
  std::unordered_set<Vertex const *> removed_properties_;

  ConstraintRelevance relevance_{};
};
}  // namespace memgraph::storage
