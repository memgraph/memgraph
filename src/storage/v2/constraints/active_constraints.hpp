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

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::storage {

struct Vertex;
struct Transaction;

/// ActiveConstraints provides a snapshot of constraints at transaction start time.
/// This mirrors the ActiveIndices pattern for consistency.
///
/// Each constraint type has its own interface defined below, and the aggregating
/// ActiveConstraints struct holds unique_ptrs to implementations of each.

/// Interface for existence constraint snapshot access.
/// NOTE: No AbortProcessor/GC needed - existence constraints store only schema metadata
/// ({label, property} -> status), not per-vertex entries. One entry per constraint definition.
struct ExistenceActiveConstraints {
  virtual ~ExistenceActiveConstraints() = default;

  /// Returns true if constraint is registered (even if still populating).
  virtual bool ConstraintRegistered(LabelId label, PropertyId property) const = 0;

  /// List constraints visible at the given timestamp.
  virtual std::vector<std::pair<LabelId, PropertyId>> ListConstraints(uint64_t start_timestamp) const = 0;
};

/// Interface for unique constraint snapshot access.
/// NOTE: Unique constraints store per-vertex entries ({values, vertex, timestamp}) in skip lists,
/// requiring AbortProcessor to remove entries on abort and GC for MVCC version cleanup.
struct UniqueActiveConstraints {
  using ConstraintKey = std::pair<LabelId, std::set<PropertyId>>;

  /// Information collected during abort processing, organized by constraint for efficient iteration.
  /// For each constraint key, stores a list of (property_values, vertex) pairs to remove.
  using AbortableInfo = std::map<ConstraintKey, std::vector<std::pair<std::vector<PropertyValue>, Vertex const *>>>;

  /// Processor that collects abort information during delta processing.
  /// Organized by constraint key so AbortEntries can iterate constraints (not vertices) in outer loop.
  struct AbortProcessor {
    explicit AbortProcessor() = default;
    explicit AbortProcessor(std::vector<ConstraintKey> constraint_keys)
        : constraint_keys_(std::move(constraint_keys)) {}

    /// Collect a vertex entry that needs to be removed from the constraint.
    void Collect(ConstraintKey const &key, std::vector<PropertyValue> values, Vertex const *vertex) {
      if (std::binary_search(constraint_keys_.begin(), constraint_keys_.end(), key)) {
        cleanup_collection_[key].emplace_back(std::move(values), vertex);
      }
    }

    std::vector<ConstraintKey> constraint_keys_;  // Sorted for binary search
    AbortableInfo cleanup_collection_;
  };

  virtual ~UniqueActiveConstraints() = default;

  /// Returns true if constraint is registered (even if still populating).
  virtual bool ConstraintRegistered(LabelId label, std::set<PropertyId> const &properties) const = 0;

  /// List constraints visible at the given timestamp.
  virtual std::vector<std::pair<LabelId, std::set<PropertyId>>> ListConstraints(uint64_t start_timestamp) const = 0;

  /// Update constraint entries before commit (for validation).
  virtual void UpdateBeforeCommit(const Vertex *vertex, const Transaction &tx) = 0;

  /// Get an abort processor for collecting entries to remove during abort.
  virtual auto GetAbortProcessor() const -> AbortProcessor = 0;

  /// Collect abort info for a single vertex into the processor.
  /// Call this for each vertex that needs its constraint entries removed.
  virtual void CollectForAbort(AbortProcessor &processor, Vertex const *vertex) const = 0;

  /// Abort constraint entries using collected information (constraint outer loop, vertex inner loop).
  virtual void AbortEntries(AbortableInfo const &info, uint64_t exact_start_timestamp) = 0;
};

/// Interface for type constraint snapshot access.
/// NOTE: No AbortProcessor/GC needed - type constraints store only schema metadata
/// ({label, property} -> status + kind), not per-vertex entries. One entry per constraint definition.
struct TypeActiveConstraints {
  virtual ~TypeActiveConstraints() = default;

  /// Returns true if constraint is registered (even if still populating).
  virtual bool ConstraintRegistered(LabelId label, PropertyId property) const = 0;

  /// List constraints visible at the given timestamp.
  virtual std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> ListConstraints(
      uint64_t start_timestamp) const = 0;
};

/// Aggregating struct that holds all constraint type snapshots.
/// Stored on Transaction for consistent access during the transaction lifetime.
struct ActiveConstraints {
  // Nested type aliases for backwards compatibility with ActiveConstraints::Existence syntax
  using Existence = ExistenceActiveConstraints;
  using Unique = UniqueActiveConstraints;
  using Type = TypeActiveConstraints;

  ActiveConstraints() = delete;
  explicit ActiveConstraints(std::unique_ptr<Existence> existence, std::unique_ptr<Unique> unique,
                             std::unique_ptr<Type> type)
      : existence_{std::move(existence)}, unique_{std::move(unique)}, type_{std::move(type)} {}

  std::unique_ptr<Existence> existence_;
  std::unique_ptr<Unique> unique_;
  std::unique_ptr<Type> type_;
};

}  // namespace memgraph::storage
