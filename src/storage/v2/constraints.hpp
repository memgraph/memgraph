// Copyright 2021 Memgraph Ltd.
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
#include <set>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/logging.hpp"
#include "utils/result.hpp"
#include "utils/skip_list.hpp"

namespace storage {

// NOLINTNEXTLINE(misc-definitions-in-headers)
const size_t kUniqueConstraintsMaxProperties = 32;

/// Utility class to store data in a fixed size array. The array is used
/// instead of `std::vector` to avoid `std::bad_alloc` exception where not
/// necessary.
template <class T>
struct FixedCapacityArray {
  size_t size;
  T values[kUniqueConstraintsMaxProperties];

  explicit FixedCapacityArray(size_t array_size) : size(array_size) {
    MG_ASSERT(size <= kUniqueConstraintsMaxProperties, "Invalid array size!");
  }
};

using PropertyIdArray = FixedCapacityArray<PropertyId>;

struct ConstraintViolation {
  enum class Type {
    EXISTENCE,
    UNIQUE,
  };

  Type type;
  LabelId label;

  // While multiple properties are supported by unique constraints, the
  // `properties` set will always have exactly one element in the case of
  // existence constraint violation.
  std::set<PropertyId> properties;
};

bool operator==(const ConstraintViolation &lhs, const ConstraintViolation &rhs);

class UniqueConstraints {
 private:
  struct Entry {
    std::vector<PropertyValue> values;
    const Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs);
    bool operator==(const Entry &rhs);

    bool operator<(const std::vector<PropertyValue> &rhs);
    bool operator==(const std::vector<PropertyValue> &rhs);
  };

 public:
  /// Status for creation of unique constraints.
  /// Note that this does not cover the case when the constraint is violated.
  enum class CreationStatus {
    SUCCESS,
    ALREADY_EXISTS,
    EMPTY_PROPERTIES,
    PROPERTIES_SIZE_LIMIT_EXCEEDED,
  };

  /// Status for deletion of unique constraints.
  enum class DeletionStatus {
    SUCCESS,
    NOT_FOUND,
    EMPTY_PROPERTIES,
    PROPERTIES_SIZE_LIMIT_EXCEEDED,
  };

  /// Indexes the given vertex for relevant labels and properties.
  /// This method should be called before committing and validating vertices
  /// against unique constraints.
  /// @throw std::bad_alloc
  void UpdateBeforeCommit(const Vertex *vertex, const Transaction &tx);

  /// Creates unique constraint on the given `label` and a list of `properties`.
  /// Returns constraint violation if there are multiple vertices with the same
  /// label and property values. Returns `CreationStatus::ALREADY_EXISTS` if
  /// constraint already existed, `CreationStatus::EMPTY_PROPERTIES` if the
  /// given list of properties is empty,
  /// `CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED` if the list of properties
  /// exceeds the maximum allowed number of properties, and
  /// `CreationStatus::SUCCESS` on success.
  /// @throw std::bad_alloc
  utils::BasicResult<ConstraintViolation, CreationStatus> CreateConstraint(LabelId label,
                                                                           const std::set<PropertyId> &properties,
                                                                           utils::SkipList<Vertex>::Accessor vertices);

  /// Deletes the specified constraint. Returns `DeletionStatus::NOT_FOUND` if
  /// there is not such constraint in the storage,
  /// `DeletionStatus::EMPTY_PROPERTIES` if the given set of `properties` is
  /// empty, `DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED` if the given set
  /// of `properties` exceeds the maximum allowed number of properties, and
  /// `DeletionStatus::SUCCESS` on success.
  DeletionStatus DropConstraint(LabelId label, const std::set<PropertyId> &properties);

  bool ConstraintExists(LabelId label, const std::set<PropertyId> &properties) {
    return constraints_.find({label, properties}) != constraints_.end();
  }

  /// Validates the given vertex against unique constraints before committing.
  /// This method should be called while commit lock is active with
  /// `commit_timestamp` being a potential commit timestamp of the transaction.
  /// @throw std::bad_alloc
  std::optional<ConstraintViolation> Validate(const Vertex &vertex, const Transaction &tx,
                                              uint64_t commit_timestamp) const;

  std::vector<std::pair<LabelId, std::set<PropertyId>>> ListConstraints() const;

  /// GC method that removes outdated entries from constraints' storages.
  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  void Clear() { constraints_.clear(); }

 private:
  std::map<std::pair<LabelId, std::set<PropertyId>>, utils::SkipList<Entry>> constraints_;
};

struct Constraints {
  std::vector<std::pair<LabelId, PropertyId>> existence_constraints;
  UniqueConstraints unique_constraints;
};

/// Adds a unique constraint to `constraints`. Returns true if the constraint
/// was successfully added, false if it already exists and a
/// `ConstraintViolation` if there is an existing vertex violating the
/// constraint.
///
/// @throw std::bad_alloc
/// @throw std::length_error
inline utils::BasicResult<ConstraintViolation, bool> CreateExistenceConstraint(
    Constraints *constraints, LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices) {
  if (utils::Contains(constraints->existence_constraints, std::make_pair(label, property))) {
    return false;
  }
  for (const auto &vertex : vertices) {
    if (!vertex.deleted && utils::Contains(vertex.labels, label) && !vertex.properties.HasProperty(property)) {
      return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set<PropertyId>{property}};
    }
  }
  constraints->existence_constraints.emplace_back(label, property);
  return true;
}

/// Removes a unique constraint from `constraints`. Returns true if the
/// constraint was removed, and false if it doesn't exist.
inline bool DropExistenceConstraint(Constraints *constraints, LabelId label, PropertyId property) {
  auto it = std::find(constraints->existence_constraints.begin(), constraints->existence_constraints.end(),
                      std::make_pair(label, property));
  if (it == constraints->existence_constraints.end()) {
    return false;
  }
  constraints->existence_constraints.erase(it);
  return true;
}

/// Verifies that the given vertex satisfies all existence constraints. Returns
/// `std::nullopt` if all checks pass, and `ConstraintViolation` describing the
/// violated constraint otherwise.
[[nodiscard]] inline std::optional<ConstraintViolation> ValidateExistenceConstraints(const Vertex &vertex,
                                                                                     const Constraints &constraints) {
  for (const auto &[label, property] : constraints.existence_constraints) {
    if (!vertex.deleted && utils::Contains(vertex.labels, label) && !vertex.properties.HasProperty(property)) {
      return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set<PropertyId>{property}};
    }
  }
  return std::nullopt;
}

/// Returns a list of all created existence constraints.
inline std::vector<std::pair<LabelId, PropertyId>> ListExistenceConstraints(const Constraints &constraints) {
  return constraints.existence_constraints;
}

}  // namespace storage
