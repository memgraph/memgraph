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
#include <set>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/logging.hpp"
#include "utils/result.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

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

    bool operator<(const Entry &rhs) const;
    bool operator==(const Entry &rhs) const;

    bool operator<(const std::vector<PropertyValue> &rhs) const;
    bool operator==(const std::vector<PropertyValue> &rhs) const;
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

  bool ConstraintExists(LabelId label, const std::set<PropertyId> &properties) const;

  /// Validates the given vertex against unique constraints before committing.
  /// This method should be called while commit lock is active with
  /// `commit_timestamp` being a potential commit timestamp of the transaction.
  /// @throw std::bad_alloc
  std::optional<ConstraintViolation> Validate(const Vertex &vertex, const Transaction &tx,
                                              uint64_t commit_timestamp) const;

  std::vector<std::pair<LabelId, std::set<PropertyId>>> ListConstraints() const;

  /// GC method that removes outdated entries from constraints' storages.
  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  void Clear();

 private:
  std::map<std::pair<LabelId, std::set<PropertyId>>, utils::SkipList<Entry>> constraints_;
};

class ExistenceConstraints {
 public:
  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVertexOnConstraint(const Vertex &vertex,
                                                                                     LabelId label,
                                                                                     PropertyId property) {
    if (!vertex.deleted && utils::Contains(vertex.labels, label) && !vertex.properties.HasProperty(property)) {
      return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set<PropertyId>{property}};
    }
    return std::nullopt;
  }

  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVerticesOnConstraint(
      utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property) {
    for (const auto &vertex : vertices) {
      if (auto violation = ValidateVertexOnConstraint(vertex, label, property); violation.has_value()) {
        return violation;
      }
    }
    return std::nullopt;
  }

  bool ConstraintExists(LabelId label, PropertyId property) const;

  void InsertConstraint(LabelId label, PropertyId property);

  /// Returns true if the constraint was removed, and false if it doesn't exist.
  bool DropConstraint(LabelId label, PropertyId property);

  ///  Returns `std::nullopt` if all checks pass, and `ConstraintViolation` describing the violated constraint
  ///  otherwise.
  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex);

  std::vector<std::pair<LabelId, PropertyId>> ListConstraints() const;

 private:
  std::vector<std::pair<LabelId, PropertyId>> constraints_;
};

struct Constraints {
  /// TODO: andi Pass storage mode here
  Constraints();

  Constraints(const Constraints &) = delete;
  Constraints(Constraints &&) = delete;
  Constraints &operator=(const Constraints &) = delete;
  Constraints &operator=(Constraints &&) = delete;
  ~Constraints() = default;

  std::unique_ptr<ExistenceConstraints> existence_constraints_;
  std::unique_ptr<UniqueConstraints> unique_constraints_;
};

}  // namespace memgraph::storage
