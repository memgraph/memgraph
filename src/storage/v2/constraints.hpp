#pragma once

#include <optional>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/result.hpp"
#include "utils/skip_list.hpp"

namespace storage {

struct ConstraintViolation {
  enum class Type {
    EXISTENCE,
    UNIQUE,
  };

  Type type;
  LabelId label;
  PropertyId property;
};

bool operator==(const ConstraintViolation &lhs,
                const ConstraintViolation &rhs);

// TODO(tsabolcec): Support property sets. Unique constraints could be defined
// for pairs (label, property set). However, current implementation supports
// only pairs of label and a single property.
class UniqueConstraints {
 private:
  struct Entry {
    PropertyValue value;
    const Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs);
    bool operator==(const Entry &rhs);

    bool operator<(const PropertyValue &rhs);
    bool operator==(const PropertyValue &rhs);
  };

 public:
  /// Indexes the given vertex for relevant labels and properties.
  /// This method should be called before committing and validating vertices
  /// against unique constraints.
  /// @throw std::bad_alloc
  void UpdateBeforeCommit(const Vertex *vertex, const Transaction &tx);

  /// Creates unique constraint on the given `label` and `property`.
  /// Returns constraint violation if there are multiple vertices with the same
  /// label and property values. Returns false if constraint already existed and
  /// true on success.
  /// @throw std::bad_alloc
  utils::BasicResult<ConstraintViolation, bool> CreateConstraint(
      LabelId label, PropertyId property,
      utils::SkipList<Vertex>::Accessor vertices);

  bool DropConstraint(LabelId label, PropertyId property) {
    return constraints_.erase({label, property}) > 0;
  }

  bool ConstraintExists(LabelId label, PropertyId property) const {
    return constraints_.find({label, property}) != constraints_.end();
  }

  /// Validates the given vertex against unique constraints before committing.
  /// This method should be called while commit lock is active with
  /// `commit_timestamp` being a potential commit timestamp of the transaction.
  std::optional<ConstraintViolation> Validate(const Vertex &vertex,
                                              const Transaction &tx,
                                              uint64_t commit_timestamp) const;

  std::vector<std::pair<LabelId, PropertyId>> ListConstraints() const;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  void Clear() { constraints_.clear(); }

 private:
  std::map<std::pair<LabelId, PropertyId>, utils::SkipList<Entry>> constraints_;
};

struct Constraints {
  std::vector<std::pair<LabelId, PropertyId>> existence_constraints;
  UniqueConstraints unique_constraints;
};

/// Adds a unique constraint to `constraints`. Returns true if the constraint
/// was successfuly added, false if it already exists and an
/// `ExistenceConstraintViolation` if there is an existing vertex violating the
/// constraint.
///
/// @throw std::bad_alloc
/// @throw std::length_error
inline utils::BasicResult<ConstraintViolation, bool>
CreateExistenceConstraint(Constraints *constraints, LabelId label,
                          PropertyId property,
                          utils::SkipList<Vertex>::Accessor vertices) {
  if (utils::Contains(constraints->existence_constraints,
                      std::make_pair(label, property))) {
    return false;
  }
  for (const auto &vertex : vertices) {
    if (!vertex.deleted && utils::Contains(vertex.labels, label) &&
        vertex.properties.find(property) == vertex.properties.end()) {
      return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label,
                                 property};
    }
  }
  constraints->existence_constraints.emplace_back(label, property);
  return true;
}

/// Removes a unique constraint from `constraints`. Returns true if the
/// constraint was removed, and false if it doesn't exist.
inline bool DropExistenceConstraint(Constraints *constraints, LabelId label,
                                    PropertyId property) {
  auto it = std::find(constraints->existence_constraints.begin(),
                      constraints->existence_constraints.end(),
                      std::make_pair(label, property));
  if (it == constraints->existence_constraints.end()) {
    return false;
  }
  constraints->existence_constraints.erase(it);
  return true;
}

/// Verifies that the given vertex satisfies all existence constraints. Returns
/// nullopt if all checks pass, and `ExistenceConstraintViolation` describing
/// the violated constraint otherwise.
[[nodiscard]] inline std::optional<ConstraintViolation>
ValidateExistenceConstraints(const Vertex &vertex,
                             const Constraints &constraints) {
  for (const auto &[label, property] : constraints.existence_constraints) {
    if (!vertex.deleted && utils::Contains(vertex.labels, label) &&
        vertex.properties.find(property) == vertex.properties.end()) {
      return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label,
                                 property};
    }
  }
  return std::nullopt;
}

/// Returns a list of all created existence constraints.
inline std::vector<std::pair<LabelId, PropertyId>> ListExistenceConstraints(
    const Constraints &constraints) {
  return constraints.existence_constraints;
}

}  // namespace storage
