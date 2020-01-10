#pragma once

#include <optional>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/result.hpp"
#include "utils/skip_list.hpp"

namespace storage {

struct Constraints {
  std::vector<std::pair<LabelId, PropertyId>> existence_constraints;
};

struct ExistenceConstraintViolation {
  LabelId label;
  PropertyId property;
};

/// Adds a unique constraint to `constraints`. Returns true if the constraint
/// was successfuly added, false if it already exists and an
/// `ExistenceConstraintViolation` if there is an existing vertex violating the
/// constraint.
///
/// @throw std::bad_alloc
/// @throw std::length_error
inline utils::BasicResult<ExistenceConstraintViolation, bool>
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
      return ExistenceConstraintViolation{label, property};
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
[[nodiscard]] inline std::optional<ExistenceConstraintViolation>
ValidateExistenceConstraints(const Vertex &vertex,
                             const Constraints &constraints) {
  for (const auto &[label, property] : constraints.existence_constraints) {
    if (!vertex.deleted && utils::Contains(vertex.labels, label) &&
        vertex.properties.find(property) == vertex.properties.end()) {
      return ExistenceConstraintViolation{label, property};
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
