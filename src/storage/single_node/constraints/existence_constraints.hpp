/// @file
#pragma once

#include <list>
#include <unordered_map>
#include <vector>

#include "storage/common/types/property_value.hpp"
#include "storage/common/types/types.hpp"
#include "storage/single_node/vertex.hpp"
#include "transactions/type.hpp"

namespace database {

/// Existence rule defines label -> set of properties rule. This means that
/// every vertex with that label has to have every property in the set of
/// properties. This rule doesn't care about the PropertyValues for those
/// properties.
struct ExistenceRule {
  storage::Label label;
  std::vector<storage::Property> properties;

  bool operator==(const ExistenceRule &rule) const {
    return label == rule.label &&
           std::is_permutation(properties.begin(), properties.end(),
                               rule.properties.begin());
  }

  bool operator!=(const ExistenceRule &rule) const { return !(*this == rule); }
};

bool CheckIfSatisfiesExistenceRule(const Vertex *vertex,
                                   const database::ExistenceRule &rule);

/// ExistenceConstraints contains all active constrains. Existence constraints
/// are restriction on the vertices and are defined by ExistenceRule.
/// To create and delete constraint, the caller must ensure that there are no
/// other transactions running in parallel.
/// Additionally, for adding constraint caller must check existing vertices for
/// constraint violations before adding that constraint. You may use
/// CheckIfSatisfiesExistenceRule function for that.
/// This is needed to ensure logical correctness of transactions.
/// Once created, the client uses method CheckIfSatisfies to check that updated
/// vertex doesn't violate any of the existing constraints. If it does, that
/// update must be reverted.
class ExistenceConstraints {
 public:
  ExistenceConstraints() = default;
  ExistenceConstraints(const ExistenceConstraints &) = delete;
  ExistenceConstraints(ExistenceConstraints &&) = delete;
  ExistenceConstraints &operator=(const ExistenceConstraints &) = delete;
  ExistenceConstraints &operator=(ExistenceConstraints &&) = delete;

  /// Adds new constraint, if the constraint already exists this method does
  /// nothing. This method doesn't check if any of the existing vertices breaks
  /// this constraint. Caller must do that instead. Caller must also ensure
  /// that no other transaction is running in parallel.
  void AddConstraint(const ExistenceRule &rule);

  /// Removes existing constraint, if the constraint doesn't exist this method
  /// does nothing. Caller must ensure that no other transaction is running in
  /// parallel.
  void RemoveConstraint(const ExistenceRule &rule);

  /// Checks whether given constraint is visible.
  bool Exists(const ExistenceRule &rule) const;

  /// Check if add label update satisfies all visible constraints.
  /// @return true if all constraints are satisfied, false otherwise
  bool CheckOnAddLabel(const Vertex *vertex, storage::Label label) const;

  /// Check if remove property update satisfies all visible constraints.
  /// @return true if all constraints are satisfied, false otherwise
  bool CheckOnRemoveProperty(const Vertex *vertex,
                             storage::Property property) const;

  /// Returns list of all constraints.
  const std::vector<ExistenceRule> &ListConstraints() const;

 private:
  std::vector<ExistenceRule> constraints_;
};
};  // namespace database
