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

#include "storage/v2/constraints.hpp"

#include <algorithm>
#include <cstring>
#include <map>

#include "storage/v2/mvcc.hpp"
#include "utils/logging.hpp"

namespace storage {
namespace {

/// Helper function that determines position of the given `property` in the
/// sorted `property_array` using binary search. In the case that `property`
/// cannot be found, `std::nullopt` is returned.
std::optional<size_t> FindPropertyPosition(const PropertyIdArray &property_array, PropertyId property) {
  auto it = std::lower_bound(property_array.values, property_array.values + property_array.size, property);
  if (it == property_array.values + property_array.size || *it != property) {
    return std::nullopt;
  }

  return it - property_array.values;
}

/// Helper function for validating unique constraints on commit. Returns true if
/// the last committed version of the given vertex contains the given label and
/// set of property values. This function should be called when commit lock is
/// active.
bool LastCommittedVersionHasLabelProperty(const Vertex &vertex, LabelId label, const std::set<PropertyId> &properties,
                                          const std::vector<PropertyValue> &value_array, const Transaction &transaction,
                                          uint64_t commit_timestamp) {
  MG_ASSERT(properties.size() == value_array.size(), "Invalid database state!");

  PropertyIdArray property_array(properties.size());
  bool current_value_equal_to_value[kUniqueConstraintsMaxProperties];
  memset(current_value_equal_to_value, 0, sizeof(current_value_equal_to_value));

  // Since the commit lock is active, any transaction that tries to write to
  // a vertex which is part of the given `transaction` will result in a
  // serialization error. But, note that the given `vertex`'s data does not have
  // to be modified in the current `transaction`, meaning that a guard lock to
  // access vertex's data is still necessary because another active transaction
  // could modify it in the meantime.
  Delta *delta;
  bool deleted;
  bool has_label;
  {
    std::lock_guard<utils::SpinLock> guard(vertex.lock);
    delta = vertex.delta;
    deleted = vertex.deleted;
    has_label = utils::Contains(vertex.labels, label);

    size_t i = 0;
    for (const auto &property : properties) {
      current_value_equal_to_value[i] = vertex.properties.IsPropertyEqual(property, value_array[i]);
      property_array.values[i] = property;
      i++;
    }
  }

  while (delta != nullptr) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    if (ts < commit_timestamp || ts == transaction.transaction_id) {
      break;
    }

    switch (delta->action) {
      case Delta::Action::SET_PROPERTY: {
        auto pos = FindPropertyPosition(property_array, delta->property.key);
        if (pos) {
          current_value_equal_to_value[*pos] = delta->property.value == value_array[*pos];
        }
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        MG_ASSERT(!deleted, "Invalid database state!");
        deleted = true;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        MG_ASSERT(deleted, "Invalid database state!");
        deleted = false;
        break;
      }
      case Delta::Action::ADD_LABEL: {
        if (delta->label == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
          break;
        }
      }
      case Delta::Action::REMOVE_LABEL: {
        if (delta->label == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
          break;
        }
      }
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }

    delta = delta->next.load(std::memory_order_acquire);
  }

  for (size_t i = 0; i < properties.size(); ++i) {
    if (!current_value_equal_to_value[i]) {
      return false;
    }
  }

  return !deleted && has_label;
}

/// Helper function for unique constraint garbage collection. Returns true if
/// there's a reachable version of the vertex that has the given label and
/// property values.
bool AnyVersionHasLabelProperty(const Vertex &vertex, LabelId label, const std::set<PropertyId> &properties,
                                const std::vector<PropertyValue> &values, uint64_t timestamp) {
  MG_ASSERT(properties.size() == values.size(), "Invalid database state!");

  PropertyIdArray property_array(properties.size());
  bool current_value_equal_to_value[kUniqueConstraintsMaxProperties];
  memset(current_value_equal_to_value, 0, sizeof(current_value_equal_to_value));

  bool has_label;
  bool deleted;
  Delta *delta;
  {
    std::lock_guard<utils::SpinLock> guard(vertex.lock);
    has_label = utils::Contains(vertex.labels, label);
    deleted = vertex.deleted;
    delta = vertex.delta;

    size_t i = 0;
    for (const auto &property : properties) {
      current_value_equal_to_value[i] = vertex.properties.IsPropertyEqual(property, values[i]);
      property_array.values[i] = property;
      i++;
    }
  }

  {
    bool all_values_match = true;
    for (size_t i = 0; i < values.size(); ++i) {
      if (!current_value_equal_to_value[i]) {
        all_values_match = false;
        break;
      }
    }
    if (!deleted && has_label && all_values_match) {
      return true;
    }
  }

  while (delta != nullptr) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    if (ts < timestamp) {
      break;
    }
    switch (delta->action) {
      case Delta::Action::ADD_LABEL:
        if (delta->label == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
        }
        break;
      case Delta::Action::REMOVE_LABEL:
        if (delta->label == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
        }
        break;
      case Delta::Action::SET_PROPERTY: {
        auto pos = FindPropertyPosition(property_array, delta->property.key);
        if (pos) {
          current_value_equal_to_value[*pos] = delta->property.value == values[*pos];
        }
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        MG_ASSERT(deleted, "Invalid database state!");
        deleted = false;
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        MG_ASSERT(!deleted, "Invalid database state!");
        deleted = true;
        break;
      }
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }

    bool all_values_match = true;
    for (size_t i = 0; i < values.size(); ++i) {
      if (!current_value_equal_to_value[i]) {
        all_values_match = false;
        break;
      }
    }
    if (!deleted && has_label && all_values_match) {
      return true;
    }
    delta = delta->next.load(std::memory_order_acquire);
  }
  return false;
}

/// Helper function that, given the set of `properties`, extracts corresponding
/// property values from the `vertex`.
/// @throw std::bad_alloc
std::optional<std::vector<PropertyValue>> ExtractPropertyValues(const Vertex &vertex,
                                                                const std::set<PropertyId> &properties) {
  std::vector<PropertyValue> value_array;
  value_array.reserve(properties.size());
  for (const auto &prop : properties) {
    auto value = vertex.properties.GetProperty(prop);
    if (value.IsNull()) {
      return std::nullopt;
    }
    value_array.emplace_back(std::move(value));
  }
  return std::move(value_array);
}

}  // namespace

bool operator==(const ConstraintViolation &lhs, const ConstraintViolation &rhs) {
  return lhs.type == rhs.type && lhs.label == rhs.label && lhs.properties == rhs.properties;
}

bool UniqueConstraints::Entry::operator<(const Entry &rhs) {
  if (values < rhs.values) {
    return true;
  }
  if (rhs.values < values) {
    return false;
  }
  return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
}

bool UniqueConstraints::Entry::operator==(const Entry &rhs) {
  return values == rhs.values && vertex == rhs.vertex && timestamp == rhs.timestamp;
}

bool UniqueConstraints::Entry::operator<(const std::vector<PropertyValue> &rhs) { return values < rhs; }

bool UniqueConstraints::Entry::operator==(const std::vector<PropertyValue> &rhs) { return values == rhs; }

void UniqueConstraints::UpdateBeforeCommit(const Vertex *vertex, const Transaction &tx) {
  for (auto &[label_props, storage] : constraints_) {
    if (!utils::Contains(vertex->labels, label_props.first)) {
      continue;
    }
    auto values = ExtractPropertyValues(*vertex, label_props.second);
    if (values) {
      auto acc = storage.access();
      acc.insert(Entry{std::move(*values), vertex, tx.start_timestamp});
    }
  }
}

utils::BasicResult<ConstraintViolation, UniqueConstraints::CreationStatus> UniqueConstraints::CreateConstraint(
    LabelId label, const std::set<PropertyId> &properties, utils::SkipList<Vertex>::Accessor vertices) {
  if (properties.empty()) {
    return CreationStatus::EMPTY_PROPERTIES;
  }
  if (properties.size() > kUniqueConstraintsMaxProperties) {
    return CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED;
  }

  auto [constraint, emplaced] =
      constraints_.emplace(std::piecewise_construct, std::forward_as_tuple(label, properties), std::forward_as_tuple());

  if (!emplaced) {
    // Constraint already exists.
    return CreationStatus::ALREADY_EXISTS;
  }

  bool violation_found = false;

  {
    auto acc = constraint->second.access();

    for (const Vertex &vertex : vertices) {
      if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
        continue;
      }
      auto values = ExtractPropertyValues(vertex, properties);
      if (!values) {
        continue;
      }

      // Check whether there already is a vertex with the same values for the
      // given label and property.
      auto it = acc.find_equal_or_greater(*values);
      if (it != acc.end() && it->values == *values) {
        violation_found = true;
        break;
      }

      acc.insert(Entry{std::move(*values), &vertex, 0});
    }
  }

  if (violation_found) {
    // In the case of the violation, storage for the current constraint has to
    // be removed.
    constraints_.erase(constraint);
    return ConstraintViolation{ConstraintViolation::Type::UNIQUE, label, properties};
  }
  return CreationStatus::SUCCESS;
}

UniqueConstraints::DeletionStatus UniqueConstraints::DropConstraint(LabelId label,
                                                                    const std::set<PropertyId> &properties) {
  if (properties.empty()) {
    return UniqueConstraints::DeletionStatus::EMPTY_PROPERTIES;
  }
  if (properties.size() > kUniqueConstraintsMaxProperties) {
    return UniqueConstraints::DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED;
  }
  if (constraints_.erase({label, properties}) > 0) {
    return UniqueConstraints::DeletionStatus::SUCCESS;
  }
  return UniqueConstraints::DeletionStatus::NOT_FOUND;
}

std::optional<ConstraintViolation> UniqueConstraints::Validate(const Vertex &vertex, const Transaction &tx,
                                                               uint64_t commit_timestamp) const {
  if (vertex.deleted) {
    return std::nullopt;
  }
  for (const auto &[label_props, storage] : constraints_) {
    const auto &label = label_props.first;
    const auto &properties = label_props.second;
    if (!utils::Contains(vertex.labels, label)) {
      continue;
    }

    auto value_array = ExtractPropertyValues(vertex, properties);
    if (!value_array) {
      continue;
    }
    auto acc = storage.access();
    auto it = acc.find_equal_or_greater(*value_array);
    for (; it != acc.end(); ++it) {
      if (*value_array < it->values) {
        break;
      }

      // The `vertex` that is going to be committed violates a unique constraint
      // if it's different than a vertex indexed in the list of constraints and
      // has the same label and property value as the last committed version of
      // the vertex from the list.
      if (&vertex != it->vertex &&
          LastCommittedVersionHasLabelProperty(*it->vertex, label, properties, *value_array, tx, commit_timestamp)) {
        return ConstraintViolation{ConstraintViolation::Type::UNIQUE, label, properties};
      }
    }
  }
  return std::nullopt;
}

std::vector<std::pair<LabelId, std::set<PropertyId>>> UniqueConstraints::ListConstraints() const {
  std::vector<std::pair<LabelId, std::set<PropertyId>>> ret;
  ret.reserve(constraints_.size());
  for (const auto &[label_props, _] : constraints_) {
    ret.push_back(label_props);
  }
  return ret;
}

void UniqueConstraints::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  for (auto &[label_props, storage] : constraints_) {
    auto acc = storage.access();
    for (auto it = acc.begin(); it != acc.end();) {
      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      if ((next_it != acc.end() && it->vertex == next_it->vertex && it->values == next_it->values) ||
          !AnyVersionHasLabelProperty(*it->vertex, label_props.first, label_props.second, it->values,
                                      oldest_active_start_timestamp)) {
        acc.remove(*it);
      }
      it = next_it;
    }
  }
}

}  // namespace storage
