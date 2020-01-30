#include "storage/v2/constraints.hpp"

#include <set>

#include "storage/v2/mvcc.hpp"

namespace storage {
namespace {

/// Helper function for validating unique constraints on commit. Returns true if
/// the last commited version of the given vertex contains the given label and
/// property value. This function should be called when commit lock is active.
bool LastCommitedVersionHasLabelProperty(const Vertex &vertex, LabelId label,
                                         PropertyId key, const PropertyValue &value,
                                         const Transaction &transaction,
                                         uint64_t commit_timestamp) {
  // Note that a guard lock isn't necessary to access vertex's data.
  // Any transaction that tries to write to that vertex will result in
  // serialization error.
  bool deleted = vertex.deleted;
  bool has_label = utils::Contains(vertex.labels, label);
  bool current_value_equal_to_value = value.IsNull();
  auto it = vertex.properties.find(key);
  if (it != vertex.properties.end()) {
    current_value_equal_to_value = it->second == value;
  }

  for (Delta *delta = vertex.delta; delta != nullptr;
      delta = delta->next.load(std::memory_order_acquire)) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    if (ts < commit_timestamp || ts == transaction.transaction_id) {
      break;
    }

    switch (delta->action) {
      case Delta::Action::SET_PROPERTY: {
        if (delta->property.key == key) {
          current_value_equal_to_value = (delta->property.value == value);
        }
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        CHECK(!deleted) << "Invalid database state!";
        deleted = true;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        CHECK(deleted) << "Invalid database state!";
        deleted = false;
        break;
      }
      case Delta::Action::ADD_LABEL: {
        if (delta->label == label) {
          CHECK(!has_label) << "Invalid database state!";
          has_label = true;
          break;
        }
      }
      case Delta::Action::REMOVE_LABEL: {
        if (delta->label == label) {
          CHECK(has_label) << "Invalid database state!";
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
  }

  return !deleted && has_label && current_value_equal_to_value;
}

/// Helper function for unique constraint garbage collection. Returns true if
/// there's a reachable version of the vertex that has the given label and
/// property value.
bool AnyVersionHasLabelProperty(const Vertex &vertex, LabelId label,
                                PropertyId key, const PropertyValue &value,
                                uint64_t timestamp) {
  bool has_label;
  bool deleted;
  bool current_value_equal_to_value = value.IsNull();
  Delta *delta;
  {
    std::lock_guard<utils::SpinLock> guard(vertex.lock);
    has_label = utils::Contains(vertex.labels, label);
    auto it = vertex.properties.find(key);
    if (it != vertex.properties.end()) {
      current_value_equal_to_value = it->second == value;
    }
    deleted = vertex.deleted;
    delta = vertex.delta;
  }

  if (!deleted && has_label && current_value_equal_to_value) {
    return true;
  }

  while (delta != nullptr) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    if (ts < timestamp) {
      break;
    }
    switch (delta->action) {
      case Delta::Action::ADD_LABEL:
        if (delta->label == label) {
          CHECK(!has_label) << "Invalid database state!";
          has_label = true;
        }
        break;
      case Delta::Action::REMOVE_LABEL:
        if (delta->label == label) {
          CHECK(has_label) << "Invalid database state!";
          has_label = false;
        }
        break;
      case Delta::Action::SET_PROPERTY:
        if (delta->property.key == key) {
          current_value_equal_to_value = delta->property.value == value;
        }
        break;
      case Delta::Action::RECREATE_OBJECT: {
        CHECK(deleted) << "Invalid database state!";
        deleted = false;
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        CHECK(!deleted) << "Invalid database state!";
        deleted = true;
        break;
      }
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
    if (!deleted && has_label && current_value_equal_to_value) {
      return true;
    }
    delta = delta->next.load(std::memory_order_acquire);
  }
  return false;
}

}  // namespace

bool operator==(const ConstraintViolation &lhs,
                const ConstraintViolation &rhs) {
  return lhs.type == rhs.type && lhs.label == rhs.label &&
         lhs.property == rhs.property;
}

bool UniqueConstraints::Entry::operator<(const Entry &rhs) {
  if (value < rhs.value) {
    return true;
  }
  if (rhs.value < value) {
    return false;
  }
  return std::make_tuple(vertex, timestamp) <
         std::make_tuple(rhs.vertex, rhs.timestamp);
}

bool UniqueConstraints::Entry::operator==(const Entry &rhs) {
  return value == rhs.value && vertex == rhs.vertex &&
         timestamp == rhs.timestamp;
}

bool UniqueConstraints::Entry::operator<(const PropertyValue &rhs) {
  return value < rhs;
}

bool UniqueConstraints::Entry::operator==(const PropertyValue &rhs) {
  return value == rhs;
}

void UniqueConstraints::UpdateBeforeCommit(const Vertex *vertex,
                                           const Transaction &tx) {
  for (auto &[label_prop, storage] : constraints_) {
    if (!utils::Contains(vertex->labels, label_prop.first)) {
      continue;
    }
    auto it = vertex->properties.find(label_prop.second);
    if (it != vertex->properties.end() && !it->second.IsNull()) {
      auto acc = storage.access();
      acc.insert(Entry{it->second, vertex, tx.start_timestamp});
    }
  }
}

utils::BasicResult<ConstraintViolation, bool> UniqueConstraints::CreateConstraint(
    LabelId label, PropertyId property,
    utils::SkipList<Vertex>::Accessor vertices) {
  auto [constraint, emplaced] = constraints_.emplace(
                                    std::piecewise_construct,
                                    std::forward_as_tuple(label, property),
                                    std::forward_as_tuple());
  if (!emplaced) {
    // Constraint already exists.
    return false;
  }

  bool violation_found = false;

  {
    auto acc = constraint->second.access();

    for (const Vertex &vertex : vertices) {
      if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
        continue;
      }
      auto property_it = vertex.properties.find(property);
      if (property_it == vertex.properties.end()) {
        continue;
      }
      const PropertyValue &value = property_it->second;

      // Check whether there already is a vertex with the same value for the given
      // label and property.
      auto it = acc.find_equal_or_greater(value);
      if (it != acc.end() && it->value == value) {
        violation_found = true;
        break;
      }

      acc.insert(Entry{value, &vertex, 0});
    }
  }

  if (violation_found) {
    // In the case of the violation, storage for the current constraint has to
    // be removed.
    constraints_.erase(constraint);
    return ConstraintViolation{ConstraintViolation::Type::UNIQUE, label,
                               property};
  }
  return true;
}

std::optional<ConstraintViolation> UniqueConstraints::Validate(
    const Vertex &vertex, const Transaction &tx,
    uint64_t commit_timestamp) const {
  if  (vertex.deleted) {
    return std::nullopt;
  }
  for (const auto &[label_prop, storage] : constraints_) {
    const auto &label = label_prop.first;
    const auto &property = label_prop.second;
    auto property_it = vertex.properties.find(property);
    if (!utils::Contains(vertex.labels, label) ||
        property_it == vertex.properties.end()) {
      // We are only interested in constraints relevant to the current vertex,
      // i.e. having (label, property) included in the given vertex.
      continue;
    }

    const auto &value = property_it->second;
    auto acc = storage.access();
    auto it = acc.find_equal_or_greater(value);
    for (; it != acc.end(); ++it) {
      if (value < it->value) {
        break;
      }

      // The `vertex` that is going to be commited violates a unique constraint
      // if it's different than a vertex indexed in the list of constraints and
      // has the same label and property value as the last commited version of
      // the vertex from the list.
      if (&vertex != it->vertex &&
          LastCommitedVersionHasLabelProperty(*it->vertex, label, property,
                                              value, tx, commit_timestamp)) {
        return ConstraintViolation{ConstraintViolation::Type::UNIQUE, label,
                                   property};
      }
    }
  }
  return std::nullopt;
}

std::vector<std::pair<LabelId, PropertyId>> UniqueConstraints::ListConstraints()
    const {
  std::vector<std::pair<LabelId, PropertyId>> ret;
  ret.reserve(constraints_.size());
  for (const auto &item : constraints_) {
    ret.push_back(item.first);
  }
  return ret;
}

void UniqueConstraints::RemoveObsoleteEntries(
    uint64_t oldest_active_start_timestamp) {
  for (auto &[label_prop, storage] : constraints_) {
    auto acc = storage.access();
    for (auto it = acc.begin(); it != acc.end();) {
      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      if ((next_it != acc.end() && it->vertex == next_it->vertex &&
           it->value == next_it->value) ||
          !AnyVersionHasLabelProperty(*it->vertex, label_prop.first,
                                      label_prop.second, it->value,
                                      oldest_active_start_timestamp)) {
        acc.remove(*it);
      }
      it = next_it;
    }
  }
}

}  // namespace storage
