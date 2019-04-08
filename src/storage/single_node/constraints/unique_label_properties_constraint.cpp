#include "storage/single_node/constraints/unique_label_properties_constraint.hpp"

#include <algorithm>

#include "storage/single_node/constraints/common.hpp"
#include "storage/single_node/vertex_accessor.hpp"

namespace storage::constraints {
auto FindIn(storage::Label label,
            const std::vector<storage::Property> &properties,
            const std::list<impl::LabelPropertiesEntry> &constraints) {
  return std::find_if(
      constraints.begin(), constraints.end(), [label, properties](auto &c) {
        return c.label == label &&
               std::is_permutation(properties.begin(), properties.end(),
                                   c.properties.begin(), c.properties.end());
      });
}

auto FindEntry(std::list<impl::LabelPropertyPair> &version_pairs,
               const std::vector<PropertyValue> &values) {
  return std::find_if(version_pairs.begin(), version_pairs.end(),
                      [values](auto &p) { return p.values == values; });
}

void UniqueLabelPropertiesConstraint::AddConstraint(
    storage::Label label, const std::vector<storage::Property> &properties,
    const tx::Transaction &t) {
  auto constraint = FindIn(label, properties, constraints_);
  if (constraint == constraints_.end())
    constraints_.emplace_back(label, properties);
}

void UniqueLabelPropertiesConstraint::RemoveConstraint(
    storage::Label label, const std::vector<storage::Property> &properties) {
  auto constraint = FindIn(label, properties, constraints_);
  if (constraint != constraints_.end()) constraints_.erase(constraint);
}

bool UniqueLabelPropertiesConstraint::Exists(
    storage::Label label,
    const std::vector<storage::Property> &properties) const {
  return FindIn(label, properties, constraints_) != constraints_.end();
}

std::vector<LabelProperties> UniqueLabelPropertiesConstraint::ListConstraints()
    const {
  std::vector<LabelProperties> constraints;
  constraints.reserve(constraints_.size());
  std::transform(constraints_.begin(), constraints_.end(),
                 std::back_inserter(constraints), [](auto &c) {
                   return LabelProperties{c.label, c.properties};
                 });
  return constraints;
}

void UniqueLabelPropertiesConstraint::UpdateOnAddLabel(
    storage::Label label, const VertexAccessor &accessor,
    const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
   if (constraint.label != label) continue;
   std::vector<PropertyValue> values;
   for (auto p : constraint.properties) {
     auto value = vertex.properties_.at(p);
     if (value.IsNull()) break;
     values.emplace_back(value);
   }
   if (values.size() != constraint.properties.size()) continue;
   auto entry = FindEntry(constraint.version_pairs, values);
   if (entry != constraint.version_pairs.end()) {
     entry->record.Insert(accessor.gid(), t);
   } else {
     constraint.version_pairs.emplace_back(accessor.gid(), values, t);
   }
  }
}

void UniqueLabelPropertiesConstraint::UpdateOnRemoveLabel(
    storage::Label label, const VertexAccessor &accessor,
    const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    if (constraint.label != label) continue;
    std::vector<PropertyValue> values;
    for (auto p : constraint.properties) {
      auto value = vertex.properties_.at(p);
      if (value.IsNull()) break;
      values.emplace_back(value);
    }
    if (values.size() != constraint.properties.size()) continue;
    auto entry = FindEntry(constraint.version_pairs, values);
    if (entry != constraint.version_pairs.end())
      entry->record.Remove(accessor.gid(), t);
  }
}

void UniqueLabelPropertiesConstraint::UpdateOnAddProperty(
    storage::Property property, const PropertyValue &value,
    const VertexAccessor &accessor, const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    if (!utils::Contains(constraint.properties, property)) continue;
    if (!utils::Contains(vertex.labels_, constraint.label)) continue;
    std::vector<PropertyValue> values;
    for (auto p : constraint.properties) {
      auto value = vertex.properties_.at(p);
      if (value.IsNull()) break;
      values.emplace_back(value);
    }
    if (values.size() != constraint.properties.size()) continue;
    auto entry = FindEntry(constraint.version_pairs, values);
    if (entry != constraint.version_pairs.end()) {
      entry->record.Insert(accessor.gid(), t);
    } else {
      constraint.version_pairs.emplace_back(accessor.gid(), values, t);
    }
  }
}

void UniqueLabelPropertiesConstraint::UpdateOnRemoveProperty(
    storage::Property property, const PropertyValue &value,
    const VertexAccessor &accessor, const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    if (!utils::Contains(constraint.properties, property)) continue;
    if (!utils::Contains(vertex.labels_, constraint.label)) continue;

    std::vector<PropertyValue> values;
    for (auto p : constraint.properties) {
      if (p == property) {
        values.emplace_back(value);
      } else {
        auto v = vertex.properties_.at(p);
        if (v.IsNull()) break;
        values.emplace_back(v);
      }
    }

    if (values.size() != constraint.properties.size()) continue;
    auto entry = FindEntry(constraint.version_pairs, values);
    if (entry != constraint.version_pairs.end())
      entry->record.Remove(accessor.gid(), t);
  }
}

void UniqueLabelPropertiesConstraint::Refresh(const tx::Snapshot &snapshot,
                                              const tx::Engine &engine) {
  common::UniqueConstraintRefresh(snapshot, engine, constraints_, lock_);
}
} // namespace storage::constraints
