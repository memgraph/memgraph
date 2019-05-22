#include "storage/common/constraints/unique_constraints.hpp"

#include <algorithm>

#include "storage/vertex_accessor.hpp"

namespace storage::constraints {

namespace {
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
}  // anonymous namespace

bool UniqueConstraints::AddConstraint(const ConstraintEntry &entry) {
  auto constraint = FindIn(entry.label, entry.properties, constraints_);
  if (constraint == constraints_.end()) {
    constraints_.emplace_back(entry.label, entry.properties);
    return true;
  }
  return false;
}

bool UniqueConstraints::RemoveConstraint(const ConstraintEntry &entry) {
  auto constraint = FindIn(entry.label, entry.properties, constraints_);
  if (constraint != constraints_.end()) {
    constraints_.erase(constraint);
    return true;
  }
  return false;
}

bool UniqueConstraints::Exists(
    storage::Label label,
    const std::vector<storage::Property> &properties) const {
  return FindIn(label, properties, constraints_) != constraints_.end();
}

std::vector<ConstraintEntry> UniqueConstraints::ListConstraints() const {
  std::vector<ConstraintEntry> constraints(constraints_.size());
  std::transform(constraints_.begin(), constraints_.end(), constraints.begin(),
                 [](auto &c) {
                   return ConstraintEntry{c.label, c.properties};
                 });
  return constraints;
}

void UniqueConstraints::Update(const RecordAccessor<Vertex> &accessor,
                               const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    if (!utils::Contains(vertex.labels_, constraint.label)) continue;
    std::vector<PropertyValue> values;
    for (auto p : constraint.properties) {
      auto value = vertex.properties_.at(p);
      if (value.IsNull()) break;
      values.emplace_back(value);
    }
    if (values.size() != constraint.properties.size()) continue;
    auto entry = std::find_if(constraint.version_pairs.begin(),
                              constraint.version_pairs.end(),
                              [values](const impl::LabelPropertyPair &p) {
                                return p.values == values;
                              });
    if (entry != constraint.version_pairs.end()) {
      entry->record.Insert(accessor.gid(), t);
    } else {
      constraint.version_pairs.emplace_back(accessor.gid(), values, t);
    }
  }
}

void UniqueConstraints::UpdateOnAddLabel(storage::Label label,
                                         const RecordAccessor<Vertex> &accessor,
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
    auto entry = std::find_if(constraint.version_pairs.begin(),
                              constraint.version_pairs.end(),
                              [values](const impl::LabelPropertyPair &p) {
                                return p.values == values;
                              });
    if (entry != constraint.version_pairs.end()) {
      entry->record.Insert(accessor.gid(), t);
    } else {
      constraint.version_pairs.emplace_back(accessor.gid(), values, t);
    }
  }
}

void UniqueConstraints::UpdateOnRemoveLabel(
    storage::Label label, const RecordAccessor<Vertex> &accessor,
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
    auto entry = std::find_if(constraint.version_pairs.begin(),
                              constraint.version_pairs.end(),
                              [values](const impl::LabelPropertyPair &p) {
                                return p.values == values;
                              });
    if (entry != constraint.version_pairs.end())
      entry->record.Remove(accessor.gid(), t);
  }
}

void UniqueConstraints::UpdateOnAddProperty(
    storage::Property property, const PropertyValue &previous_value,
    const PropertyValue &new_value, const RecordAccessor<Vertex> &accessor,
    const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    if (!utils::Contains(vertex.labels_, constraint.label)) continue;
    if (!utils::Contains(constraint.properties, property)) continue;

    std::vector<PropertyValue> old_values;
    std::vector<PropertyValue> new_values;
    for (auto p : constraint.properties) {
      auto value = vertex.properties_.at(p);

      if (p == property) {
        if (!previous_value.IsNull()) old_values.emplace_back(previous_value);
        if (!new_value.IsNull()) new_values.emplace_back(new_value);
      } else {
        if (value.IsNull()) break;
        old_values.emplace_back(value);
        new_values.emplace_back(value);
      }
    }

    // First we need to remove the old entry if there was one.
    if (old_values.size() == constraint.properties.size()) {
      auto entry = std::find_if(constraint.version_pairs.begin(),
                                constraint.version_pairs.end(),
                                [old_values](const impl::LabelPropertyPair &p) {
                                  return p.values == old_values;
                                });
      if (entry != constraint.version_pairs.end())
        entry->record.Remove(accessor.gid(), t);
    }

    if (new_values.size() != constraint.properties.size()) continue;
    auto entry = std::find_if(constraint.version_pairs.begin(),
                              constraint.version_pairs.end(),
                              [new_values](const impl::LabelPropertyPair &p) {
                                return p.values == new_values;
                              });
    if (entry != constraint.version_pairs.end()) {
      entry->record.Insert(accessor.gid(), t);
    } else {
      constraint.version_pairs.emplace_back(accessor.gid(), new_values, t);
    }
  }
}

void UniqueConstraints::UpdateOnRemoveProperty(
    storage::Property property, const PropertyValue &previous_value,
    const RecordAccessor<Vertex> &accessor, const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    if (!utils::Contains(vertex.labels_, constraint.label)) continue;
    if (!utils::Contains(constraint.properties, property)) continue;

    std::vector<PropertyValue> values;
    for (auto p : constraint.properties) {
      auto value = vertex.properties_.at(p);
      if (p == property) {
        values.emplace_back(previous_value);
      } else {
        if (value.IsNull()) break;
        values.emplace_back(value);
      }
    }

    if (values.size() != constraint.properties.size()) continue;
    auto entry = std::find_if(constraint.version_pairs.begin(),
                              constraint.version_pairs.end(),
                              [values](const impl::LabelPropertyPair &p) {
                                return p.values == values;
                              });
    if (entry != constraint.version_pairs.end()) {
      entry->record.Remove(accessor.gid(), t);
    }
  }
}

void UniqueConstraints::UpdateOnRemoveVertex(
    const RecordAccessor<Vertex> &accessor, const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    if (!utils::Contains(vertex.labels_, constraint.label)) continue;

    std::vector<PropertyValue> values;
    for (auto p : constraint.properties) {
      auto value = vertex.properties_.at(p);
      if (value.IsNull()) break;
      values.emplace_back(value);
    }

    if (values.size() != constraint.properties.size()) continue;
    auto entry = std::find_if(constraint.version_pairs.begin(),
                              constraint.version_pairs.end(),
                              [values](const impl::LabelPropertyPair &p) {
                                return p.values == values;
                              });
    if (entry != constraint.version_pairs.end()) {
      entry->record.Remove(accessor.gid(), t);
    }
  }
}

void UniqueConstraints::Refresh(const tx::Snapshot &snapshot,
                                const tx::Engine &engine) {
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    for (auto p = constraint.version_pairs.begin();
         p != constraint.version_pairs.end();) {
      auto exp_id = p->record.tx_id_exp;
      auto cre_id = p->record.tx_id_cre;
      if ((exp_id != 0 && exp_id < snapshot.back() &&
           engine.Info(exp_id).is_committed() && !snapshot.contains(exp_id)) ||
          (cre_id < snapshot.back() && engine.Info(cre_id).is_aborted())) {
        p = constraint.version_pairs.erase(p);
      } else {
        ++p;
      }
    }
  }
}
}  // namespace storage::constraints
