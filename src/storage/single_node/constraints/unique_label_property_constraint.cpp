#include "storage/single_node/constraints/unique_label_property_constraint.hpp"

#include "storage/single_node/record_accessor.hpp"
#include "storage/single_node/vertex.hpp"
#include "utils/algorithm.hpp"

namespace storage::constraints {
auto FindIn(storage::Label label, storage::Property property,
            const std::list<impl::LabelPropertyEntry> &constraints) {
  return std::find_if(constraints.begin(), constraints.end(),
                      [label, property](auto &c) {
                        return c.label == label && c.property == property;
                      });
}

bool UniqueLabelPropertyConstraint::AddConstraint(storage::Label label,
                                                  storage::Property property,
                                                  const tx::Transaction &t) {
  auto found = FindIn(label, property, constraints_);
  if (found == constraints_.end()) {
    constraints_.emplace_back(label, property);
    return true;
  } else {
    return false;
  }
}

bool UniqueLabelPropertyConstraint::RemoveConstraint(
    storage::Label label, storage::Property property) {
  auto found = FindIn(label, property, constraints_);
  if (found != constraints_.end()) {
    constraints_.erase(found);
    return true;
  } else {
    return false;
  }
}

bool UniqueLabelPropertyConstraint::Exists(storage::Label label,
                                           storage::Property property) const {
  return FindIn(label, property, constraints_) != constraints_.end();
}

std::vector<LabelProperty> UniqueLabelPropertyConstraint::ListConstraints()
    const {
  std::vector<LabelProperty> constraints;
  constraints.reserve(constraints_.size());
  std::transform(constraints_.begin(), constraints_.end(),
                 std::back_inserter(constraints), [](auto &c) {
                   return LabelProperty{c.label, c.property};
                 });
  return constraints;
}

void UniqueLabelPropertyConstraint::Update(
    const RecordAccessor<Vertex> &accessor, const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    bool contains_label = utils::Contains(vertex.labels_, constraint.label);
    auto value = vertex.properties_.at(constraint.property);
    if (contains_label && !value.IsNull()) {
      bool found = false;
      for (auto &p : constraint.version_pairs) {
        if (p.value == value) {
          p.record.Insert(accessor.gid(), t);
          found = true;
          break;
        }
      }

      if (!found) {
        constraint.version_pairs.emplace_back(accessor.gid(), value, t);
      }
    }
  }
}

void UniqueLabelPropertyConstraint::UpdateOnAddLabel(
    storage::Label label, const RecordAccessor<Vertex> &accessor,
    const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    auto value = vertex.properties_.at(constraint.property);
    if (constraint.label == label && !value.IsNull()) {
      bool found = false;
      for (auto &p : constraint.version_pairs) {
        if (p.value == value) {
          p.record.Insert(accessor.gid(), t);
          found = true;
          break;
        }
      }

      if (!found) {
        constraint.version_pairs.emplace_back(accessor.gid(), value, t);
      }
    }
  }
}
void UniqueLabelPropertyConstraint::UpdateOnRemoveLabel(
    storage::Label label, const RecordAccessor<Vertex> &accessor,
    const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    auto value = vertex.properties_.at(constraint.property);
    if (constraint.label == label && !value.IsNull()) {
      for (auto &p : constraint.version_pairs) {
        if (p.value == value) {
          p.record.Remove(accessor.gid(), t);
          break;
        }
      }
    }
  }
}

void UniqueLabelPropertyConstraint::UpdateOnAddProperty(
    storage::Property property, const PropertyValue &value,
    const RecordAccessor<Vertex> &accessor, const tx::Transaction &t) {
  auto &vertex = accessor.current();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    if (constraint.property == property &&
        utils::Contains(vertex.labels_, constraint.label)) {
      bool found = false;
      for (auto &p : constraint.version_pairs) {
        if (p.value == value) {
          p.record.Insert(accessor.gid(), t);
          found = true;
          break;
        }
      }

      if (!found) {
        constraint.version_pairs.emplace_back(accessor.gid(), value, t);
      }
    }
  }
}

void UniqueLabelPropertyConstraint::UpdateOnRemoveProperty(
    storage::Property property, const RecordAccessor<Vertex> &accessor,
    const tx::Transaction &t) {
  auto &vertex = accessor.current();
  auto gid = accessor.gid();
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    if (constraint.property == property &&
        utils::Contains(vertex.labels_, constraint.label)) {
      for (auto &p : constraint.version_pairs) {
        if (p.record.curr_gid == gid && p.record.tx_id_exp == 0) {
          p.record.Remove(gid, t);
          break;
        }
      }
    }
  }
}

void UniqueLabelPropertyConstraint::Refresh(const tx::Snapshot &snapshot,
                                            const tx::Engine &engine) {
  std::lock_guard<std::mutex> guard(lock_);
  for (auto &constraint : constraints_) {
    for (auto p = constraint.version_pairs.begin();
         p != constraint.version_pairs.end(); ++p) {
      auto exp_id = p->record.tx_id_exp;
      auto cre_id = p->record.tx_id_cre;
      if ((exp_id != 0 && exp_id < snapshot.back() &&
           engine.Info(exp_id).is_committed() && !snapshot.contains(exp_id)) ||
          (cre_id < snapshot.back() && engine.Info(cre_id).is_aborted())) {
        constraint.version_pairs.erase(p);
      }
    }
  }
}
}  // namespace storage::constraints
