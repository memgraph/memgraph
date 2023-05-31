// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/disk/unique_constraints.hpp"
#include "storage/v2/constraints/unique_constraints.hpp"

namespace memgraph::storage {

DiskUniqueConstraints::DiskUniqueConstraints() {}

void DiskUniqueConstraints::InsertConstraint(LabelId label, const std::set<PropertyId> &properties) {
  constraints_.insert(std::make_pair(label, properties));
}

bool DiskUniqueConstraints::CheckIfConstraintCanBeCreated(LabelId label, const std::set<PropertyId> &properties) const {
  return !(properties.empty() || properties.size() > kUniqueConstraintsMaxProperties ||
           constraints_.find(std::make_pair(label, properties)) != constraints_.end());
};

DiskUniqueConstraints::DeletionStatus DiskUniqueConstraints::DropConstraint(LabelId label,
                                                                            const std::set<PropertyId> &properties) {
  if (auto drop_properties_check_result = UniqueConstraints::CheckPropertiesBeforeDeletion(properties);
      drop_properties_check_result != UniqueConstraints::DeletionStatus::SUCCESS) {
    return drop_properties_check_result;
  }
  if (constraints_.erase({label, properties}) > 0) {
    return UniqueConstraints::DeletionStatus::SUCCESS;
  }
  return UniqueConstraints::DeletionStatus::NOT_FOUND;
}

bool DiskUniqueConstraints::ConstraintExists(LabelId label, const std::set<PropertyId> &properties) const {
  return constraints_.find({label, properties}) != constraints_.end();
}

std::optional<ConstraintViolation> DiskUniqueConstraints::Validate(const Vertex &vertex, const Transaction &tx,
                                                                   uint64_t commit_timestamp) const {
  return std::nullopt;
}

std::vector<std::pair<LabelId, std::set<PropertyId>>> DiskUniqueConstraints::ListConstraints() const {
  return std::vector<std::pair<LabelId, std::set<PropertyId>>>(constraints_.begin(), constraints_.end());
}

/// TODO: andi. Clear RocksDB instance.
void DiskUniqueConstraints::Clear() { constraints_.clear(); }

}  // namespace memgraph::storage
