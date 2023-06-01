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

#pragma once

#include "storage/v2/config.hpp"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/disk/rocksdb_storage.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

class DiskUniqueConstraints : public UniqueConstraints {
 public:
  explicit DiskUniqueConstraints(const Config &config);

  CreationStatus CheckIfConstraintCanBeCreated(LabelId label, const std::set<PropertyId> &properties) const;

  void InsertConstraint(LabelId label, const std::set<PropertyId> &properties,
                        const std::vector<std::pair<std::string, std::string>> &vertices_under_constraint);

  std::optional<ConstraintViolation> Validate(const Vertex &vertex,
                                              std::vector<std::vector<PropertyValue>> &unique_storage) const;

  DeletionStatus DropConstraint(LabelId label, const std::set<PropertyId> &properties) override;

  bool ConstraintExists(LabelId label, const std::set<PropertyId> &properties) const override;

  std::vector<std::pair<LabelId, std::set<PropertyId>>> ListConstraints() const override;

  void Clear() override;

 private:
  std::set<std::pair<LabelId, std::set<PropertyId>>> constraints_;
  std::unique_ptr<RocksDBStorage> kvstore_;

  bool DifferentVertexExistsWithPropertyValues(std::vector<PropertyValue> property_values,
                                               const std::vector<std::vector<PropertyValue>> &unique_storage,
                                               const Gid &gid) const;
};

}  // namespace memgraph::storage
