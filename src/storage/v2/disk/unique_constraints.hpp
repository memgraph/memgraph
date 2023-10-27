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
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/rocksdb_serialization.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class DiskUniqueConstraints : public UniqueConstraints {
 public:
  explicit DiskUniqueConstraints(const Config &config);

  CreationStatus CheckIfConstraintCanBeCreated(LabelId label, const std::set<PropertyId> &properties) const;

  [[nodiscard]] bool InsertConstraint(
      LabelId label, const std::set<PropertyId> &properties,
      const std::vector<std::pair<std::string, std::string>> &vertices_under_constraint);

  std::optional<ConstraintViolation> Validate(const Vertex &vertex,
                                              std::vector<std::vector<PropertyValue>> &unique_storage) const;

  [[nodiscard]] bool ClearDeletedVertex(std::string_view gid, uint64_t transaction_commit_timestamp) const;

  [[nodiscard]] bool DeleteVerticesWithRemovedConstraintLabel(uint64_t transaction_start_timestamp,
                                                              uint64_t transaction_commit_timestamp);

  [[nodiscard]] bool SyncVertexToUniqueConstraintsStorage(const Vertex &vertex, uint64_t commit_timestamp) const;

  DeletionStatus DropConstraint(LabelId label, const std::set<PropertyId> &properties) override;

  [[nodiscard]] bool ConstraintExists(LabelId label, const std::set<PropertyId> &properties) const override;

  void UpdateOnRemoveLabel(LabelId removed_label, const Vertex &vertex_before_update,
                           uint64_t transaction_start_timestamp) override;

  void UpdateOnAddLabel(LabelId added_label, const Vertex &vertex_before_update,
                        uint64_t transaction_start_timestamp) override;

  std::vector<std::pair<LabelId, std::set<PropertyId>>> ListConstraints() const override;

  uint64_t ApproximateVertexCount(const LabelId &label, const PropertyId &property) const override;
  uint64_t ApproximateVertexCount(const LabelId &label, const PropertyId &property,
                                  const PropertyValue &value) const override;
  uint64_t ApproximateVertexCount(const LabelId &label, const PropertyId &property,
                                  const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override;
  void Clear() override;

  RocksDBStorage *GetRocksDBStorage() const;

  void LoadUniqueConstraints(const std::vector<std::string> &keys);

 private:
  utils::Synchronized<std::map<uint64_t, std::map<Gid, std::set<std::pair<LabelId, std::set<PropertyId>>>>>>
      entries_for_deletion;
  std::set<std::pair<LabelId, std::set<PropertyId>>> constraints_;
  std::unique_ptr<RocksDBStorage> kvstore_;

  [[nodiscard]] std::optional<ConstraintViolation> TestIfVertexSatisifiesUniqueConstraint(
      const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage, const LabelId &constraint_label,
      const std::set<PropertyId> &constraint_properties) const;

  bool VertexIsUnique(const std::vector<PropertyValue> &property_values,
                      const std::vector<std::vector<PropertyValue>> &unique_storage, const LabelId &constraint_label,
                      const std::set<PropertyId> &constraint_properties, Gid gid) const;
};

}  // namespace memgraph::storage
