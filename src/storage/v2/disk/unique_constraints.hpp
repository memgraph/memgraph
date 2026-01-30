// Copyright 2026 Memgraph Ltd.
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
#include "storage/v2/constraints/active_constraints.hpp"
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
  using DeletionEntries = std::map<uint64_t, std::map<Gid, std::set<std::pair<LabelId, SortedPropertyIds>>>>;

  /// ActiveConstraints implementation for disk unique constraints.
  /// Disk storage doesn't have MVCC for constraints, so this is a simple wrapper.
  class ActiveConstraints final : public UniqueConstraints::ActiveConstraints {
   public:
    explicit ActiveConstraints(const DiskUniqueConstraints *constraints) : constraints_{constraints} {}

    auto ListConstraints(uint64_t start_timestamp) const -> std::vector<std::pair<LabelId, SortedPropertyIds>> override;

    auto GetValidationProcessor() const -> ValidationProcessor override;
    auto ValidateAndCommitEntries(ValidationInfo &&info, uint64_t start_timestamp, const Transaction *tx,
                                  uint64_t commit_timestamp) -> std::expected<void, ConstraintViolation> override;
    void AbortEntries(ValidationInfo &&info, uint64_t exact_start_timestamp) override;
    bool empty() const override;

    void UpdateOnRemoveLabel(LabelId removed_label, const Vertex &vertex_before_update,
                             uint64_t transaction_start_timestamp) override;

    void UpdateOnAddLabel(LabelId added_label, const Vertex &vertex_before_update,
                          uint64_t transaction_start_timestamp) override;

    const DiskUniqueConstraints *constraints_;
    DeletionEntries entries_for_deletion;
  };

  auto GetActiveConstraints() const -> std::unique_ptr<UniqueConstraints::ActiveConstraints> override;

  explicit DiskUniqueConstraints(const Config &config);

  CreationStatus CheckIfConstraintCanBeCreated(LabelId label, SortedPropertyIds const &properties) const;

  [[nodiscard]] bool InsertConstraint(
      LabelId label, SortedPropertyIds const &properties,
      const std::vector<std::pair<std::string, std::string>> &vertices_under_constraint);

  std::expected<void, ConstraintViolation> Validate(const Vertex &vertex,
                                                    std::vector<std::vector<PropertyValue>> &unique_storage) const;

  [[nodiscard]] bool ClearDeletedVertex(std::string_view gid, uint64_t transaction_commit_timestamp) const;

  [[nodiscard]] bool DeleteVerticesWithRemovedConstraintLabel(DeletionEntries &deletion_entries,
                                                              uint64_t transaction_start_timestamp,
                                                              uint64_t transaction_commit_timestamp);

  [[nodiscard]] bool SyncVertexToUniqueConstraintsStorage(const Vertex &vertex, uint64_t commit_timestamp) const;

  DeletionStatus DropConstraint(LabelId label, SortedPropertyIds const &properties) override;

  void Clear() override;

  RocksDBStorage *GetRocksDBStorage() const;

  void LoadUniqueConstraints(const std::vector<std::string> &keys);

 private:
  std::set<std::pair<LabelId, SortedPropertyIds>> constraints_;
  std::unique_ptr<RocksDBStorage> kvstore_;

  [[nodiscard]] std::expected<void, ConstraintViolation> TestIfVertexSatisifiesUniqueConstraint(
      const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage, const LabelId &constraint_label,
      SortedPropertyIds const &constraint_properties) const;

  bool VertexIsUnique(const std::vector<PropertyValue> &property_values,
                      const std::vector<std::vector<PropertyValue>> &unique_storage, const LabelId &constraint_label,
                      SortedPropertyIds const &constraint_properties, Gid gid) const;
};
}  // namespace memgraph::storage
