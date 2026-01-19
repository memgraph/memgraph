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

#include <memory>
#include <set>

#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

struct Transaction;

// NOLINTNEXTLINE(misc-definitions-in-headers)
const size_t kUniqueConstraintsMaxProperties = 32;

class UniqueConstraints {
 public:
  UniqueConstraints() = default;
  UniqueConstraints(const UniqueConstraints &) = delete;
  UniqueConstraints(UniqueConstraints &&) = delete;
  UniqueConstraints &operator=(const UniqueConstraints &) = delete;
  UniqueConstraints &operator=(UniqueConstraints &&) = delete;
  virtual ~UniqueConstraints() = default;

  using ConstraintValue = std::vector<std::pair<std::vector<PropertyValue>, Vertex const *>>;

  /// Information collected during abort processing, organized by constraint for efficient iteration.
  /// For each constraint key, stores a list of (property_values, vertex) pairs to remove.
  using AbortableInfo = std::map<LabelId, std::map<std::set<PropertyId>, ConstraintValue>>;

  struct ActiveConstraints;

  /// Processor that collects abort information during delta processing.
  /// Organized by constraint key so AbortEntries can iterate constraints (not vertices) in outer loop.
  struct AbortProcessor {
    explicit AbortProcessor() = default;
    explicit AbortProcessor(AbortableInfo &&interesting) : abortable_info_(std::move(interesting)) {}

    void Collect(Vertex const *vertex);

    AbortableInfo abortable_info_;
  };

  struct ActiveConstraints {
    virtual ~ActiveConstraints() = default;
    virtual auto ListConstraints(uint64_t start_timestamp) const
        -> std::vector<std::pair<LabelId, std::set<PropertyId>>> = 0;
    virtual void UpdateBeforeCommit(const Vertex *vertex, const Transaction &tx) = 0;
    virtual auto GetAbortProcessor() const -> AbortProcessor = 0;
    virtual void CollectForAbort(AbortProcessor &processor, Vertex const *vertex) const = 0;
    virtual void AbortEntries(AbortableInfo &&info, uint64_t exact_start_timestamp) = 0;
    virtual bool empty() const = 0;

    virtual void UpdateOnRemoveLabel(LabelId removed_label, const Vertex &vertex_before_update,
                                     uint64_t transaction_start_timestamp) = 0;

    virtual void UpdateOnAddLabel(LabelId added_label, const Vertex &vertex_before_update,
                                  uint64_t transaction_start_timestamp) = 0;
  };

  virtual auto GetActiveConstraints() const -> std::unique_ptr<UniqueConstraints::ActiveConstraints> = 0;

  enum class CreationStatus {
    SUCCESS,
    ALREADY_EXISTS,
    EMPTY_PROPERTIES,
    PROPERTIES_SIZE_LIMIT_EXCEEDED,
  };

  enum class DeletionStatus {
    SUCCESS,
    NOT_FOUND,
    EMPTY_PROPERTIES,
    PROPERTIES_SIZE_LIMIT_EXCEEDED,
  };

  virtual DeletionStatus DropConstraint(LabelId label, const std::set<PropertyId> &properties) = 0;

  virtual void Clear() = 0;

 protected:
  static DeletionStatus CheckPropertiesBeforeDeletion(const std::set<PropertyId> &properties) {
    if (properties.empty()) {
      return UniqueConstraints::DeletionStatus::EMPTY_PROPERTIES;
    }
    if (properties.size() > kUniqueConstraintsMaxProperties) {
      return UniqueConstraints::DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED;
    }
    return UniqueConstraints::DeletionStatus::SUCCESS;
  }
};

}  // namespace memgraph::storage
