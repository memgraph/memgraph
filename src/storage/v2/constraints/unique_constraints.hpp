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
#include "storage/v2/id_types.hpp"
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

  /// Information collected during validation processing, organized by constraint for efficient iteration.
  /// For each constraint key, stores a list of (property_values, vertex) pairs to process.
  using ValidationInfo = std::map<LabelId, std::map<SortedPropertyIds, ConstraintValue>>;

  struct ActiveConstraints;

  /// Processor that collects validation information during delta processing.
  /// Organized by constraint key so bulk operations can iterate constraints (not vertices) in outer loop.
  /// Used for both commit (bulk insert) and abort (bulk remove) operations.
  struct ValidationProcessor {
    explicit ValidationProcessor() = default;

    explicit ValidationProcessor(ValidationInfo &&info) : validation_info_(std::move(info)) {}

    void Collect(Vertex const *vertex);

    ValidationInfo validation_info_;
  };

  struct ActiveConstraints {
    virtual ~ActiveConstraints() = default;
    virtual auto ListConstraints(uint64_t start_timestamp) const
        -> std::vector<std::pair<LabelId, SortedPropertyIds>> = 0;

    /// Get a ValidationProcessor initialized with the current constraint structure.
    /// The processor can then be used to collect entries via Collect() and
    /// bulk process them via CommitEntries() or AbortEntries().
    virtual auto GetValidationProcessor() const -> ValidationProcessor = 0;

    /// Validate and commit entries in a single pass.
    /// For in-memory: validates then inserts each entry with single accessor per constraint.
    /// For disk: no-op (disk handles validation differently), returns success.
    /// @param info The collected validation info with property values
    /// @param start_timestamp Transaction start timestamp for entry insertion
    /// @param tx Transaction for MVCC visibility checks (can be nullptr for disk)
    /// @param commit_timestamp Commit timestamp for MVCC visibility checks
    /// @return Success or constraint violation error
    virtual auto ValidateAndCommitEntries(ValidationInfo &&info, uint64_t start_timestamp, const Transaction *tx,
                                          uint64_t commit_timestamp) -> std::expected<void, ConstraintViolation> = 0;

    /// Bulk remove all collected entries from their respective constraint skip lists.
    /// Uses one accessor per constraint for efficiency.
    virtual void AbortEntries(ValidationInfo &&info, uint64_t exact_start_timestamp) = 0;

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

  virtual DeletionStatus DropConstraint(LabelId label, SortedPropertyIds const &properties) = 0;

  virtual void Clear() = 0;

 protected:
  static DeletionStatus CheckPropertiesBeforeDeletion(SortedPropertyIds const &properties) {
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
