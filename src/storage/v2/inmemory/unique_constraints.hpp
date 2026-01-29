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
#include <optional>
#include <variant>
#include "storage/v2/constraints/active_constraints.hpp"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/constraints_mvcc.hpp"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "utils/rw_lock.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

struct Transaction;

class InMemoryUniqueConstraints : public UniqueConstraints {
 public:
  struct Entry {
    std::vector<PropertyValue> values;
    const Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) const;
    bool operator==(const Entry &rhs) const;

    bool operator<(const std::vector<PropertyValue> &rhs) const;
    bool operator==(const std::vector<PropertyValue> &rhs) const;
  };

  struct MultipleThreadsConstraintValidation {
    auto operator()(const utils::SkipList<Vertex>::Accessor &vertex_accessor,
                    utils::SkipList<Entry>::Accessor &constraint_accessor, const LabelId &label,
                    const std::set<PropertyId> &properties,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) const
        -> std::expected<void, ConstraintViolation>;

    const durability::ParallelizedSchemaCreationInfo &parallel_exec_info;
  };

  struct SingleThreadConstraintValidation {
    auto operator()(const utils::SkipList<Vertex>::Accessor &vertex_accessor,
                    utils::SkipList<Entry>::Accessor &constraint_accessor, const LabelId &label,
                    const std::set<PropertyId> &properties,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) const
        -> std::expected<void, ConstraintViolation>;
  };

  // constraints are created and dropped with read only access
  // a status is needed to not drop the constraint before it gets validated
  // new writes can't happen during this time due to read only access
  struct IndividualConstraint {
    ~IndividualConstraint();
    void Publish(uint64_t commit_timestamp);

    utils::SkipList<Entry> skiplist;
    ConstraintStatus status{};  // MVCC status tracking
  };

  using IndividualConstraintPtr = std::shared_ptr<IndividualConstraint>;

  using Container = std::map<LabelId, std::map<std::set<PropertyId>, IndividualConstraintPtr>>;

  using ContainerPtr = std::shared_ptr<Container const>;

  /// ActiveConstraints implementation for unique constraints.
  /// Provides snapshot-based access for a transaction's lifetime.
  class ActiveConstraints final : public UniqueConstraints::ActiveConstraints {
   public:
    explicit ActiveConstraints(ContainerPtr snapshot = std::make_shared<Container>())
        : container_{std::move(snapshot)} {}

    auto ListConstraints(uint64_t start_timestamp) const
        -> std::vector<std::pair<LabelId, std::set<PropertyId>>> override;
    void UpdateBeforeCommit(const Vertex *vertex, const Transaction &tx) override;
    auto GetAbortProcessor() const -> AbortProcessor override;
    void CollectForAbort(AbortProcessor &processor, Vertex const *vertex) const override;
    void AbortEntries(AbortableInfo &&info, uint64_t exact_start_timestamp) override;
    bool empty() const override;

    // Unique constraints are validated at commit time via UpdateBeforeCommit(),
    // so label changes don't require incremental updates during the transaction.
    void UpdateOnRemoveLabel(LabelId /*removed_label*/, const Vertex & /*vertex_before_update*/,
                             const uint64_t /*transaction_start_timestamp*/) override {}

    void UpdateOnAddLabel(LabelId /*added_label*/, const Vertex & /*vertex_before_update*/,
                          uint64_t /*transaction_start_timestamp*/) override {}

   private:
    ContainerPtr container_;
  };

  /// Creates an ActiveConstraints snapshot for transaction use.
  auto GetActiveConstraints() const -> std::unique_ptr<UniqueConstraints::ActiveConstraints> override;

  /// Creates unique constraint on the given `label` and a list of `properties`.
  /// Returns constraint violation if there are multiple vertices with the same
  /// label and property values. Returns `CreationStatus::ALREADY_EXISTS` if
  /// constraint already existed, `CreationStatus::EMPTY_PROPERTIES` if the
  /// given list of properties is empty,
  /// `CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED` if the list of properties
  /// exceeds the maximum allowed number of properties, and
  /// `CreationStatus::SUCCESS` on success.
  /// @throw std::bad_alloc
  auto CreateConstraint(LabelId label, const std::set<PropertyId> &properties,
                        const utils::SkipList<Vertex>::Accessor &vertex_accessor,
                        const std::optional<durability::ParallelizedSchemaCreationInfo> &par_exec_info,
                        std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt)
      -> std::expected<CreationStatus, ConstraintViolation>;

  /// Publishes a constraint after validation, making it visible at the given commit timestamp.
  bool PublishConstraint(LabelId label, const std::set<PropertyId> &properties, uint64_t commit_timestamp);

  auto DropConstraint(LabelId label, const std::set<PropertyId> &properties) -> DeletionStatus override;

  /// Validates the given vertex against unique constraints before committing.
  /// This method should be called while commit lock is active with
  /// `commit_timestamp` being a potential commit timestamp of the transaction.
  /// @throw std::bad_alloc
  auto Validate(const std::unordered_set<Vertex const *> &vertices, const Transaction &tx,
                uint64_t commit_timestamp) const -> std::expected<void, ConstraintViolation>;

  /// GC method that removes outdated entries from constraints' storages.
  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, const std::stop_token &token);

  void Clear() override;

  void DropGraphClearConstraints();

  static auto GetCreationFunction(const std::optional<durability::ParallelizedSchemaCreationInfo> &)
      -> std::variant<MultipleThreadsConstraintValidation, SingleThreadConstraintValidation>;

  void RunGC();

 private:
  auto GetIndividualConstraint(const LabelId label, const std::set<PropertyId> &properties) const
      -> IndividualConstraintPtr;
  utils::Synchronized<ContainerPtr, utils::WritePrioritizedRWLock> container_{std::make_shared<Container const>()};
};

}  // namespace memgraph::storage
