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
#include <vector>
#include "memory/db_arena_fwd.hpp"
#include "metrics/metric_handles.hpp"
#include "metrics/scoped_gauge.hpp"
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/constraints/active_constraints.hpp"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/constraints_mvcc.hpp"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/index_arming.hpp"
#include "utils/rw_lock.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

struct Transaction;
class Storage;

class InMemoryUniqueConstraints : public UniqueConstraints {
 public:
  explicit InMemoryUniqueConstraints(metrics::GaugeHandle gauge = {}) : gauge_{gauge} {}

  struct Entry {
    std::vector<PropertyValue> values;
    const Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) const;
    bool operator==(const Entry &rhs) const;

    bool operator<(const std::vector<PropertyValue> &rhs) const;
    bool operator==(const std::vector<PropertyValue> &rhs) const;
  };

  /// Both validators call `cancel_check` once per vertex and throw PopulateCancel when it returns true. The parallel
  /// one reports it through a flag and re-throws after joining, so an escaping exception can never terminate the
  /// process.
  struct MultipleThreadsConstraintValidation {
    auto operator()(const utils::SkipListDb<Vertex>::Accessor &vertex_accessor,
                    utils::SkipListDb<Entry>::Accessor &constraint_accessor, const LabelId &label,
                    const std::set<PropertyId> &properties, ProgressCallback const &on_progress = {},
                    CheckCancelFunction const &cancel_check = neverCancel) const
        -> std::expected<void, ConstraintViolation>;

    const durability::ParallelizedSchemaCreationInfo &parallel_exec_info;
  };

  struct SingleThreadConstraintValidation {
    auto operator()(const utils::SkipListDb<Vertex>::Accessor &vertex_accessor,
                    utils::SkipListDb<Entry>::Accessor &constraint_accessor, const LabelId &label,
                    const std::set<PropertyId> &properties, ProgressCallback const &on_progress = {},
                    CheckCancelFunction const &cancel_check = neverCancel) const
        -> std::expected<void, ConstraintViolation>;
  };

  // constraints are created and dropped with read only access
  // a status is needed to not drop the constraint before it gets validated
  // new writes can't happen during this time due to read only access
  struct IndividualConstraint {
    explicit IndividualConstraint() : skiplist{} {}

    ~IndividualConstraint();
    void Publish(uint64_t commit_timestamp, metrics::GaugeHandle gauge);

    utils::SkipListDb<Entry> skiplist;
    ConstraintStatus status{};  // MVCC status tracking
    metrics::ScopedGauge gauge_{};
  };

  using IndividualConstraintPtr = std::shared_ptr<IndividualConstraint>;

  using PropertiesConstraints =
      std::map<std::set<PropertyId>, IndividualConstraintPtr, std::less<std::set<PropertyId>>,
               memory::DbAwareAllocator<std::pair<const std::set<PropertyId>, IndividualConstraintPtr>>>;

  using Container = std::map<LabelId, PropertiesConstraints, std::less<LabelId>,
                             memory::DbAwareAllocator<std::pair<const LabelId, PropertiesConstraints>>>;

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
  auto GetActiveConstraints() const -> std::shared_ptr<UniqueConstraints::ActiveConstraints> override;

  /// Creates unique constraint on the given `label` and a list of `properties`.
  /// Returns constraint violation if there are multiple vertices with the same
  /// label and property values. Returns `CreationStatus::ALREADY_EXISTS` if
  /// constraint already existed, `CreationStatus::EMPTY_PROPERTIES` if the
  /// given list of properties is empty,
  /// `CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED` if the list of properties
  /// exceeds the maximum allowed number of properties, and
  /// `CreationStatus::SUCCESS` on success.
  /// @throw std::bad_alloc
  /// @throw PopulateCancel if `cancel_check` asks to stop; the caller is responsible for deregistering the constraint.
  auto CreateConstraint(LabelId label, const std::set<PropertyId> &properties,
                        const utils::SkipListDb<Vertex>::Accessor &vertex_accessor,
                        const std::optional<durability::ParallelizedSchemaCreationInfo> &par_exec_info,
                        ProgressCallback const &on_progress = {}, CheckCancelFunction const &cancel_check = neverCancel)
      -> std::expected<CreationStatus, ConstraintViolation>;

  /// Publishes a constraint after validation, making it visible at the given commit timestamp.
  bool PublishConstraint(LabelId label, const std::set<PropertyId> &properties, uint64_t commit_timestamp);

  /// Drops a constraint. Returns the evicted IndividualConstraint so the caller
  /// can reinstall it via RestoreConstraint on abort, alongside the deletion
  /// status. {SUCCESS, ptr} on success; {NOT_FOUND/EMPTY_PROPERTIES/..., nullptr}
  /// otherwise.
  struct DropResult {
    DeletionStatus status;
    IndividualConstraintPtr evicted;
  };

  [[nodiscard]] auto DropConstraint(LabelId label, const std::set<PropertyId> &properties) -> DropResult;

  /// Reinstalls a previously-evicted IndividualConstraint. No-op if the slot
  /// has been reclaimed by a concurrent CREATE (constraint DDL runs under
  /// READ_ONLY/UNIQUE, which does not serialize peers).
  void RestoreConstraint(LabelId label, const std::set<PropertyId> &properties, IndividualConstraintPtr evicted);

  /// Hands an evicted constraint over for reclamation once the DROP is known to have committed. Its skiplist holds one
  /// entry per constrained vertex, so freeing it is O(constrained vertices) -- minutes on a large tenant. Without this
  /// the last reference dies with the committing transaction's callbacks, running that teardown inline on whichever
  /// thread committed, which for a replica is the RPC handler its peer is waiting on. GC reaps it instead, once no
  /// reader snapshot references it any more.
  void RetireConstraint(IndividualConstraintPtr evicted);

  /// Validates the given vertex against unique constraints before committing.
  /// This method should be called while commit lock is active with
  /// `commit_timestamp` being a potential commit timestamp of the transaction.
  /// @throw std::bad_alloc
  auto Validate(const std::unordered_set<Vertex const *> &vertices, const Transaction &tx,
                uint64_t commit_timestamp) const -> std::expected<void, ConstraintViolation>;

  /// GC method that removes outdated entries from constraints' storages. Sweeps only the
  /// constraints whose label or one of whose properties `arming` names, and answers with how
  /// many that was.
  uint64_t RemoveObsoleteEntries(Storage *storage, uint64_t oldest_active_start_timestamp, const std::stop_token &token,
                                 IndexArming const &arming);

  void Clear() override;

  void DropGraphClearConstraints();

  static auto GetCreationFunction(const std::optional<durability::ParallelizedSchemaCreationInfo> &)
      -> std::variant<MultipleThreadsConstraintValidation, SingleThreadConstraintValidation>;

  void RunGC();

 private:
  auto GetIndividualConstraint(const LabelId label, const std::set<PropertyId> &properties) const
      -> IndividualConstraintPtr;

  // Installs ptr if the slot is absent; returns the installed ptr or nullptr.
  // Shared by CreateConstraint (validates via the returned skiplist) and RestoreConstraint.
  auto InstallConstraint_(LabelId label, const std::set<PropertyId> &properties, IndividualConstraintPtr ptr)
      -> IndividualConstraintPtr;

  // Reaps anything in retired_ that only this list still references. Called from GC, so the skiplist teardown lands
  // there rather than on a committing thread.
  void ReclaimRetiredConstraints();

  // Drops every reference retired_ holds, for teardown paths where the whole container is going away. Unconditional
  // rather than refcount-gated: anything a reader snapshot still points at stays alive on its own reference and dies
  // with that reader. Without this the list keeps entries until the object is destroyed, and with periodic GC off
  // nothing else would ever release them.
  void ReleaseRetiredConstraints();

  metrics::GaugeHandle gauge_{};
  utils::Synchronized<ContainerPtr, utils::WritePrioritizedRWLock> container_{std::make_shared<Container const>()};
  // Dropped constraints awaiting reclamation. A reader that took an ActiveConstraints snapshot before the DROP can
  // still be iterating one of these, so the refcount -- not this list -- decides when the memory actually goes.
  utils::Synchronized<std::vector<IndividualConstraintPtr>, utils::SpinLock> retired_{};
};

}  // namespace memgraph::storage
