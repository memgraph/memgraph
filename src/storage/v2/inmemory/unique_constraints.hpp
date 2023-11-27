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

#include <thread>
#include "storage/v2/constraints/unique_constraints.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

/// Utility class to store data in a fixed size array. The array is used
/// instead of `std::vector` to avoid `std::bad_alloc` exception where not
/// necessary.
template <class T>
struct FixedCapacityArray {
  size_t size;
  T values[kUniqueConstraintsMaxProperties];

  explicit FixedCapacityArray(size_t array_size) : size(array_size) {
    MG_ASSERT(size <= kUniqueConstraintsMaxProperties, "Invalid array size!");
  }
};

using PropertyIdArray = FixedCapacityArray<PropertyId>;

class InMemoryUniqueConstraints : public UniqueConstraints {
 private:
  struct Entry {
    std::vector<PropertyValue> values;
    const Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) const;
    bool operator==(const Entry &rhs) const;

    bool operator<(const std::vector<PropertyValue> &rhs) const;
    bool operator==(const std::vector<PropertyValue> &rhs) const;
  };

 public:
  using ParallelizedConstraintCreationInfo =
      std::pair<std::vector<std::pair<Gid, uint64_t>> /*vertex_recovery_info*/, uint64_t /*thread_count*/>;

  /// Indexes the given vertex for relevant labels and properties.
  /// This method should be called before committing and validating vertices
  /// against unique constraints.
  /// @throw std::bad_alloc
  void UpdateBeforeCommit(const Vertex *vertex, const Transaction &tx);

  void UpdateBeforeCommit(const Vertex *vertex, std::unordered_set<LabelId> &added_labels,
                          std::unordered_set<PropertyId> &added_properties, const Transaction &tx);

  /// Creates unique constraint on the given `label` and a list of `properties`.
  /// Returns constraint violation if there are multiple vertices with the same
  /// label and property values. Returns `CreationStatus::ALREADY_EXISTS` if
  /// constraint already existed, `CreationStatus::EMPTY_PROPERTIES` if the
  /// given list of properties is empty,
  /// `CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED` if the list of properties
  /// exceeds the maximum allowed number of properties, and
  /// `CreationStatus::SUCCESS` on success.
  /// @throw std::bad_alloc
  utils::BasicResult<ConstraintViolation, CreationStatus> CreateConstraint(
      LabelId label, const std::set<PropertyId> &properties, utils::SkipList<Vertex>::Accessor vertex_accessor,
      const std::optional<ParallelizedConstraintCreationInfo> &par_exec_info);

  /// Deletes the specified constraint. Returns `DeletionStatus::NOT_FOUND` if
  /// there is not such constraint in the storage,
  /// `DeletionStatus::EMPTY_PROPERTIES` if the given set of `properties` is
  /// empty, `DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED` if the given set
  /// of `properties` exceeds the maximum allowed number of properties, and
  /// `DeletionStatus::SUCCESS` on success.
  DeletionStatus DropConstraint(LabelId label, const std::set<PropertyId> &properties) override;

  bool ConstraintExists(LabelId label, const std::set<PropertyId> &properties) const override;

  void UpdateOnRemoveLabel(LabelId removed_label, const Vertex &vertex_before_update,
                           const uint64_t transaction_start_timestamp) override {}

  void UpdateOnAddLabel(LabelId added_label, const Vertex &vertex_before_update,
                        uint64_t transaction_start_timestamp) override{};

  /// Validates the given vertex against unique constraints before committing.
  /// This method should be called while commit lock is active with
  /// `commit_timestamp` being a potential commit timestamp of the transaction.
  /// @throw std::bad_alloc
  std::optional<ConstraintViolation> Validate(const Vertex &vertex, const Transaction &tx,
                                              uint64_t commit_timestamp) const;

  std::vector<std::pair<LabelId, std::set<PropertyId>>> ListConstraints() const override;

  /// GC method that removes outdated entries from constraints' storages.
  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  void Clear() override;

  template <typename TFunc>
  static bool ValidateConstraintMultipleThreads(utils::SkipList<Vertex>::Accessor &vertex_accessor,
                                                const TFunc &validation_func,
                                                const ParallelizedConstraintCreationInfo &parallel_exec_info) {
    utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

    const auto &vertex_batches = parallel_exec_info.first;
    const auto thread_count = std::min(parallel_exec_info.second, vertex_batches.size());

    MG_ASSERT(!vertex_batches.empty(),
              "The size of batches should always be greater than zero if you want to use the parallel version of index "
              "creation!");

    std::atomic<uint64_t> batch_counter = 0;
    memgraph::utils::Synchronized<bool, utils::RWSpinLock> maybe_error{false};
    {
      std::vector<std::jthread> threads;
      threads.reserve(thread_count);

      for (auto i{0U}; i < thread_count; ++i) {
        threads.emplace_back([&validation_func, &vertex_batches, &maybe_error, &batch_counter, &vertex_accessor]() {
          while (!(*maybe_error.ReadLock())) {
            const auto batch_index = batch_counter.fetch_add(1, std::memory_order_acquire);
            if (batch_index >= vertex_batches.size()) {
              return;
            }
            const auto &[gid_start, batch_size] = vertex_batches[batch_index];

            auto it = vertex_accessor.find(gid_start);

            for (auto i{0U}; i < batch_size; ++i, ++it) {
              if (validation_func(*it)) [[unlikely]] {
                *maybe_error.Lock() = true;
                break;
              }
            }
          }
        });
      }
    }
    return *maybe_error.Lock();
  }

  template <typename TFunc>
  static bool ValidateConstraintSingleThread(utils::SkipList<Vertex>::Accessor &vertex_accessor,
                                             const TFunc &validation_func) {
    for (const Vertex &vertex : vertex_accessor) {
      if (validation_func(vertex)) {
        return true;
        break;
      }
    }
    return false;
  }

 private:
  std::map<std::pair<LabelId, std::set<PropertyId>>, utils::SkipList<Entry>> constraints_;
  std::map<LabelId, std::map<std::set<PropertyId>, utils::SkipList<Entry> *>> constraints_by_label_;
};

}  // namespace memgraph::storage
