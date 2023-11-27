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

#include <atomic>
#include <optional>
#include <thread>

#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class ExistenceConstraints {
  using ParallelizedConstraintCreationInfo =
      std::pair<std::vector<std::pair<Gid, uint64_t>> /*vertex_recovery_info*/, uint64_t /*thread_count*/>;

 public:
  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVertexOnConstraint(const Vertex &vertex,
                                                                                     LabelId label,
                                                                                     PropertyId property) {
    if (!vertex.deleted && utils::Contains(vertex.labels, label) && !vertex.properties.HasProperty(property)) {
      return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set<PropertyId>{property}};
    }
    return std::nullopt;
  }

  template <typename TFunc>
  static std::optional<ConstraintViolation> ValidateVerticesOnConstraintOnSingleThread(
      utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property, TFunc func) {
    for (Vertex &vertex : vertices) {
      if (auto violation = func(vertex, label, property); violation.has_value()) {
        return violation;
      }
    }
    return std::nullopt;
  }

  template <typename TFunc>
  static inline std::optional<ConstraintViolation> ValidateVerticesOnConstraintOnMultipleThreads(
      utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property,
      const ParallelizedConstraintCreationInfo &parallel_exec_info, const TFunc &func) {
    utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

    const auto &vertex_batches = parallel_exec_info.first;
    const auto thread_count = std::min(parallel_exec_info.second, vertex_batches.size());

    MG_ASSERT(!vertex_batches.empty(),
              "The size of batches should always be greater than zero if you want to use the parallel version of index "
              "creation!");

    std::atomic<uint64_t> batch_counter = 0;
    // using ReturnValue = std::optional<ConstraintViolation>;
    memgraph::utils::Synchronized<std::optional<ConstraintViolation>, utils::SpinLock> maybe_error{};
    {
      std::vector<std::jthread> threads;
      threads.reserve(thread_count);

      for (auto i{0U}; i < thread_count; ++i) {
        threads.emplace_back([&label, &property, &func, &vertex_batches, &maybe_error, &batch_counter, &vertices]() {
          while (!maybe_error.Lock()->has_value()) {
            const auto batch_index = batch_counter.fetch_add(1, std::memory_order_acquire);
            if (batch_index >= vertex_batches.size()) {
              return;
            }
            const auto &[gid_start, batch_size] = vertex_batches[batch_index];

            auto it = vertices.find(gid_start);

            for (auto i{0U}; i < batch_size; ++i, ++it) {
              if (const auto violation = func(*it, label, property); violation.has_value()) {
                *maybe_error.Lock() = std::move(*violation);
                break;
              }
            }
          }
        });
      }
    }
    if (maybe_error.Lock()->has_value()) {
      return maybe_error->value();
    }
    return std::nullopt;
  }

  [[nodiscard]] static std::optional<ConstraintViolation> ValidateVerticesOnConstraint(
      utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property,
      const std::optional<ParallelizedConstraintCreationInfo> &parallel_exec_info) {
    auto constraint_violation_check = [](const auto &vertex, const LabelId &label,
                                         const PropertyId &property) -> decltype(auto) {
      return ValidateVertexOnConstraint(vertex, label, property);
    };

    if (parallel_exec_info) {
      return ValidateVerticesOnConstraintOnMultipleThreads(vertices, label, property, *parallel_exec_info,
                                                           constraint_violation_check);
    }
    return ValidateVerticesOnConstraintOnSingleThread(vertices, label, property, constraint_violation_check);
    return std::nullopt;
  }

  bool ConstraintExists(LabelId label, PropertyId property) const;

  void InsertConstraint(LabelId label, PropertyId property);

  /// Returns true if the constraint was removed, and false if it doesn't exist.
  bool DropConstraint(LabelId label, PropertyId property);

  ///  Returns `std::nullopt` if all checks pass, and `ConstraintViolation` describing the violated constraint
  ///  otherwise.
  [[nodiscard]] std::optional<ConstraintViolation> Validate(const Vertex &vertex);

  std::vector<std::pair<LabelId, PropertyId>> ListConstraints() const;

  void LoadExistenceConstraints(const std::vector<std::string> &keys);

 private:
  std::vector<std::pair<LabelId, PropertyId>> constraints_;
};

}  // namespace memgraph::storage
