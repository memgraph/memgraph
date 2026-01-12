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

#include "storage/v2/constraints/existence_constraints.hpp"
#include <expected>
#include "storage/v2/constraints/utils.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::storage {

bool ExistenceConstraints::ConstraintExists(LabelId label, PropertyId property) const {
  return constraints_.WithReadLock([&](auto &constraints) { return constraints.contains({label, property}); });
}

bool ExistenceConstraints::RegisterConstraint(LabelId label, PropertyId property) {
  return constraints_.WithLock([&](auto &constraints) {
    auto [it, inserted] =
        constraints.emplace(IndividualConstraint{.label = label, .property = property}, ValidationStatus::VALIDATING);
    return inserted;
  });
}

void ExistenceConstraints::PublishConstraint(LabelId label, PropertyId property) {
  constraints_.WithLock([&](auto &constraints) {
    auto [it, inserted] =
        constraints.try_emplace(IndividualConstraint{.label = label, .property = property}, ValidationStatus::READY);
    if (!inserted) {
      it->second = ValidationStatus::READY;
    }
  });
}

bool ExistenceConstraints::DropConstraint(LabelId label, PropertyId property) {
  return constraints_.WithLock([&](auto &constraints) {
    auto it = constraints.find({label, property});
    if (it == constraints.end()) [[unlikely]] {
      return false;
    }
    constraints.erase(it);
    return true;
  });
}

std::vector<std::pair<LabelId, PropertyId>> ExistenceConstraints::ListConstraints() const {
  return constraints_.WithReadLock([](auto &constraints) {
    auto result = constraints | std::views::filter([](const auto &c) { return c.second == ValidationStatus::READY; }) |
                  std::views::transform([](const auto &c) {
                    return std::pair{c.first.label, c.first.property};
                  }) |
                  std::ranges::to<std::vector<std::pair<LabelId, PropertyId>>>();
    std::ranges::sort(result);
    return result;
  });
}

[[nodiscard]] std::expected<void, ConstraintViolation> ExistenceConstraints::Validate(const Vertex &vertex) {
  return constraints_.WithReadLock([&](auto &constraints) -> std::expected<void, ConstraintViolation> {
    for (const auto &[constraint, status] : constraints) {
      if (auto violation = ValidateVertexOnConstraint(vertex, constraint.label, constraint.property);
          violation.has_value()) {
        return std::unexpected{violation.error()};
      }
    }
    return {};
  });
}

// only used for on disk
void ExistenceConstraints::LoadExistenceConstraints(const std::vector<std::string> &keys) {
  constraints_.WithLock([&](auto &constraints) {
    for (const auto &key : keys) {
      const std::vector<std::string> parts = utils::Split(key, ",");
      constraints.emplace(
          IndividualConstraint{.label = LabelId::FromString(parts[0]), .property = PropertyId::FromString(parts[1])},
          ValidationStatus::READY);
    }
  });
}

[[nodiscard]] std::expected<void, ConstraintViolation> ExistenceConstraints::ValidateVertexOnConstraint(
    const Vertex &vertex, const LabelId &label, const PropertyId &property) {
  if (!vertex.deleted && std::ranges::contains(vertex.labels, label) && !vertex.properties.HasProperty(property)) {
    return std::unexpected{
        ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set<PropertyId>{property}}};
  }
  return {};
}

std::variant<ExistenceConstraints::MultipleThreadsConstraintValidation,
             ExistenceConstraints::SingleThreadConstraintValidation>
ExistenceConstraints::GetCreationFunction(
    const std::optional<durability::ParallelizedSchemaCreationInfo> &par_exec_info) {
  if (par_exec_info) {
    return ExistenceConstraints::MultipleThreadsConstraintValidation{par_exec_info.value()};
  }
  return ExistenceConstraints::SingleThreadConstraintValidation{};
}

[[nodiscard]] std::expected<void, ConstraintViolation> ExistenceConstraints::ValidateVerticesOnConstraint(
    utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto calling_existence_validation_function = GetCreationFunction(parallel_exec_info);
  return std::visit([&vertices, &label, &property, &snapshot_info](
                        auto &calling_object) { return calling_object(vertices, label, property, snapshot_info); },
                    calling_existence_validation_function);
}

std::expected<void, ConstraintViolation> ExistenceConstraints::MultipleThreadsConstraintValidation::operator()(
    const utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  const auto &vertex_batches = parallel_exec_info.vertex_recovery_info;
  MG_ASSERT(!vertex_batches.empty(),
            "The size of batches should always be greater than zero if you want to use the parallel version of index "
            "creation!");
  const auto thread_count = std::min(parallel_exec_info.thread_count, vertex_batches.size());

  std::atomic<uint64_t> batch_counter = 0;
  utils::Synchronized<std::expected<void, ConstraintViolation>, utils::RWSpinLock> maybe_error{};
  {
    std::vector<std::jthread> threads;
    threads.reserve(thread_count);

    for (auto i{0U}; i < thread_count; ++i) {
      threads.emplace_back(
          [&maybe_error, &vertex_batches, &batch_counter, &vertices, &label, &property, &snapshot_info]() {
            do_per_thread_validation(maybe_error, ValidateVertexOnConstraint, vertex_batches, batch_counter, vertices,
                                     snapshot_info, label, property);
          });
    }
  }
  if (!maybe_error.Lock()->has_value()) {
    return std::unexpected{maybe_error->error()};
  }
  return {};
}

std::expected<void, ConstraintViolation> ExistenceConstraints::SingleThreadConstraintValidation::operator()(
    const utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  for (const Vertex &vertex : vertices) {
    if (auto violation = ValidateVertexOnConstraint(vertex, label, property); violation.has_value()) {
      return std::unexpected{violation.error()};
    }
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VERTICES);
    }
  }
  return {};
}

void ExistenceConstraints::DropGraphClearConstraints() {
  constraints_.WithLock([](auto &constraints) { constraints.clear(); });
}

}  // namespace memgraph::storage
