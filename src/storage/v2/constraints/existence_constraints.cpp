// Copyright 2024 Memgraph Ltd.
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
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/constraints/utils.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"
namespace memgraph::storage {

bool ExistenceConstraints::ConstraintExists(LabelId label, PropertyId property) const {
  return utils::Contains(constraints_, std::make_pair(label, property));
}

void ExistenceConstraints::InsertConstraint(LabelId label, PropertyId property) {
  if (ConstraintExists(label, property)) {
    return;
  }
  constraints_.emplace_back(label, property);
}

bool ExistenceConstraints::DropConstraint(LabelId label, PropertyId property) {
  auto it = std::find(constraints_.begin(), constraints_.end(), std::make_pair(label, property));
  if (it == constraints_.end()) {
    return false;
  }
  constraints_.erase(it);
  return true;
}

std::vector<std::pair<LabelId, PropertyId>> ExistenceConstraints::ListConstraints() const { return constraints_; }

[[nodiscard]] std::optional<ConstraintViolation> ExistenceConstraints::Validate(const Vertex &vertex) {
  for (const auto &[label, property] : constraints_) {
    if (auto violation = ValidateVertexOnConstraint(vertex, label, property); violation.has_value()) {
      return violation;
    }
  }
  return std::nullopt;
}

void ExistenceConstraints::LoadExistenceConstraints(const std::vector<std::string> &keys) {
  for (const auto &key : keys) {
    const std::vector<std::string> parts = utils::Split(key, ",");
    constraints_.emplace_back(LabelId::FromString(parts[0]), PropertyId::FromString(parts[1]));
  }
}

[[nodiscard]] std::optional<ConstraintViolation> ExistenceConstraints::ValidateVertexOnConstraint(
    const Vertex &vertex, const LabelId &label, const PropertyId &property) {
  if (!vertex.deleted() && utils::Contains(vertex.labels, label) && !vertex.properties.HasProperty(property)) {
    return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set<PropertyId>{property}};
  }
  return std::nullopt;
}

std::variant<ExistenceConstraints::MultipleThreadsConstraintValidation,
             ExistenceConstraints::SingleThreadConstraintValidation>
ExistenceConstraints::GetCreationFunction(
    const std::optional<durability::ParallelizedSchemaCreationInfo> &par_exec_info) {
  if (par_exec_info.has_value()) {
    return ExistenceConstraints::MultipleThreadsConstraintValidation{par_exec_info.value()};
  }
  return ExistenceConstraints::SingleThreadConstraintValidation{};
}

[[nodiscard]] std::optional<ConstraintViolation> ExistenceConstraints::ValidateVerticesOnConstraint(
    utils::SkipList<Vertex>::Accessor vertices, LabelId label, PropertyId property,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info) {
  auto calling_existence_validation_function = GetCreationFunction(parallel_exec_info);
  return std::visit(
      [&vertices, &label, &property](auto &calling_object) { return calling_object(vertices, label, property); },
      calling_existence_validation_function);
}

std::optional<ConstraintViolation> ExistenceConstraints::MultipleThreadsConstraintValidation::operator()(
    const utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  const auto &vertex_batches = parallel_exec_info.vertex_recovery_info;
  MG_ASSERT(!vertex_batches.empty(),
            "The size of batches should always be greater than zero if you want to use the parallel version of index "
            "creation!");
  const auto thread_count = std::min(parallel_exec_info.thread_count, vertex_batches.size());

  std::atomic<uint64_t> batch_counter = 0;
  memgraph::utils::Synchronized<std::optional<ConstraintViolation>, utils::RWSpinLock> maybe_error{};
  {
    std::vector<std::jthread> threads;
    threads.reserve(thread_count);

    for (auto i{0U}; i < thread_count; ++i) {
      threads.emplace_back([&maybe_error, &vertex_batches, &batch_counter, &vertices, &label, &property]() {
        do_per_thread_validation(maybe_error, ValidateVertexOnConstraint, vertex_batches, batch_counter, vertices,
                                 label, property);
      });
    }
  }
  if (maybe_error.Lock()->has_value()) {
    return maybe_error->value();
  }
  return std::nullopt;
}

std::optional<ConstraintViolation> ExistenceConstraints::SingleThreadConstraintValidation::operator()(
    const utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property) {
  for (const Vertex &vertex : vertices) {
    if (auto violation = ValidateVertexOnConstraint(vertex, label, property); violation.has_value()) {
      return violation;
    }
  }
  return std::nullopt;
}

void ExistenceConstraints::DropGraphClearConstraints() { constraints_.clear(); }

}  // namespace memgraph::storage
