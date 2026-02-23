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
#include "storage/v2/storage.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::storage {

namespace {
[[nodiscard]] std::expected<void, ConstraintViolation> ValidateVertexOnConstraint(const Vertex &vertex,
                                                                                  const LabelId &label,
                                                                                  const PropertyId &property) {
  if (!vertex.deleted() && std::ranges::contains(vertex.labels, label) && !vertex.properties.HasProperty(property)) {
    return std::unexpected{ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set{property}}};
  }
  return {};
}
}  // namespace

// --- IndividualConstraint implementation ---

ExistenceConstraints::IndividualConstraint::~IndividualConstraint() {
  if (status.IsReady()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveExistenceConstraints);
  }
}

// --- ActiveConstraints implementation ---

std::vector<std::pair<LabelId, PropertyId>> ExistenceConstraints::ActiveConstraints::ListConstraints(
    uint64_t start_timestamp) const {
  namespace r = std::ranges;
  namespace rv = std::views;
  auto result = *container_ |
                rv::filter([start_timestamp](const auto &c) { return c.second->status.IsVisible(start_timestamp); }) |
                rv::transform([](const auto &c) { return std::pair{c.first.label, c.first.property}; }) |
                r::to<std::vector<std::pair<LabelId, PropertyId>>>();
  std::ranges::sort(result);
  return result;
}

bool ExistenceConstraints::ActiveConstraints::empty() const { return container_->empty(); }

auto ExistenceConstraints::GetActiveConstraints() const -> std::unique_ptr<ActiveConstraints> {
  return std::make_unique<ActiveConstraints>(constraints_.WithReadLock(std::identity{}));
}

// --- ExistenceConstraints methods ---

bool ExistenceConstraints::ConstraintExists(LabelId label, PropertyId property) const {
  auto constraints = constraints_.WithReadLock(std::identity{});
  return constraints->contains({label, property});
}

auto ExistenceConstraints::GetIndividualConstraint(LabelId label, PropertyId property) const
    -> IndividualConstraintPtr {
  return constraints_.WithReadLock([&](ContainerPtr const &constraints) -> IndividualConstraintPtr {
    const auto it = constraints->find({label, property});
    if (it == constraints->end()) [[unlikely]] {
      return {};
    }
    return it->second;
  });
}

bool ExistenceConstraints::RegisterConstraint(LabelId label, PropertyId property) {
  return constraints_.WithLock([&](ContainerPtr &constraints) {
    // Check if constraint already exists
    if (constraints->contains({label, property})) {
      return false;
    }
    // Copy-on-write: create new container with the new constraint
    auto new_constraints = std::make_shared<Container>(*constraints);
    new_constraints->emplace(ConstraintKey{.label = label, .property = property},
                             std::make_shared<IndividualConstraint>());  // Starts in populating state
    constraints = std::move(new_constraints);
    return true;
  });
}

bool ExistenceConstraints::PublishConstraint(LabelId label, PropertyId property, uint64_t commit_timestamp) const {
  auto constraint = GetIndividualConstraint(label, property);
  if (!constraint) [[unlikely]] {
    DMG_ASSERT(false, "Existence constraint not found during publish");
    return false;
  }
  constraint->status.Commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveExistenceConstraints);
  return true;
}

bool ExistenceConstraints::DropConstraint(LabelId label, PropertyId property) {
  return constraints_.WithLock([&](ContainerPtr &constraints) -> bool {
    auto new_constraints = std::make_shared<Container>(*constraints);
    if (const auto count = new_constraints->erase({label, property}); count == 0) {
      return false;
    }
    constraints = std::move(new_constraints);
    return true;
  });
}

[[nodiscard]] std::expected<void, ConstraintViolation> ExistenceConstraints::Validate(
    const std::unordered_set<Vertex const *> &vertices_to_check) const {
  auto constraints = constraints_.WithReadLock(std::identity{});
  auto validate = [&](const Vertex &vertex) -> std::expected<void, ConstraintViolation> {
    for (const auto &[key, constraint] : *constraints) {
      DMG_ASSERT(constraint->status.IsReady(), "For a WRITE query, all constraints MUST already be ready");
      if (auto validation_result = ValidateVertexOnConstraint(vertex, key.label, key.property);
          !validation_result.has_value()) [[unlikely]] {
        return std::unexpected{validation_result.error()};
      }
    }
    return {};
  };

  for (auto const *vertex : vertices_to_check) {
    // No need to take any locks here because we modified this vertex and no
    // one else can touch it until we commit.
    if (auto validation_result = validate(*vertex); !validation_result.has_value()) {
      return std::unexpected{validation_result.error()};
    }
  }
  return {};
}

[[nodiscard]] std::expected<void, ConstraintViolation> ExistenceConstraints::PerVertexValidate(
    Vertex const &vertex) const {
  auto constraints = constraints_.WithReadLock(std::identity{});
  for (const auto &[key, constraint] : *constraints) {
    // Only validate against ready (committed) constraints - with copy-on-write, dropped constraints are erased
    if (!constraint->status.IsReady()) {
      continue;
    }
    if (auto validation_result = ValidateVertexOnConstraint(vertex, key.label, key.property);
        !validation_result.has_value()) [[unlikely]] {
      return std::unexpected{validation_result.error()};
    }
  }
  return {};
}

// only used for on disk
void ExistenceConstraints::LoadExistenceConstraints(const std::vector<std::string> &keys) {
  constraints_.WithLock([&](ContainerPtr &constraints) {
    auto new_constraints = std::make_shared<Container>(*constraints);
    for (const auto &key : keys) {
      const std::vector<std::string> parts = utils::Split(key, ",");
      auto constraint_key =
          ConstraintKey{.label = LabelId::FromString(parts[0]), .property = PropertyId::FromString(parts[1])};
      auto [it, inserted] = new_constraints->emplace(constraint_key, std::make_shared<IndividualConstraint>());
      if (inserted) {
        // Immediately commit with timestamp 0 so constraint is visible to all transactions
        it->second->status.Commit(kTimestampInitialId);
        memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveExistenceConstraints);
      }
    }
    constraints = std::move(new_constraints);
  });
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
    std::optional<SnapshotObserverInfo> const &snapshot_info) const {
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
            do_per_thread_validation(maybe_error,
                                     ValidateVertexOnConstraint,
                                     vertex_batches,
                                     batch_counter,
                                     vertices,
                                     snapshot_info,
                                     label,
                                     property);
          });
    }
  }
  return *maybe_error.Lock();
}

std::expected<void, ConstraintViolation> ExistenceConstraints::SingleThreadConstraintValidation::operator()(
    const utils::SkipList<Vertex>::Accessor &vertices, const LabelId &label, const PropertyId &property,
    std::optional<SnapshotObserverInfo> const &snapshot_info) const {
  for (const Vertex &vertex : vertices) {
    if (auto validation_result = ValidateVertexOnConstraint(vertex, label, property); !validation_result.has_value()) {
      return std::unexpected{validation_result.error()};
    }
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VERTICES);
    }
  }
  return {};
}

void ExistenceConstraints::DropGraphClearConstraints() {
  constraints_.WithLock([](ContainerPtr &constraints) { constraints = std::make_shared<Container const>(); });
}

}  // namespace memgraph::storage
