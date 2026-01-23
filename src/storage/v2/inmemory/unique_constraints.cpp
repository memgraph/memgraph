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

#include "storage/v2/inmemory/unique_constraints.hpp"
#include <algorithm>
#include <array>
#include <bitset>
#include <ranges>
#include <tuple>
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/utils.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

namespace {

auto DoValidate(const Vertex &vertex, utils::SkipList<InMemoryUniqueConstraints::Entry>::Accessor &constraint_accessor,
                const LabelId &label, SortedPropertyIds const &properties) -> std::expected<void, ConstraintViolation> {
  if (vertex.deleted || !std::ranges::contains(vertex.labels, label)) {
    return {};
  }
  auto values = vertex.properties.ExtractPropertyValues(properties);
  if (!values) {
    return {};
  }

  // Check whether there already is a vertex with the same values for the
  // given label and property.
  const auto it = constraint_accessor.find_equal_or_greater(*values);
  if (it != constraint_accessor.end() && it->values == *values) {
    return std::unexpected{ConstraintViolation{ConstraintViolation::Type::UNIQUE, label, properties}};
  }

  constraint_accessor.insert(
      InMemoryUniqueConstraints::Entry{.values = std::move(*values), .vertex = &vertex, .timestamp = 0});
  return {};
}

/// Utility class to store data in a fixed size array. The array is used
/// instead of `std::vector` to avoid `std::bad_alloc` exception where not
/// necessary.
template <class T>
struct FixedCapacityArray {
  size_t size;
  std::array<T, kUniqueConstraintsMaxProperties> values;

  explicit FixedCapacityArray(size_t array_size) : size(array_size) {
    MG_ASSERT(size <= kUniqueConstraintsMaxProperties, "Invalid array size!");
  }

  template <std::ranges::input_range R>
    requires std::convertible_to<std::ranges::range_value_t<R>, T>
  explicit FixedCapacityArray(R &&range) : size(std::ranges::size(range)) {
    MG_ASSERT(size <= kUniqueConstraintsMaxProperties, "Invalid array size!");
    std::ranges::copy(std::forward<R>(range), values.begin());
  }

  constexpr T *begin() noexcept { return values.data(); }

  constexpr T *end() noexcept { return values.data() + size; }

  constexpr const T *begin() const noexcept { return values.data(); }

  constexpr const T *end() const noexcept { return values.data() + size; }
};

using PropertyIdArray = FixedCapacityArray<PropertyId>;

/// Helper function that determines position of the given `property` in the
/// sorted `property_array` using binary search. In the case that `property`
/// cannot be found, `std::nullopt` is returned.
std::optional<size_t> FindPropertyPosition(const PropertyIdArray &property_array, PropertyId property) {
  auto const *const it = std::ranges::lower_bound(property_array, property);
  if (it == property_array.end() || *it != property) {
    return std::nullopt;
  }
  return static_cast<size_t>(it - property_array.begin());
}

/// Helper function for validating unique constraints on commit. Returns true if
/// the last committed version of the given vertex contains the given label and
/// set of property values. This function should be called when commit lock is
/// active.
bool LastCommittedVersionHasLabelProperty(const Vertex &vertex, LabelId label, SortedPropertyIds const &properties,
                                          const std::vector<PropertyValue> &value_array, const Transaction &transaction,
                                          uint64_t commit_timestamp) {
  MG_ASSERT(properties.size() == value_array.size(), "Invalid database state!");

  auto const property_array = PropertyIdArray{properties};

  std::bitset<kUniqueConstraintsMaxProperties> current_value_equal_to_value;

  // Since the commit lock is active, any transaction that tries to write to
  // a vertex which is part of the given `transaction` will result in a
  // serialization error. But, note that the given `vertex`'s data does not have
  // to be modified in the current `transaction`, meaning that a guard lock to
  // access vertex's data is still necessary because another active transaction
  // could modify it in the meantime.
  Delta *delta;
  bool deleted;
  bool has_label;
  {
    auto guard = std::shared_lock{vertex.lock};
    delta = vertex.delta;
    deleted = vertex.deleted;
    has_label = std::ranges::contains(vertex.labels, label);

    for (const auto &[i, property] : std::views::enumerate(properties)) {
      current_value_equal_to_value[i] = vertex.properties.IsPropertyEqual(property, value_array[i]);
    }
  }

  while (delta != nullptr) {
    const auto ts = delta->timestamp->load(std::memory_order_acquire);
    if (ts < commit_timestamp || ts == transaction.transaction_id) {
      break;
    }

    switch (delta->action) {
      case Delta::Action::SET_PROPERTY: {
        auto pos = FindPropertyPosition(property_array, delta->property.key);
        if (pos) {
          current_value_equal_to_value[*pos] = *delta->property.value == value_array[*pos];
        }
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        MG_ASSERT(!deleted, "Invalid database state!");
        deleted = true;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        MG_ASSERT(deleted, "Invalid database state!");
        deleted = false;
        break;
      }
      case Delta::Action::ADD_LABEL: {
        if (delta->label.value == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
        }
        break;
      }
      case Delta::Action::REMOVE_LABEL: {
        if (delta->label.value == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
        }
        break;
      }
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }

    delta = delta->next.load(std::memory_order_acquire);
  }

  for (size_t i = 0; i < properties.size(); ++i) {
    if (!current_value_equal_to_value[i]) {
      return false;
    }
  }

  return !deleted && has_label;
}

/// Helper function for unique constraint garbage collection. Returns true if
/// there's a reachable version of the vertex that has the given label and
/// property values.
bool AnyVersionHasLabelProperty(const Vertex &vertex, LabelId label, SortedPropertyIds const &properties,
                                const std::vector<PropertyValue> &values, uint64_t timestamp) {
  MG_ASSERT(properties.size() == values.size(), "Invalid database state!");

  PropertyIdArray property_array(properties.size());
  std::bitset<kUniqueConstraintsMaxProperties> current_value_equal_to_value;

  bool has_label;
  bool deleted;
  Delta *delta;
  {
    auto guard = std::shared_lock{vertex.lock};
    has_label = std::ranges::contains(vertex.labels, label);
    deleted = vertex.deleted;
    delta = vertex.delta;

    // Avoid IsPropertyEqual if already not possible
    if (delta == nullptr && (deleted || !has_label)) return false;

    if (delta) {
      // If delta we need to fetch for later processing
      for (const auto &[i, property] : std::views::enumerate(properties)) {
        current_value_equal_to_value[i] = vertex.properties.IsPropertyEqual(property, values[i]);
        property_array.values[i] = property;
      }
    } else {
      // otherwise do a short-circuiting check (we already know !deleted && has_label)
      return std::ranges::all_of(std::views::zip(properties, values), [&](const auto &prop_val) {
        const auto &[property, value] = prop_val;
        return vertex.properties.IsPropertyEqual(property, value);
      });
    }
  }

  {
    bool all_values_match = true;
    for (size_t i = 0; i < values.size(); ++i) {
      if (!current_value_equal_to_value[i]) {
        all_values_match = false;
        break;
      }
    }
    if (!deleted && has_label && all_values_match) {
      return true;
    }
  }

  while (delta != nullptr) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    if (ts < timestamp) {
      break;
    }
    switch (delta->action) {
      case Delta::Action::ADD_LABEL:
        if (delta->label.value == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
        }
        break;
      case Delta::Action::REMOVE_LABEL:
        if (delta->label.value == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
        }
        break;
      case Delta::Action::SET_PROPERTY: {
        auto pos = FindPropertyPosition(property_array, delta->property.key);
        if (pos) {
          current_value_equal_to_value[*pos] = *delta->property.value == values[*pos];
        }
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        MG_ASSERT(deleted, "Invalid database state!");
        deleted = false;
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        MG_ASSERT(!deleted, "Invalid database state!");
        deleted = true;
        break;
      }
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }

    bool all_values_match = true;
    for (size_t i = 0; i < values.size(); ++i) {
      if (!current_value_equal_to_value[i]) {
        all_values_match = false;
        break;
      }
    }
    if (!deleted && has_label && all_values_match) {
      return true;
    }
    delta = delta->next.load(std::memory_order_acquire);
  }
  return false;
}

}  // namespace

// --- IndividualConstraint implementation ---

InMemoryUniqueConstraints::IndividualConstraint::~IndividualConstraint() {
  if (status.IsReady()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveUniqueConstraints);
  }
}

void InMemoryUniqueConstraints::IndividualConstraint::Publish(uint64_t commit_timestamp) {
  status.Commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveUniqueConstraints);
}

// --- ActiveConstraints implementation ---
auto InMemoryUniqueConstraints::ActiveConstraints::ListConstraints(uint64_t start_timestamp) const
    -> std::vector<std::pair<LabelId, SortedPropertyIds>> {
  auto result = std::vector<std::pair<LabelId, SortedPropertyIds>>{};
  for (auto const &[label, inner] : *container_) {
    for (auto const &[properties, constraint] : inner) {
      if (constraint->status.IsVisible(start_timestamp)) {
        result.emplace_back(label, properties);
      }
    }
  }
  std::ranges::sort(result);
  return result;
}

auto InMemoryUniqueConstraints::ActiveConstraints::GetValidationProcessor() const -> ValidationProcessor {
  auto initial = ValidationInfo{};
  for (const auto &[label, val] : *container_) {
    auto &inner = initial[label];
    for (auto props : val | std::ranges::views::keys) {
      inner.emplace(props, ConstraintValue{});
    }
  }
  return ValidationProcessor{std::move(initial)};
}

auto InMemoryUniqueConstraints::ActiveConstraints::ValidateAndCommitEntries(
    ValidationInfo &&info,  // NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
    uint64_t start_timestamp, const Transaction *tx, uint64_t commit_timestamp)
    -> std::expected<void, ConstraintViolation> {
  MG_ASSERT(tx != nullptr, "Transaction must be provided for in-memory unique constraint validation");

  // Constraint outer loop - one accessor per constraint (efficient)
  for (auto &[label, inner] : info) {
    for (auto &[properties, entries] : inner) {
      // Empty entries means we have nothing to process for a given key
      if (entries.empty()) continue;

      auto const it1 = container_->find(label);
      if (it1 == container_->end()) [[unlikely]] {
        DMG_ASSERT(false, "ValidationInfo should only match what our constraints use");
        continue;
      }

      auto const it2 = it1->second.find(properties);
      if (it2 == it1->second.end()) [[unlikely]] {
        DMG_ASSERT(false, "ValidationInfo should only match what our constraints use");
        continue;
      }

      // Single accessor for both validation and insertion
      auto acc = it2->second->skiplist.access();

      for (auto &[values, vertex] : entries) {
        // Skip deleted vertices
        if (vertex->deleted) continue;

        // First: check for conflicts (validation)
        // Find all entries with same property values
        auto it = acc.find_equal_or_greater(values);
        while (it != acc.end() && it->values == values) {
          if (it->vertex != vertex) {
            // Potential conflict with different vertex - check MVCC visibility
            if (LastCommittedVersionHasLabelProperty(*it->vertex, label, properties, values, *tx, commit_timestamp)) {
              return std::unexpected{ConstraintViolation{ConstraintViolation::Type::UNIQUE, label, properties}};
            }
          }
          ++it;
        }

        // No conflict found, insert the entry
        acc.insert(Entry{.values = std::move(values), .vertex = vertex, .timestamp = start_timestamp});
      }
    }
  }
  return {};
}

void InMemoryUniqueConstraints::ActiveConstraints::AbortEntries(
    ValidationInfo &&info,  // NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
    uint64_t exact_start_timestamp) {
  // Constraint outer loop - one accessor per constraint (efficient)
  for (auto &[label, inner] : info) {
    for (auto &[properties, entries] : inner) {
      // Empty entries means we have nothing to cleanup for a given key
      if (entries.empty()) continue;

      auto const it1 = container_->find(label);
      if (it1 == container_->end()) [[unlikely]] {
        DMG_ASSERT(false, "ValidationInfo should only match what our constraints use");
        continue;
      }

      auto const it2 = it1->second.find(properties);
      if (it2 == it1->second.end()) [[unlikely]] {
        DMG_ASSERT(false, "ValidationInfo should only match what our constraints use");
        continue;
      }

      // Single access to bulk remove all entries from this constraint's skip list
      auto acc = it2->second->skiplist.access();
      for (auto &[values, vertex] : entries) {
        acc.remove(Entry{.values = std::move(values), .vertex = vertex, .timestamp = exact_start_timestamp});
      }
    }
  }
}

bool InMemoryUniqueConstraints::ActiveConstraints::empty() const { return container_->empty(); }

auto InMemoryUniqueConstraints::GetActiveConstraints() const -> std::unique_ptr<UniqueConstraints::ActiveConstraints> {
  return std::make_unique<ActiveConstraints>(container_.WithReadLock(std::identity{}));
}

// --- InMemoryUniqueConstraints methods ---

bool InMemoryUniqueConstraints::Entry::operator<(const Entry &rhs) const {
  return std::tie(values, vertex, timestamp) < std::tie(rhs.values, rhs.vertex, rhs.timestamp);
}

bool InMemoryUniqueConstraints::Entry::operator==(const Entry &rhs) const {
  return std::tie(values, vertex, timestamp) == std::tie(rhs.values, rhs.vertex, rhs.timestamp);
}

bool InMemoryUniqueConstraints::Entry::operator<(const std::vector<PropertyValue> &rhs) const { return values < rhs; }

bool InMemoryUniqueConstraints::Entry::operator==(const std::vector<PropertyValue> &rhs) const { return values == rhs; }

auto InMemoryUniqueConstraints::GetCreationFunction(
    const std::optional<durability::ParallelizedSchemaCreationInfo> &par_exec_info)
    -> std::variant<InMemoryUniqueConstraints::MultipleThreadsConstraintValidation,
                    InMemoryUniqueConstraints::SingleThreadConstraintValidation> {
  if (par_exec_info) {
    return InMemoryUniqueConstraints::MultipleThreadsConstraintValidation{par_exec_info.value()};
  }
  return InMemoryUniqueConstraints::SingleThreadConstraintValidation{};
}

auto InMemoryUniqueConstraints::MultipleThreadsConstraintValidation::operator()(
    const utils::SkipList<Vertex>::Accessor &vertex_accessor, utils::SkipList<Entry>::Accessor &constraint_accessor,
    const LabelId &label, SortedPropertyIds const &properties,
    std::optional<SnapshotObserverInfo> const &snapshot_info) const -> std::expected<void, ConstraintViolation> {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  const auto &vertex_batches = parallel_exec_info.vertex_recovery_info;
  MG_ASSERT(!vertex_batches.empty(),
            "The size of batches should always be greater than zero if you want to use the parallel version of index "
            "creation!");
  const auto thread_count = std::min(parallel_exec_info.thread_count, vertex_batches.size());

  std::atomic<uint64_t> batch_counter = 0;
  utils::Synchronized<std::expected<void, ConstraintViolation>, utils::RWSpinLock> result{};
  {
    std::vector<std::jthread> threads;
    threads.reserve(thread_count);
    for (auto i{0U}; i < thread_count; ++i) {
      threads.emplace_back([&result,
                            &vertex_batches,
                            &batch_counter,
                            &vertex_accessor,
                            &constraint_accessor,
                            &label,
                            &properties,
                            &snapshot_info]() {
        do_per_thread_validation(result,
                                 DoValidate,
                                 vertex_batches,
                                 batch_counter,
                                 vertex_accessor,
                                 snapshot_info,
                                 constraint_accessor,
                                 label,
                                 properties);
      });
    }
  }
  return *result.Lock();
}

auto InMemoryUniqueConstraints::SingleThreadConstraintValidation::operator()(
    const utils::SkipList<Vertex>::Accessor &vertex_accessor, utils::SkipList<Entry>::Accessor &constraint_accessor,
    const LabelId &label, SortedPropertyIds const &properties,
    std::optional<SnapshotObserverInfo> const &snapshot_info) const -> std::expected<void, ConstraintViolation> {
  for (const Vertex &vertex : vertex_accessor) {
    if (auto result = DoValidate(vertex, constraint_accessor, label, properties); !result.has_value()) {
      return result;
    }
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VERTICES);
    }
  }
  return {};
}

auto InMemoryUniqueConstraints::CreateConstraint(
    LabelId label, SortedPropertyIds const &properties, const utils::SkipList<Vertex>::Accessor &vertex_accessor,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &par_exec_info,
    std::optional<SnapshotObserverInfo> const &snapshot_info) -> std::expected<CreationStatus, ConstraintViolation> {
  // TODO: we should do the proper register -> populate(with cancel + parallel) -> publish pattern

  if (properties.empty()) {
    return CreationStatus::EMPTY_PROPERTIES;
  }
  if (properties.size() > kUniqueConstraintsMaxProperties) {
    return CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED;
  }

  auto constraint_ptr =
      container_.WithLock([&](ContainerPtr &container) -> std::expected<IndividualConstraintPtr, CreationStatus> {
        auto new_container = std::make_shared<Container>(*container);
        auto &inner = (*new_container)[label];
        auto [it, inserted] = inner.try_emplace(properties, std::make_shared<IndividualConstraint>());
        if (!inserted) return std::unexpected{CreationStatus::ALREADY_EXISTS};
        container = std::move(new_container);
        return it->second;
      });

  if (!constraint_ptr) return constraint_ptr.error();

  try {
    auto validation_result = std::invoke([&] {
      // `constraint_accessor` is inside this IIFE on purpose.
      // This accessor MUST be released before we erase if a violation was found
      auto constraint_accessor = constraint_ptr.value()->skiplist.access();

      auto multi_single_thread_processing = GetCreationFunction(par_exec_info);

      return std::visit(
          [&vertex_accessor, &constraint_accessor, &label, &properties, &snapshot_info](
              auto &multi_single_thread_processing) {
            return multi_single_thread_processing(
                vertex_accessor, constraint_accessor, label, properties, snapshot_info);
          },
          multi_single_thread_processing);
    });

    if (!validation_result.has_value()) {
      DropConstraint(label, properties);
      return std::unexpected{validation_result.error()};
    }
    return CreationStatus::SUCCESS;
  } catch (const utils::OutOfMemoryException &) {
    DropConstraint(label, properties);
    throw;
  }
}

bool InMemoryUniqueConstraints::PublishConstraint(LabelId label, SortedPropertyIds const &properties,
                                                  uint64_t commit_timestamp) {
  auto constraint = GetIndividualConstraint(label, properties);
  if (!constraint) return false;
  constraint->Publish(commit_timestamp);
  return true;
}

auto InMemoryUniqueConstraints::DropConstraint(LabelId label, SortedPropertyIds const &properties) -> DeletionStatus {
  if (auto status = CheckPropertiesBeforeDeletion(properties); status != DeletionStatus::SUCCESS) {
    return status;
  }

  auto erased = container_.WithLock([&](ContainerPtr &container) -> bool {
    auto new_container = std::make_shared<Container>(*container);
    auto label_it = new_container->find(label);
    if (label_it == new_container->end()) {
      return false;
    }
    auto const count = label_it->second.erase(properties);
    if (count == 0) return false;
    if (label_it->second.empty()) {
      new_container->erase(label_it);
    }

    container = std::move(new_container);
    return true;
  });

  return erased ? DeletionStatus::SUCCESS : DeletionStatus::NOT_FOUND;
}

void InMemoryUniqueConstraints::RemoveObsoleteEntries(uint64_t const oldest_active_start_timestamp,
                                                      const std::stop_token &token) {
  auto container = container_.WithReadLock(std::identity{});
  auto maybe_stop = utils::ResettableCounter(2048);

  for (const auto &[label, map] : *container) {
    for (const auto &[properties, individual_constraint] : map) {
      // before starting constraint, check if stop_requested
      if (token.stop_requested()) return;

      auto acc = individual_constraint->skiplist.access();
      for (auto it = acc.begin(); it != acc.end();) {
        // Hot loop, don't check stop_requested every time
        if (maybe_stop() && token.stop_requested()) return;

        auto next_it = it;
        ++next_it;

        // Cannot delete it yet
        if (it->timestamp >= oldest_active_start_timestamp) {
          it = next_it;
          continue;
        }

        if ((next_it != acc.end() && it->vertex == next_it->vertex && it->values == next_it->values) ||
            !AnyVersionHasLabelProperty(*it->vertex, label, properties, it->values, oldest_active_start_timestamp)) {
          acc.remove(*it);
        }
        it = next_it;
      }
    }
  }
}

void InMemoryUniqueConstraints::Clear() {
  container_.WithLock([](ContainerPtr &container) { container = std::make_shared<Container const>(); });
}

void InMemoryUniqueConstraints::DropGraphClearConstraints() {
  container_.WithLock([](ContainerPtr &container) { container = std::make_shared<Container const>(); });
}

void InMemoryUniqueConstraints::RunGC() {
  const auto container = container_.WithReadLock(std::identity{});
  for (const auto &map : *container | std::views::values) {
    for (const auto &individual_constraint : map | std::views::values) {
      individual_constraint->skiplist.run_gc();
    }
  }
}

auto InMemoryUniqueConstraints::GetIndividualConstraint(const LabelId label, SortedPropertyIds const &properties) const
    -> IndividualConstraintPtr {
  return container_.WithReadLock([&](ContainerPtr const &index) -> IndividualConstraintPtr {
    auto it1 = index->find(label);
    if (it1 == index->cend()) [[unlikely]]
      return {};
    const auto &inner = it1->second;
    auto it2 = inner.find(properties);
    if (it2 == inner.cend()) [[unlikely]]
      return {};
    return it2->second;
  });
}

}  // namespace memgraph::storage
