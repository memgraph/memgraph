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
#include <memory>
#include <ranges>
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/utils.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"
#include "utils/skip_list.hpp"
namespace memgraph::storage {

namespace {

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
bool LastCommittedVersionHasLabelProperty(const Vertex &vertex, LabelId label, const std::set<PropertyId> &properties,
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
bool AnyVersionHasLabelProperty(const Vertex &vertex, LabelId label, const std::set<PropertyId> &properties,
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

bool InMemoryUniqueConstraints::Entry::operator<(const Entry &rhs) const {
  return std::tie(values, vertex, timestamp) < std::tie(rhs.values, rhs.vertex, rhs.timestamp);
}

bool InMemoryUniqueConstraints::Entry::operator==(const Entry &rhs) const {
  return std::tie(values, vertex, timestamp) == std::tie(rhs.values, rhs.vertex, rhs.timestamp);
}

bool InMemoryUniqueConstraints::Entry::operator<(const std::vector<PropertyValue> &rhs) const { return values < rhs; }

bool InMemoryUniqueConstraints::Entry::operator==(const std::vector<PropertyValue> &rhs) const { return values == rhs; }

void InMemoryUniqueConstraints::UpdateBeforeCommit(const Vertex *vertex, const Transaction &tx) {
  constraints_by_label_.WithReadLock([&](auto &constraints_by_label) {
    for (const auto &label : vertex->labels) {
      const auto &constraint = constraints_by_label.find(label);
      if (constraint == constraints_by_label.end()) {
        continue;
      }

      for (auto &[props, individual_constraint] : constraint->second) {
        // creation can only happen with read only access and here a write happened
        // therefore the constraint is already registered/validated and we don't need to check status
        auto values = vertex->properties.ExtractPropertyValues(props);

        if (!values) {
          continue;
        }

        // hacky -> we are accessing the skiplist with the read lock
        // via using a pointer to the skiplist but skiplist is thread safe so its fine
        auto acc = individual_constraint->skiplist.access();
        acc.insert(Entry{std::move(*values), vertex, tx.start_timestamp});
      }
    }
  });
}

std::variant<InMemoryUniqueConstraints::MultipleThreadsConstraintValidation,
             InMemoryUniqueConstraints::SingleThreadConstraintValidation>
InMemoryUniqueConstraints::GetCreationFunction(
    const std::optional<durability::ParallelizedSchemaCreationInfo> &par_exec_info) {
  if (par_exec_info) {
    return InMemoryUniqueConstraints::MultipleThreadsConstraintValidation{par_exec_info.value()};
  }
  return InMemoryUniqueConstraints::SingleThreadConstraintValidation{};
}

bool InMemoryUniqueConstraints::MultipleThreadsConstraintValidation::operator()(
    const utils::SkipList<Vertex>::Accessor &vertex_accessor, utils::SkipList<Entry>::Accessor &constraint_accessor,
    const LabelId &label, const std::set<PropertyId> &properties,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  const auto &vertex_batches = parallel_exec_info.vertex_recovery_info;
  MG_ASSERT(!vertex_batches.empty(),
            "The size of batches should always be greater than zero if you want to use the parallel version of index "
            "creation!");
  const auto thread_count = std::min(parallel_exec_info.thread_count, vertex_batches.size());

  std::atomic<uint64_t> batch_counter = 0;
  utils::Synchronized<std::optional<ConstraintViolation>, utils::RWSpinLock> has_error;
  {
    std::vector<std::jthread> threads;
    threads.reserve(thread_count);
    for (auto i{0U}; i < thread_count; ++i) {
      threads.emplace_back([&has_error, &vertex_batches, &batch_counter, &vertex_accessor, &constraint_accessor, &label,
                            &properties, &snapshot_info]() {
        do_per_thread_validation(has_error, DoValidate, vertex_batches, batch_counter, vertex_accessor, snapshot_info,
                                 constraint_accessor, label, properties);
      });
    }
  }
  return has_error.Lock()->has_value();
}

bool InMemoryUniqueConstraints::SingleThreadConstraintValidation::operator()(
    const utils::SkipList<Vertex>::Accessor &vertex_accessor, utils::SkipList<Entry>::Accessor &constraint_accessor,
    const LabelId &label, const std::set<PropertyId> &properties,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  for (const Vertex &vertex : vertex_accessor) {
    if (const auto violation = DoValidate(vertex, constraint_accessor, label, properties); violation.has_value()) {
      return true;
    }
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VERTICES);
    }
  }
  return false;
}

std::optional<ConstraintViolation> InMemoryUniqueConstraints::DoValidate(
    const Vertex &vertex, utils::SkipList<Entry>::Accessor &constraint_accessor, const LabelId &label,
    const std::set<PropertyId> &properties) {
  if (vertex.deleted || !std::ranges::contains(vertex.labels, label)) {
    return std::nullopt;
  }
  auto values = vertex.properties.ExtractPropertyValues(properties);
  if (!values) {
    return std::nullopt;
  }

  // Check whether there already is a vertex with the same values for the
  // given label and property.
  auto it = constraint_accessor.find_equal_or_greater(*values);
  if (it != constraint_accessor.end() && it->values == *values) {
    return ConstraintViolation{ConstraintViolation::Type::UNIQUE, label, properties};
  }

  constraint_accessor.insert(Entry{std::move(*values), &vertex, 0});
  return std::nullopt;
}

void InMemoryUniqueConstraints::AbortEntries(std::span<Vertex const *const> const vertices,
                                             uint64_t const exact_start_timestamp) {
  constraints_by_label_.WithReadLock([&](auto &constraints_by_label) {
    for (const auto &vertex : vertices) {
      for (const auto &label : vertex->labels) {
        const auto &constraint = constraints_by_label.find(label);
        if (constraint == constraints_by_label.end()) {
          continue;
        }

        for (auto &[props, individual_constraint] : constraint->second) {
          // creation can only happen with read only access and here a write happened
          // therefore the constraint is already registered/validated and we don't need to check status
          auto values = vertex->properties.ExtractPropertyValues(props);

          if (!values) {
            continue;
          }

          auto acc = individual_constraint->skiplist.access();
          acc.remove(Entry{std::move(*values), vertex, exact_start_timestamp});
        }
      }
    }
  });
}

std::expected<InMemoryUniqueConstraints::CreationStatus, ConstraintViolation>
InMemoryUniqueConstraints::CreateConstraint(
    LabelId label, const std::set<PropertyId> &properties, const utils::SkipList<Vertex>::Accessor &vertex_accessor,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &par_exec_info,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  if (properties.empty()) {
    return CreationStatus::EMPTY_PROPERTIES;
  }
  if (properties.size() > kUniqueConstraintsMaxProperties) {
    return CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED;
  }

  auto contained = constraints_.WithReadLock([&](auto &constraints) {
    return constraints.contains({label, properties});
  });
  if (contained) {
    return CreationStatus::ALREADY_EXISTS;
  }

  auto it = constraints_.WithLock([&](auto &constraints) {
    auto [it, _] = constraints.emplace(
        std::make_pair(label, properties),
        IndividualConstraint{.skiplist = utils::SkipList<Entry>{}, .status = ValidationStatus::PENDING});
    return it;
  });
  // constraint can't get dropped before it gets validated/fails -> this is safe
  auto constraint_accessor = it->second.skiplist.access();

  auto multi_single_thread_processing = GetCreationFunction(par_exec_info);

  bool const violation_found = std::visit(
      [&vertex_accessor, &constraint_accessor, &label, &properties,
       &snapshot_info](auto &multi_single_thread_processing) {
        return multi_single_thread_processing(vertex_accessor, constraint_accessor, label, properties, snapshot_info);
      },
      multi_single_thread_processing);

  if (violation_found) {
    constraints_.WithLock([&](auto &constraints) { constraints.erase(it); });
    return std::unexpected{ConstraintViolation{ConstraintViolation::Type::UNIQUE, label, properties}};
  }

  // iterator can only get invalidated by this thread -> this is safe (i think?)
  it->second.status = ValidationStatus::VALIDATED;

  // Add the new constraint to the optimized structure only if there are no violations.
  constraints_by_label_.WithLock([&](auto &constraints_by_label) {
    constraints_by_label[label].insert({properties, &it->second});
  });
  return CreationStatus::SUCCESS;
}

InMemoryUniqueConstraints::DeletionStatus InMemoryUniqueConstraints::DropConstraint(
    LabelId label, const std::set<PropertyId> &properties) {
  if (auto drop_properties_check_result = UniqueConstraints::CheckPropertiesBeforeDeletion(properties);
      drop_properties_check_result != UniqueConstraints::DeletionStatus::SUCCESS) {
    return drop_properties_check_result;
  }

  auto erased = constraints_.WithLock([&](auto &constraints) { return constraints.erase({label, properties}) > 0; });
  if (!erased) return UniqueConstraints::DeletionStatus::NOT_FOUND;

  // this will always succeed because constraint is checked in constraints_ map
  constraints_by_label_.WithLock([&](auto &constraints_by_label) {
    auto it = constraints_by_label.find(label);
    if (it != constraints_by_label.end()) {
      it->second.erase(properties);
    }
  });
  return UniqueConstraints::DeletionStatus::SUCCESS;
}

bool InMemoryUniqueConstraints::ConstraintExists(LabelId label, const std::set<PropertyId> &properties) const {
  return constraints_.WithReadLock([&](auto const &constraints) { return constraints.contains({label, properties}); });
}

std::optional<ConstraintViolation> InMemoryUniqueConstraints::Validate(const Vertex &vertex, const Transaction &tx,
                                                                       uint64_t commit_timestamp) const {
  if (vertex.deleted) {
    return std::nullopt;
  }
  return constraints_by_label_.WithReadLock([&](auto &constraints_by_label) -> std::optional<ConstraintViolation> {
    for (const auto &label : vertex.labels) {
      const auto &constraint = constraints_by_label.find(label);
      if (constraint == constraints_by_label.end()) {
        continue;
      }

      for (const auto &[properties, individual_constraint] : constraint->second) {
        auto value_array = vertex.properties.ExtractPropertyValues(properties);

        if (!value_array) {
          continue;
        }

        auto possible_conflicting = std::invoke([&] {
          // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
          auto acc = individual_constraint->skiplist.access();
          auto it = acc.find_equal_or_greater(*value_array);
          std::unordered_set<Vertex const *> res;
          for (; it != acc.end(); ++it) {
            if (*value_array != it->values) {
              break;
            }

            // The `vertex` that is going to be committed violates a unique constraint
            // if it's different than a vertex indexed in the list of constraints and
            // has the same label and property value as the last committed version of
            // the vertex from the list.
            if (&vertex != it->vertex) {
              res.insert(it->vertex);
            }
          }
          return res;
        });

        for (auto const *v : possible_conflicting) {
          if (LastCommittedVersionHasLabelProperty(*v, label, properties, *value_array, tx, commit_timestamp)) {
            return ConstraintViolation{ConstraintViolation::Type::UNIQUE, label, properties};
          }
        }
      }
    }
    return std::nullopt;
  });
}

std::vector<std::pair<LabelId, std::set<PropertyId>>> InMemoryUniqueConstraints::ListConstraints() const {
  return constraints_.WithReadLock([&](const auto &constraints) {
    std::vector<std::pair<LabelId, std::set<PropertyId>>> ret;
    ret.reserve(constraints.size());
    for (const auto &[label_props, _] : constraints) {
      ret.push_back(label_props);
    }
    return ret;
  });
}

void InMemoryUniqueConstraints::RemoveObsoleteEntries(uint64_t const oldest_active_start_timestamp,
                                                      std::stop_token token) {
  constraints_by_label_.WithReadLock([&](auto &constraints_by_label) {
    auto maybe_stop = utils::ResettableCounter(2048);

    for (auto &[label, map] : constraints_by_label) {
      for (auto &[properties, individual_constraint] : map) {
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
  });
}

void InMemoryUniqueConstraints::Clear() {
  constraints_.WithLock([](auto &constraints) { constraints.clear(); });
  constraints_by_label_.WithLock([](auto &constraints_by_label) { constraints_by_label.clear(); });
}

bool InMemoryUniqueConstraints::empty() const {
  return constraints_.WithReadLock([](auto const &constraints) { return constraints.empty(); }) &&
         constraints_by_label_.WithReadLock(
             [](auto const &constraints_by_label) { return constraints_by_label.empty(); });
}

void InMemoryUniqueConstraints::RunGC() {
  constraints_by_label_.WithReadLock([&](auto &constraints_by_label) {
    for (auto &[_, map] : constraints_by_label) {
      for (auto &[_, individual_constraint] : map) {
        individual_constraint->skiplist.run_gc();
      }
    }
  });
}

}  // namespace memgraph::storage
