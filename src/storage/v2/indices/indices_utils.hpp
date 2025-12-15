// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

#include <atomic>
#include <thread>

namespace memgraph::storage {

namespace details {

template <Delta::Action... actions>
struct ActionSet {
  constexpr bool contains(Delta::Action action) const { return ((action == actions) || ...); }
};

/// Traverses deltas visible from transaction with start timestamp greater than
/// the provided timestamp, and calls the provided callback function for each
/// delta. If the callback ever returns true, traversal is stopped and the
/// function returns true. Otherwise, the function returns false.
template <ActionSet interesting, typename TCallback>
inline bool AnyVersionSatisfiesPredicate(uint64_t timestamp, const Delta *delta, const TCallback &predicate) {
  while (delta != nullptr) {
    const auto ts = delta->timestamp->load(std::memory_order_acquire);
    // This is a committed change that we see so we shouldn't undo it.
    if (ts < timestamp) {
      break;
    }
    if (interesting.contains(delta->action) && predicate(*delta)) {
      return true;
    }
    // Move to the next delta.
    delta = delta->next.load(std::memory_order_acquire);
  }
  return false;
}

}  // namespace details

/// Helper function for label index garbage collection. Returns true if there's
/// a reachable version of the vertex that has the given label.
inline bool AnyVersionHasLabel(const Vertex &vertex, LabelId label, uint64_t timestamp) {
  bool has_label{false};
  bool deleted{false};
  const Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex.lock};
    has_label = std::ranges::contains(vertex.labels, label);
    deleted = vertex.deleted;
    delta = vertex.delta;
  }
  if (!deleted && has_label) {
    return true;
  }
  constexpr auto interesting =
      details::ActionSet<Delta::Action::ADD_LABEL, Delta::Action::REMOVE_LABEL, Delta::Action::RECREATE_OBJECT,
                         Delta::Action::DELETE_DESERIALIZED_OBJECT, Delta::Action::DELETE_OBJECT>{};
  return details::AnyVersionSatisfiesPredicate<interesting>(timestamp, delta,
                                                            [&has_label, &deleted, label](const Delta &delta) {
                                                              switch (delta.action) {
                                                                case Delta::Action::ADD_LABEL:
                                                                  if (delta.label.value == label) {
                                                                    MG_ASSERT(!has_label, "Invalid database state!");
                                                                    has_label = true;
                                                                  }
                                                                  break;
                                                                case Delta::Action::REMOVE_LABEL:
                                                                  if (delta.label.value == label) {
                                                                    MG_ASSERT(has_label, "Invalid database state!");
                                                                    has_label = false;
                                                                  }
                                                                  break;
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
                                                                case Delta::Action::SET_PROPERTY:
                                                                case Delta::Action::ADD_IN_EDGE:
                                                                case Delta::Action::ADD_OUT_EDGE:
                                                                case Delta::Action::REMOVE_IN_EDGE:
                                                                case Delta::Action::REMOVE_OUT_EDGE:
                                                                  break;
                                                              }
                                                              return !deleted && has_label;
                                                            });
}

inline bool AnyVersionIsVisible(Edge *edge, uint64_t timestamp) {
  bool deleted{false};
  const Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{edge->lock};
    deleted = edge->deleted;
    delta = edge->delta;
  }
  if (!deleted) {
    return true;
  }

  constexpr auto interesting =
      details::ActionSet<Delta::Action::RECREATE_OBJECT, Delta::Action::DELETE_DESERIALIZED_OBJECT,
                         Delta::Action::DELETE_OBJECT>{};
  return details::AnyVersionSatisfiesPredicate<interesting>(timestamp, delta, [&deleted](const Delta &delta) {
    switch (delta.action) {
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
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
    return !deleted;
  });
}

/// Helper function for edgetype-property index garbage collection. Returns true if
/// there's a reachable version of the edge that has the given property value.
inline bool AnyVersionHasProperty(const Edge &edge, PropertyId key, const PropertyValue &value, uint64_t timestamp) {
  Delta const *delta;
  bool deleted;
  bool current_value_equal_to_value;
  {
    auto guard = std::shared_lock{edge.lock};
    delta = edge.delta;
    deleted = edge.deleted;
    // Avoid IsPropertyEqual if already not possible
    if (delta == nullptr && deleted) return false;
    current_value_equal_to_value = edge.properties.IsPropertyEqual(key, value);
  }

  if (!deleted && current_value_equal_to_value) {
    return true;
  }

  constexpr auto interesting =
      details::ActionSet<Delta::Action::SET_PROPERTY, Delta::Action::RECREATE_OBJECT,
                         Delta::Action::DELETE_DESERIALIZED_OBJECT, Delta::Action::DELETE_OBJECT>{};
  return details::AnyVersionSatisfiesPredicate<interesting>(
      timestamp, delta, [&current_value_equal_to_value, &deleted, key, &value](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::SET_PROPERTY:
            if (delta.property.key == key) {
              current_value_equal_to_value = *delta.property.value == value;
            }
            break;
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
          case Delta::Action::ADD_LABEL:
          case Delta::Action::REMOVE_LABEL:
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
        return !deleted && current_value_equal_to_value;
      });
}

// Helper function for iterating through label-property index. Returns true if
// this transaction can see the given vertex, and the visible version has the
// given label and property.
inline bool CurrentEdgeVersionHasProperty(const Edge &edge, PropertyId key, const PropertyValue &value,
                                          Transaction *transaction, View view) {
  bool exists = true;
  bool deleted = false;
  bool current_value_equal_to_value = value.IsNull();
  const Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{edge.lock};
    deleted = edge.deleted;
    current_value_equal_to_value = edge.properties.IsPropertyEqual(key, value);
    delta = edge.delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    ApplyDeltasForRead(transaction, delta, view, [&, key](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Deleted_ActionMethod(deleted),
        Exists_ActionMethod(exists),
        PropertyValueMatch_ActionMethod(current_value_equal_to_value, key,value)
      });
      // clang-format on
    });
  }

  return exists && !deleted && current_value_equal_to_value;
}

template <typename TSkipListAccessorFactory, typename TFunc>
inline void PopulateIndexOnMultipleThreads(utils::SkipList<Vertex>::Accessor &vertices,
                                           TSkipListAccessorFactory &&accessor_factory, const TFunc &func,
                                           durability::ParallelizedSchemaCreationInfo const &parallel_exec_info) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  const auto &vertex_batches = parallel_exec_info.vertex_recovery_info;
  const auto thread_count = std::min(parallel_exec_info.thread_count, vertex_batches.size());

  MG_ASSERT(!vertex_batches.empty(),
            "The size of batches should always be greater than zero if you want to use the parallel version of index "
            "creation!");

  std::atomic<uint64_t> batch_counter = 0;

  // TODO(composite_index): return std::optional<utils::OutOfMemoryException>, handle index cleanup from caller
  auto maybe_error = utils::Synchronized<std::optional<utils::OutOfMemoryException>, utils::SpinLock>{};
  {
    std::vector<std::jthread> threads;
    threads.reserve(thread_count);

    for (auto i{0U}; i < thread_count; ++i) {
      threads.emplace_back([&, func /*local copy incase there is local state*/]() {
        auto acc = accessor_factory();
        while (!maybe_error.Lock()->has_value()) {
          const auto batch_index = batch_counter++;
          if (batch_index >= vertex_batches.size()) {
            return;
          }
          const auto &batch = vertex_batches[batch_index];
          auto it = vertices.find(batch.first);

          try {
            for (auto i{0U}; i < batch.second; ++i, ++it) {
              func(*it, acc);
            }

          } catch (utils::OutOfMemoryException &failure) {
            utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
            *maybe_error.Lock() = std::move(failure);
          }
        }
      });
    }
  }
  auto error = maybe_error.Lock();
  if (error->has_value()) {
    throw *std::move(*error);
  }
}

template <typename TSkipListAccessorFactory, typename TFunc>
inline void PopulateIndexOnSingleThread(utils::SkipList<Vertex>::Accessor &vertices,
                                        TSkipListAccessorFactory &&accessor_factory, const TFunc &func) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  auto acc = accessor_factory();
  for (Vertex &vertex : vertices) {
    func(vertex, acc);
  }
}

struct PopulateCancel : std::exception {};

template <typename TSkipListAccessorFactory, typename TFunc>
inline void PopulateIndexDispatch(utils::SkipList<Vertex>::Accessor &vertices,
                                  TSkipListAccessorFactory &&accessor_factory, const TFunc &insert_function,
                                  CheckCancelFunction &&cancel_check,
                                  std::optional<durability::ParallelizedSchemaCreationInfo> const &parallel_exec_info) {
  auto checked_insert_function =
      [&, cancel_check = std::move(cancel_check) /*need to be owned, if parallel these will be copied per thread*/](
          Vertex &vertex, auto &index_accessor) {
        if (cancel_check()) {
          throw PopulateCancel{};
        }
        insert_function(vertex, index_accessor);
      };

  if (parallel_exec_info && parallel_exec_info->thread_count > 1) {
    PopulateIndexOnMultipleThreads(vertices, std::forward<TSkipListAccessorFactory>(accessor_factory),
                                   checked_insert_function, *parallel_exec_info);
  } else {
    PopulateIndexOnSingleThread(vertices, std::forward<TSkipListAccessorFactory>(accessor_factory),
                                checked_insert_function);
  }
}

// @TODO Is `Create` the correct term here? Should this be `PopulateIndexOnSingleThread`?
template <typename TSkiplistIter, typename TIndex, typename TFunc>
inline void CreateIndexOnSingleThread(utils::SkipList<Vertex>::Accessor &vertices, TSkiplistIter it, TIndex &index,
                                      const TFunc &func) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  try {
    auto acc = it->second.access();
    for (Vertex &vertex : vertices) {
      func(vertex, acc);
    }
  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    index.erase(it);
    throw;
  }
}

template <typename TIndex, typename TSKiplistIter, typename TFunc>
inline void CreateIndexOnMultipleThreads(utils::SkipList<Vertex>::Accessor &vertices, TSKiplistIter skiplist_iter,
                                         TIndex &index,
                                         const durability::ParallelizedSchemaCreationInfo &parallel_exec_info,
                                         const TFunc &func) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  const auto &vertex_batches = parallel_exec_info.vertex_recovery_info;
  const auto thread_count = std::min(parallel_exec_info.thread_count, vertex_batches.size());

  MG_ASSERT(!vertex_batches.empty(),
            "The size of batches should always be greater than zero if you want to use the parallel version of index "
            "creation!");

  std::atomic<uint64_t> batch_counter = 0;

  // TODO(composite_index): return std::optional<utils::OutOfMemoryException>, handle index cleanup from caller
  utils::Synchronized<std::optional<utils::OutOfMemoryException>, utils::SpinLock> maybe_error{};
  {
    std::vector<std::jthread> threads;
    threads.reserve(thread_count);

    for (auto i{0U}; i < thread_count; ++i) {
      threads.emplace_back([&]() mutable {
        while (!maybe_error.Lock()->has_value()) {
          const auto batch_index = batch_counter++;
          if (batch_index >= vertex_batches.size()) {
            return;
          }
          const auto &batch = vertex_batches[batch_index];
          auto index_accessor = skiplist_iter->second.access();
          auto it = vertices.find(batch.first);

          try {
            for (auto i{0U}; i < batch.second; ++i, ++it) {
              func(*it, index_accessor);
            }

          } catch (utils::OutOfMemoryException &failure) {
            utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
            index.erase(skiplist_iter);  // TODO(composite_index): make this safe...only should only be called once
            *maybe_error.Lock() = std::move(failure);
          }
        }
      });
    }
  }
  if (maybe_error.Lock()->has_value()) {
    throw utils::OutOfMemoryException((*maybe_error.Lock())->what());
  }
}

// Helper function that determines, if a transaction has an original start timestamp
// (for example in a periodic commit when it is necessary to preserve initial index iterators)
// whether we are allowed to see the entity in the index data structures
// Returns true if we are allowed to see the entity in the index data structures
// If the method returns true, the reverts of the deltas will finally decide what's the version
// of the graph entity
inline bool CanSeeEntityWithTimestamp(uint64_t insertion_timestamp, Transaction *transaction, View view) {
  // Not enough information, need to rely on MVCC to fully check entry
  if (transaction->command_id != 0) return true;
  auto const original_start_timestamp = transaction->original_start_timestamp.value_or(transaction->start_timestamp);
  if (view == View::OLD) {
    return insertion_timestamp < original_start_timestamp;
  }
  return insertion_timestamp <= original_start_timestamp;
}

inline bool ValidateBounds(std::optional<utils::Bound<PropertyValue>> &lower_bound,
                           std::optional<utils::Bound<PropertyValue>> &upper_bound) {
  // Handle the bounds that the user provided to us. If the user
  // provided only one bound we should make sure that only values of that type
  // are returned by the iterator. We ensure this by supplying either an
  // inclusive lower bound of the same type, or an exclusive upper bound of the
  // following type. If neither bound is set we yield all items in the index.
  // Remove any bounds that are set to `Null` because that isn't a valid value.
  if (lower_bound && lower_bound->value().IsNull()) {
    lower_bound = std::nullopt;
  }
  if (upper_bound && upper_bound->value().IsNull()) {
    upper_bound = std::nullopt;
  }

  // Check whether the bounds are of comparable types if both are supplied.
  if (lower_bound && upper_bound) {
    if (!AreComparableTypes(lower_bound->value().type(), upper_bound->value().type()) ||
        lower_bound->value() > upper_bound->value()) {
      return false;
    }
  }
  // Set missing bounds.
  if (lower_bound && !upper_bound) {
    upper_bound = UpperBoundForType(lower_bound->value().type());
  }
  if (upper_bound && !lower_bound) {
    lower_bound = LowerBoundForType(upper_bound->value().type());
  }
  return true;
}

using LowerAndUpperBounds =
    std::tuple<std::optional<utils::Bound<PropertyValue>>, std::optional<utils::Bound<PropertyValue>>, bool>;
inline auto MakeBoundsFromRange(PropertyValueRange const &range) -> LowerAndUpperBounds {
  std::optional<utils::Bound<PropertyValue>> lower_bound;
  std::optional<utils::Bound<PropertyValue>> upper_bound;

  if (range.type_ == PropertyRangeType::INVALID) {
    return {std::nullopt, std::nullopt, false};
  } else if (range.type_ == PropertyRangeType::IS_NOT_NULL) {
    lower_bound = LowerBoundForType(PropertyValueType::Bool);
  } else if (range.type_ == PropertyRangeType::BOUNDED) {
    // We have to fix the bounds that the user provided to us. If the user
    // provided only one bound we should make sure that only values of that type
    // are returned by the iterator. We ensure this by supplying either an
    // inclusive lower bound of the same type, or an exclusive upper bound of the
    // following type. If neither bound is set we yield all items in the index.
    lower_bound = std::move(range.lower_);
    upper_bound = std::move(range.upper_);

    // Remove any bounds that are set to `Null` because that isn't a valid value.
    if (lower_bound && lower_bound->value().IsNull()) {
      lower_bound = std::nullopt;
    }
    if (upper_bound && upper_bound->value().IsNull()) {
      upper_bound = std::nullopt;
    }

    auto const are_comparable_ranges = [](auto const &lower_bound, auto const &upper_bound) {
      if (AreComparableTypes(lower_bound.value().type(), upper_bound.value().type())) {
        return true;
      } else if (upper_bound.IsInclusive()) {
        return false;
      } else {
        auto const upper_bound_for_lower_bound_type = storage::UpperBoundForType(lower_bound.value().type());
        return upper_bound_for_lower_bound_type && upper_bound.value() == upper_bound_for_lower_bound_type->value();
      };
    };

    // If both bounds are set, but are incomparable types, then this is an
    // invalid range and will yield an empty result set.
    if (lower_bound && upper_bound && !are_comparable_ranges(*lower_bound, *upper_bound)) {
      return {std::nullopt, std::nullopt, false};
    }

    // Set missing bounds.
    if (lower_bound && !upper_bound) {
      // Here we need to supply an upper bound. The upper bound is set to an
      // exclusive lower bound of the following type.
      upper_bound = UpperBoundForType(lower_bound->value().type());
    }

    if (upper_bound && !lower_bound) {
      // Here we need to supply a lower bound. The lower bound is set to an
      // inclusive lower bound of the current type.
      lower_bound = LowerBoundForType(upper_bound->value().type());
    }
  }

  return {std::move(lower_bound), std::move(upper_bound), true};
}

inline bool ValidateBounds(const std::span<PropertyValueRange const> &ranges,
                           std::vector<std::optional<utils::Bound<PropertyValue>>> &lower_bound,
                           std::vector<std::optional<utils::Bound<PropertyValue>>> &upper_bound) {
  // Handle the range to bounds conversion
  lower_bound.reserve(ranges.size());
  upper_bound.reserve(ranges.size());

  for (auto &&range : ranges) {
    auto [lb, ub, valid] = MakeBoundsFromRange(range);
    if (!valid) {
      lower_bound.clear();
      upper_bound.clear();
      return false;
    }
    lower_bound.emplace_back(std::move(lb));
    upper_bound.emplace_back(std::move(ub));
  }
  return true;
}

inline std::optional<std::vector<PropertyValue>> GenerateBounds(
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &bounds, const PropertyValue &default_value) {
  if (ranges::any_of(bounds, [](auto &&ub) { return ub.has_value(); })) {
    return bounds | ranges::views::transform([&default_value](auto &&bound) -> storage::PropertyValue {
             if (bound) {
               return bound->value();
             }
             return default_value;
           }) |
           ranges::to_vector;
  }
  return std::nullopt;
}

template <typename T>
inline void RechunkIndex(typename T::ChunkCollection &chunks, auto &&compare_fn) {
  // Index can have duplicate vertex entries, we need to make sure each unique vertex is inside a single chunk.
  // Chunks are divided at the skiplist level, we need to move each adjacent chunk's star/end to valid entries
  for (int i = 1; i < chunks.size(); ++i) {
    auto &chunk = chunks[i];
    auto begin = chunk.begin();
    auto end = chunk.end();
    auto null = typename T::ChunkedIterator{};
    // Special case where whole chunk is invalid
    if (begin != null && end != null && compare_fn(*begin, *end)) [[unlikely]] {
      auto &prev_chunk = chunks[i - 1];
      prev_chunk = typename T::Chunk{prev_chunk.begin(), end};
      chunks.erase(chunks.begin() + i);
      --i;
      continue;
    }
    // Since skiplist has only forward links, we cannot check if the previous vertex is the same as the current one.
    // We need to iterate through the chunk to find the first valid vertex.
    auto prev_v = begin;
    while (begin != end) {
      if (!compare_fn(*prev_v, *begin)) break;
      prev_v = begin;
      ++begin;
    }
    // Update
    auto &prev = chunks[i - 1];
    prev = typename T::Chunk{prev.begin(), begin};
    chunk = typename T::Chunk{begin, end};
  }
}

}  // namespace memgraph::storage
