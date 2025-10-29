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
    has_label = utils::Contains(vertex.labels, label);
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
                                                                case Delta::Action::SET_VECTOR_PROPERTY:
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
          case Delta::Action::SET_VECTOR_PROPERTY:
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

}  // namespace memgraph::storage
