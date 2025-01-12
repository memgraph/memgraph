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

#include <thread>
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

template <Delta::Action... actions>
struct ActionSet {
  constexpr bool contains(Delta::Action action) const { return ((action == actions) || ...); }
};

namespace {

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

}  // namespace

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
      ActionSet<Delta::Action::ADD_LABEL, Delta::Action::REMOVE_LABEL, Delta::Action::RECREATE_OBJECT,
                Delta::Action::DELETE_DESERIALIZED_OBJECT, Delta::Action::DELETE_OBJECT>{};
  return AnyVersionSatisfiesPredicate<interesting>(timestamp, delta, [&has_label, &deleted, label](const Delta &delta) {
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

/// Helper function for label-property index garbage collection. Returns true if
/// there's a reachable version of the vertex that has the given label and
/// property value.
inline bool AnyVersionHasLabelProperty(const Vertex &vertex, LabelId label, PropertyId key, const PropertyValue &value,
                                       uint64_t timestamp) {
  Delta const *delta;
  bool deleted;
  bool has_label;
  bool current_value_equal_to_value;
  {
    auto guard = std::shared_lock{vertex.lock};
    delta = vertex.delta;
    deleted = vertex.deleted;
    has_label = utils::Contains(vertex.labels, label);
    // Avoid IsPropertyEqual if already not possible
    if (delta == nullptr && (deleted || !has_label)) return false;
    current_value_equal_to_value = vertex.properties.IsPropertyEqual(key, value);
  }

  if (!deleted && has_label && current_value_equal_to_value) {
    return true;
  }

  constexpr auto interesting = ActionSet<Delta::Action::ADD_LABEL, Delta::Action::REMOVE_LABEL,
                                         Delta::Action::SET_PROPERTY, Delta::Action::RECREATE_OBJECT,
                                         Delta::Action::DELETE_DESERIALIZED_OBJECT, Delta::Action::DELETE_OBJECT>{};
  return AnyVersionSatisfiesPredicate<interesting>(
      timestamp, delta, [&has_label, &current_value_equal_to_value, &deleted, label, key, &value](const Delta &delta) {
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
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
        return !deleted && has_label && current_value_equal_to_value;
      });
}

inline bool AnyVersionHasLabelProperties(const Vertex &vertex, LabelId label, const std::vector<PropertyId> &properties,
                                         const std::vector<PropertyValue> &values, uint64_t timestamp) {
  Delta const *delta;
  bool deleted;
  bool has_label;
  std::unordered_map<PropertyId, bool> equality_values;
  std::unordered_map<PropertyId, uint64_t> equality_indices;

  auto initialize_maps = [&vertex, &properties, &values, &equality_values, &equality_indices]() {
    for (uint64_t i = 0; i < properties.size(); i++) {
      equality_indices[properties[i]] = i;
      if (vertex.properties.IsPropertyEqual(properties[i], values[i])) {
        equality_values[properties[i]] = true;
      } else {
        equality_values[properties[i]] = false;
      }
    }
  };

  auto equality_valid = [](const auto &map) -> bool {
    return std::all_of(map.begin(), map.end(), [](const auto &pair) { return pair.second; });
  };

  {
    auto guard = std::shared_lock{vertex.lock};
    delta = vertex.delta;
    deleted = vertex.deleted;
    has_label = utils::Contains(vertex.labels, label);
    // Avoid IsPropertyEqual if already not possible
    if (delta == nullptr && (deleted || !has_label)) return false;
    initialize_maps();
  }

  if (!deleted && has_label && equality_valid(equality_values)) {
    return true;
  }

  constexpr auto interesting = ActionSet<Delta::Action::ADD_LABEL, Delta::Action::REMOVE_LABEL,
                                         Delta::Action::SET_PROPERTY, Delta::Action::RECREATE_OBJECT,
                                         Delta::Action::DELETE_DESERIALIZED_OBJECT, Delta::Action::DELETE_OBJECT>{};
  return AnyVersionSatisfiesPredicate<interesting>(
      timestamp, delta,
      [&has_label, &equality_values, &equality_indices, &equality_valid, &deleted, label, &values](const Delta &delta) {
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
          case Delta::Action::SET_PROPERTY:
            if (equality_values.find(delta.property.key) != equality_values.end()) {
              equality_values[delta.property.key] =
                  *delta.property.value == values[equality_indices[delta.property.key]];
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
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
        return !deleted && has_label && equality_valid(equality_values);
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

  constexpr auto interesting = ActionSet<Delta::Action::SET_PROPERTY, Delta::Action::RECREATE_OBJECT,
                                         Delta::Action::DELETE_DESERIALIZED_OBJECT, Delta::Action::DELETE_OBJECT>{};
  return AnyVersionSatisfiesPredicate<interesting>(
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
inline bool CurrentVersionHasLabelProperty(const Vertex &vertex, LabelId label, PropertyId key,
                                           const PropertyValue &value, Transaction *transaction, View view) {
  bool exists = true;
  bool deleted = false;
  bool has_label = false;
  bool current_value_equal_to_value = value.IsNull();
  const Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex.lock};
    deleted = vertex.deleted;
    has_label = utils::Contains(vertex.labels, label);
    current_value_equal_to_value = vertex.properties.IsPropertyEqual(key, value);
    delta = vertex.delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction->manyDeltasCache;
      if (auto resError = HasError(view, cache, &vertex, false); resError) return false;
      auto resLabel = cache.GetHasLabel(view, &vertex, label);
      if (resLabel && *resLabel) {
        auto resProp = cache.GetProperty(view, &vertex, key);
        if (resProp && resProp->get() == value) return true;
      }
    }

    auto const n_processed = ApplyDeltasForRead(transaction, delta, view, [&, label, key](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Deleted_ActionMethod(deleted),
        Exists_ActionMethod(exists),
        HasLabel_ActionMethod(has_label, label),
        PropertyValueMatch_ActionMethod(current_value_equal_to_value, key,value)
      });
      // clang-format on
    });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction->manyDeltasCache;
      cache.StoreExists(view, &vertex, exists);
      cache.StoreDeleted(view, &vertex, deleted);
      cache.StoreHasLabel(view, &vertex, label, has_label);
      if (current_value_equal_to_value) {
        cache.StoreProperty(view, &vertex, key, value);
      }
    }
  }

  return exists && !deleted && has_label && current_value_equal_to_value;
}

inline bool CurrentVersionHasLabelProperties(const Vertex &vertex, LabelId label,
                                             const std::vector<PropertyId> &properties,
                                             const std::vector<PropertyValue> &values, Transaction *transaction,
                                             View view) {
  bool exists = true;
  bool deleted = false;
  bool has_label = false;
  const Delta *delta = nullptr;
  std::unordered_map<PropertyId, bool> equality_values;
  std::unordered_map<PropertyId, PropertyValue> property_values;

  auto initialize_maps = [&equality_values, &property_values, &vertex, &properties, &values]() {
    for (uint64_t i = 0; i < properties.size(); i++) {
      equality_values[properties[i]] = vertex.properties.IsPropertyEqual(properties[i], values[i]);
      property_values[properties[i]] = values[i];
    }
  };

  auto all_true_func = [](const auto &map) -> bool {
    return std::all_of(map.begin(), map.end(), [](const auto &pair) { return pair.second; });
  };

  bool are_all_properties_equal = true;
  {
    auto guard = std::shared_lock{vertex.lock};
    deleted = vertex.deleted;
    has_label = utils::Contains(vertex.labels, label);
    initialize_maps();
    are_all_properties_equal = all_true_func(equality_values);
    delta = vertex.delta;
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION

    auto const useCache = transaction->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction->manyDeltasCache;
      if (auto resError = HasError(view, cache, &vertex, false); resError) return false;
      auto resLabel = cache.GetHasLabel(view, &vertex, label);
      if (resLabel && *resLabel) {
        bool all_match =
            std::ranges::all_of(ranges::views::zip(properties, values), [&cache, view, &vertex](const auto &pair) {
              const auto &[key, value] = pair;
              auto resProp = cache.GetProperty(view, &vertex, key);
              return resProp && resProp->get() == value;
            });
        if (all_match) {
          return true;
        }
      }
    }

    auto const n_processed = ApplyDeltasForRead(transaction, delta, view, [&, label](const Delta &delta) {
      // clang-format off

      DeltaDispatch(delta, utils::ChainedOverloaded{
        Deleted_ActionMethod(deleted),
        Exists_ActionMethod(exists),
        HasLabel_ActionMethod(has_label, label),
        PropertyValuesMatch_ActionMethod(equality_values, property_values),
      });

      // clang-format on
    });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction->manyDeltasCache;
      cache.StoreExists(view, &vertex, exists);
      cache.StoreDeleted(view, &vertex, deleted);
      cache.StoreHasLabel(view, &vertex, label, has_label);
      if (are_all_properties_equal) {
        for (const auto &[key, value] : ranges::views::zip(properties, values)) {
          cache.StoreProperty(view, &vertex, key, value);
        }
      }
    }
  }

  return exists && !deleted && has_label && are_all_properties_equal;
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
    auto const n_processed = ApplyDeltasForRead(transaction, delta, view, [&, key](const Delta &delta) {
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

template <typename TIndexAccessor>
inline void TryInsertLabelIndex(Vertex &vertex, LabelId label, TIndexAccessor &index_accessor) {
  if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
    return;
  }

  index_accessor.insert({&vertex, 0});
}

template <typename TIndexAccessor>
inline void TryInsertLabelPropertyIndex(Vertex &vertex, std::pair<LabelId, PropertyId> label_property_pair,
                                        TIndexAccessor &index_accessor) {
  if (vertex.deleted || !utils::Contains(vertex.labels, label_property_pair.first)) {
    return;
  }
  auto value = vertex.properties.GetProperty(label_property_pair.second);
  if (value.IsNull()) {
    return;
  }
  index_accessor.insert({std::move(value), &vertex, 0});
}

template <typename TIndexAccessor>
inline void TryInsertLabelPropertyCompositeIndex(
    Vertex &vertex, std::pair<LabelId, std::vector<PropertyId>> label_property_composite_pair,
    TIndexAccessor &index_accessor) {
  if (vertex.deleted || !utils::Contains(vertex.labels, label_property_composite_pair.first)) {
    return;
  }
  std::vector<PropertyValue> values;
  values.reserve(label_property_composite_pair.second.size());
  for (const auto &property_id : label_property_composite_pair.second) {
    auto value = vertex.properties.GetProperty(property_id);
    if (value.IsNull()) {
      return;
    }
    values.emplace_back(std::move(value));
  }
  index_accessor.insert({std::move(values), &vertex, 0});
}

template <typename TSkiplistIter, typename TIndex, typename TIndexKey, typename TFunc>
inline void CreateIndexOnSingleThread(utils::SkipList<Vertex>::Accessor &vertices, TSkiplistIter it, TIndex &index,
                                      TIndexKey key, const TFunc &func) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  try {
    auto acc = it->second.access();
    for (Vertex &vertex : vertices) {
      func(vertex, key, acc);
    }
  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    index.erase(it);
    throw;
  }
}

template <typename TIndex, typename TIndexKey, typename TSKiplistIter, typename TFunc>
inline void CreateIndexOnMultipleThreads(utils::SkipList<Vertex>::Accessor &vertices, TSKiplistIter skiplist_iter,
                                         TIndex &index, TIndexKey key,
                                         const durability::ParallelizedSchemaCreationInfo &parallel_exec_info,
                                         const TFunc &func) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  const auto &vertex_batches = parallel_exec_info.vertex_recovery_info;
  const auto thread_count = std::min(parallel_exec_info.thread_count, vertex_batches.size());

  MG_ASSERT(!vertex_batches.empty(),
            "The size of batches should always be greater than zero if you want to use the parallel version of index "
            "creation!");

  std::atomic<uint64_t> batch_counter = 0;

  utils::Synchronized<std::optional<utils::OutOfMemoryException>, utils::SpinLock> maybe_error{};
  {
    std::vector<std::jthread> threads;
    threads.reserve(thread_count);

    for (auto i{0U}; i < thread_count; ++i) {
      threads.emplace_back(
          [&skiplist_iter, &func, &index, &vertex_batches, &maybe_error, &batch_counter, &key, &vertices]() {
            while (!maybe_error.Lock()->has_value()) {
              const auto batch_index = batch_counter++;
              if (batch_index >= vertex_batches.size()) {
                return;
              }
              const auto &batch = vertex_batches[batch_index];
              auto index_accessor = index.at(key).access();
              auto it = vertices.find(batch.first);

              try {
                for (auto i{0U}; i < batch.second; ++i, ++it) {
                  func(*it, key, index_accessor);
                }

              } catch (utils::OutOfMemoryException &failure) {
                utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
                index.erase(skiplist_iter);
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
inline bool CanSeeEntityWithTimestamp(uint64_t insertion_timestamp, Transaction *transaction) {
  if (!transaction->original_start_timestamp.has_value()) {
    return true;
  }

  return insertion_timestamp < transaction->original_start_timestamp.value();
}

}  // namespace memgraph::storage
