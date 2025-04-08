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

#include <thread>
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_constants.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

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

  constexpr auto interesting =
      details::ActionSet<Delta::Action::ADD_LABEL, Delta::Action::REMOVE_LABEL, Delta::Action::SET_PROPERTY,
                         Delta::Action::RECREATE_OBJECT, Delta::Action::DELETE_DESERIALIZED_OBJECT,
                         Delta::Action::DELETE_OBJECT>{};
  return details::AnyVersionSatisfiesPredicate<interesting>(
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

inline bool AnyVersionHasProperty(const Edge &edge, std::pair<EdgeTypeId, PropertyId> key, const PropertyValue &value,
                                  uint64_t timestamp) {
  return AnyVersionHasProperty(edge, key.second, value, timestamp);
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

template <typename TSkiplistIter, typename TIndex, typename TIndexKey, typename TFunc>
inline void CreateIndexOnSingleThread(utils::SkipList<Vertex>::Accessor &vertices, TSkiplistIter it, TIndex &index,
                                      TIndexKey key, const TFunc &func,
                                      std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  try {
    auto acc = it->second.access();
    for (Vertex &vertex : vertices) {
      func(vertex, key, acc);
      if (snapshot_info) {
        snapshot_info->Update(UpdateType::VERTICES);
      }
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
                                         const TFunc &func,
                                         std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) {
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
      threads.emplace_back([&skiplist_iter, &func, &index, &vertex_batches, &maybe_error, &batch_counter, &key,
                            &vertices, &snapshot_info]() mutable {
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
              if (snapshot_info) {
                snapshot_info->Update(UpdateType::VERTICES);
              }
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
inline bool CanSeeEntityWithTimestamp(uint64_t insertion_timestamp, Transaction *transaction, View view) {
  // Not enough information, need to rely on MVCC to fully check entry
  if (transaction->command_id != 0) return true;
  auto const original_start_timestamp = transaction->original_start_timestamp.value_or(transaction->start_timestamp);
  if (view == View::OLD) {
    return insertion_timestamp < original_start_timestamp;
  }
  return insertion_timestamp <= original_start_timestamp;
}

inline std::optional<utils::Bound<PropertyValue>> MaxBound(PropertyValue::Type type) {
  switch (type) {
    case PropertyValue::Type::Null:
      // This shouldn't happen because of the nullopt-ing above.
      LOG_FATAL("Invalid database state!");
      break;
    case PropertyValue::Type::Bool:
      return utils::MakeBoundExclusive(kSmallestNumber);
    case PropertyValue::Type::Int:
    case PropertyValue::Type::Double:
      // Both integers and doubles are treated as the same type in
      // `PropertyValue` and they are interleaved when sorted.
      return utils::MakeBoundExclusive(kSmallestString);
    case PropertyValue::Type::String:
      return utils::MakeBoundExclusive(kSmallestList);
    case PropertyValue::Type::List:
      return utils::MakeBoundExclusive(kSmallestMap);
    case PropertyValue::Type::Map:
      return utils::MakeBoundExclusive(kSmallestTemporalData);
    case PropertyValue::Type::TemporalData:
      return utils::MakeBoundExclusive(kSmallestZonedTemporalData);
    case PropertyValue::Type::ZonedTemporalData:
      return utils::MakeBoundExclusive(kSmallestEnum);
    case PropertyValue::Type::Enum:
      return utils::MakeBoundExclusive(kSmallestPoint2d);
    case PropertyValue::Type::Point2d:
      return utils::MakeBoundExclusive(kSmallestPoint3d);
    case PropertyValue::Type::Point3d:
      // This is the last type in the order so we leave the upper bound empty.
      break;
  }
  return std::nullopt;
}

inline std::optional<utils::Bound<PropertyValue>> MinBound(PropertyValue::Type type) {
  switch (type) {
    case PropertyValue::Type::Null:
      // This shouldn't happen because of the nullopt-ing above.
      LOG_FATAL("Invalid database state!");
      break;
    case PropertyValue::Type::Bool:
      return utils::MakeBoundInclusive(kSmallestBool);
    case PropertyValue::Type::Int:
    case PropertyValue::Type::Double:
      // Both integers and doubles are treated as the same type in
      // `PropertyValue` and they are interleaved when sorted.
      return utils::MakeBoundInclusive(kSmallestNumber);
    case PropertyValue::Type::String:
      return utils::MakeBoundInclusive(kSmallestString);
    case PropertyValue::Type::List:
      return utils::MakeBoundInclusive(kSmallestList);
    case PropertyValue::Type::Map:
      return utils::MakeBoundInclusive(kSmallestMap);
    case PropertyValue::Type::TemporalData:
      return utils::MakeBoundInclusive(kSmallestTemporalData);
    case PropertyValue::Type::ZonedTemporalData:
      return utils::MakeBoundInclusive(kSmallestZonedTemporalData);
    case PropertyValue::Type::Enum:
      return utils::MakeBoundInclusive(kSmallestEnum);
    case PropertyValue::Type::Point2d:
      return utils::MakeBoundExclusive(kSmallestPoint2d);
    case PropertyValue::Type::Point3d:
      return utils::MakeBoundExclusive(kSmallestPoint3d);
  }
  return std::nullopt;
}

inline bool IsLowerBound(const PropertyValue &value, std::optional<utils::Bound<PropertyValue>> const &bound) {
  if (bound) {
    if (value < bound->value()) {
      return false;
    }
    if (!bound->IsInclusive() && value == bound->value()) {
      return false;
    }
  }
  return true;
}

inline bool IsUpperBound(const PropertyValue &value, std::optional<utils::Bound<PropertyValue>> const &bound) {
  if (bound) {
    if (bound->value() < value) {
      return false;
    }
    if (!bound->IsInclusive() && value == bound->value()) {
      return false;
    }
  }
  return true;
}

inline void EdgePropertyIndexRemoveObsoleteEntries(auto &indices, uint64_t oldest_active_start_timestamp,
                                                   std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter<2048>();

  for (auto &specific_index : indices) {
    if (token.stop_requested()) return;

    auto edges_acc = specific_index.second.access();
    for (auto it = edges_acc.begin(); it != edges_acc.end();) {
      if (maybe_stop() && token.stop_requested()) return;

      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      const bool vertices_deleted = it->from_vertex->deleted || it->to_vertex->deleted;
      const bool edge_deleted = it->edge->deleted;
      const bool has_next = next_it != edges_acc.end();

      // When we update specific entries in the index, we don't delete the previous entry.
      // The way they are removed from the index is through this check. The entries should
      // be right next to each other(in terms of iterator semantics) and the older one
      // should be removed here.
      const bool redundant_duplicate = has_next && it->value == next_it->value &&
                                       it->from_vertex == next_it->from_vertex && it->to_vertex == next_it->to_vertex &&
                                       it->edge == next_it->edge;
      if (redundant_duplicate || vertices_deleted || edge_deleted ||
          !AnyVersionHasProperty(*it->edge, specific_index.first, it->value, oldest_active_start_timestamp)) {
        edges_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

}  // namespace memgraph::storage
