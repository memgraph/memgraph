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

#include "storage/v2/schema_info.hpp"

#include <atomic>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/node_hash_map.h"
#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schema_info_types.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "utils/conccurent_unordered_map.hpp"
#include "utils/logging.hpp"
#include "utils/small_vector.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::storage {
namespace {

template <typename Key, typename Tp, typename Predicate>
inline auto erase_if(memgraph::utils::ConcurrentUnorderedMap<Key, Tp> &cont, Predicate &&pred) {
  return cont.erase_if(std::forward<Predicate>(pred));
}

template <typename Key, typename Tp, typename Predicate>
inline auto erase_if(absl::node_hash_map<Key, Tp> &cont, Predicate &&pred) {
  return absl::erase_if(cont, std::forward<Predicate>(pred));
}

/// This function iterates through the undo buffers from an object (starting
/// from the supplied delta) and determines what deltas should be applied to get
/// the version of the object just before delta_fin. The function applies the delta
/// via the callback function passed as a parameter to the callback. It is up to the
/// caller to apply the deltas.
template <typename TCallback>
inline void ApplyDeltasForRead(const Delta *delta, uint64_t current_commit_timestamp, const TCallback &callback) {
  while (delta) {
    const auto ts = delta->timestamp->load(std::memory_order_acquire);
    // We need to stop if:
    //  there are no other deltas
    //  the timestamp is a commited timestamp
    //  the timestamp is the current commit timestamp
    if (ts == current_commit_timestamp || ts < kTransactionInitialId) break;
    // This delta must be applied, call the callback.
    callback(*delta);
    // Move to the next delta in this transaction.
    delta = delta->next.load(std::memory_order_acquire);
  }
}

template <typename TCallback>
inline void ApplyUncommittedDeltasForRead(const Delta *delta, const TCallback &callback) {
  while (delta) {
    // Break when delta is not part of a transaction (its timestamp is not equal to a transaction id)
    if (delta->timestamp->load(std::memory_order_relaxed) < kTransactionInitialId) break;
    // This delta must be applied, call the callback.
    callback(*delta);
    // Move to the next delta in this transaction.
    delta = delta->next.load(std::memory_order_acquire);
  }
}

inline void ApplyDeltasForRead(const Delta *delta, uint64_t current_commit_timestamp, auto &&uncommitted_cb,
                               auto &&committed_cb) {
  while (delta) {
    const auto ts = delta->timestamp->load(std::memory_order_acquire);
    // Break when delta is not part of a transaction (its timestamp is not equal to a transaction id)
    if (ts < kTransactionInitialId) break;

    if (ts != current_commit_timestamp) {
      // Apply deltas that are not part of this tx
      committed_cb(*delta);
    }

    // As long as there are uncommitted deltas, apply cb
    uncommitted_cb(*delta);

    // Move to the next delta in this transaction.
    delta = delta->next.load(std::memory_order_acquire);
  }
}

inline auto PropertyTypes_ActionMethod(std::map<PropertyId, ExtendedPropertyType> &properties) {
  using enum Delta::Action;
  return ActionMethod<SET_PROPERTY>([&](Delta const &delta) {
    auto it = properties.find(delta.property.key);
    if (it != properties.end()) {
      if (delta.property.value->IsNull()) {
        // remove the property
        properties.erase(it);
      } else {
        // set the value
        it->second = ExtendedPropertyType{*delta.property.value};
      }
    } else if (!delta.property.value->IsNull()) {
      properties.emplace(delta.property.key, delta.property.value->type());
    }
  });
}

inline std::pair<const utils::small_vector<LabelId> &, const utils::small_vector<LabelId> &> GetLabels(
    const Vertex &vertex, uint64_t commit_timestamp, auto &uncommitted_cache, auto &committed_cache) {
  if (!vertex.delta) return {vertex.labels, vertex.labels};

  auto uncommitted_cache_it = uncommitted_cache.find(&vertex);
  auto committed_cache_it = committed_cache.find(&vertex);

  if (uncommitted_cache_it != uncommitted_cache.end() && committed_cache_it != committed_cache.end()) {
    return {uncommitted_cache_it->second, committed_cache_it->second};
  }

  auto uncommitted_labels = vertex.labels;
  auto committed_labels = vertex.labels;
  ApplyDeltasForRead(
      vertex.delta, commit_timestamp,
      [&uncommitted_labels](const Delta &delta) {
        // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Labels_ActionMethod(uncommitted_labels)
      });
        // clang-format on
      },
      [&committed_labels](const Delta &delta) {
        // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Labels_ActionMethod(committed_labels)
      });
        // clang-format on
      });

  auto [uncommitted_it, ub] = uncommitted_cache.emplace(&vertex, std::move(uncommitted_labels));
  auto [committed_it, cb] = committed_cache.emplace(&vertex, std::move(committed_labels));
  return {uncommitted_it->second, committed_it->second};
}

std::map<PropertyId, ExtendedPropertyType> GetCommittedProperty(const Edge &edge) {
  auto lock = std::shared_lock{edge.lock};
  auto props = edge.properties.ExtendedPropertyTypes();
  if (edge.delta) {
    ApplyUncommittedDeltasForRead(edge.delta, [&props](const Delta &delta) {
      // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          PropertyTypes_ActionMethod(props)
        });
      // clang-format on
    });
  }
  return props;
}

}  // namespace

template <>
TrackingInfo<absl::node_hash_map> &LocalSchemaTracking::edge_lookup(const EdgeKeyRef &key) {
  auto itr = edge_state_.find(key);
  if (itr != edge_state_.end()) return itr->second;
  auto [new_itr, _] = edge_state_.emplace(std::piecewise_construct, std::forward_as_tuple(key.type, key.from, key.to),
                                          std::make_tuple());
  return new_itr->second;
}

template <>
TrackingInfo<utils::ConcurrentUnorderedMap> &SharedSchemaTracking::edge_lookup(const EdgeKeyRef &key) {
  return edge_state_[key];
}

template <template <class...> class TContainer>
template <template <class...> class TOtherContainer>
void SchemaTracking<TContainer>::ProcessTransaction(
    const SchemaTracking<TOtherContainer> &diff, absl::flat_hash_set<SchemaInfoPostProcess> &post_process,
    absl::flat_hash_map<Edge *, absl::flat_hash_map<PropertyId, std::pair<ExtendedPropertyType, ExtendedPropertyType>>>
        &edge_prop_change,
    uint64_t commit_ts, bool property_on_edges) {
  // Update schema based on the diff
  {
    auto l = vertex_state_.get_lock();
    for (const auto &[vertex_key, info] : diff.vertex_state_) {
      vertex_state_.bulk_get_or_insert(vertex_key) += info;
    }
  }
  {
    auto l = edge_state_.get_lock();
    for (const auto &[edge_key, info] : diff.edge_state_) {
      edge_state_.bulk_get_or_insert(edge_key) += info;
    }
  }

  absl::flat_hash_map<const Vertex *, utils::small_vector<LabelId>> uncommitted_labels_cache;
  absl::flat_hash_map<const Vertex *, utils::small_vector<LabelId>> committed_labels_cache;

  // Post process (edge updates)
  for (const auto &[edge_ref, edge_type, from, to, edge_deleted] : post_process) {
    auto from_lock = std::shared_lock{from->lock, std::defer_lock};
    auto to_lock = std::shared_lock{to->lock, std::defer_lock};

    if (to == from) {
      from_lock.lock();
    } else if (to->gid < from->gid) {
      to_lock.lock();
      from_lock.lock();
    } else {
      from_lock.lock();
      to_lock.lock();
    }

    if (committed_labels_cache.size() + 2 >= committed_labels_cache.capacity())
      committed_labels_cache.reserve(committed_labels_cache.size() * 2);
    if (uncommitted_labels_cache.size() + 2 >= uncommitted_labels_cache.capacity())
      uncommitted_labels_cache.reserve(uncommitted_labels_cache.size() * 2);

    const auto from_labels = GetLabels(*from, commit_ts, uncommitted_labels_cache, committed_labels_cache);
    const auto to_labels = std::invoke([&] {
      if (to == from) return from_labels;
      return GetLabels(*to, commit_ts, uncommitted_labels_cache, committed_labels_cache);
    });

    // Can't lock once before loop because of possible deadlock from vertex
    auto pre_key = EdgeKeyRef{edge_type, from_labels.second, to_labels.second};
    auto post_key = EdgeKeyRef{edge_type, from_labels.first, to_labels.first};
    auto *tracking_post_info = &edge_lookup(EdgeKeyRef{edge_type, from_labels.first, to_labels.first});
    decltype(tracking_post_info) tracking_pre_info{};
    if (pre_key == post_key)
      tracking_pre_info = tracking_post_info;
    else if (!edge_deleted)
      tracking_pre_info = &edge_lookup(EdgeKeyRef{edge_type, from_labels.second, to_labels.second});

    from_lock.unlock();
    if (to_lock.owns_lock()) to_lock.unlock();

    // Step 1: Move committed stats in case edge identification changed
    if (edge_deleted || tracking_pre_info != tracking_post_info) {
      --tracking_pre_info->n;
      if (!edge_deleted) ++tracking_post_info->n;
      if (property_on_edges) {
        for (const auto &[key, type] : GetCommittedProperty(*edge_ref.ptr)) {
          tracking_pre_info->Decrement(key, type);
          if (!edge_deleted) {
            tracking_post_info->Increment(key, type);
          }
        }
      }
    }

    // Step 2: Update any property changes from this tx while referencing new edge identification
    if (property_on_edges && !edge_deleted) {
      for (const auto &[key, diff] : edge_prop_change[edge_ref.ptr]) {
        if (diff.second == diff.first) continue;  // Nothing to do
        auto &info = tracking_post_info->properties[key];
        // Then
        if (diff.second == ExtendedPropertyType{}) {
          // No value <=> new property
          ++info.n;
        } else {
          --info.types[diff.second];
        }
        // Now
        if (diff.first == ExtendedPropertyType{}) {
          // No value <=> removed property
          --info.n;
        } else {
          ++info.types[diff.first];
        }
      }
    }
  }

  // Clean up unused stats
  auto stats_cleanup = [](auto &info) {
    erase_if(info, [](auto &elem) { return elem.second.n <= 0; });
    for (auto &[_, val] : info) {
      erase_if(val.properties, [](auto &elem) { return elem.second.n <= 0; });
      for (auto &[_, val] : val.properties) {
        erase_if(val.types, [](auto &elem) { return elem.second <= 0; });
      }
    }
  };
  stats_cleanup(vertex_state_);
  stats_cleanup(edge_state_);
}

template <template <class...> class TContainer>
nlohmann::json SchemaTracking<TContainer>::ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const {
  auto json = nlohmann::json::object();

  // Handle NODES
  const auto &[nodes_itr, _] = json.emplace("nodes", nlohmann::json::array());
  auto &nodes = nodes_itr.value();
  for (const auto &[labels, info] : vertex_state_) {
    auto node = nlohmann::json::object();
    const auto &[labels_itr, _] = node.emplace("labels", nlohmann::json::array_t{});
    for (const auto labelId : labels) {
      labels_itr->emplace_back(name_id_mapper.IdToName(labelId.AsUint()));
    }
    std::sort(labels_itr->begin(), labels_itr->end());
    node.update(info.ToJson(name_id_mapper, enum_store));
    nodes.emplace_back(std::move(node));
  }

  // Handle EDGES
  const auto &[edges_itr, dummy] = json.emplace("edges", nlohmann::json::array());
  auto &edges = edges_itr.value();
  for (const auto &[edge_type, info] : edge_state_) {
    auto edge = nlohmann::json::object();
    edge.emplace("type", name_id_mapper.IdToName(edge_type.type.AsUint()));
    const auto &[out_labels_itr, _] = edge.emplace("start_node_labels", nlohmann::json::array_t{});
    for (const auto labelId : edge_type.from) {
      out_labels_itr->emplace_back(name_id_mapper.IdToName(labelId.AsUint()));
    }
    std::sort(out_labels_itr->begin(), out_labels_itr->end());
    const auto &[in_labels_itr, _b] = edge.emplace("end_node_labels", nlohmann::json::array_t{});
    for (const auto labelId : edge_type.to) {
      in_labels_itr->emplace_back(name_id_mapper.IdToName(labelId.AsUint()));
    }
    std::sort(in_labels_itr->begin(), in_labels_itr->end());
    edge.update(info.ToJson(name_id_mapper, enum_store));
    edges.emplace_back(std::move(edge));
  }

  // Cleanup result (we must leave unused stats in analytical)
  auto stats_cleanup = [&json](const std::string &main_key) {
    erase_if(json[main_key].get_ref<nlohmann::json::array_t &>(), [](auto &elem) { return elem["count"] <= 0; });
    for (auto &val : json[main_key].get_ref<nlohmann::json::array_t &>()) {
      erase_if(val["properties"].get_ref<nlohmann::json::array_t &>(), [](auto &elem) { return elem["count"] <= 0; });
      for (auto &val : val["properties"].get_ref<nlohmann::json::array_t &>()) {
        erase_if(val["types"].get_ref<nlohmann::json::array_t &>(), [](auto &elem) { return elem["count"] <= 0; });
      }
    }
  };
  stats_cleanup("nodes");
  stats_cleanup("edges");

  return json;
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::DeleteVertex(Vertex *vertex) {
  auto &info = vertex_state_[vertex->labels];
  --info.n;
  for (const auto &[key, val] : vertex->properties.ExtendedPropertyTypes()) {
    info.Decrement(key, val);
  }
  // No edges should be present at this point
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                                              const utils::small_vector<LabelId> &new_labels) {
  // Move all stats to new labels
  auto &old_tracking = vertex_state_[old_labels];
  auto &new_tracking = vertex_state_[new_labels];
  --old_tracking.n;
  ++new_tracking.n;
  for (const auto &[property, type] : vertex->properties.ExtendedPropertyTypes()) {
    old_tracking.Decrement(property, type);
    new_tracking.Increment(property, type);
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                                              const utils::small_vector<LabelId> &new_labels, bool prop_on_edges) {
  // Update vertex stats
  UpdateLabels(vertex, old_labels, new_labels);
  // Update edge stats
  auto update_edge = [&](EdgeTypeId edge_type, EdgeRef edge_ref, Vertex *from, Vertex *to,
                         const utils::small_vector<LabelId> *old_from_labels,
                         const utils::small_vector<LabelId> *old_to_labels) {
    auto &old_tracking = edge_lookup(EdgeKeyRef(edge_type, old_from_labels ? *old_from_labels : from->labels,
                                                old_to_labels ? *old_to_labels : to->labels));
    auto &new_tracking = edge_lookup(EdgeKeyRef(edge_type, from->labels, to->labels));
    --old_tracking.n;
    ++new_tracking.n;
    if (prop_on_edges) {
      // No need for edge lock since all edge property operations are unique schema access
      for (const auto &[property, type] : edge_ref.ptr->properties.ExtendedPropertyTypes()) {
        old_tracking.Decrement(property, type);
        new_tracking.Increment(property, type);
      }
    }
  };

  for (const auto &[edge_type, other_vertex, edge_ref] : vertex->in_edges) {
    update_edge(edge_type, edge_ref, other_vertex, vertex, {}, &old_labels);
  }
  for (const auto &[edge_type, other_vertex, edge_ref] : vertex->out_edges) {
    update_edge(edge_type, edge_ref, vertex, other_vertex, &old_labels, {});
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) {
  auto &tracking_info = edge_lookup(EdgeKeyRef{edge_type, from->labels, to->labels});
  ++tracking_info.n;
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::DeleteEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to,
                                            bool prop_on_edges) {
  auto &tracking_info = edge_lookup(EdgeKeyRef{edge_type, from->labels, to->labels});
  --tracking_info.n;
  if (prop_on_edges) {
    for (const auto &[key, type] : edge.ptr->properties.ExtendedPropertyTypes()) {
      tracking_info.Decrement(key, type);
    }
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::SetProperty(auto &tracking_info, PropertyId property, const ExtendedPropertyType &now,
                                             const ExtendedPropertyType &before) {
  if (now == before) return;  // Nothing to do
  if (before != ExtendedPropertyType{}) {
    tracking_info.Decrement(property, before);
  }
  if (now != ExtendedPropertyType{}) {
    tracking_info.Increment(property, now);
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::Clear() {
  vertex_state_.clear();
  edge_state_.clear();
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::SetProperty(Vertex *vertex, PropertyId property, const ExtendedPropertyType &now,
                                             const ExtendedPropertyType &before) {
  auto &tracking_info = vertex_state_[vertex->labels];
  SetProperty(tracking_info, property, now, before);
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property,
                                             const ExtendedPropertyType &now, const ExtendedPropertyType &before,
                                             bool prop_on_edges) {
  if (prop_on_edges) {
    auto &tracking_info = edge_lookup(EdgeKeyRef{type, from->labels, to->labels});
    SetProperty(tracking_info, property, now, before);
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property,
                                             const ExtendedPropertyType &now, const ExtendedPropertyType &before,
                                             bool prop_on_edges, auto &&guard, auto &&other_guard) {
  if (prop_on_edges) {
    auto &tracking_info = edge_lookup(EdgeKeyRef{type, from->labels, to->labels});
    if (guard.owns_lock()) guard.unlock();
    if (other_guard.owns_lock()) other_guard.unlock();
    SetProperty(tracking_info, property, now, before);
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::UpdateEdgeStats(EdgeRef edge_ref, EdgeTypeId edge_type,
                                                 const VertexKey &new_from_labels, const VertexKey &new_to_labels,
                                                 const VertexKey &old_from_labels, const VertexKey &old_to_labels,
                                                 bool prop_on_edges) {
  // Lookup needs to happen while holding the locks, but the update itself does not
  auto &new_tracking = edge_lookup({edge_type, new_from_labels, new_to_labels});
  auto &old_tracking = edge_lookup({edge_type, old_from_labels, old_to_labels});
  UpdateEdgeStats(new_tracking, old_tracking, edge_ref, prop_on_edges);
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::UpdateEdgeStats(auto &new_tracking, auto &old_tracking, EdgeRef edge_ref,
                                                 bool prop_on_edges) {
  --old_tracking.n;
  ++new_tracking.n;

  if (prop_on_edges) {
    // No need for edge lock since all edge property operations are unique access
    for (const auto &[property, type] : edge_ref.ptr->properties.ExtendedPropertyTypes()) {
      old_tracking.Decrement(property, type);
      new_tracking.Increment(property, type);
    }
  }
}

//
//
// Snapshot recovery
//
//
template <template <class...> class TContainer>
void SchemaTracking<TContainer>::RecoverVertex(Vertex *vertex) {
  // No locking, since this should only be used to recover data
  auto &info = vertex_state_[vertex->labels];
  ++info.n;
  for (const auto &[property, type] : vertex->properties.ExtendedPropertyTypes()) {
    info.Increment(property, type);
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::RecoverEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to,
                                             bool prop_on_edges) {
  auto &tracking_info = edge_lookup(EdgeKeyRef{edge_type, from->labels, to->labels});
  ++tracking_info.n;
  if (prop_on_edges) {
    for (const auto &[key, val] : edge.ptr->properties.ExtendedPropertyTypes()) {
      tracking_info.Increment(key, val);
    }
  }
}

// Vertex
// Calling this after change has been applied
// Special case for when the vertex has edges
void SchemaInfo::TransactionalEdgeModifyingAccessor::AddLabel(Vertex *vertex, LabelId label) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  auto old_labels = vertex->labels;
  auto itr = std::find(old_labels.begin(), old_labels.end(), label);
  DMG_ASSERT(itr != old_labels.end(), "Trying to recreate labels pre commit, but label not found!");
  *itr = old_labels.back();
  old_labels.pop_back();
  // Update vertex stats
  tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
  // Update edge stats
  UpdateTransactionalEdges(vertex, old_labels);
}

// Calling this after change has been applied
// Special case for when the vertex has edges
void SchemaInfo::TransactionalEdgeModifyingAccessor::RemoveLabel(Vertex *vertex, LabelId label) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  // Move all stats and edges to new label
  auto old_labels = vertex->labels;
  old_labels.push_back(label);
  // Update vertex stats
  tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
  // Update edge stats
  UpdateTransactionalEdges(vertex, old_labels);
}

void SchemaInfo::TransactionalEdgeModifyingAccessor::UpdateTransactionalEdges(
    Vertex *vertex, const utils::small_vector<LabelId> &old_labels) {
  static constexpr bool InEdge = true;
  static constexpr bool OutEdge = !InEdge;
  auto process = [&](auto &edge, const auto edge_dir) {
    const auto [edge_type, other_vertex, edge_ref] = edge;
    tracking_->UpdateEdgeStats(edge_ref, edge_type, (edge_dir == InEdge) ? other_vertex->labels : vertex->labels,
                               (edge_dir == InEdge) ? vertex->labels : other_vertex->labels,
                               (edge_dir == InEdge) ? other_vertex->labels : old_labels,
                               (edge_dir == InEdge) ? old_labels : other_vertex->labels, properties_on_edges_);
  };

  for (const auto &edge : vertex->in_edges) {
    process(edge, InEdge);
  }
  for (const auto &edge : vertex->out_edges) {
    process(edge, OutEdge);
  }
}

void SchemaInfo::AnalyticalEdgeModifyingAccessor::UpdateAnalyticalEdges(
    Vertex *vertex, const utils::small_vector<LabelId> &old_labels) {
  // No need to lock anything, we are here, meaning this is the only modifying function currently active
  constexpr bool InEdge = true;
  constexpr bool OutEdge = !InEdge;
  auto process = [&](auto &edge, const auto edge_dir) {
    const auto [edge_type, other_vertex, edge_ref] = edge;
    tracking_->UpdateEdgeStats(edge_ref, edge_type, (edge_dir == InEdge) ? other_vertex->labels : vertex->labels,
                               (edge_dir == InEdge) ? vertex->labels : other_vertex->labels,
                               (edge_dir == InEdge) ? other_vertex->labels : old_labels,
                               (edge_dir == InEdge) ? old_labels : other_vertex->labels, properties_on_edges_);
  };

  for (const auto &edge : vertex->in_edges) {
    process(edge, InEdge);
  }
  for (const auto &edge : vertex->out_edges) {
    process(edge, OutEdge);
  }
}

// Vertex
// Calling this after change has been applied
// Special case for when the vertex has edges
void SchemaInfo::AnalyticalEdgeModifyingAccessor::AddLabel(Vertex *vertex, LabelId label) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  auto old_labels = vertex->labels;
  auto itr = std::find(old_labels.begin(), old_labels.end(), label);
  DMG_ASSERT(itr != old_labels.end(), "Trying to recreate labels pre commit, but label not found!");
  *itr = old_labels.back();
  old_labels.pop_back();
  // Update vertex stats
  tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
  // Update edge stats
  UpdateAnalyticalEdges(vertex, old_labels);
}

// Calling this after change has been applied
// Special case for when the vertex has edges
void SchemaInfo::AnalyticalEdgeModifyingAccessor::RemoveLabel(Vertex *vertex, LabelId label) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  // Move all stats and edges to new label
  auto old_labels = vertex->labels;
  old_labels.push_back(label);
  // Update vertex stats
  tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
  // Update edge stats
  UpdateAnalyticalEdges(vertex, old_labels);
}

// Vertex
void SchemaInfo::VertexModifyingAccessor::CreateVertex(Vertex *vertex) { tracking_->AddVertex(vertex); }

void SchemaInfo::VertexModifyingAccessor::DeleteVertex(Vertex *vertex) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  tracking_->DeleteVertex(vertex);
}

// Special case for vertex without any edges
void SchemaInfo::VertexModifyingAccessor::AddLabel(Vertex *vertex, LabelId label) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  // For vertex with edges, this needs to be a unique access....
  DMG_ASSERT(vertex->in_edges.empty() && vertex->out_edges.empty(),
             "Trying to remove label from vertex with edges; LINE {}", __LINE__);
  // Move all stats and edges to new label
  auto old_labels = vertex->labels;
  auto itr = std::find(old_labels.begin(), old_labels.end(), label);
  DMG_ASSERT(itr != old_labels.end(), "Trying to recreate labels pre commit, but label not found!");
  *itr = old_labels.back();
  old_labels.pop_back();
  tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
}

// Special case for vertex without any edges
void SchemaInfo::VertexModifyingAccessor::RemoveLabel(Vertex *vertex, LabelId label) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  // For vertex with edges, this needs to be a unique access....
  DMG_ASSERT(vertex->in_edges.empty() && vertex->out_edges.empty(),
             "Trying to remove label from vertex with edges; LINE {}", __LINE__);
  // Move all stats and edges to new label
  auto old_labels = vertex->labels;
  old_labels.push_back(label);
  tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
}

void SchemaInfo::VertexModifyingAccessor::CreateEdge(EdgeRef edge, Vertex *from, Vertex *to, EdgeTypeId edge_type) {
  DMG_ASSERT(from->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  DMG_ASSERT(to->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  // Empty edge; just update the top level stats
  tracking_->CreateEdge(from, to, edge_type);
}

void SchemaInfo::VertexModifyingAccessor::DeleteEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type, EdgeRef edge_ref) {
  // Edge has been cleared from both directions
  tracking_->DeleteEdge(edge_type, edge_ref, from, to, properties_on_edges_);
}

void SchemaInfo::VertexModifyingAccessor::SetProperty(Vertex *vertex, PropertyId property, ExtendedPropertyType now,
                                                      ExtendedPropertyType before) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  if (now == before) return;  // Nothing to do
  tracking_->SetProperty(vertex, property, now, before);
}

void SchemaInfo::VertexModifyingAccessor::SetProperty(EdgeRef edge, EdgeTypeId type, Vertex *from, Vertex *to,
                                                      PropertyId property, ExtendedPropertyType now,
                                                      ExtendedPropertyType before) {
  DMG_ASSERT(properties_on_edges_, "Trying to modify property on edge when explicitly disabled.");
  DMG_ASSERT(edge.ptr->lock.is_locked(), "Trying to read from an unlocked edge; LINE {}", __LINE__);
  if (now == before) return;  // Nothing to do
  tracking_->SetProperty(type, from, to, property, now, before, properties_on_edges_);
}

// Prob not needed
void SchemaInfo::AnalyticalEdgeModifyingAccessor::SetProperty(EdgeRef edge, EdgeTypeId type, Vertex *from, Vertex *to,
                                                              PropertyId property, ExtendedPropertyType now,
                                                              ExtendedPropertyType before) {
  DMG_ASSERT(properties_on_edges_, "Trying to modify property on edge when explicitly disabled.");
  DMG_ASSERT(edge.ptr->lock.is_locked(), "Trying to read from an unlocked edge; LINE {}", __LINE__);
  if (now == before) return;  // Nothing to do
  tracking_->SetProperty(type, from, to, property, now, before, properties_on_edges_);
}
void SchemaInfo::TransactionalEdgeModifyingAccessor::SetProperty(EdgeRef edge, EdgeTypeId type, Vertex *from,
                                                                 Vertex *to, PropertyId property,
                                                                 ExtendedPropertyType now,
                                                                 ExtendedPropertyType before) {
  DMG_ASSERT(properties_on_edges_, "Trying to modify property on edge when explicitly disabled.");
  DMG_ASSERT(edge.ptr->lock.is_locked(), "Trying to read from an unlocked edge; LINE {}", __LINE__);
  if (now == before) return;  // Nothing to do
  tracking_->SetProperty(type, from, to, property, now, before, properties_on_edges_);
}

}  // namespace memgraph::storage

template struct memgraph::storage::SchemaTracking<absl::node_hash_map>;
template struct memgraph::storage::SchemaTracking<memgraph::utils::ConcurrentUnorderedMap>;
template void memgraph::storage::SchemaTracking<memgraph::utils::ConcurrentUnorderedMap>::ProcessTransaction(
    const memgraph::storage::LocalSchemaTracking &diff, absl::flat_hash_set<SchemaInfoPostProcess> &post_process,
    absl::flat_hash_map<Edge *, absl::flat_hash_map<PropertyId, std::pair<ExtendedPropertyType, ExtendedPropertyType>>>
        &edge_prop_change,
    uint64_t commit_ts, bool property_on_edges);
