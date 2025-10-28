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
#include <shared_mutex>
#include <unordered_map>
#include <utility>

#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schema_info_types.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/transaction_constants.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/small_vector.hpp"
#include "utils/variant_helpers.hpp"

#include <nlohmann/json.hpp>

namespace memgraph::storage {
namespace {

template <typename Key, typename Tp, typename Predicate>
inline auto erase_if(memgraph::utils::ConcurrentUnorderedMap<Key, Tp> &cont, Predicate &&pred) {
  return cont.erase_if(std::forward<Predicate>(pred));
}

template <typename Key, typename Tp, typename Predicate>
inline auto erase_if(std::unordered_map<Key, Tp> &cont, Predicate &&pred) {
  return std::erase_if(cont, std::forward<Predicate>(pred));
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

// Apply deltas from other transactions
inline void ApplyDeltasForRead(const Delta *delta, uint64_t start_timestamp, auto &&callback) {
  // Avoid work if no deltas
  if (!delta) return;

  while (delta != nullptr) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    // Went back far enough
    if (ts < start_timestamp) break;
    // This delta must be applied, call the callback.
    callback(*delta);
    // Move to the next delta.
    delta = delta->next.load(std::memory_order_acquire);
  }
}

enum State { NO_CHANGE, THIS_TX, ANOTHER_TX };

inline State GetState(const Delta *delta, uint64_t start_timestamp, uint64_t commit_timestamp) {
  // This tx is running, so no deltas means there are no changes made after the tx started
  if (delta == nullptr) return State::NO_CHANGE;
  const auto ts = delta->timestamp->load(std::memory_order_acquire);
  if (ts == commit_timestamp) return State::THIS_TX;  // Our commit ts means we changed it
  // Ts larger then our start ts means another tx changed it (committed or is still running)
  if (ts > start_timestamp) return State::ANOTHER_TX;
  return State::NO_CHANGE;  // Irrelevant deltas, no changes
}

// Keep v locked as we could return a reference to labels
inline const utils::small_vector<LabelId> *GetLabelsViewOld(const Vertex *v, uint64_t start_timestamp, auto &cache) {
  // Check if already cached
  auto v_cached = cache.find(v);
  if (v_cached != cache.end()) return &v_cached->second;
  // Apply deltas and cache values
  auto labels_copy = v->labels;
  ApplyDeltasForRead(v->delta, start_timestamp, [&labels_copy](const Delta &delta) {
    // clang-format off
    DeltaDispatch(delta, utils::ChainedOverloaded{
      Labels_ActionMethod(labels_copy)
    });
    // clang-format on
  });
  auto [it, _] = cache.emplace(v, std::move(labels_copy));
  return &it->second;
}

// Keep v locked as we could return a reference to labels
inline std::pair<const utils::small_vector<LabelId> *, bool> GetLabels(const Vertex *v, uint64_t start_timestamp,
                                                                       uint64_t commit_timestamp, auto &cache) {
  const auto state = GetState(v->delta, start_timestamp, commit_timestamp);
  const auto *labels = &v->labels;
  if (state == ANOTHER_TX) {
    labels = GetLabelsViewOld(v, start_timestamp, cache);
  }
  return std::pair{labels, state != THIS_TX};
}

struct Labels {
  const utils::small_vector<LabelId> *from;
  const utils::small_vector<LabelId> *to;
  bool needs_pp{false};
};

// Cache needs to be reference stable because we are using it as a key
inline Labels GetLabels(const Vertex *from, const Vertex *to, uint64_t start_timestamp, uint64_t commit_timestamp,
                        auto &cache) {
  const auto from_res = GetLabels(from, start_timestamp, commit_timestamp, cache);
  const auto to_res = GetLabels(to, start_timestamp, commit_timestamp, cache);
  return {from_res.first, to_res.first, from_res.second || to_res.second};
}

struct LabelsDiff {
  const utils::small_vector<LabelId> *pre;
  const utils::small_vector<LabelId> *post;
};

// Cache needs to be reference stable because we are using it as a key
inline LabelsDiff GetLabelsDiff(const Vertex *v, State state, uint64_t timestamp, auto &cache, auto &post_cache) {
  // NO CHANGES
  if (state == NO_CHANGE) return {&v->labels, &v->labels};

  // Labels as seen at transaction start (cached)
  auto pre_labels = GetLabelsViewOld(v, timestamp, cache);

  // THIS TX
  if (state == THIS_TX) {
    return {pre_labels, &v->labels};
  }

  // ANOTHER TX
  // Reusing get labels with kTransactionInitialId to get committed labels (cached via temporary cache)
  auto post_labels = GetLabelsViewOld(v, kTransactionInitialId, post_cache);
  return {pre_labels, post_labels};
}

// TODO Cache this as well
struct Properties {
  std::map<PropertyId, ExtendedPropertyType> types;
  bool needs_pp{false};
};

inline std::map<PropertyId, ExtendedPropertyType> GetPropertiesViewOld(const Edge *edge, uint64_t start_timestamp) {
  auto edge_props = edge->properties.ExtendedPropertyTypes();
  // Apply deltas
  ApplyDeltasForRead(edge->delta, start_timestamp, [&edge_props](const Delta &delta) {
    // clang-format off
    DeltaDispatch(delta, utils::ChainedOverloaded{
      PropertyTypes_ActionMethod(edge_props)
    });
    // clang-format on
  });
  return edge_props;
}

inline Properties GetProperties(const Edge *edge, uint64_t start_timestamp, uint64_t commit_timestamp) {
  const auto state = GetState(edge->delta, start_timestamp, commit_timestamp);
  // TODO Should we cache this as well
  auto edge_props = edge->properties.ExtendedPropertyTypes();

  if (state == ANOTHER_TX) {
    // Apply deltas
    ApplyDeltasForRead(edge->delta, start_timestamp, [&edge_props](const Delta &delta) {
      // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          PropertyTypes_ActionMethod(edge_props)
        });
      // clang-format on
    });
  }

  return {std::move(edge_props), state != THIS_TX};
}

struct PropertiesDiff {
  std::map<PropertyId, ExtendedPropertyType> pre;
  std::map<PropertyId, ExtendedPropertyType> post;
};

// Cache needs to be reference stable because we are using it as a key
inline PropertiesDiff GetPropertiesDiff(const Edge *edge, State state, uint64_t timestamp) {
  // NO CHANGES
  auto edge_props = edge->properties.ExtendedPropertyTypes();
  if (state == NO_CHANGE) return {edge_props, edge_props};

  // Properties as seen at transaction start
  auto pre_props = GetPropertiesViewOld(edge, timestamp);

  // THIS TX
  if (state == THIS_TX) {
    return {pre_props, edge_props};
  }

  // ANOTHER TX
  // Reusing get properties with kTransactionInitialId to get committed labels
  auto post_props = GetPropertiesViewOld(edge, kTransactionInitialId);
  return {pre_props, post_props};
}

}  // namespace

template <>
TrackingInfo<std::unordered_map> &LocalSchemaTracking::edge_lookup(const EdgeKeyRef &key) {
  auto itr = edge_state_.find(key);
  if (itr != edge_state_.end()) return itr->second;
  auto [new_itr, _] =
      edge_state_.emplace(std::piecewise_construct, std::make_tuple(key.type, key.from, key.to), std::make_tuple());
  return new_itr->second;
}

template <>
TrackingInfo<utils::ConcurrentUnorderedMap> &SharedSchemaTracking::edge_lookup(const EdgeKeyRef &key) {
  return edge_state_[key];
}

template <template <class...> class TContainer>
template <template <class...> class TOtherContainer>
void SchemaTracking<TContainer>::ProcessTransaction(const SchemaTracking<TOtherContainer> &diff,
                                                    SchemaInfoPostProcess &post_process, uint64_t start_ts,
                                                    uint64_t commit_ts, bool property_on_edges) {
  // Update schema based on the diff
  for (const auto &[vertex_key, info] : diff.vertex_state_) {
    vertex_state_[vertex_key] += info;
  }
  for (const auto &[edge_key, info] : diff.edge_state_) {
    edge_state_[edge_key] += info;
  }

  std::unordered_map<const Vertex *, VertexKey> post_vertex_cache;

  // Post process (edge updates)
  for (const auto &[edge_ref, edge_type, from, to] : post_process.edges) {
    auto v_locks = SchemaInfo::ReadLockFromTo(from, to);

    // An edge can be added to post process by modifying the edge directly or one of the vertices
    // We need to check all 3 objects
    const auto from_state = GetState(from->delta, start_ts, commit_ts);
    const auto to_state = GetState(to->delta, start_ts, commit_ts);

    State edge_state{NO_CHANGE};
    std::shared_lock<decltype(edge_ref.ptr->lock)> edge_lock;
    if (property_on_edges) {
      edge_lock = std::shared_lock{edge_ref.ptr->lock};
      edge_state = GetState(edge_ref.ptr->delta, start_ts, commit_ts);
    }

    if (from_state != ANOTHER_TX && to_state != ANOTHER_TX && edge_state != ANOTHER_TX) {
      continue;  // All is as it should be
    }

    auto from_l_diff = GetLabelsDiff(from, from_state, start_ts, post_process.vertex_cache, post_vertex_cache);
    auto to_l_diff = GetLabelsDiff(to, to_state, start_ts, post_process.vertex_cache, post_vertex_cache);

    PropertiesDiff edge_prop_diff;
    if (property_on_edges) {
      edge_prop_diff = GetPropertiesDiff(edge_ref.ptr, edge_state, start_ts);
    }

    // TODO Possible optimization: check if labels or props changed and skip some lookups/updates

    // Revert local changes
    auto &tracking_11 = edge_lookup(EdgeKeyRef{edge_type, *from_l_diff.pre, *to_l_diff.pre});
    ++tracking_11.n;
    auto &tracking_12 =
        edge_lookup(EdgeKeyRef{edge_type, (from_state == ANOTHER_TX) ? *from_l_diff.pre : *from_l_diff.post,
                               (to_state == ANOTHER_TX) ? *to_l_diff.pre : *to_l_diff.post});
    --tracking_12.n;
    // Edge props
    if (property_on_edges) {
      for (const auto &[key, type] : edge_prop_diff.pre) {
        auto &pre_info = tracking_11.properties[key];
        ++pre_info.n;
        ++pre_info.types[type];
      }
      for (const auto &[key, type] : (edge_state == ANOTHER_TX) ? edge_prop_diff.pre : edge_prop_diff.post) {
        auto &post_info = tracking_12.properties[key];
        --post_info.n;
        --post_info.types[type];
      }
    }

    // Revert committed changes
    auto &tracking_21 =
        edge_lookup(EdgeKeyRef{edge_type, (from_state == ANOTHER_TX) ? *from_l_diff.post : *from_l_diff.pre,
                               (to_state == ANOTHER_TX) ? *to_l_diff.post : *to_l_diff.pre});
    --tracking_21.n;
    // Edge props
    if (property_on_edges) {
      for (const auto &[key, type] : (edge_state == ANOTHER_TX) ? edge_prop_diff.post : edge_prop_diff.pre) {
        auto &post_info = tracking_21.properties[key];
        --post_info.n;
        --post_info.types[type];
      }
    }

    // Apply the correct changes
    auto &tracking_22 = edge_lookup(EdgeKeyRef{edge_type, *from_l_diff.post, *to_l_diff.post});
    ++tracking_22.n;
    // Edge props
    if (property_on_edges) {
      for (const auto &[key, type] : edge_prop_diff.post) {
        auto &pre_info = tracking_22.properties[key];
        ++pre_info.n;
        ++pre_info.types[type];
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
  return ToJson(
      name_id_mapper, enum_store, [](auto /*value*/) { return true; }, [](auto /*value*/) { return true; });
}

template <template <class...> class TContainer>
nlohmann::json SchemaTracking<TContainer>::ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store,
                                                  const std::function<bool(LabelId)> &node_predicate,
                                                  const std::function<bool(EdgeTypeId)> &edge_predicate) const {
  auto json = nlohmann::json::object();

  // Handle NODES
  const auto &[nodes_itr, _] = json.emplace("nodes", nlohmann::json::array());
  auto &nodes = nodes_itr.value();
  for (const auto &[labels, info] : vertex_state_) {
    auto node = nlohmann::json::object();
    const auto &[labels_itr, _] = node.emplace("labels", nlohmann::json::array_t{});
    bool has_access = true;
    for (const auto labelId : labels) {
      if (!node_predicate(labelId)) {
        has_access = false;
        break;
      }
      labels_itr->emplace_back(name_id_mapper.IdToName(labelId.AsUint()));
    }
    if (!has_access) continue;
    std::sort(labels_itr->begin(), labels_itr->end());
    node.update(info.ToJson(name_id_mapper, enum_store));
    nodes.emplace_back(std::move(node));
  }

  // Handle EDGES
  const auto &[edges_itr, dummy] = json.emplace("edges", nlohmann::json::array());
  auto &edges = edges_itr.value();
  for (const auto &[edge_type, info] : edge_state_) {
    auto edge = nlohmann::json::object();
    if (!edge_predicate(edge_type.type)) continue;
    edge.emplace("type", name_id_mapper.IdToName(edge_type.type.AsUint()));
    const auto &[out_labels_itr, _] = edge.emplace("start_node_labels", nlohmann::json::array_t{});
    bool has_access = true;
    for (const auto labelId : edge_type.from) {
      if (!node_predicate(labelId)) {
        has_access = false;
        break;
      }
      out_labels_itr->emplace_back(name_id_mapper.IdToName(labelId.AsUint()));
    }
    if (!has_access) continue;
    std::sort(out_labels_itr->begin(), out_labels_itr->end());
    const auto &[in_labels_itr, _b] = edge.emplace("end_node_labels", nlohmann::json::array_t{});
    for (const auto labelId : edge_type.to) {
      if (!node_predicate(labelId)) {
        has_access = false;
        break;
      }
      in_labels_itr->emplace_back(name_id_mapper.IdToName(labelId.AsUint()));
    }
    if (!has_access) continue;
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
    auto &prop_info = info.properties[key];
    --prop_info.n;
    --prop_info.types[val];
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
    auto &old_info = old_tracking.properties[property];
    --old_info.n;
    --old_info.types[type];
    auto &new_info = new_tracking.properties[property];
    ++new_info.n;
    ++new_info.types[type];
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
      // No need for edge lock since all edge property operations are unique access
      for (const auto &[property, type] : edge_ref.ptr->properties.ExtendedPropertyTypes()) {
        auto &old_info = old_tracking.properties[property];
        --old_info.n;
        --old_info.types[type];
        auto &new_info = new_tracking.properties[property];
        ++new_info.n;
        ++new_info.types[type];
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
      auto &prop_info = tracking_info.properties[key];
      --prop_info.n;
      --prop_info.types[type];
    }
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::SetProperty(auto &tracking_info, PropertyId property, const ExtendedPropertyType &now,
                                             const ExtendedPropertyType &before) {
  if (now == before) return;  // Nothing to do
  auto &info = tracking_info.properties[property];
  if (before != ExtendedPropertyType{}) {
    --info.n;
    --info.types[before];
  }
  if (now != ExtendedPropertyType{}) {
    ++info.n;
    ++info.types[now];
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
void SchemaTracking<TContainer>::SetProperty(EdgeTypeId type, const utils::small_vector<LabelId> &from,
                                             const utils::small_vector<LabelId> &to, PropertyId property,
                                             const ExtendedPropertyType &now, const ExtendedPropertyType &before,
                                             bool prop_on_edges) {
  if (prop_on_edges) {
    auto &tracking_info = edge_lookup(EdgeKeyRef{type, from, to});
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
      auto &old_info = old_tracking.properties[property];
      --old_info.n;
      --old_info.types[type];
      auto &new_info = new_tracking.properties[property];
      ++new_info.n;
      ++new_info.types[type];
    }
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::UpdateEdgeStats(EdgeRef edge_ref, EdgeTypeId edge_type,
                                                 const VertexKey &new_from_labels, const VertexKey &new_to_labels,
                                                 const VertexKey &old_from_labels, const VertexKey &old_to_labels,
                                                 const std::map<PropertyId, ExtendedPropertyType> &edge_props) {
  // Lookup needs to happen while holding the locks, but the update itself does not
  auto &new_tracking = edge_lookup({edge_type, new_from_labels, new_to_labels});
  auto &old_tracking = edge_lookup({edge_type, old_from_labels, old_to_labels});
  UpdateEdgeStats(new_tracking, old_tracking, edge_ref, edge_props);
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::UpdateEdgeStats(auto &new_tracking, auto &old_tracking, EdgeRef edge_ref,
                                                 const std::map<PropertyId, ExtendedPropertyType> &edge_props) {
  --old_tracking.n;
  ++new_tracking.n;

  for (const auto &[property, type] : edge_props) {
    auto &old_info = old_tracking.properties[property];
    --old_info.n;
    --old_info.types[type];
    auto &new_info = new_tracking.properties[property];
    ++new_info.n;
    ++new_info.types[type];
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
    auto &prop_info = info.properties[property];
    ++prop_info.n;
    ++prop_info.types[type];
  }
}

template <template <class...> class TContainer>
void SchemaTracking<TContainer>::RecoverEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to,
                                             bool prop_on_edges) {
  auto &tracking_info = edge_lookup(EdgeKeyRef{edge_type, from->labels, to->labels});
  ++tracking_info.n;
  if (prop_on_edges) {
    for (const auto &[key, val] : edge.ptr->properties.ExtendedPropertyTypes()) {
      auto &prop_post_info = tracking_info.properties[key];
      ++prop_post_info.n;
      ++prop_post_info.types[val];
    }
  }
}

// Vertex
// Calling this after change has been applied
// Special case for when the vertex has edges
void SchemaInfo::TransactionalEdgeModifyingAccessor::AddLabel(Vertex *vertex, LabelId label,
                                                              std::unique_lock<utils::RWSpinLock> v_lock) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  auto old_labels = vertex->labels;
  auto itr = std::find(old_labels.begin(), old_labels.end(), label);
  DMG_ASSERT(itr != old_labels.end(), "Trying to recreate labels pre commit, but label not found!");
  *itr = old_labels.back();
  old_labels.pop_back();
  // Update vertex stats
  tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
  // Update edge stats
  UpdateTransactionalEdges(vertex, old_labels, std::move(v_lock));
}

// Calling this after change has been applied
// Special case for when the vertex has edges
void SchemaInfo::TransactionalEdgeModifyingAccessor::RemoveLabel(Vertex *vertex, LabelId label,
                                                                 std::unique_lock<utils::RWSpinLock> v_lock) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  // Move all stats and edges to new label
  auto old_labels = vertex->labels;
  old_labels.push_back(label);
  // Update vertex stats
  tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
  // Update edge stats
  UpdateTransactionalEdges(vertex, old_labels, std::move(v_lock));
}

void SchemaInfo::TransactionalEdgeModifyingAccessor::UpdateTransactionalEdges(
    Vertex *vertex, const utils::small_vector<LabelId> &old_labels, std::unique_lock<utils::RWSpinLock> v_lock) {
  DMG_ASSERT(post_process_, "Missing post process in transactional accessor");
  static constexpr bool InEdge = true;
  static constexpr bool OutEdge = !InEdge;

  // Have to loop though edges and lock in order
  v_lock.unlock();

  auto process = [&](auto &edge, const auto edge_dir) {
    const auto [edge_type, other_vertex, edge_ref] = edge;

    auto *from_vertex = (edge_dir == InEdge) ? other_vertex : vertex;
    auto *to_vertex = (edge_dir == InEdge) ? vertex : other_vertex;

    // We have a global schema lock here, so we can unlock/lock all objects without the worry they could change
    auto vlocks = SchemaInfo::ReadLockFromTo(from_vertex, to_vertex);
    auto edge_lock =
        properties_on_edges_ ? std::shared_lock{edge_ref.ptr->lock} : std::shared_lock<decltype(edge_ref.ptr->lock)>{};

    auto other_labels = GetLabels(other_vertex, start_ts_, commit_ts_, post_process_->vertex_cache);
    Properties edge_props{};
    if (properties_on_edges_) edge_props = GetProperties(edge_ref.ptr, start_ts_, commit_ts_);

    tracking_->UpdateEdgeStats(edge_ref, edge_type, (edge_dir == InEdge) ? *other_labels.first : vertex->labels,
                               (edge_dir == InEdge) ? vertex->labels : *other_labels.first,
                               (edge_dir == InEdge) ? *other_labels.first : old_labels,
                               (edge_dir == InEdge) ? old_labels : *other_labels.first, edge_props.types);

    SchemaInfoEdge pp_item{edge_ref, edge_type, from_vertex, to_vertex};
    if (other_labels.second || edge_props.needs_pp) {
      post_process_->edges.emplace(pp_item);
    } else {
      post_process_->edges.erase(pp_item);
    }
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

void SchemaInfo::VertexModifyingAccessor::CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) {
  DMG_ASSERT(from->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  DMG_ASSERT(to->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  // Empty edge; just update the top level stats
  tracking_->CreateEdge(from, to, edge_type);
}

void SchemaInfo::VertexModifyingAccessor::DeleteEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type, EdgeRef edge_ref) {
  // Deleting edges touches all 3 objects. That means there cannot be any other modifying tx now or before this one

  // Analytical: no need to lock since the vertex labels cannot change due to shared lock
  // Transactional: no need to lock since all objects are touched by this tx
  tracking_->DeleteEdge(edge_type, edge_ref, from, to, properties_on_edges_);

  // No post-process -> analytical
  if (post_process_) {
    // This edge could have had some modifications before deletion
    // This would case the edge to be added to the post process list
    // We need to remove it
    // Vertices cannot change, so no need to post process anything
    post_process_->edges.erase({edge_ref, edge_type, from, to});
  }
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
  DMG_ASSERT(from->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  DMG_ASSERT(to->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  if (now == before) return;  // Nothing to do

  // No post-process -> analytical
  if (!post_process_) {
    // Analytical: Read states as they are
    tracking_->SetProperty(type, from, to, property, now, before, properties_on_edges_);
  } else {
    // Transactional:
    // All 3 objects are locked
    // In case the from/to vertices are touched by this tx, we are safe, no need to post process (remove edge)
    // If one of the vertices has not been changes, we need to append this edge to the post-process list
    // We also need to get labels as they are seen by this tx
    auto labels = GetLabels(from, to, start_ts_, commit_ts_, post_process_->vertex_cache);
    tracking_->SetProperty(type, *labels.from, *labels.to, property, now, before, properties_on_edges_);
    if (labels.needs_pp) {
      post_process_->edges.emplace(edge, type, from, to);
    } else {
      // All 3 objects have been modified by this tx, so we can remove it from the post process list
      post_process_->edges.erase({edge, type, from, to});
    }
  }
}

}  // namespace memgraph::storage

template struct memgraph::storage::SchemaTracking<std::unordered_map>;
template struct memgraph::storage::SchemaTracking<memgraph::utils::ConcurrentUnorderedMap>;
template void memgraph::storage::SchemaTracking<memgraph::utils::ConcurrentUnorderedMap>::ProcessTransaction(
    const memgraph::storage::SchemaTracking<std::unordered_map> &diff, SchemaInfoPostProcess &post_process,
    uint64_t start_ts, uint64_t commit_ts, bool property_on_edges);
