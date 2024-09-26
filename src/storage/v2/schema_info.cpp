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

#include "storage/v2/schema_info.hpp"

#include <atomic>
#include <mutex>
#include <utility>

#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "utils/logging.hpp"
#include "utils/small_vector.hpp"
#include "utils/variant_helpers.hpp"

namespace {

/// This function iterates through the undo buffers from an object (starting
/// from the supplied delta) and determines what deltas should be applied to get
/// the version of the object just before delta_fin. The function applies the delta
/// via the callback function passed as a parameter to the callback. It is up to the
/// caller to apply the deltas.
template <typename TCallback>
inline void ApplyDeltasForRead(const memgraph::storage::Delta *delta, uint64_t current_commit_timestamp,
                               const TCallback &callback) {
  while (delta) {
    const auto ts = delta->timestamp->load(std::memory_order_acquire);
    // We need to stop if:
    //  there are no other deltas
    //  the timestamp is a commited timestamp
    //  the timestamp is the current commit timestamp
    // TODO Rename commit timestamp to transaction id
    if (ts == current_commit_timestamp || ts < memgraph::storage::kTransactionInitialId) break;
    // This delta must be applied, call the callback.
    callback(*delta);
    // Move to the next delta in this transaction.
    delta = delta->next.load(std::memory_order_acquire);
  }
}

template <typename TCallback>
inline void ApplyUncommittedDeltasForRead(const memgraph::storage::Delta *delta, const TCallback &callback) {
  while (delta) {
    // Break when delta is not part of a transaction (it's timestamp is not equal to a transaction id)
    if (delta->timestamp->load(std::memory_order_acquire) < memgraph::storage::kTransactionInitialId) break;
    // This delta must be applied, call the callback.
    callback(*delta);
    // Move to the next delta in this transaction.
    delta = delta->next.load(std::memory_order_acquire);
  }
}

auto PropertyType_ActionMethod(
    std::unordered_map<memgraph::storage::PropertyId,
                       std::pair<memgraph::storage::ExtendedPropertyType, memgraph::storage::ExtendedPropertyType>>
        &diff) {
  // clang-format off
  using enum memgraph::storage::Delta::Action;
  return memgraph::storage::ActionMethod<SET_PROPERTY>([&diff](memgraph::storage::Delta const &delta) {
    const auto key = delta.property.key;
    const auto old_type = memgraph::storage::ExtendedPropertyType{*delta.property.value};
    auto itr = diff.find(key);
    if (itr == diff.end()) {
      // NOTE: For vertices we pre-fill the diff
      // If diff is pre-filled, missing property means it got deleted; new value should always be Null
      diff.emplace(key, std::make_pair(memgraph::storage::ExtendedPropertyType{}, old_type));
    } else {
      // We are going from newest to oldest; overwrite so we remain with the value pre tx
      itr->second.second = memgraph::storage::ExtendedPropertyType{old_type};
    }
  });
  // clang-format on
}

}  // namespace

namespace memgraph::storage {

ExtendedPropertyType GenPropertyType(const storage::pmr::PropertyValue *property_value) {
  const auto type = property_value->type();
  if (type == PropertyValueType::TemporalData) {
    return ExtendedPropertyType{property_value->ValueTemporalData().type};
  }
  if (type == PropertyValueType::Enum) {
    return ExtendedPropertyType{property_value->ValueEnum().type_id()};
  }
  return ExtendedPropertyType{type};
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
        it->second = GenPropertyType(delta.property.value);
      }
    } else if (!delta.property.value->IsNull()) {
      properties.emplace(delta.property.key, delta.property.value->type());
    }
  });
}

void Tracking::CleanUp() {
  // Erase all elements that don't have any vertices associated
  std::erase_if(vertex_state_, [](auto &elem) { return elem.second.n <= 0; });
  for (auto &[_, val] : vertex_state_) {
    std::erase_if(val.properties, [](auto &elem) { return elem.second.n <= 0; });
    for (auto &[_, val] : val.properties) {
      std::erase_if(val.types, [](auto &elem) { return elem.second <= 0; });
    }
  }
  // Edge state cleanup
  std::erase_if(edge_state_, [](auto &elem) { return elem.second.n <= 0; });
  for (auto &[_, val] : edge_state_) {
    std::erase_if(val.properties, [](auto &elem) { return elem.second.n <= 0; });
    for (auto &[_, val] : val.properties) {
      std::erase_if(val.types, [](auto &elem) { return elem.second <= 0; });
    }
  }
}

nlohmann::json PropertyInfo::ToJson(const EnumStore &enum_store, std::string_view key, uint32_t max_count) const {
  nlohmann::json::object_t property_info;
  property_info.emplace("key", key);
  property_info.emplace("count", n);
  property_info.emplace("filling_factor", (100.0 * n) / max_count);
  const auto &[types_itr, _] = property_info.emplace("types", nlohmann::json::array_t{});
  for (const auto &type : types) {
    nlohmann::json::object_t type_info;
    std::stringstream ss;
    if (type.first.type == PropertyValueType::TemporalData) {
      ss << type.first.temporal_type;
    } else if (type.first.type == PropertyValueType::Enum) {
      ss << "Enum::" << *enum_store.ToTypeString(type.first.enum_type);
    } else {
      // Unify formatting
      switch (type.first.type) {
        break;
        case PropertyValueType::Null:
          ss << "Null";
          break;
        case PropertyValueType::Bool:
          ss << "Boolean";
          break;
        case PropertyValueType::Int:
          ss << "Integer";
          break;
        case PropertyValueType::Double:
          ss << "Float";
          break;
        case PropertyValueType::String:
          ss << "String";
          break;
        case PropertyValueType::List:
          ss << "List";
          break;
        case PropertyValueType::Map:
          ss << "Map";
          break;
        case PropertyValueType::TemporalData:
          ss << "TemporalData";
          break;
        case PropertyValueType::ZonedTemporalData:
          ss << "ZonedDateTime";
          break;
        case PropertyValueType::Enum:
          ss << "Enum";
          break;
        case PropertyValueType::Point2d:
          ss << "Point2D";
          break;
        case PropertyValueType::Point3d:
          ss << "Point3D";
          break;
      }
    }
    type_info.emplace("type", ss.str());
    type_info.emplace("count", type.second);
    types_itr->second.emplace_back(std::move(type_info));
  }
  return property_info;
}

nlohmann::json TrackingInfo::ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const {
  nlohmann::json::object_t tracking_info;
  tracking_info.emplace("count", n);
  const auto &[prop_itr, _] = tracking_info.emplace("properties", nlohmann::json::array_t{});
  for (const auto &[p, info] : properties) {
    prop_itr->second.emplace_back(info.ToJson(enum_store, name_id_mapper.IdToName(p.AsUint()), std::max(n, 1)));
  }
  return tracking_info;
}

void Tracking::ProcessTransaction(const Tracking &diff, std::unordered_set<PostProcessPOC> &post_process,
                                  uint64_t commit_ts, bool property_on_edges) {
  // Update schema based on the diff
  for (const auto &[vertex_key, info] : diff.vertex_state_) {
    vertex_state_[vertex_key] += info;
  }
  for (const auto &[edge_key, info] : diff.edge_state_) {
    edge_state_[edge_key] += info;
  }

  // Post process (edge updates)
  for (const auto &[edge_ref, edge_type, from, to] : post_process) {
    auto from_lock = std::unique_lock{from->lock, std::defer_lock};
    auto to_lock = std::unique_lock{to->lock, std::defer_lock};

    if (to == from) {
      from_lock.lock();
    } else if (to->gid < from->gid) {
      to_lock.lock();
      from_lock.lock();
    } else {
      from_lock.lock();
      to_lock.lock();
    }

    auto &tracking_pre_info = (*this)[EdgeKeyRef{edge_type, GetCommittedLabels(*from), GetCommittedLabels(*to)}];
    auto &tracking_post_info = (*this)[EdgeKeyRef{edge_type, GetLabels(*from, commit_ts), GetLabels(*to, commit_ts)}];

    from_lock.unlock();
    if (to_lock.owns_lock()) to_lock.unlock();

    // Step 1: Move committed stats in case edge identification changed
    if (&tracking_pre_info != &tracking_post_info) {
      --tracking_pre_info.n;
      ++tracking_post_info.n;
      if (property_on_edges) {
        for (const auto &[key, type] : GetCommittedProperty(*edge_ref.ptr)) {
          auto &pre_info = tracking_pre_info.properties[key];
          --pre_info.n;
          --pre_info.types[type];
          auto &post_info = tracking_post_info.properties[key];
          ++post_info.n;
          ++post_info.types[type];
        }
      }
    }

    // Step 2: Update any property changes from this tx while referencing new edge identification
    if (property_on_edges) {
      for (const auto &[key, diff] : GetPropertyDiff(edge_ref.ptr, commit_ts)) {
        if (diff.second == diff.first) return;  // Nothing to do
        auto &info = tracking_post_info.properties[key];
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
}

nlohmann::json Tracking::ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) {
  // Clean up unused stats
  CleanUp();

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

  return json;
}

void Tracking::DeleteVertex(Vertex *vertex) {
  auto &info = (*this)[vertex->labels];
  --info.n;
  for (const auto &[key, val] : vertex->properties.ExtendedPropertyTypes()) {
    auto &prop_info = info.properties[key];
    --prop_info.n;
    --prop_info.types[val];
  }
  // No edges should be present at this point
}

void Tracking::UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                            const utils::small_vector<LabelId> &new_labels, bool prop_on_edges) {
  // Update vertex stats
  auto &old_tracking = (*this)[old_labels];
  auto &new_tracking = (*this)[new_labels];
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
  // Update edge stats
  auto update_edge = [&](EdgeTypeId edge_type, EdgeRef edge_ref, Vertex *from, Vertex *to,
                         const utils::small_vector<LabelId> *old_from_labels,
                         const utils::small_vector<LabelId> *old_to_labels) {
    auto &old_tracking = (*this)[EdgeKeyRef(edge_type, old_from_labels ? *old_from_labels : from->labels,
                                            old_to_labels ? *old_to_labels : to->labels)];
    auto &new_tracking = (*this)[EdgeKeyRef(edge_type, from->labels, to->labels)];
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

void Tracking::CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) {
  auto &tracking_info = (*this)[EdgeKeyRef{edge_type, from->labels, to->labels}];
  ++tracking_info.n;
}

void Tracking::DeleteEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges) {
  auto &tracking_info = (*this)[EdgeKeyRef{edge_type, from->labels, to->labels}];
  --tracking_info.n;
  if (prop_on_edges) {
    for (const auto &[key, type] : edge.ptr->properties.ExtendedPropertyTypes()) {
      auto &prop_info = tracking_info.properties[key];
      --prop_info.n;
      --prop_info.types[type];
    }
  }
}

void Tracking::SetProperty(auto &tracking_info, PropertyId property, const ExtendedPropertyType &now,
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

inline utils::small_vector<LabelId> GetCommittedLabels(const Vertex &vertex) {
  utils::small_vector<LabelId> labels = vertex.labels;
  if (vertex.delta) {
    ApplyUncommittedDeltasForRead(vertex.delta, [&labels](const Delta &delta) {
      // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Labels_ActionMethod(labels)
        });
      // clang-format on
    });
  }
  return labels;
}

std::map<PropertyId, ExtendedPropertyType> GetCommittedProperty(const Edge &edge) {
  auto lock = std::unique_lock{edge.lock};
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

inline utils::small_vector<LabelId> GetLabels(const Vertex &vertex, uint64_t commit_timestamp, uint64_t start_ts) {
  utils::small_vector<LabelId> labels = vertex.labels;
  if (vertex.delta) {
    ApplyDeltasForRead(vertex.delta, commit_timestamp, [&labels](const Delta &delta) {
      // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Labels_ActionMethod(labels)
        });
      // clang-format on
    });
  }
  return labels;
}

std::unordered_map<PropertyId, std::pair<ExtendedPropertyType, ExtendedPropertyType>> GetPropertyDiff(
    Edge *edge, uint64_t commit_ts) {
  std::unordered_map<PropertyId, std::pair<ExtendedPropertyType, ExtendedPropertyType>> diff;
  auto lock = std::unique_lock{edge->lock};
  // Check if edge was modified by this transaction
  if (!edge->delta || edge->delta->timestamp->load(std::memory_order::acquire) != commit_ts) return diff;
  // Prefill diff with current properties (as if no change has been made)
  for (const auto &[key, type] : edge->properties.ExtendedPropertyTypes()) {
    diff.emplace(key, std::pair{edge->deleted ? ExtendedPropertyType{} : type, type});
  }

  ApplyUncommittedDeltasForRead(edge->delta, [&diff](const memgraph::storage::Delta &delta) {
    // clang-format off
    DeltaDispatch(delta, memgraph::utils::ChainedOverloaded{
                             PropertyType_ActionMethod(diff),
                         });
    // clang-format on
  });

  return diff;
}

size_t EdgeKey::equal_to::operator()(const EdgeKey &lhs, const EdgeKey &rhs) const { return lhs == rhs; }
size_t EdgeKey::equal_to::operator()(const EdgeKey &lhs, const EdgeKeyRef &rhs) const { return lhs == rhs; }
size_t EdgeKey::equal_to::operator()(const EdgeKeyRef &lhs, const EdgeKey &rhs) const { return rhs == lhs; }

//
//
// Snapshot recovery
//
//
void SchemaInfo::RecoverVertex(Vertex *vertex) {
  // No locking, since this should only be used to recover data
  auto &info = tracking_[vertex->labels];
  ++info.n;
  for (const auto &[property, type] : vertex->properties.ExtendedPropertyTypes()) {
    auto &prop_info = info.properties[property];
    ++prop_info.n;
    ++prop_info.types[type];
  }
}

void SchemaInfo::RecoverEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges) {
  auto &tracking_info = tracking_[EdgeKeyRef{edge_type, from->labels, to->labels}];
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
void SchemaInfo::TransactionalEdgeModifyingAccessor::AddLabel(Vertex *vertex, LabelId label) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  auto old_labels = vertex->labels;
  auto itr = std::find(old_labels.begin(), old_labels.end(), label);
  DMG_ASSERT(itr != old_labels.end(), "Trying to recreate labels pre commit, but label not found!");
  *itr = old_labels.back();
  old_labels.pop_back();
  // Update vertex stats
  SchemaInfo::UpdateLabel(*tracking_, vertex, old_labels, vertex->labels);
  // Update edge stats
  UpdateEdges<true>(vertex, old_labels);
  UpdateEdges<false>(vertex, old_labels);
}

// Calling this after change has been applied
// Special case for when the vertex has edges
void SchemaInfo::TransactionalEdgeModifyingAccessor::RemoveLabel(Vertex *vertex, LabelId label) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  // Move all stats and edges to new label
  auto old_labels = vertex->labels;
  old_labels.push_back(label);
  // Update vertex stats
  SchemaInfo::UpdateLabel(*tracking_, vertex, old_labels, vertex->labels);
  // Update edge stats
  UpdateEdges<true>(vertex, old_labels);
  UpdateEdges<false>(vertex, old_labels);
}

void SchemaInfo::TransactionalEdgeModifyingAccessor::SetProperty(EdgeRef edge, EdgeTypeId type, Vertex *from,
                                                                 Vertex *to, PropertyId property,
                                                                 ExtendedPropertyType now,
                                                                 ExtendedPropertyType before) {
  DMG_ASSERT(properties_on_edges_, "Trying to modify property on edge when explicitly disabled.");
  if (now == before) return;  // Nothing to do

  if (EdgeCreatedDuringThisTx(edge)) {  // We can process in-place if created during this transaction
    auto &tracking_info = (*tracking_)[EdgeKeyRef{type, from->labels, to->labels}];
    tracking_->SetProperty(tracking_info, property, now, before);
  } else {  // Not created during this tx; needs to be post-processed
    post_process_->emplace(edge, type, from, to);
  }
}

// Vertex
// Calling this after change has been applied
// Special case for when the vertex has edges
void SchemaInfo::AnalyticalEdgeModifyingAccessor::AddLabel(Vertex *vertex, LabelId label,
                                                           std::unique_lock<utils::RWSpinLock> vertex_guard) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  auto old_labels = vertex->labels;
  auto itr = std::find(old_labels.begin(), old_labels.end(), label);
  DMG_ASSERT(itr != old_labels.end(), "Trying to recreate labels pre commit, but label not found!");
  *itr = old_labels.back();
  old_labels.pop_back();
  // Update vertex stats
  {
    auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
      if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
      return {};
    });
    SchemaInfo::UpdateLabel(*tracking_, vertex, old_labels, vertex->labels);
  }
  // Update edge stats
  UpdateEdges<true, true>(vertex, old_labels);
  UpdateEdges<false, true>(vertex, old_labels);
  // Unlock vertex and loop through the ones we couldn't lock
  vertex_guard.unlock();
  UpdateEdges<true, false>(vertex, old_labels);
  UpdateEdges<false, false>(vertex, old_labels);
}

// Calling this after change has been applied
// Special case for when the vertex has edges
void SchemaInfo::AnalyticalEdgeModifyingAccessor::RemoveLabel(Vertex *vertex, LabelId label,
                                                              std::unique_lock<utils::RWSpinLock> vertex_guard) {
  DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
  // Move all stats and edges to new label
  auto old_labels = vertex->labels;
  old_labels.push_back(label);
  // Update vertex stats
  {
    auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
      if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
      return {};
    });
    SchemaInfo::UpdateLabel(*tracking_, vertex, old_labels, vertex->labels);
  }
  // Update edge stats
  UpdateEdges<true, true>(vertex, old_labels);
  UpdateEdges<false, true>(vertex, old_labels);
  // Unlock vertex and loop through the ones we couldn't lock
  vertex_guard.unlock();
  UpdateEdges<true, false>(vertex, old_labels);
  UpdateEdges<false, false>(vertex, old_labels);
}

// TODO Analytical locks for ordering, transactional does not lock so needs to read deltas
// Edge SET_PROPERTY delta
void SchemaInfo::AnalyticalEdgeModifyingAccessor::SetProperty(EdgeTypeId type, Vertex *from, Vertex *to,
                                                              PropertyId property, ExtendedPropertyType now,
                                                              ExtendedPropertyType before) {
  DMG_ASSERT(properties_on_edges_, "Trying to modify property on edge when explicitly disabled.");
  if (now == before) return;  // Nothing to do

  auto from_lock = std::unique_lock{from->lock, std::defer_lock};
  auto to_lock = std::unique_lock{to->lock, std::defer_lock};

  if (from->gid == to->gid) {
    from_lock.lock();
  } else if (from->gid < to->gid) {
    from_lock.lock();
    to_lock.lock();
  } else {
    to_lock.lock();
    from_lock.lock();
  }

  auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
    if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
    return {};
  });
  auto &tracking_info = (*tracking_)[EdgeKeyRef{type, from->labels, to->labels}];

  from_lock.unlock();
  if (to_lock.owns_lock()) to_lock.unlock();

  tracking_->SetProperty(tracking_info, property, now, before);
}

}  // namespace memgraph::storage
