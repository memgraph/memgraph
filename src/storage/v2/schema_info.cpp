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
#include <mutex>
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "utils/small_vector.hpp"
#include "utils/variant_helpers.hpp"

namespace {
constexpr bool kOutEdge = true;
constexpr bool kInEdge = !kOutEdge;

/// This function iterates through the undo buffers from an object (starting
/// from the supplied delta) and determines what deltas should be applied to get
/// the version of the object just before delta_fin. The function applies the delta
/// via the callback function passed as a parameter to the callback. It is up to the
/// caller to apply the deltas.
template <typename TCallback>
inline void ApplyDeltasForRead(const memgraph::storage::Delta *delta, uint64_t current_commit_timestamp,
                               const TCallback &callback) {
  while (true) {
    if (delta == nullptr || delta->timestamp->load(std::memory_order_acquire) != current_commit_timestamp) break;
    // This delta must be applied, call the callback.
    callback(*delta);
    // Move to the next delta in this transaction.
    auto *older = delta->next.load(std::memory_order_acquire);
    delta = older;
  }
}

memgraph::storage::Delta *NextDelta(const memgraph::storage::Delta *delta, uint64_t commit_timestamp) {
  auto *older = delta->next.load(std::memory_order_acquire);
  if (older == nullptr || older->timestamp->load(std::memory_order_acquire) != commit_timestamp) return {};
  return older;
}

template <typename TCallback>
inline void ApplyUncommittedDeltasForRead(const memgraph::storage::Delta *delta, const TCallback &callback) {
  while (true) {
    // Break when delta is not part of a transaction (it's timestamp is not equal to transaction id)
    if (delta == nullptr ||
        delta->timestamp->load(std::memory_order_acquire) < memgraph::storage::kTransactionInitialId)
      break;
    // This delta must be applied, call the callback.
    callback(*delta);
    // Move to the next delta in this transaction.
    auto *older = delta->next.load(std::memory_order_acquire);
    delta = older;
  }
}
}  // namespace

namespace memgraph::storage {

inline auto PropertyTypes_ActionMethod(std::map<PropertyId, PropertyValue::Type> &properties) {
  using enum Delta::Action;
  return ActionMethod<SET_PROPERTY>([&](Delta const &delta) {
    auto it = properties.find(delta.property.key);
    if (it != properties.end()) {
      if (delta.property.value->IsNull()) {
        // remove the property
        properties.erase(it);
      } else {
        // set the value
        it->second = delta.property.value->type();
      }
    } else if (!delta.property.value->IsNull()) {
      properties.emplace(delta.property.key, delta.property.value->type());
    }
  });
}

void Tracking::CleanUp() {
  // Erase all elements that don't have any vertices associated
  std::erase_if(vertex_state_, [](auto &elem) { return elem.second.N() <= 0; });
  for (auto &[_, val] : vertex_state_) {
    std::erase_if(val.properties, [](auto &elem) { return elem.second.n <= 0; });
    for (auto &[_, val] : val.properties) {
      std::erase_if(val.types, [](auto &elem) { return elem.second <= 0; });
    }
  }
  // Edge state cleanup
  std::erase_if(edge_state_, [](auto &elem) { return elem.second.N() <= 0; });
  for (auto &[_, val] : edge_state_) {
    std::erase_if(val.properties, [](auto &elem) { return elem.second.n <= 0; });
    for (auto &[_, val] : val.properties) {
      std::erase_if(val.types, [](auto &elem) { return elem.second <= 0; });
    }
  }
}

void Tracking::Print(NameIdMapper &name_id_mapper, bool properties_on_edges) {
  std::cout << "SCHEMA INFO\n";
  std::cout << ToJson(name_id_mapper, properties_on_edges) << std::endl;
}

nlohmann::json Tracking::ToJson(NameIdMapper &name_id_mapper, bool properties_on_edges) {
  auto json = nlohmann::json::object();

  // Clean up unused stats
  CleanUp();

  // Handle NODES
  const auto &[nodes_itr, _] = json.emplace("nodes", nlohmann::json::array());
  auto &nodes = nodes_itr.value();
  for (const auto &[labels, info] : vertex_state_) {
    auto node = nlohmann::json::object();
    const auto &[labels_itr, _] = node.emplace("labels", nlohmann::json::array_t{});
    for (const auto l : labels) {
      labels_itr->emplace_back(name_id_mapper.IdToName(l.AsUint()));
    }
    node.emplace("count", info.N());
    const auto &[prop_itr, __] = node.emplace("properties", nlohmann::json::array_t{});
    for (const auto &[p, info] : info.properties) {
      nlohmann::json::object_t property_info;
      property_info.emplace("key", name_id_mapper.IdToName(p.AsUint()));
      property_info.emplace("count", info.n);
      const auto &[types_itr, _] = property_info.emplace("types", nlohmann::json::array_t{});
      for (const auto &type : info.types) {
        nlohmann::json::object_t type_info;
        std::stringstream ss;
        ss << type.first;
        type_info.emplace("type", ss.str());
        type_info.emplace("count", type.second);
        types_itr->second.emplace_back(std::move(type_info));
      }
      prop_itr->emplace_back(std::move(property_info));
    }
    nodes.emplace_back(std::move(node));
  }

  // Handle EDGES
  const auto &[edges_itr, _b] = json.emplace("edges", nlohmann::json::array());
  auto &edges = edges_itr.value();
  std::unordered_map<EdgeType, TrackingEdgeInfo, EdgeType::hasher> final_edge_state;

  // From tracking to temporary json-like structure
  for (const auto &[edge_id, info] : edge_state_) {
    // We are already locked on the edge; no where else can the edge and vertex be locked at the same time
    // Lock the to/from in order of GID
    // Get their labels as seen at time of commit
    // const auto sum_edges = info.N();
    for (const auto [edge_ref, from_to] : info.edges) {
      const auto [from_v, to_v] = from_to;
      // TODO: Need to apply deltas...
      // This is called out-side a transaction
      auto from_lock = std::unique_lock(from_v->lock, std::defer_lock);
      auto to_lock = std::unique_lock(to_v->lock, std::defer_lock);

      // Lock in order of GID
      if (from_v->gid == to_v->gid) {
        from_lock.lock();
      } else if (from_v->gid > to_v->gid) {
        to_lock.lock();
        from_lock.lock();
      } else {
        from_lock.lock();
        to_lock.lock();
      }

      // Apply deltas to get last commited labels
      // TODO: Cache
      const auto from_labels = GetCommittedLabels(*from_v);
      const auto to_labels = GetCommittedLabels(*to_v);

      from_lock.unlock();
      if (to_lock.owns_lock()) to_lock.unlock();

      const auto edge_type = EdgeType{edge_id, from_labels, to_labels};
      auto &state = final_edge_state[edge_type];

      // There are not other points where the edge is locked at the same time vertices are locked; no need to lock in
      // order
      auto edge_lock = std::unique_lock(to_v->lock);
      const auto edge_properties = GetCommittedPropertyTypes(edge_ref, properties_on_edges);
      edge_lock.unlock();
      for (const auto &[key, val] : edge_properties) {
        ++state.properties[key].n;
        ++state.properties[key].types[val];
      }
      ++state.n;
    }
  }

  // From temporary structure to json
  for (const auto &[edge_type, info] : final_edge_state) {
    auto edge = nlohmann::json::object();

    edge.emplace("type", name_id_mapper.IdToName(edge_type.type.AsUint()));
    const auto &[out_labels_itr, _] = edge.emplace("start_node_labels", nlohmann::json::array_t{});
    for (const auto l : edge_type.from_v_type) {
      out_labels_itr->emplace_back(name_id_mapper.IdToName(l.AsUint()));
    }
    const auto &[in_labels_itr, _b] = edge.emplace("end_node_labels", nlohmann::json::array_t{});
    for (const auto l : edge_type.to_v_type) {
      in_labels_itr->emplace_back(name_id_mapper.IdToName(l.AsUint()));
    }

    edge.emplace("count", info.n);
    const auto &[prop_itr, __] = edge.emplace("properties", nlohmann::json::array_t{});
    for (const auto &[p, info] : info.properties) {
      nlohmann::json::object_t property_info;
      property_info.emplace("key", name_id_mapper.IdToName(p.AsUint()));
      property_info.emplace("count", info.n);
      const auto &[types_itr, _] = property_info.emplace("types", nlohmann::json::array_t{});
      for (const auto &type : info.types) {
        nlohmann::json::object_t type_info;
        std::stringstream ss;
        ss << type.first;
        type_info.emplace("type", ss.str());
        type_info.emplace("count", type.second);
        types_itr->second.emplace_back(std::move(type_info));
      }
      prop_itr->emplace_back(std::move(property_info));
    }
    edges.emplace_back(std::move(edge));
  }

  return json;
}

utils::small_vector<LabelId> GetPreLabels(const Vertex &vertex, uint64_t commit_timestamp) {
  auto pre_labels = vertex.labels;
  if (vertex.delta) {
    ApplyDeltasForRead(vertex.delta, commit_timestamp, [&pre_labels](const Delta &delta) {
      // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Labels_ActionMethod(pre_labels)
        });
      // clang-format on
    });
  }
  return pre_labels;
}

std::map<PropertyId, PropertyValue::Type> GetCommittedPropertyTypes(const EdgeRef &edge_ref, bool properties_on_edges) {
  std::map<PropertyId, PropertyValue::Type> properties;
  if (properties_on_edges) {
    const auto *edge = edge_ref.ptr;
    properties = edge->properties.PropertyTypes();
    if (edge->delta) {
      ApplyUncommittedDeltasForRead(edge->delta, [&properties](const Delta &delta) {
        // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          PropertyTypes_ActionMethod(properties)
        });
        // clang-format on
      });
    }
  }
  return properties;
}

utils::small_vector<LabelId> GetCommittedLabels(const Vertex &vertex) {
  auto pre_labels = vertex.labels;
  if (vertex.delta) {
    ApplyUncommittedDeltasForRead(vertex.delta, [&pre_labels](const Delta &delta) {
      // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Labels_ActionMethod(pre_labels)
        });
      // clang-format on
    });
  }
  return pre_labels;
}

Gid GetEdgeGid(const EdgeRef &edge_ref, bool properties_on_edges) {
  if (properties_on_edges) {
    return edge_ref.ptr->gid;
  }
  return edge_ref.gid;
}

void VertexHandler::PreProcess() {
  const auto *delta = vertex_.delta;
  DMG_ASSERT(delta, "Handling a vertex without deltas");

  // TODO: Do only what is necessary
  pre_labels_ = vertex_.labels;
  pre_properties_ = vertex_.properties.PropertyTypes();

  ApplyDeltasForRead(delta, commit_timestamp_, [this](const Delta &delta) {
    // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          PropertyTypes_ActionMethod(pre_properties_),
          Labels_ActionMethod(pre_labels_),
        });
    // clang-format on
  });
}

Delta *VertexHandler::NextDelta(const Delta *delta) const { return ::NextDelta(delta, commit_timestamp_); }

void VertexHandler::PostProcess(TransactionEdgeHandler &edge_handler) {
  if (label_update_) {
    // Move all info as is at the start
    // Other deltas will take care of updating the end result
    if (!vertex_added_) {
      --tracking_[PreLabels()].n;
      remove_property_.insert(pre_properties_.begin(), pre_properties_.end());
    }
    if (!vertex_.deleted) {
      const auto post_properties = vertex_.properties.PropertyTypes();
      ++tracking_[PostLabels()].n;
      add_property_.insert(post_properties.begin(), post_properties.end());
    }
  }

  // We are only removing properties that have existed before transaction
  for (const auto &[id, val] : remove_property_) {
    --tracking_[PreLabels()].properties[id].n;
    --tracking_[PreLabels()].properties[id].types[val];
  }
  // Add the new (or updated) properties
  for (const auto &[id, val] : add_property_) {
    ++tracking_[PostLabels()].properties[id].n;
    ++tracking_[PostLabels()].properties[id].types[val];
  }
}

void SchemaInfo::ProcessVertex(VertexHandler &vertex_handler, TransactionEdgeHandler &tx_edge_handler) {
  vertex_handler.PreProcess();

  // Processing deltas from newest to oldest
  // We need to be careful because we are traversing the chain in reverse
  // Edges are done at the very end; except when created or destroyed inside the transaction
  // Property values are stored such that when post-processing, we have the value as it was before the tx

  // DELETE_DESERIALIZED_OBJECT: <
  // DELETE_OBJECT:   <- (adds) add vertex as defined at commit
  // RECREATE_OBJECT: <- (removes) remove vertex as defined at start
  // SET_PROPERTY:    <- (both adds and removes) remove property-label at start add property-label at commit
  // ADD_LABEL:       <
  // REMOVE_LABEL:    <- update v labels and (if any edges at start) also edges
  // ADD_OUT_EDGE:    <- (removes) remove edge as defined at start
  // REMOVE_OUT_EDGE: <- (adds)  add edges as defined at commit

  const auto *delta = vertex_handler.FirstDelta();

  while (delta) {
    switch (delta->action) {
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        // This is an empty object without any labels or properties
        vertex_handler.AddVertex();
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        // This is an empty object without any labels or properties
        vertex_handler.RemoveVertex();
        break;
      }
      case Delta::Action::SET_PROPERTY: {
        vertex_handler.UpdateProperty(delta->property.key, *delta->property.value);
        break;
      }
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL: {
        vertex_handler.UpdateLabel();
        break;
      }
      case Delta::Action::ADD_OUT_EDGE: {
        // This delta covers both vertices; we can update count in place
        tx_edge_handler.RemoveEdge(delta, vertex_handler.PreLabels());
        break;
      }
      case Delta::Action::REMOVE_OUT_EDGE: {
        // This delta covers both vertices; we can update count in place
        // New edge; add to the final labels
        tx_edge_handler.AddEdge(delta, vertex_handler.PostLabels(), &vertex_handler.vertex_);
        break;
      }
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
        // These actions are already encoded in the *_OUT_EDGE actions. This
        // function should never be called for this type of deltas.
        // LOG_FATAL("Invalid delta action!");
        break;
    }

    // Advance along the chain
    delta = vertex_handler.NextDelta(delta);
  }

  // Post process
  vertex_handler.PostProcess(tx_edge_handler);
}

std::map<PropertyId, PropertyValue> TransactionEdgeHandler::GetPreProperties(const EdgeRef &edge_ref) const {
  std::map<PropertyId, PropertyValue> properties;
  if (properties_on_edges_) {
    const auto *edge = edge_ref.ptr;
    properties = edge->properties.Properties();
    if (edge->delta) {
      ApplyDeltasForRead(edge->delta, commit_timestamp_, [&properties](const Delta &delta) {
        // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Properties_ActionMethod(properties)
        });
        // clang-format on
      });
    }
  }
  return properties;
}

std::map<PropertyId, PropertyValue> TransactionEdgeHandler::GetPostProperties(const EdgeRef &edge_ref) const {
  std::map<PropertyId, PropertyValue> properties;
  if (properties_on_edges_) {
    const auto *edge = edge_ref.ptr;
    properties = edge->properties.Properties();
  }
  return properties;
}

void EdgeHandler::PostProcess() {
  // We are only removing properties that have existed before transaction
  for (const auto &[id, val] : remove_property_) {
    --tracking_[edge_type_.type].properties[id].n;
    --tracking_[edge_type_.type].properties[id].types[val.type()];
  }
  // Add the new (or updated) properties
  for (const auto &[id, val] : add_property_) {
    ++tracking_[edge_type_.type].properties[id].n;
    ++tracking_[edge_type_.type].properties[id].types[val.type()];
  }
}

void EdgeHandler::PreProcessProperties() {
  pre_properties_ = edge_.properties.Properties();

  ApplyDeltasForRead(edge_.delta, commit_timestamp_, [this](const Delta &delta) {
    // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Properties_ActionMethod(pre_properties_),
        });
    // clang-format on
  });
}

Delta *EdgeHandler::NextDelta(const Delta *delta) const { return ::NextDelta(delta, commit_timestamp_); }

}  // namespace memgraph::storage
