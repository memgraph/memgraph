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
#include "utils/logging.hpp"
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
    // We need to stop if:
    //  there are no other deltas
    //  the timestamp is a commited timestamp
    //  the timestamp is the current commit timestamp
    // TODO Rename commit timestamp to transaction id
    if (delta == nullptr ||
        delta->timestamp->load(std::memory_order_acquire) < memgraph::storage::kTransactionInitialId ||
        delta->timestamp->load(std::memory_order_acquire) == current_commit_timestamp)
      break;
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

template <memgraph::storage::EdgeDirection dir>
auto RemainingEdges_ActionMethod(
    memgraph::utils::small_vector<
        std::tuple<memgraph::storage::EdgeTypeId, memgraph::storage::Vertex *, memgraph::storage::EdgeRef>> &edges) {
  // clang-format off
  using enum memgraph::storage::Delta::Action;
  return memgraph::utils::Overloaded{
      // If we see ADD, that means that this edge got removed (we are not interested in those)
      // ActionMethod <(dir == EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
      //     [&](Delta const &delta) {
      //     }
      // ),
      // If we see REMOVE, that means that this is a new edge that was added during the transaction
      memgraph::storage::ActionMethod <(dir == memgraph::storage::EdgeDirection::IN) ? REMOVE_IN_EDGE : REMOVE_OUT_EDGE> (
          [&](memgraph::storage::Delta const &delta) {
              auto it = std::find(edges.begin(), edges.end(),
                            std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge});
              DMG_ASSERT(it != edges.end(), "Invalid database state!");
              *it = edges.back();
              edges.pop_back();
          }
      )
  };
  // clang-format on
}

template <memgraph::storage::EdgeDirection Dir>
auto GetRemainingPreEdges(const memgraph::storage::Vertex &vertex) {
  decltype(vertex.in_edges) edges;

  if (Dir == memgraph::storage::EdgeDirection::OUT) {
    edges = vertex.out_edges;
  } else {
    edges = vertex.in_edges;
  }

  ApplyUncommittedDeltasForRead(vertex.delta, [&edges](const memgraph::storage::Delta &delta) {
    // clang-format off
    DeltaDispatch(delta, memgraph::utils::ChainedOverloaded{
      RemainingEdges_ActionMethod<Dir>(edges),
    });
    // clang-format on
  });

  return edges;
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
  const auto &[edges_itr, dummy] = json.emplace("edges", nlohmann::json::array());
  auto &edges = edges_itr.value();
  for (const auto &[edge_type, info] : edge_state_) {
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

utils::small_vector<LabelId> GetLabels(const Vertex &vertex, uint64_t commit_timestamp) {
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

std::map<PropertyId, PropertyValue::Type> GetPropertyTypes(const EdgeRef &edge_ref, uint64_t commit_timestamp,
                                                           bool properties_on_edges) {
  std::map<PropertyId, PropertyValue::Type> properties;
  if (properties_on_edges) {
    const auto *edge = edge_ref.ptr;
    properties = edge->properties.PropertyTypes();
    if (edge->delta) {
      ApplyDeltasForRead(edge->delta, commit_timestamp, [&properties](const Delta &delta) {
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
  // Here the vertex is locked and changed by this transaction (this means that the only change could be from this tx)
  pre_labels_ = vertex_.labels;
  pre_properties_ = vertex_.properties.PropertyTypes();

  ApplyUncommittedDeltasForRead(delta, [this](const Delta &delta) {
    // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          PropertyTypes_ActionMethod(pre_properties_),
          Labels_ActionMethod(pre_labels_),
        });
    // clang-format on
  });
}

Delta *VertexHandler::NextDelta(const Delta *delta) const { return ::NextDelta(delta, commit_timestamp_); }

void VertexHandler::PostProcess(TransactionEdgeHandler &tx_edge_handler) {
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
    if (!vertex_.in_edges.empty() || !vertex_.out_edges.empty()) {
      tx_edge_handler.AppendToPostProcess(&vertex_);
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
        const auto *in_vertex = delta->vertex_edge.vertex;
        if (in_vertex->gid < vertex_handler.vertex_.gid) {
          // In vertex has a lower GID; needs to be locked first
          // Handle this edge via ADD_IN_EDGE delta
          break;
        }
        tx_edge_handler.RemoveEdge(
            delta,
            [edge_type = delta->vertex_edge.edge_type, from_vertex = &vertex_handler.vertex_,
             to_vertex = in_vertex]() -> Tracking::EdgeType {
              // We are deleting an edge, so we need to generate the EdgeType using pre-transaction labels In case the
              // edge is created during this transaction, the REMOVE_OUT_EDGE will accordingly handle that
              auto lock = std::unique_lock{to_vertex->lock};
              return {edge_type, GetCommittedLabels(*from_vertex), GetCommittedLabels(*to_vertex)};
            });
        break;
      }
      case Delta::Action::REMOVE_OUT_EDGE: {
        const auto *in_vertex = delta->vertex_edge.vertex;
        if (in_vertex->gid < vertex_handler.vertex_.gid) {
          // In vertex has a lower GID; needs to be locked first
          // Handle this edge via REMOVE_IN_EDGE delta
          break;
        }
        tx_edge_handler.AddEdge(
            delta,
            [edge_type = delta->vertex_edge.edge_type, from_vertex = &vertex_handler.vertex_, to_vertex = in_vertex,
             commit_timestamp = vertex_handler.commit_timestamp_]() -> Tracking::EdgeType {
              auto lock = std::unique_lock{to_vertex->lock};
              return {edge_type, GetLabels(*from_vertex, commit_timestamp), GetLabels(*to_vertex, commit_timestamp)};
            });
        break;
      }
      case Delta::Action::ADD_IN_EDGE: {
        const auto *out_vertex = delta->vertex_edge.vertex;
        if (out_vertex->gid < vertex_handler.vertex_.gid) {
          // Out vertex has a lower GID; needs to be locked first
          // Handle this edge via ADD_OUT_EDGE delta
          break;
        }
        tx_edge_handler.RemoveEdge(
            delta,
            [edge_type = delta->vertex_edge.edge_type, from_vertex = out_vertex,
             to_vertex = &vertex_handler.vertex_]() -> Tracking::EdgeType {
              // We are deleting an edge, so we need to generate the EdgeType using pre-transaction labels In case the
              // edge is created during this transaction, the REMOVE_OUT_EDGE will accordingly handle that
              auto lock = std::unique_lock{from_vertex->lock};
              return {edge_type, GetCommittedLabels(*from_vertex), GetCommittedLabels(*to_vertex)};
            });
        break;
      }
      case Delta::Action::REMOVE_IN_EDGE: {
        const auto *out_vertex = delta->vertex_edge.vertex;
        if (out_vertex->gid < vertex_handler.vertex_.gid) {
          // Out vertex has a lower GID; needs to be locked first
          // Handle this edge via REMOVE_OUT_EDGE delta
          break;
        }
        tx_edge_handler.AddEdge(
            delta,
            [edge_type = delta->vertex_edge.edge_type, from_vertex = out_vertex, to_vertex = &vertex_handler.vertex_,
             commit_timestamp = vertex_handler.commit_timestamp_]() -> Tracking::EdgeType {
              auto lock = std::unique_lock{from_vertex->lock};
              return {edge_type, GetLabels(*from_vertex, commit_timestamp), GetLabels(*to_vertex, commit_timestamp)};
            });
        break;
      } break;
    }

    // Advance along the chain
    delta = vertex_handler.NextDelta(delta);
  }

  // Post process
  vertex_handler.PostProcess(tx_edge_handler);
}

void TransactionEdgeHandler::PostVertexProcess() {
  // Remove edges
  for (const auto &[ref, info] : remove_edges_) {
    auto &tracking_info = tracking_[info];
    --tracking_info.n;
    for (const auto &[key, val] : GetPostProperties(ref)) {
      auto &prop_info = tracking_info.properties[key];
      --prop_info.n;
      --prop_info.types[val.type()];
    }
  }

  // Update edges
  for (const auto *v : post_process_vertices_) {
    decltype(v->in_edges) in_edges;
    decltype(v->out_edges) out_edges;
    {
      auto v_lock = std::unique_lock{v->lock};
      in_edges = GetRemainingPreEdges<EdgeDirection::IN>(*v);
      out_edges = GetRemainingPreEdges<EdgeDirection::OUT>(*v);
    }

    // IN EDGES
    for (const auto &edge : in_edges) {
      const auto edge_type = std::get<0>(edge);
      auto *other_vertex = std::get<1>(edge);
      const auto edge_ref = std::get<2>(edge);

      if (ShouldHandleEdge(edge_ref)) {
        // auto plc_itr = pre_labels_cache_.find(other_vertex);
        // if (plc_itr == pre_labels_cache_.end()) {
        //   const auto [itr, _] =
        //       pre_labels_cache_.emplace(other_vertex, GetPreLabels(*other_vertex, commit_timestamp_));
        //   plc_itr = itr;
        // }

        auto from_lock = std::unique_lock{other_vertex->lock, std::defer_lock};
        auto to_lock = std::unique_lock{v->lock, std::defer_lock};

        if (v == other_vertex) {
          from_lock.lock();
        } else if (v->gid < other_vertex->gid) {
          to_lock.lock();
          from_lock.lock();
        } else {
          from_lock.lock();
          to_lock.lock();
        }

        const auto pre_edge_properties = GetCommittedPropertyTypes(edge_ref, properties_on_edges_);
        const auto post_edge_properties = GetPropertyTypes(edge_ref, commit_timestamp_, properties_on_edges_);

        auto &tracking_pre_info =
            tracking_[Tracking::EdgeType{edge_type, GetCommittedLabels(*other_vertex), GetCommittedLabels(*v)}];
        --tracking_pre_info.n;
        for (const auto &[key, val] : pre_edge_properties) {
          auto &prop_info = tracking_pre_info.properties[key];
          --prop_info.n;
          --prop_info.types[val];
        }
        auto &tracking_post_info = tracking_[Tracking::EdgeType{edge_type, GetLabels(*other_vertex, commit_timestamp_),
                                                                GetLabels(*v, commit_timestamp_)}];
        ++tracking_post_info.n;
        for (const auto &[key, val] : post_edge_properties) {
          auto &prop_info = tracking_post_info.properties[key];
          ++prop_info.n;
          ++prop_info.types[val];
        }
      }
    }
    // OUT EDGE
    for (const auto &edge : out_edges) {
      const auto edge_type = std::get<0>(edge);
      auto *other_vertex = std::get<1>(edge);
      const auto edge_ref = std::get<2>(edge);

      if (ShouldHandleEdge(edge_ref)) {
        // auto plc_itr = pre_labels_cache_.find(other_vertex);
        // if (plc_itr == pre_labels_cache_.end()) {
        //   const auto [itr, _] =
        //       pre_labels_cache_.emplace(other_vertex, GetPreLabels(*other_vertex, commit_timestamp_));
        //   plc_itr = itr;
        // }

        auto from_lock = std::unique_lock{other_vertex->lock, std::defer_lock};
        auto to_lock = std::unique_lock{v->lock, std::defer_lock};

        if (v == other_vertex) {
          from_lock.lock();
        } else if (v->gid < other_vertex->gid) {
          to_lock.lock();
          from_lock.lock();
        } else {
          from_lock.lock();
          to_lock.lock();
        }

        const auto pre_edge_properties = GetCommittedPropertyTypes(edge_ref, properties_on_edges_);
        const auto post_edge_properties = GetPropertyTypes(edge_ref, commit_timestamp_, properties_on_edges_);

        auto &tracking_pre_info =
            tracking_[Tracking::EdgeType{edge_type, GetCommittedLabels(*v), GetCommittedLabels(*other_vertex)}];
        --tracking_pre_info.n;
        for (const auto &[key, val] : pre_edge_properties) {
          auto &prop_info = tracking_pre_info.properties[key];
          --prop_info.n;
          --prop_info.types[val];
        }
        auto &tracking_post_info = tracking_[Tracking::EdgeType{edge_type, GetLabels(*v, commit_timestamp_),
                                                                GetLabels(*other_vertex, commit_timestamp_)}];
        ++tracking_post_info.n;
        for (const auto &[key, val] : post_edge_properties) {
          auto &prop_info = tracking_post_info.properties[key];
          ++prop_info.n;
          ++prop_info.types[val];
        }
      }
    }
  }
}

std::map<PropertyId, PropertyValue> TransactionEdgeHandler::GetPreProperties(const EdgeRef &edge_ref) const {
  std::map<PropertyId, PropertyValue> properties;
  if (properties_on_edges_) {
    const auto *edge = edge_ref.ptr;
    auto lock = std::unique_lock{edge->lock};
    properties = edge->properties.Properties();
    if (edge->delta) {
      ApplyUncommittedDeltasForRead(edge->delta, [&properties](const Delta &delta) {
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
    auto lock = std::unique_lock{edge->lock};
    properties = edge->properties.Properties();
    ApplyDeltasForRead(edge->delta, commit_timestamp_, [&properties](const Delta &delta) {
      // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Properties_ActionMethod(properties)
        });
      // clang-format on
    });
  }
  return properties;
}

void EdgeHandler::PostProcess() {
  if (!edge_type_) return;  // Nothing to update
  // We are only removing properties that have existed before transaction
  for (const auto &[id, val] : remove_property_) {
    --tracking_[*edge_type_].properties[id].n;
    --tracking_[*edge_type_].properties[id].types[val];
  }
  // Add the new (or updated) properties
  for (const auto &[id, val] : add_property_) {
    ++tracking_[*edge_type_].properties[id].n;
    ++tracking_[*edge_type_].properties[id].types[val];
  }
}

void EdgeHandler::PreProcessProperties() {
  post_properties_ = GetPropertyTypes(EdgeRef{&edge_}, commit_timestamp_, true /* properties on edges */);
  pre_properties_ = post_properties_;

  ApplyUncommittedDeltasForRead(edge_.delta, [this](const Delta &delta) {
    // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          PropertyTypes_ActionMethod(pre_properties_),
        });
    // clang-format on
  });
}

Delta *EdgeHandler::NextDelta(const Delta *delta) const { return ::NextDelta(delta, commit_timestamp_); }

void EdgeHandler::UpdateProperty(const Delta *delta) {
  const auto key = delta->property.key;
  const auto type = delta->property.value->type();

  if (!edge_type_) {
    // We are only interested in the final position. All other changes are handled elsewhere.
    // We still need to lock the vertices

    const auto *out_vertex = delta->property.out_vertex;

    decltype(out_vertex->out_edges) out_edges;
    {
      auto v_lock = std::unique_lock{out_vertex->lock};
      out_edges = GetRemainingPreEdges<EdgeDirection::OUT>(*out_vertex);
    }

    for (const auto &out_edge : out_edges) {
      if (std::get<2>(out_edge).ptr == &edge_) {
        const auto *in_vertex = std::get<1>(out_edge);

        auto from_lock = std::unique_lock{out_vertex->lock, std::defer_lock};
        auto to_lock = std::unique_lock{in_vertex->lock, std::defer_lock};

        if (in_vertex == out_vertex) {
          from_lock.lock();
        } else if (in_vertex->gid < out_vertex->gid) {
          to_lock.lock();
          from_lock.lock();
        } else {
          from_lock.lock();
          to_lock.lock();
        }

        edge_type_ = Tracking::EdgeType{std::get<0>(out_edge), GetLabels(*out_vertex, commit_timestamp_),
                                        GetLabels(*in_vertex, commit_timestamp_)};
        break;
      }
    }
  }
  // Is this a property that existed pre transaction
  if (pre_properties_.contains(key)) {
    // We are going in order from newest to oldest; so we don't actually want the first, but the last value
    remove_property_.emplace(key, type);
  }
  // Still holding on to the property after commit?
  if (auto type_itr = post_properties_.find(key); type_itr != post_properties_.end()) {
    // We are going in order from newest to oldest; so we only want the first hit (latest value)
    add_property_.try_emplace(key, type_itr->second);
  }
}

}  // namespace memgraph::storage
