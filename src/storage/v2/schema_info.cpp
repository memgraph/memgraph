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

template <memgraph::storage::EdgeDirection dir>
auto RemainingEdges_ActionMethod(
    memgraph::utils::small_vector<
        std::tuple<memgraph::storage::EdgeTypeId, memgraph::storage::Vertex *, memgraph::storage::EdgeRef>> &edges) {
  // clang-format off
  using enum memgraph::storage::Delta::Action;
  return memgraph::utils::Overloaded{
      // If we see ADD, that means that this edge got removed (we are not interested in those)
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

template <memgraph::storage::EdgeDirection dir>
auto RemovedEdges_ActionMethod(
    memgraph::utils::small_vector<
        std::tuple<memgraph::storage::EdgeTypeId, memgraph::storage::Vertex *, memgraph::storage::EdgeRef>> &edges) {
  // clang-format off
  using enum memgraph::storage::Delta::Action;
  return memgraph::utils::Overloaded{
      // If we see ADD, that means that this edge got removed (we are not interested in those)
      memgraph::storage::ActionMethod <(dir == memgraph::storage::EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
          [&](memgraph::storage::Delta const &delta) {
              edges.emplace_back(delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge);
          }
      ),
      // If we see REMOVE, that means that this is a new edge that was added during the transaction
      // need to check that a removed edge was not also added during the tx
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

template <memgraph::storage::EdgeDirection Dir>
auto GetRemovedPreEdges(const memgraph::storage::Vertex &vertex) {
  decltype(vertex.in_edges) edges;

  ApplyUncommittedDeltasForRead(vertex.delta, [&edges](const memgraph::storage::Delta &delta) {
    // clang-format off
    DeltaDispatch(delta, memgraph::utils::ChainedOverloaded{
      RemovedEdges_ActionMethod<Dir>(edges),
    });
    // clang-format on
  });

  return edges;
}

template <memgraph::storage::EdgeDirection Dir>
auto GetEdges(const memgraph::storage::Vertex &vertex, uint64_t commit_timestamp) {
  decltype(vertex.in_edges) edges;

  if (Dir == memgraph::storage::EdgeDirection::OUT) {
    edges = vertex.out_edges;
  } else {
    edges = vertex.in_edges;
  }

  ApplyDeltasForRead(vertex.delta, commit_timestamp, [&edges](const memgraph::storage::Delta &delta) {
    // clang-format off
    DeltaDispatch(delta, memgraph::utils::ChainedOverloaded{
      Edges_ActionMethod<Dir>(edges, {/* all types */}, {/* all destinations */}),
    });
    // clang-format on
  });

  return edges;
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

inline auto Existence_ActionMethod(bool &exists) {
  // clang-format off
  using enum memgraph::storage::Delta::Action;
  return memgraph::utils::Overloaded{ActionMethod<DELETE_OBJECT>([&](Delta const &/*delta*/) { exists = false; }),
                                     ActionMethod<DELETE_DESERIALIZED_OBJECT>([&](Delta const &/*delta*/) { exists = false; })};
  // clang-format on
}

void Tracking::ProcessTransaction(Transaction &transaction, bool properties_on_edges) {
  // NOTE: commit_timestamp is actually the transaction ID, sa this is called before we finalize the commit
  const auto commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);

  // Edges have multiple objects that define their changes, this object keeps track of all the changes during a tx
  TransactionEdgeHandler tx_edge_handler{*this, commit_timestamp, properties_on_edges};

  // First run through all vertices
  for (const auto &delta : transaction.deltas) {
    // Find VERTEX; handle object's delta chain
    auto prev = delta.prev.Get();
    DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type == PreviousPtr::Type::VERTEX) {
      VertexHandler vertex_handler{*prev.vertex, *this, commit_timestamp};
      vertex_handler.Process(tx_edge_handler);
    }
  }

  // Handle any lingering transaction-wide changes
  tx_edge_handler.PostVertexProcess();

  // Handle edges only after vertices are handled
  // Here we will handle only edge property changes
  // Other edge changes are handled by vertex deltas
  if (properties_on_edges) {
    for (const auto &delta : transaction.deltas) {
      // Find EDGE; handle object's delta chain
      auto prev = delta.prev.Get();
      DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type == PreviousPtr::Type::EDGE) {
        EdgeHandler edge_handler{*prev.edge, *this, commit_timestamp};
        edge_handler.Process();
      }
    }
  }
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
  const auto &[prop_itr, __] = tracking_info.emplace("properties", nlohmann::json::array_t{});
  for (const auto &[p, info] : properties) {
    prop_itr->second.emplace_back(info.ToJson(enum_store, name_id_mapper.IdToName(p.AsUint()), std::max(n, 1)));
  }
  return tracking_info;
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
    for (const auto l : labels) {
      labels_itr->emplace_back(name_id_mapper.IdToName(l.AsUint()));
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
    for (const auto l : edge_type.from) {
      out_labels_itr->emplace_back(name_id_mapper.IdToName(l.AsUint()));
    }
    std::sort(out_labels_itr->begin(), out_labels_itr->end());
    const auto &[in_labels_itr, _b] = edge.emplace("end_node_labels", nlohmann::json::array_t{});
    for (const auto l : edge_type.to) {
      in_labels_itr->emplace_back(name_id_mapper.IdToName(l.AsUint()));
    }
    std::sort(in_labels_itr->begin(), in_labels_itr->end());
    edge.update(info.ToJson(name_id_mapper, enum_store));
    edges.emplace_back(std::move(edge));
  }

  return json;
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

inline utils::small_vector<LabelId> GetLabels(const Vertex &vertex, uint64_t commit_timestamp) {
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

void VertexHandler::UpdateProperty(PropertyId key, const PropertyValue &value) {
  update_property_ = true;
  // TODO Move the post process property diff here
}

const auto &VertexHandler::PreLabels() const {
  if (!pre_labels_) {
    // Here the vertex is locked and changed by this transaction (this means that the only change could be from this
    // tx)
    pre_labels_ = vertex_.labels;
    ApplyUncommittedDeltasForRead(vertex_.delta, [this](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Labels_ActionMethod(*pre_labels_),
      });
      // clang-format on
    });
  }
  return *pre_labels_;
}

auto VertexHandler::GetPropertyDiff() const {
  std::unordered_map<memgraph::storage::PropertyId,
                     std::pair<memgraph::storage::ExtendedPropertyType, memgraph::storage::ExtendedPropertyType>>
      diff;

  // Prefill diff with current properties (as if no change has been made)
  for (const auto &[key, type] : vertex_.properties.ExtendedPropertyTypes()) {
    diff.emplace(key, std::make_pair(vertex_.deleted ? ExtendedPropertyType{} : type, type));
  }

  ApplyUncommittedDeltasForRead(vertex_.delta, [&diff](const memgraph::storage::Delta &delta) {
    // clang-format off
    DeltaDispatch(delta, memgraph::utils::ChainedOverloaded{
                             PropertyType_ActionMethod(diff),
                         });
    // clang-format on
  });

  return diff;
}

void VertexHandler::PostProcess(TransactionEdgeHandler &tx_edge_handler) {
  auto *tracking_post_info = &tracking_[PostLabels()];
  auto *tracking_pre_info = tracking_post_info;

  // this is true on create; maybe a way to avoid
  if (label_update_) {
    // Vertex still active, update new label stats
    if (!vertex_.deleted) {
      ++tracking_post_info->n;
    }

    // Vertex existed before this tx
    if (!vertex_added_) {
      // Stats have moved; re-read labels pre tx and remove stats
      tracking_pre_info = &tracking_[PreLabels()];
      --tracking_pre_info->n;

      // Edges are defined via start/end labels, this identification has changed so update all persistent edges
      for (const auto &edge : GetRemainingPreEdges<EdgeDirection::IN>(vertex_)) {
        const auto edge_type = std::get<0>(edge);
        auto *other_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);

        tx_edge_handler.AppendToPostProcess(edge_ref, edge_type, other_vertex, &vertex_);
      }
      for (const auto &edge : GetRemainingPreEdges<EdgeDirection::OUT>(vertex_)) {
        const auto edge_type = std::get<0>(edge);
        auto *other_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);

        tx_edge_handler.AppendToPostProcess(edge_ref, edge_type, &vertex_, other_vertex);
      }

      // Deleted edges are using the latest label; update key
      for (const auto &edge : GetRemovedPreEdges<EdgeDirection::IN>(vertex_)) {
        const auto edge_type = std::get<0>(edge);
        auto *other_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);

        tx_edge_handler.UpdateRemovedEdgeKey(edge_ref, edge_type, other_vertex, &vertex_);
      }
      for (const auto &edge : GetRemovedPreEdges<EdgeDirection::OUT>(vertex_)) {
        const auto edge_type = std::get<0>(edge);
        auto *other_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);

        tx_edge_handler.UpdateRemovedEdgeKey(edge_ref, edge_type, &vertex_, other_vertex);
      }
    }
  }

  // Update property stats if labels (vertex stats identification) or properties have changed
  if (label_update_ || update_property_) {
    for (const auto &[key, diff] : GetPropertyDiff()) {
      if (label_update_ || diff.first != diff.second) {
        if (diff.second != ExtendedPropertyType{}) {
          auto &info = tracking_pre_info->properties[key];
          --info.n;
          --info.types[diff.second];
        }
        if (diff.first != ExtendedPropertyType{}) {
          auto &info = tracking_post_info->properties[key];
          ++info.n;
          ++info.types[diff.first];
        }
      }
    }
  }
}

void VertexHandler::Process(TransactionEdgeHandler &tx_edge_handler) {
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

  const auto *delta = vertex_.delta;

  while (delta) {
    if (delta->timestamp->load(std::memory_order_acquire) != commit_timestamp_) break;
    switch (delta->action) {
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        // This is an empty object without any labels or properties
        AddVertex();
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        // This is an empty object without any labels or properties
        RemoveVertex();
        break;
      }
      case Delta::Action::SET_PROPERTY: {
        UpdateProperty(delta->property.key, *delta->property.value);
        break;
      }
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL: {
        UpdateLabel();
        break;
      }
      case Delta::Action::ADD_OUT_EDGE: {
        tx_edge_handler.RemoveEdge(delta, delta->vertex_edge.edge_type, &vertex_, delta->vertex_edge.vertex);
        break;
      }
      case Delta::Action::REMOVE_OUT_EDGE: {
        tx_edge_handler.AddEdge(delta, delta->vertex_edge.edge_type, &vertex_, delta->vertex_edge.vertex);
        break;
      }
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
        // Handling all via OUT_EDGE
        break;
    }

    // Advance along the chain
    delta = delta->next.load(std::memory_order_acquire);
  }

  // Post process
  PostProcess(tx_edge_handler);
}

void TransactionEdgeHandler::RemoveEdge(const Delta *delta, EdgeTypeId edge_type, Vertex *from_vertex,
                                        Vertex *to_vertex) {
  // Optimistically adding EdgeKeyRef; post process will update if vertex label changed
  remove_edges_.emplace(std::piecewise_construct, std::make_tuple(delta->vertex_edge.edge),
                        std::make_tuple(EdgeKeyWrapper::RefTag{}, edge_type, std::cref(from_vertex->labels),
                                        std::cref(to_vertex->labels)));
}

void TransactionEdgeHandler::UpdateRemovedEdgeKey(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex,
                                                  Vertex *to_vertex) {
  // Labels changed, update key
  // TODO Avoid erasing
  if (remove_edges_.erase(edge) != 0) {
    remove_edges_.emplace(std::piecewise_construct, std::make_tuple(edge),
                          std::make_tuple(EdgeKeyWrapper::CopyTag{}, edge_type, GetCommittedLabels(*from_vertex),
                                          GetCommittedLabels(*to_vertex)));
  }
}

void TransactionEdgeHandler::AddEdge(const Delta *delta, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex) {
  // DMG_ASSERT(delta->action == Delta::Action::REMOVE_OUT_EDGE, "Trying to add edge using the wrong delta");

  // ADD is processed after all REMOVEs, so just remove from the set
  const auto erased = remove_edges_.erase(delta->vertex_edge.edge);
  if (erased != 0) {
    // This edge will be erased in the future, no need to add it at all
    return;
  }

  // New edge (update all stats, that way we don't have to re-lock anything afterwards)
  // Both edges get updated, so both need to be changed only by this tx (otherwise a
  // serialization error occurs); no need to apply deltas
  auto &tracking_info = tracking_[EdgeKeyRef{edge_type, from_vertex->labels, to_vertex->labels}];
  ++tracking_info.n;
  if (properties_on_edges_) {
    for (const auto &[key, val] : delta->vertex_edge.edge.ptr->properties.ExtendedPropertyTypes()) {
      auto &prop_post_info = tracking_info.properties[key];
      ++prop_post_info.n;
      ++prop_post_info.types[val];
    }
  }
}

void TransactionEdgeHandler::PostVertexProcess() {
  // Remove edges
  for (const auto &[ref, info] : remove_edges_) {
    auto &tracking_info = tracking_[info];
    --tracking_info.n;
    for (const auto &[key, val] : GetPrePropertyTypes(ref, false)) {
      auto &prop_info = tracking_info.properties[key];
      --prop_info.n;
      --prop_info.types[val];
    }
  }

  // Update edges
  for (const auto &edge_info : post_process_edges_) {
    const auto edge_ref = std::get<0>(edge_info);
    const auto edge_type = std::get<1>(edge_info);
    const auto *from = std::get<2>(edge_info);
    const auto *to = std::get<3>(edge_info);

    const auto pre_edge_properties = GetPrePropertyTypes(edge_ref, true);

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

    auto &tracking_pre_info = tracking_[EdgeKey{edge_type, GetCommittedLabels(*from), GetCommittedLabels(*to)}];
    auto &tracking_post_info =
        tracking_[EdgeKey{edge_type, GetLabels(*from, commit_timestamp_), GetLabels(*to, commit_timestamp_)}];

    from_lock.unlock();
    if (to_lock.owns_lock()) to_lock.unlock();

    --tracking_pre_info.n;
    ++tracking_post_info.n;
    for (const auto &[key, val] : pre_edge_properties) {
      auto &prop_pre_info = tracking_pre_info.properties[key];
      --prop_pre_info.n;
      --prop_pre_info.types[val];
      auto &prop_post_info = tracking_post_info.properties[key];
      ++prop_post_info.n;
      ++prop_post_info.types[val];
    }
  }
}

std::map<PropertyId, ExtendedPropertyType> TransactionEdgeHandler::GetPrePropertyTypes(const EdgeRef &edge_ref,
                                                                                       bool lock) const {
  std::map<PropertyId, ExtendedPropertyType> properties;
  if (properties_on_edges_) {
    const auto *edge = edge_ref.ptr;
    auto guard = std::invoke([&]() -> std::optional<std::unique_lock<decltype(edge->lock)>> {
      if (lock) return std::unique_lock{edge->lock};
      return std::nullopt;
    });
    properties = edge->properties.ExtendedPropertyTypes();
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

bool EdgeHandler::ExistedPreTx() const {
  bool res = true;
  ApplyUncommittedDeltasForRead(edge_.delta, [&res](const Delta &delta) {
    // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Exists_ActionMethod(res),
        });
    // clang-format on
  });
  return res;
}

auto EdgeHandler::PropertyType_ActionMethod(
    const memgraph::storage::PropertyStore &properties,
    std::unordered_map<memgraph::storage::PropertyId,
                       std::pair<memgraph::storage::ExtendedPropertyType, memgraph::storage::ExtendedPropertyType>>
        &diff) {
  // clang-format off
  using enum memgraph::storage::Delta::Action;
  return memgraph::storage::ActionMethod<SET_PROPERTY>([this, &properties,
                                                        &diff](memgraph::storage::Delta const &delta) {
    if (!edge_key_) {
      // Hydrate the edge key: edge is defined by the edge type, to/from labels
      // Delta contains the from vertex, find the edge and its to vertex
      // Handle both the case when edge is new and both vertices are guaranteed to have been changed by the tx
      // (no locking required) and the case when only the edge is changed (locking required).
      auto *out_vertex = delta.property.out_vertex;
      decltype(out_vertex->out_edges)::value_type out_edge{EdgeTypeId{}, nullptr, EdgeRef{nullptr}};
      auto from_lock = std::unique_lock{out_vertex->lock, std::defer_lock};
      if (needs_to_lock_) {
        from_lock.lock();
      }

      // Optimistic search
      const auto edge_itr = std::find_if(out_vertex->out_edges.begin(), out_vertex->out_edges.end(),
                                         [ptr = &edge_](const auto &elem) { return std::get<2>(elem).ptr == ptr; });
      if (edge_itr != out_vertex->out_edges.end()) {
        out_edge = *edge_itr;
      } else {
        // Not found; regenerate edges as seen by this transaction in case someone else deleted this edge
        const auto out_edges = GetEdges<EdgeDirection::OUT>(*out_vertex, commit_timestamp_);
        const auto edge_itr = std::find_if(out_edges.begin(), out_edges.end(),
                                           [ptr = &edge_](const auto &elem) { return std::get<2>(elem).ptr == ptr; });
        out_edge = *edge_itr;
      }

      auto *in_vertex = std::get<1>(out_edge);
      auto to_lock = std::unique_lock{in_vertex->lock, std::defer_lock};
      if (needs_to_lock_) {
        if (in_vertex == out_vertex) {
          // from_lock.lock(); <- already locked
        } else if (in_vertex->gid < out_vertex->gid) {
          from_lock.unlock();
          to_lock.lock();
          from_lock.lock();
        } else {
          // from_lock.lock(); <- already locked
          to_lock.lock();
        }
        // Vertex labels are not guaranteed to be stable, re-generate them and use EdgeKey (not *Ref)
        edge_key_.emplace(EdgeKeyWrapper::CopyTag{}, std::get<0>(out_edge), GetLabels(*out_vertex, commit_timestamp_),
                          GetLabels(*in_vertex, commit_timestamp_));
      } else {
        // Since the vertices are stable, no need to re-generate the labels; EdgeKeyRef can be used
        edge_key_.emplace(EdgeKeyWrapper::RefTag{}, std::get<0>(out_edge), out_vertex->labels, in_vertex->labels);
      }
    }

    const auto key = delta.property.key;
    const auto old_type = ExtendedPropertyType{*delta.property.value};
    auto itr = diff.find(key);
    if (itr == diff.end()) {
      diff.emplace(key,
                   std::make_pair(edge_.deleted ? ExtendedPropertyType{} : properties.GetExtendedPropertyType(key), old_type));
    } else {
      itr->second.second = old_type;
    }
  });
  // clang-format on
}

auto EdgeHandler::GetPropertyDiff(const memgraph::storage::PropertyStore &properties, memgraph::storage::Delta *delta) {
  std::unordered_map<memgraph::storage::PropertyId,
                     std::pair<memgraph::storage::ExtendedPropertyType, memgraph::storage::ExtendedPropertyType>>
      diff;

  ApplyUncommittedDeltasForRead(delta, [this, &properties, &diff](const memgraph::storage::Delta &delta) {
    // clang-format off
    DeltaDispatch(delta, memgraph::utils::ChainedOverloaded{
                             PropertyType_ActionMethod(properties, diff),
                         });
    // clang-format on
  });

  return diff;
}

void EdgeHandler::Process() {
  // NOTE: When adding/removing edge, all changes are handled during vertex processing
  // Uncomment this line to move edge property updates here instead of during vertex processing
  // needs_to_lock_ = !edge_.deleted && ExistedPreTx();
  if (edge_.deleted || !ExistedPreTx()) return;  // Nothing to do, all has been handled by vertex handlers

  const auto diff_ = GetPropertyDiff(edge_.properties, edge_.delta);
  if (diff_.empty()) return;

  auto &tracking_info = tracking_[*edge_key_];
  for (const auto &[key, diff] : diff_) {
    if (diff.first != diff.second) {
      auto &info = tracking_info.properties[key];
      if (diff.second != ExtendedPropertyType{}) {
        --info.n;
        --info.types[diff.second];
      }
      if (diff.first != ExtendedPropertyType{}) {
        ++info.n;
        ++info.types[diff.first];
      }
    }
  }
}

size_t EdgeKey::equal_to::operator()(const EdgeKey &lhs, const EdgeKey &rhs) const { return lhs == rhs; }
size_t EdgeKey::equal_to::operator()(const EdgeKey &lhs, const EdgeKeyRef &rhs) const { return lhs == rhs; }
size_t EdgeKey::equal_to::operator()(const EdgeKeyRef &lhs, const EdgeKey &rhs) const { return rhs == lhs; }

}  // namespace memgraph::storage
