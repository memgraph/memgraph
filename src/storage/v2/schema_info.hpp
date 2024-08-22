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

#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <ranges>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include "mgp.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/logging.hpp"
#include "utils/small_vector.hpp"
#include "utils/variant_helpers.hpp"

namespace std {
template <>
struct hash<memgraph::utils::small_vector<memgraph::storage::LabelId>> {
  size_t operator()(const memgraph::utils::small_vector<memgraph::storage::LabelId> &x) const {
    return mgp::util::FnvCollection<memgraph::utils::small_vector<memgraph::storage::LabelId>,
                                    memgraph::storage::LabelId>{}(x);
  }
};
}  // namespace std

// TODO Add namespace schema_info
namespace memgraph::storage {

using find_edge_f = std::function<std::optional<EdgeAccessor>(Gid, View)>;

utils::small_vector<LabelId> GetPreLabels(const Vertex &vertex, uint64_t commit_timestamp);

utils::small_vector<LabelId> GetCommittedLabels(const Vertex &vertex);
std::map<PropertyId, PropertyValue> GetCommittedProperties(const EdgeRef &edge_ref, bool properties_on_edges);

Gid GetEdgeGid(const EdgeRef &edge_ref, bool properties_on_edges);

template <EdgeDirection dir>
auto RemainingEdges_ActionMethod(utils::small_vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> &edges) {
  // clang-format off
  using enum Delta::Action;
  return utils::Overloaded{
      // If we see ADD, that means that this edge got removed (we are not interested in those)
      // ActionMethod <(dir == EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
      //     [&](Delta const &delta) {
      //     }
      // ),
      // If we see REMOVE, that means that this is a new edge that was added during the transaction
      ActionMethod <(dir == EdgeDirection::IN) ? REMOVE_IN_EDGE : REMOVE_OUT_EDGE> (
          [&](Delta const &delta) {
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

struct EdgeRefHasher {
  size_t operator()(const EdgeRef &ref) const {
    size_t combined_hash = 0;
    auto element_hash = [&combined_hash](const auto &elem) {
      size_t element_hash = std::hash<std::decay_t<decltype(elem)>>{}(elem);
      combined_hash ^= element_hash + 0x9e3779b9 + (combined_hash << 6) + (combined_hash >> 2);
    };

    element_hash(static_cast<void *>(ref.ptr));  // Both elements in the union are 64b; just use raw data
    return combined_hash;
  }
};

struct PropertyInfo {
  int n{0};
  std::unordered_map<PropertyValue::Type, int> types;
};
struct TrackingVertexInfo {
  auto N() const { return n; }
  int n{0};
  std::unordered_map<PropertyId, PropertyInfo> properties;
};
struct TrackingEdgeInfo {
  int n{0};
  auto N() const { return edges.size(); }
  std::unordered_map<EdgeRef, std::pair<Vertex * /* from */, Vertex * /* to */>, EdgeRefHasher> edges;
  std::unordered_map<PropertyId, PropertyInfo> properties;
};

struct Tracking {
  using v_key_type = utils::small_vector<LabelId>;

  struct EdgeType {
    EdgeTypeId type;
    v_key_type from_v_type;
    v_key_type to_v_type;

    EdgeType(EdgeTypeId id, v_key_type from, v_key_type to)
        : type{id}, from_v_type{std::move(from)}, to_v_type(std::move(to)) {
      if (!std::is_sorted(from_v_type.begin(), from_v_type.end())) {
        std::sort(from_v_type.begin(), from_v_type.end());
      }
      if (!std::is_sorted(to_v_type.begin(), to_v_type.end())) {
        std::sort(to_v_type.begin(), to_v_type.end());
      }
    }

    EdgeType() = default;
    EdgeType(const EdgeType &) = default;
    EdgeType(EdgeType &&) noexcept = default;
    EdgeType &operator=(const EdgeType &) = default;
    EdgeType &operator=(EdgeType &&) noexcept = default;

    struct hasher {
      size_t operator()(const EdgeType &et) const {
        size_t combined_hash = 0;
        auto element_hash = [&combined_hash](const auto &elem) {
          size_t element_hash = std::hash<std::decay_t<decltype(elem)>>{}(elem);
          combined_hash ^= element_hash + 0x9e3779b9 + (combined_hash << 6) + (combined_hash >> 2);
        };

        element_hash(et.type);
        element_hash(et.from_v_type);
        element_hash(et.to_v_type);
        return combined_hash;
      }
    };

    bool operator==(const EdgeType &other) const {
      return type == other.type && from_v_type == other.from_v_type && to_v_type == other.to_v_type;
    }
  };

  // We want the key to be independent of the vector order.
  // Could create a hash and equals function that is independent
  // Difficult to make them performant and low collision
  // Stick to sorting keys for now
  TrackingVertexInfo &operator[](const v_key_type &key) {
    if (!std::is_sorted(key.begin(), key.end())) {
      auto sorted_key = key;
      std::sort(sorted_key.begin(), sorted_key.end());
      return vertex_state_[sorted_key];
    }
    return vertex_state_[key];
  }

  TrackingEdgeInfo &operator[](const EdgeTypeId &key) {
    // auto [id, from, to] = key;
    // if (!std::is_sorted(from.begin(), from.end())) {
    //   std::sort(from.begin(), from.end());
    // }
    // if (!std::is_sorted(to.begin(), to.end())) {
    //   std::sort(to.begin(), to.end());
    // }
    // return edge_state_[{id, from, to}];
    return edge_state_[key];
  }

  void CleanUp();

  void Print(NameIdMapper &name_id_mapper, bool properties_on_edges);

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, bool properties_on_edges);

  // friend void from_json(const nlohmann::json &j, Tracking &info);

  std::unordered_map<v_key_type, TrackingVertexInfo> vertex_state_;
  std::unordered_map<EdgeTypeId, TrackingEdgeInfo> edge_state_;
};

class TransactionEdgeHandler {
 public:
  explicit TransactionEdgeHandler(Tracking &tracking, uint64_t commit_timestamp, bool properties_on_edges)
      : tracking_{tracking}, commit_timestamp_{commit_timestamp}, properties_on_edges_{properties_on_edges} {}

  template <bool OutEdge>
  void UpdateExistingEdge(auto edge, auto &pre_labels, auto &post_labels, Vertex *v = {}) {
    // Label deltas could move edges multiple times, from both ends
    // We are only interested in the start and end state
    // That means we only need to move the edge once, irrelevant of how many deltas could influence it
    // Labels can only move, creation or deletion is handled via other deltas

    const auto edge_type = std::get<0>(edge);
    auto *other_vertex = std::get<1>(edge);
    const auto edge_ref = std::get<2>(edge);

    if (ShouldHandleEdge(edge_ref)) {
      const auto &post_other_labels = other_vertex->labels;
      auto plc_itr = pre_labels_cache_.find(other_vertex);
      if (plc_itr == pre_labels_cache_.end()) {
        const auto [itr, _] = pre_labels_cache_.emplace(other_vertex, GetPreLabels(*other_vertex, commit_timestamp_));
        plc_itr = itr;
      }
      const auto pre_edge_properties = GetPreProperties(edge_ref);
      const auto post_edge_properties = GetPostProperties(edge_ref);

      if constexpr (OutEdge) {
        auto &tracking_pre_info = tracking_[edge_type];
        --tracking_pre_info.n;
        for (const auto &[key, val] : pre_edge_properties) {
          auto &prop_info = tracking_pre_info.properties[key];
          --prop_info.n;
          --prop_info.types[val.type()];
        }
        auto &tracking_post_info = tracking_[edge_type];
        tracking_post_info.edges.emplace(edge_ref, std::make_pair(v, other_vertex));
        for (const auto &[key, val] : pre_edge_properties) {
          auto &prop_info = tracking_post_info.properties[key];
          ++prop_info.n;
          ++prop_info.types[val.type()];
        }
      } else {
        auto &tracking_pre_info = tracking_[edge_type];
        --tracking_pre_info.n;
        for (const auto &[key, val] : post_edge_properties) {
          auto &prop_info = tracking_pre_info.properties[key];
          --prop_info.n;
          --prop_info.types[val.type()];
        }
        auto &tracking_post_info = tracking_[edge_type];
        tracking_post_info.edges.emplace(edge_ref, std::make_pair(other_vertex, v));
        for (const auto &[key, val] : post_edge_properties) {
          auto &prop_info = tracking_post_info.properties[key];
          ++prop_info.n;
          ++prop_info.types[val.type()];
        }
      }
    }
  }

  void RemoveEdge(const Delta *delta, const auto &pre_labels) {
    DMG_ASSERT(delta->action == Delta::Action::ADD_OUT_EDGE, "Trying to remove edge using the wrong delta");

    // Already flagged to be removed
    if (remove_edges_.find(delta->vertex_edge.edge) != remove_edges_.end()) return;

    // Block edge loop from updating
    // ShouldHandleEdge(delta->vertex_edge.edge);
    auto plc_itr = pre_labels_cache_.find(delta->vertex_edge.vertex);
    if (plc_itr == pre_labels_cache_.end()) {
      const auto [itr, _] = pre_labels_cache_.emplace(delta->vertex_edge.vertex,
                                                      GetPreLabels(*delta->vertex_edge.vertex, commit_timestamp_));
      plc_itr = itr;
    }
    remove_edges_.emplace(delta->vertex_edge.edge,
                          Tracking::EdgeType{delta->vertex_edge.edge_type, pre_labels, plc_itr->second});
  }

  void AddEdge(const Delta *delta, const auto &post_labels, Vertex *v = {}) {
    DMG_ASSERT(delta->action == Delta::Action::REMOVE_OUT_EDGE, "Trying to add edge using the wrong delta");

    // ADD is processed after all REMOVEs, so just remove from the set
    const auto erased = remove_edges_.erase(delta->vertex_edge.edge);
    if (erased != 0) {
      // This edge will be erased in the future, no need to add it at all
      return;
    }

    // Update inplace, but block edge loop from updating
    // ShouldHandleEdge(delta->vertex_edge.edge);
    auto &tracking_info = tracking_[delta->vertex_edge.edge_type];
    tracking_info.edges.emplace(delta->vertex_edge.edge, std::make_pair(v, delta->vertex_edge.vertex));
    // for (const auto &[key, val] : GetPostProperties(delta->vertex_edge.edge)) {
    //   auto &prop_info = tracking_info.properties[key];
    //   ++prop_info.n;
    //   ++prop_info.types[val.type()];
    // }
  }

  bool ShouldHandleEdge(const EdgeRef &edge_ref) {
    Gid edge_gid = GetEdgeGid(edge_ref, properties_on_edges_);
    return ShouldHandleEdge(edge_gid);
  }

  bool ShouldHandleEdge(Gid edge_gid) {
    const auto [_, emplaced] = handled_edges.emplace(edge_gid);
    return emplaced;
  }

  void PostVertexProcess() {
    for (const auto &[ref, info] : remove_edges_) {
      auto &tracking_info = tracking_[info.type];
      tracking_info.edges.erase(ref);
      // for (const auto &[key, val] : GetPostProperties(ref)) {
      //   auto &prop_info = tracking_info.properties[key];
      //   --prop_info.n;
      //   --prop_info.types[val.type()];
      // }
    }
  }

 private:
  std::map<PropertyId, PropertyValue> GetPreProperties(const EdgeRef &edge_ref) const;
  std::map<PropertyId, PropertyValue> GetPostProperties(const EdgeRef &edge_ref) const;

  Tracking &tracking_;
  std::unordered_set<Gid> handled_edges{};
  uint64_t commit_timestamp_{-1UL};
  bool properties_on_edges_{false};

  struct EdgeRefHasher {
    size_t operator()(const EdgeRef &ref) const {
      size_t combined_hash = 0;
      auto element_hash = [&combined_hash](const auto &elem) {
        size_t element_hash = std::hash<std::decay_t<decltype(elem)>>{}(elem);
        combined_hash ^= element_hash + 0x9e3779b9 + (combined_hash << 6) + (combined_hash >> 2);
      };

      // GID/pointer already unique, no need to combine
      element_hash(static_cast<void *>(ref.ptr));  // Both are uint64
      return combined_hash;
    }
  };

  std::unordered_map<Vertex *, utils::small_vector<LabelId>> pre_labels_cache_;
  std::unordered_map<EdgeRef, Tracking::EdgeType, EdgeRefHasher> remove_edges_;
};

class VertexHandler {
 public:
  explicit VertexHandler(Vertex &vertex, Tracking &tracking, uint64_t commit_timestamp)
      : vertex_{vertex}, commit_timestamp_{commit_timestamp}, guard_{vertex_.lock}, tracking_{tracking} {}

  void PreProcess();

  Delta *FirstDelta() const { return vertex_.delta; }

  Delta *NextDelta(const Delta *delta) const;

  void AddVertex() {
    label_update_ = true;
    vertex_added_ = true;
  }

  void RemoveVertex() {
    // vertex already has delete flag
    label_update_ = true;
  }

  void UpdateLabel() { label_update_ = true; }

  void UpdateProperty(PropertyId key, const PropertyValue &value) {
    // Is this a property that existed pre transaction
    if (pre_properties_.contains(key)) {
      // We are going in order from newest to oldest; so we don't actually want the first, but the last value
      remove_property_.emplace(key, value);
    }
    // Still holding on to the property after commit?
    if (!vertex_.deleted && vertex_.properties.HasProperty(key)) {
      add_property_.try_emplace(key, vertex_.properties.GetProperty(key));
    }
  }

  void ProcessRemainingPreEdges();

  template <bool OutEdge>
  bool GetsDeleted(auto &edge_identifier) const {
    if constexpr (OutEdge) {
      const auto &post_out_edges = vertex_.out_edges;
      return std::find(post_out_edges.begin(), post_out_edges.end(), edge_identifier) == post_out_edges.end();
    }
    const auto &post_in_edges = vertex_.in_edges;
    return std::find(post_in_edges.begin(), post_in_edges.end(), edge_identifier) == post_in_edges.end();
  }

  // TODO: Think about having an optional and hydrating only if needed
  const auto &PreLabels() const { return pre_labels_; }

  // Current labels are the post labels, multiple changes from different transactions to the same vertex would generate
  // a serialization error
  // TODO Double check
  const auto &PostLabels() const { return vertex_.labels; }

  void PostProcess(TransactionEdgeHandler &edge_handler);

  Vertex &vertex_;

 private:
  uint64_t commit_timestamp_{-1UL};
  std::shared_lock<decltype(vertex_.lock)> guard_;
  decltype(vertex_.labels) pre_labels_;
  decltype(vertex_.properties.Properties()) pre_properties_;
  decltype(vertex_.in_edges) pre_remaining_in_edges_;
  decltype(vertex_.out_edges) pre_remaining_out_edges_;

  bool label_update_{false};
  bool vertex_added_{false};
  std::unordered_map<PropertyId, PropertyValue> remove_property_;
  std::unordered_map<PropertyId, PropertyValue> add_property_;

  Tracking &tracking_;

  //
  //
  //
  // How to connect an edge to a changing vertex?
  // What is protected at delta time?
  //  - any operation on the object itself
  //  - add/remove labels/properties on vertices or edges
  //
  // If we need to lock both ends, we need to lock in order of GID
  // We never lock all 3 (in/out vertex and edge)
  //
  //
  // Covered:
  //  - vertex info label/property changes (without edge updating)
  //
  // Not covered:
  //  - anything with the edge
  //
  //
  // Lets assume we protect the schema with the engine lock (no need to think about concurrency there <- for now...)
  //
  // Edges can change due to:
  //  - out vertex add/remove edge delta
  //  - in/out vertex label change
  //  - edge property change
  //
  // All require 3 way locking <- basically impossible
  // We need to split the changes and recombine at SHOW SCHEMA INFO time
  //
  // Vertex info stores:
  //  - an unordered set of vertex pointers <- basically instead of n
  //  - still need to maintain properties as they are now; since we are locked should not be a problem
  // Edge info stores:
  //  - an unordered map of edgeref to from/to vertex pointers
  //  - properties as is (under lock so ok)
  //
  //
};

class EdgeHandler {
 public:
  explicit EdgeHandler(Edge &edge, Tracking &tracking, uint64_t commit_timestamp)
      : edge_{edge}, commit_timestamp_{commit_timestamp}, guard_{edge.lock}, tracking_{tracking} {}

  template <typename Func>
  void PreProcess(Func &&find_edge) {
    const auto edge = find_edge(Gid(), View::NEW);
    DMG_ASSERT(edge, "Trying to update a non-existent edge");
    // We are only interested in the final position. All other changes are handled elsewhere.
    edge_type_ = Tracking::EdgeType{edge->edge_type_, edge->from_vertex_->labels, edge->to_vertex_->labels};

    PreProcessProperties();
  }

  Delta *FirstDelta() const { return edge_.delta; }

  Delta *NextDelta(const Delta *delta) const;

  void UpdateProperty(PropertyId key, const PropertyValue &value) {
    // Is this a property that existed pre transaction
    if (pre_properties_.contains(key)) {
      // We are going in order from newest to oldest; so we don't actually want the first, but the last value
      remove_property_.emplace(key, value);
    }
    // Still holding on to the property after commit?
    if (!edge_.deleted && edge_.properties.HasProperty(key)) {
      // We are going in order from newest to oldest; so we only want the first hit (latest value)
      add_property_.try_emplace(key, edge_.properties.GetProperty(key));
    }
  }

  void PostProcess();

  Gid Gid() const { return edge_.gid; }

 private:
  void PreProcessProperties();

  Edge &edge_;
  uint64_t commit_timestamp_{-1UL};
  std::shared_lock<decltype(edge_.lock)> guard_;

  Tracking::EdgeType edge_type_;

  decltype(edge_.properties.Properties()) pre_properties_;

  std::unordered_map<PropertyId, PropertyValue> remove_property_;
  std::unordered_map<PropertyId, PropertyValue> add_property_;

  Tracking &tracking_;
};

struct SchemaInfo {
  template <typename Func>
  void ProcessTransaction(Transaction &transaction, bool properties_on_edges, Func &&find_edge) {
    const auto commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);
    TransactionEdgeHandler tx_edge_handler{tracking_, commit_timestamp, properties_on_edges};
    // First run through all vertices
    for (const auto &delta : transaction.deltas) {
      // Find VERTEX or EDGE; handle object's delta chain
      auto prev = delta.prev.Get();
      DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type == PreviousPtr::Type::VERTEX) {
        VertexHandler vertex_handler{*prev.vertex, tracking_, commit_timestamp};
        ProcessVertex(vertex_handler, tx_edge_handler);
      }
    }
    tx_edge_handler.PostVertexProcess();
    // // Handle edges only after vertices are handled
    // // Here we will handle only edge property changes
    // // Other edge changes are handled by vertex deltas
    // for (const auto &delta : transaction.deltas) {
    //   // Find VERTEX or EDGE; handle object's delta chain
    //   auto prev = delta.prev.Get();
    //   DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    //   if (prev.type == PreviousPtr::Type::EDGE) {
    //     EdgeHandler edge_handler{*prev.edge, tracking_, commit_timestamp};
    //     ProcessEdge(edge_handler, tx_edge_handler, std::forward<Func>(find_edge));
    //   }
    // }
  }

  void CleanUp() { tracking_.CleanUp(); }

  void Print(NameIdMapper &name_id_mapper, bool properties_on_edges) {
    tracking_.Print(name_id_mapper, properties_on_edges);
  }

  auto ToJson(NameIdMapper &name_id_mapper, bool properties_on_edges) {
    return tracking_.ToJson(name_id_mapper, properties_on_edges);
  }

  // friend void from_json(const nlohmann::json &j, SchemaInfo &info);

  void ProcessVertex(VertexHandler &vertex_handler, TransactionEdgeHandler &edge_handler);

  template <typename Func>
  void ProcessEdge(EdgeHandler &edge_handler, TransactionEdgeHandler &tx_edge_handler, Func &&find_edge) {
    if (!tx_edge_handler.ShouldHandleEdge(edge_handler.Gid())) {
      // Edge already processed
      return;
    }

    const auto *delta = edge_handler.FirstDelta();
    DMG_ASSERT(delta, "Processing edge without delta");

    edge_handler.PreProcess(std::forward<Func>(find_edge));

    while (delta) {
      switch (delta->action) {
        case Delta::Action::SET_PROPERTY: {
          // Have to use storage->FindEdge in order to find the out/in vertices
          edge_handler.UpdateProperty(delta->property.key, *delta->property.value);
          break;
        }
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
          // All other changes are handled via vertex changes
          break;
      }

      // Advance along the chain
      delta = edge_handler.NextDelta(delta);
    }

    edge_handler.PostProcess();
  }

  Tracking tracking_;
};

}  // namespace memgraph::storage
