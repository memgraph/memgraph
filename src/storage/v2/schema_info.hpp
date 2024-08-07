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
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/logging.hpp"
#include "utils/small_vector.hpp"

namespace std {
template <>
struct hash<memgraph::utils::small_vector<memgraph::storage::LabelId>> {
  size_t operator()(const memgraph::utils::small_vector<memgraph::storage::LabelId> &x) const {
    return mgp::util::FnvCollection<memgraph::utils::small_vector<memgraph::storage::LabelId>,
                                    memgraph::storage::LabelId>{}(x);
  }
};
}  // namespace std

namespace memgraph::storage {

struct TrackingInfo {
  int n{0};
  struct PropertyInfo {
    int n{0};
    std::unordered_map<PropertyValue::Type, int> types;
  };
  std::unordered_map<PropertyId, PropertyInfo> properties;
};

struct Tracking {
  using v_key_type = utils::small_vector<LabelId>;

  struct EdgeType {
    EdgeTypeId type;
    v_key_type from_v_type;
    v_key_type to_v_type;

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
  TrackingInfo &operator[](const v_key_type &key) {
    if (!std::is_sorted(key.begin(), key.end())) {
      auto sorted_key = key;
      std::sort(sorted_key.begin(), sorted_key.end());
      return vertex_state_[sorted_key];
    }
    return vertex_state_[key];
  }

  TrackingInfo &operator[](const EdgeType &key) { return edge_state_[key]; }

  void CleanUp();

  void Print(NameIdMapper &name_id_mapper);

  std::unordered_map<v_key_type, TrackingInfo> vertex_state_;
  std::unordered_map<EdgeType, TrackingInfo, EdgeType::hasher> edge_state_;
};

class EdgeHandler {
 public:
  explicit EdgeHandler(Tracking &tracking, uint64_t commit_timestamp, bool properties_on_edges)
      : tracking_{tracking}, commit_timestamp_{commit_timestamp}, properties_on_edges_{properties_on_edges} {}

  template <bool OutEdge>
  void UpdateExistingEdge(auto edge, auto &pre_labels, auto &post_labels) {
    // Label deltas could move edges multiple times, from both ends
    // We are only interested in the start and end state
    // That means we only need to move the edge once, irrelevant of how many deltas could influence it
    // Labels can only move, creation or deletion is handled via other deltas

    const auto edge_type = std::get<0>(edge);
    const auto *other_vertex = std::get<1>(edge);
    const auto edge_ref = std::get<2>(edge);

    Gid edge_gid = GetEdgeGid(edge_ref, properties_on_edges_);

    const auto [_, emplaced] = handled_edges.emplace(edge_gid);

    if (emplaced) {
      const auto &post_other_labels = other_vertex->labels;
      const auto pre_other_labels = GetPreLabels(*other_vertex, commit_timestamp_);

      if constexpr (OutEdge) {
        --tracking_[{edge_type, pre_labels, pre_other_labels}].n;
        ++tracking_[{edge_type, post_labels, post_other_labels}].n;
      } else {
        --tracking_[{edge_type, pre_other_labels, pre_labels}].n;
        ++tracking_[{edge_type, post_other_labels, post_labels}].n;
      }
    }
  }

  void RemoveExistingEdge(const Delta *delta, const auto &pre_labels) {
    DMG_ASSERT(delta->action == Delta::Action::ADD_OUT_EDGE, "Trying to remove edge using the wrong delta");
    Gid edge_gid = GetEdgeGid(delta->vertex_edge.edge, properties_on_edges_);

    const auto [_, emplaced] = handled_edges.emplace(edge_gid);
    DMG_ASSERT(emplaced, "Deleting an already handled edge");
    if (emplaced) {
      const auto pre_other_labels = GetPreLabels(*delta->vertex_edge.vertex, commit_timestamp_);
      --tracking_[{delta->vertex_edge.edge_type, pre_labels, pre_other_labels}].n;
    }
  }

  void AddNewEdge(const Delta *delta, const auto &post_labels) {
    ++tracking_[{delta->vertex_edge.edge_type, post_labels, delta->vertex_edge.vertex->labels}].n;
  }

  void RemoveNewEdge(const Delta *delta, const auto &post_labels) {
    --tracking_[{delta->vertex_edge.edge_type, post_labels, delta->vertex_edge.vertex->labels}].n;
  }

 private:
  utils::small_vector<LabelId> GetPreLabels(const Vertex &vertex, uint64_t commit_timestamp) const;

  Gid GetEdgeGid(const EdgeRef &edge_ref, bool properties_on_edges);

  Tracking &tracking_;
  std::unordered_set<Gid> handled_edges;
  uint64_t commit_timestamp_{-1UL};
  bool properties_on_edges_{false};
};

class VertexHandler {
 public:
  explicit VertexHandler(Vertex &vertex, Tracking &tracking, uint64_t commit_timestamp)
      : vertex_{vertex}, commit_timestamp_{commit_timestamp}, guard_{vertex_.lock}, tracking_{tracking} {}

  void PreProcess();

  Delta *FirstDelta() const { return vertex_.delta; }

  Delta *NextDelta(const Delta *delta) const {
    auto *older = delta->next.load(std::memory_order_acquire);
    if (older == nullptr || older->timestamp->load(std::memory_order_acquire) != commit_timestamp_) return {};
    return older;
  }

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
    if (vertex_.properties.HasProperty(key)) {
      add_property_.try_emplace(key, vertex_.properties.GetProperty(key));
    }
  }

  bool ExistingEdge(auto &vertex_edge) {
    return std::find(pre_out_edges_.begin(), pre_out_edges_.end(),
                     std::tuple<EdgeTypeId, Vertex *, EdgeRef>{vertex_edge.edge_type, vertex_edge.vertex,
                                                               vertex_edge.edge}) != pre_out_edges_.end();
  }

  const auto &PreLabels() const { return pre_labels_; }

  const auto &PostLabels() const { return vertex_.labels; }

  void PostProcess(EdgeHandler &edge_handler);

 private:
  Vertex &vertex_;
  uint64_t commit_timestamp_{-1UL};
  std::shared_lock<decltype(vertex_.lock)> guard_;
  decltype(vertex_.labels) pre_labels_;
  decltype(vertex_.properties.Properties()) pre_properties_;
  decltype(vertex_.in_edges) pre_in_edges_;
  decltype(vertex_.out_edges) pre_out_edges_;

  bool label_update_{false};
  bool vertex_added_{false};
  std::unordered_map<PropertyId, PropertyValue> remove_property_;
  std::unordered_map<PropertyId, PropertyValue> add_property_;

  Tracking &tracking_;
};

struct SchemaInfo {
  void ProcessTransaction(Transaction &transaction, bool properties_on_edges) {
    const auto commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);
    EdgeHandler edge_handler{tracking_, commit_timestamp, properties_on_edges};
    for (const auto &delta : transaction.deltas) {
      // Find VERTEX or EDGE; handle object's delta chain
      auto prev = delta.prev.Get();
      DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type == PreviousPtr::Type::VERTEX) {
        VertexHandler vertex_handler{*prev.vertex, tracking_, commit_timestamp};
        ProcessVertex(vertex_handler, edge_handler);
      } else if (prev.type == PreviousPtr::Type::EDGE) {
        // TODO
      }
    }
  }

  void CleanUp() { tracking_.CleanUp(); }

  void Print(NameIdMapper &name_id_mapper) { tracking_.Print(name_id_mapper); }

  void ProcessVertex(VertexHandler &vertex_handler, EdgeHandler &edge_handler);

  Tracking tracking_;
};

}  // namespace memgraph::storage
