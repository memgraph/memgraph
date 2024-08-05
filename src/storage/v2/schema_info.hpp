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
#include <functional>
#include <ranges>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>

#include "mgp.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
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

namespace memgraph::storage {

// WIP
struct SchemaInfo {
  void ProcessDelta(const Delta &delta, const Edge &edge, NameIdMapper &name_id_mapper, uint64_t timestamp) {}

  // Deltas are undo
  // We keep track of the labels and properties of a vertex
  // So we need to remove a vertex from the previous label/property and add to the new one
  // Because of that we need to recreate vertex as is at the moment of delta
  // TODO Avoid doing extra work

  struct UniqueIdentifier {
    utils::small_vector<LabelId> labels;
    std::map<PropertyId, PropertyValue> properties;
  };

  /// This function iterates through the undo buffers from an object (starting
  /// from the supplied delta) and determines what deltas should be applied to get
  /// the version of the object just before delta_fin. The function applies the delta
  /// via the callback function passed as a parameter to the callback. It is up to the
  /// caller to apply the deltas.
  template <typename TCallback>
  inline void ApplyDeltasForRead(const Delta *delta, const Delta *delta_fin, const TCallback &callback) {
    while (delta != delta_fin) {
      // This delta must be applied, call the callback.
      callback(*delta);
      // Move to the next delta.
      delta = delta->next.load(std::memory_order_acquire);
    }
  }

  UniqueIdentifier ApplyDeltasForVertexIdentifier(const Delta *delta, const Delta *delta_fin, const Vertex &vertex) {
    UniqueIdentifier res;
    res.labels = vertex.labels;
    res.properties = vertex.properties.Properties();
    ApplyDeltasForRead(delta, delta_fin, [&res](const Delta &delta) {
      // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Properties_ActionMethod(res.properties),
            Labels_ActionMethod(res.labels)
          });
      // clang-format on
    });
    return res;
  }

  struct Edges {
    utils::small_vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;
    utils::small_vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  };

  Edges ApplyDeltasForVertexEdges(const Delta *delta, const Delta *delta_fin, const Vertex &vertex) {
    Edges res{vertex.out_edges, vertex.in_edges};
    ApplyDeltasForRead(delta, delta_fin, [&res](const Delta &delta) {
      // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Edges_ActionMethod<EdgeDirection::OUT>(res.out_edges, {/* all edge types */}, {/* all destinations */}),
            Edges_ActionMethod<EdgeDirection::IN>(res.in_edges, {/* all edge types */}, {/* all destinations */}),
          });
      // clang-format on
    });
    return res;
  }

  // Have to get a global ordering;
  // For now this works as long as the deltas are on the same page;
  // TODO: Expand to handle multiple pages
  template <typename TCallback>
  inline void ApplyGlobalDeltasForRead(const Delta *delta, const Delta *delta_fin, const TCallback &callback) {
    while (delta && delta_tracking_.Newer(delta, delta_fin)) {
      // This delta must be applied, call the callback.
      callback(*delta);
      // Move to the next delta.
      delta = delta->next.load(std::memory_order_acquire);
    }
  }

  UniqueIdentifier ApplyGlobalDeltasForVertexIdentifier(const Delta *delta, const Delta *delta_fin,
                                                        const Vertex &vertex) {
    UniqueIdentifier res;
    res.labels = vertex.labels;
    res.properties = vertex.properties.Properties();
    if (delta)
      ApplyGlobalDeltasForRead(delta, delta_fin, [&res](const Delta &delta) {
        // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Properties_ActionMethod(res.properties),
            Labels_ActionMethod(res.labels)
          });
        // clang-format on
      });
    return res;
  }

  void ProcessDelta(const Delta &delta, const Vertex &vertex, NameIdMapper &name_id_mapper, uint64_t timestamp) {
    auto guard = std::shared_lock{vertex.lock};
    switch (delta.action) {
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT:
        // We created a empty vertex

        ++tracking_[Tracking::v_key_type{/* empty vertex; no labels or properties */}].n;
        break;
      case Delta::Action::RECREATE_OBJECT: {
        // We deleted the object; so we need to clean up its labels/properties as well
        // Vertex holds on the labels/properties; just the deleted gets set

        // Do we need to apply deltas?
        auto &v = tracking_[vertex.labels];
        --v.n;
        for (const auto &[pid, pval] : vertex.properties.Properties()) {
          auto &prop_info = v.properties[pid];
          --prop_info.n;
          --prop_info.types[pval.type()];
        }
        break;
      }
      case Delta::Action::SET_PROPERTY: {
        // This delta is both for setting and deleting properties
        // When we delete, we store the previous property value; and null when defining
        // Property defined via ID delta.property.key.AsUint()

        // If value NULL this means that we added the label;
        // How to figure out if we deleted the value or just set another one? <- look at the vertex? could lead to
        // multiple --

        const auto prev_info = ApplyDeltasForVertexIdentifier(vertex.delta, delta.next, vertex);
        const auto next_info = ApplyDeltasForVertexIdentifier(vertex.delta, &delta, vertex);
        auto &v = tracking_[prev_info.labels];  // Labels at this point

        // Decrement from current property
        for (const auto &[pid, pval] : prev_info.properties) {
          auto &prop_info = v.properties[pid];
          --prop_info.n;
          --prop_info.types[pval.type()];
        }
        // Increment to next property
        for (const auto &[pid, pval] : next_info.properties) {
          auto &prop_info = v.properties[pid];
          ++prop_info.n;
          ++prop_info.types[pval.type()];
        }
        break;
      }
      case Delta::Action::ADD_LABEL:
        // Remove label identified via ID delta.label.value.AsUint()
        // For now fallthrough; but not optimal
      case Delta::Action::REMOVE_LABEL: {
        // Add label identified via ID delta.label.value.AsUint()

        const auto prev_info = ApplyDeltasForVertexIdentifier(vertex.delta, delta.next, vertex);
        const auto next_info = ApplyDeltasForVertexIdentifier(vertex.delta, &delta, vertex);

        auto &tracking_now = tracking_[prev_info.labels];
        auto &tracking_next = tracking_[next_info.labels];

        // Decrement from current labels
        --tracking_now.n;
        for (const auto &[pid, pval] : prev_info.properties) {
          auto &prop_info = tracking_now.properties[pid];
          --prop_info.n;
          --prop_info.types[pval.type()];
        }
        // Increment to next labels
        ++tracking_next.n;
        for (const auto &[pid, pval] : next_info.properties) {
          auto &prop_info = tracking_next.properties[pid];
          ++prop_info.n;
          ++prop_info.types[pval.type()];
        }

        // TODO: Edge tracking; edges are uniquely identified via vertex labels, so this changes the edge info as well
        // No way to know here what we are actually doing
        // We signal here that something needs to be done and then we post-process in some way.

        // Get edges at this point
        // For each edge find the corresponding
        // This delta doesn't have anything to do with edges, no need to have next/prev
        const auto edges = ApplyDeltasForVertexEdges(vertex.delta, &delta, vertex);
        for (const auto &edge : edges.out_edges) {
          // Edge identifier: edge type, out labels, in labels
          const auto edge_type_id = std::get<0>(edge);
          const auto *in_vertex = std::get<1>(edge);
          // Apply deltas to get in_vertex labels at this point
          // TODO Cache this
          const auto other_vertex_info = ApplyGlobalDeltasForVertexIdentifier(in_vertex->delta, &delta, *in_vertex);
          const auto prev_edge_identifier =
              Tracking::EdgeType{edge_type_id, prev_info.labels, other_vertex_info.labels};
          // TODO: Think about storing everything under the out labels
          const auto next_edge_identifier =
              Tracking::EdgeType{edge_type_id, next_info.labels, other_vertex_info.labels};
          --tracking_[prev_edge_identifier].n;
          ++tracking_[next_edge_identifier].n;
        }
        for (const auto &edge : edges.in_edges) {
          // Edge identifier: edge type, out labels, in labels
          const auto edge_type_id = std::get<0>(edge);
          const auto *out_vertex = std::get<1>(edge);
          // Apply deltas to get in_vertex labels at this point
          // TODO Cache this
          const auto other_vertex_info = ApplyGlobalDeltasForVertexIdentifier(out_vertex->delta, &delta, *out_vertex);
          const auto prev_edge_identifier =
              Tracking::EdgeType{edge_type_id, other_vertex_info.labels, prev_info.labels};
          // TODO: Think about storing everything under the out labels
          const auto next_edge_identifier =
              Tracking::EdgeType{edge_type_id, other_vertex_info.labels, next_info.labels};
          std::cout << "\n\n\n00000\n\n\n";
          Print(name_id_mapper);
          --tracking_[prev_edge_identifier].n;
          std::cout << "\n\n\n111111\n\n\n";
          Print(name_id_mapper);
          ++tracking_[next_edge_identifier].n;
          std::cout << "\n\n\n22222\n\n\n";
          Print(name_id_mapper);
        }

        break;
      }
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE: {
        // Do we need to handle edges here? <- YES; edges only handle properties

        // Figure out the from and to vertex descriptions (labels)
        // vertex is always from
        // delta.vertex_edge.vertex is always to
        // Update edge_stats_

        // Removing edge (ADD_OUT_EDGE): vertex will already be missing it
        //  if the edges are moved around for vertex manipulation
        //  we need to remove from the vertex labels as it was at time of deletion
        //  doesn't work for IN edges, since we don't have the delta <- could have it though

        // Adding edge (REMOVE_OUT_EDGE): similar problem

        // Maybe the solution is 2 step
        // Handle both OUT and IN edges
        // Have dangling edges; ex: out with id + in -1
        // Problem getting the second vertex as it was at time of this delta

        // We are adding/removing an edge
        // Lets focus on final state
        // Add only if the vertex has not changed labels?

        // if (properties_on_edges) {
        //   delta.vertex_edge.edge.ptr->gid.AsUint();
        // } else {
        //   delta.vertex_edge.edge.gid.AsUint();
        // }
        // name_id_mapper.IdToName(delta.vertex_edge.edge_type.AsUint());
        // vertex.gid.AsUint();
        // delta.vertex_edge.vertex->gid.AsUint();

        // Edge ADD
        // All other deltas have beed applied; get the final label/key and
        // remove from list
        // Any label modification will +-1
        // Just do the same
        const auto edge_type_id = delta.vertex_edge.edge_type;
        const auto *in_vertex = delta.vertex_edge.vertex;

        const auto out_vertex_info = ApplyDeltasForVertexIdentifier(vertex.delta, &delta, vertex);
        const auto in_vertex_info = ApplyGlobalDeltasForVertexIdentifier(in_vertex->delta, &delta, *in_vertex);

        const auto edge_identifier = Tracking::EdgeType{edge_type_id, out_vertex_info.labels, in_vertex_info.labels};

        tracking_[edge_identifier].n += 1 - (2 * (delta.action == Delta::Action::ADD_OUT_EDGE));

        // Edge SET PROPERTY
        // We do this at the end, so everything should already be in place
        // Check if we are deleted
        // For the final vertex in/out labels update property
        break;
      }
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
        // These actions are already encoded in the *_OUT_EDGE actions. This
        // function should never be called for this type of deltas.
        LOG_FATAL("Invalid delta action!");
    }
  }

  void CleanUp() {
    // tracking_.CleanUp();
  }

  void Print(NameIdMapper &name_id_mapper) { tracking_.Print(name_id_mapper); }

  struct Info {
    int n;
    struct PropertyInfo {
      int n;
      std::unordered_map<PropertyValue::Type, int> types;
    };
    std::unordered_map<PropertyId, PropertyInfo> properties;
  };

  class DeltaTracking {
   public:
    // TODO Memory resource?
    void ProcessDelta(const Delta *delta) {
      auto *page = DeltaToPage(delta);
      if (last_page != page) {
        const auto [_, emplaced] = page_order.try_emplace(page, page_order.size());
        if (emplaced) last_page = page;
      }
    }

    void *DeltaToPage(const Delta *delta) const {
      // TODO A lot of static asserts to make sure this logic is always sound
      return (void *)(uint64_t(delta) & -4096UL);
    }

    bool Newer(const Delta *lhs, const Delta *rhs) const {
      auto *lhs_page = DeltaToPage(lhs);
      auto *rhs_page = DeltaToPage(rhs);
      if (lhs_page == rhs_page) {
        // Same page
        return lhs > rhs;
      }
      uint32_t lhs_page_index;
      try {
        lhs_page_index = page_order.at(lhs_page);
      } catch (std::out_of_range & /* unused */) {
        // LHS not tracked (means that delta is newer)
        return true;
      }
      uint32_t rhs_page_index;
      try {
        rhs_page_index = page_order.at(rhs_page);
      } catch (std::out_of_range & /* unused */) {
        // RHS not tracked (means that delta is newer)
        return false;
      }
      // Get page ordering
      return lhs_page_index > rhs_page_index;
    }

    std::unordered_map<void *, uint32_t> page_order;
    void *last_page;
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
    Info &operator[](const v_key_type &key) {
      if (!std::is_sorted(key.begin(), key.end())) {
        auto sorted_key = key;
        std::sort(sorted_key.begin(), sorted_key.end());
        return vertex_state_[sorted_key];
      }
      return vertex_state_[key];
    }

    Info &operator[](const EdgeType &key) { return edge_state_[key]; }

    void CleanUp() {
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

    void Print(NameIdMapper &name_id_mapper) {
      std::cout << "\n\nSCHEMA INFO\n\n";
      std::cout << "******* VERTEX INFO *******\n";
      for (const auto &[labels, info] : vertex_state_) {
        std::cout << "[";
        for (const auto l : labels) {
          std::cout << name_id_mapper.IdToName(l.AsUint()) << ", ";
        }
        std::cout << "]:\n";
        std::cout << "\t" << info.n << "\n";
        for (const auto &[p, info] : info.properties) {
          std::cout << "\t" << name_id_mapper.IdToName(p.AsUint()) << ": " << info.n << "\n";
          std::cout << "\ttypes:\n";
          for (const auto &type : info.types) {
            std::cout << "\t\t" << type.first << ": " << type.second << std::endl;
          }
        }
        std::cout << "\n";
      }
      std::cout << "\n******** EDGE INFO ********\n";
      for (const auto &[edge_type, info] : edge_state_) {
        std::cout << "[";

        std::cout << edge_type.type.AsUint() << ", ";

        std::cout << "[";
        for (const auto l : edge_type.from_v_type) {
          std::cout << name_id_mapper.IdToName(l.AsUint()) << ", ";
        }
        std::cout << "], ";

        std::cout << "[";
        for (const auto l : edge_type.to_v_type) {
          std::cout << name_id_mapper.IdToName(l.AsUint()) << ", ";
        }
        std::cout << "]";

        std::cout << "]:\n";

        std::cout << "\t" << info.n << "\n";
        for (const auto &[p, info] : info.properties) {
          std::cout << "\t" << name_id_mapper.IdToName(p.AsUint()) << ": " << info.n << "\n";
          std::cout << "\ttypes:\n";
          for (const auto &type : info.types) {
            std::cout << "\t\t" << type.first << ": " << type.second << std::endl;
          }
        }
        std::cout << "\n";
      }

      std::cout << "\n\n";
    }

    std::unordered_map<v_key_type, Info> vertex_state_;
    std::unordered_map<EdgeType, Info, EdgeType::hasher> edge_state_;

    // TODO Split for v and e (or just don't expose it)
    decltype(vertex_state_)::const_iterator begin() const { return vertex_state_.begin(); }
    decltype(vertex_state_)::const_iterator end() const { return vertex_state_.end(); }
  };

  Tracking tracking_;
  DeltaTracking delta_tracking_;
};  // namespace memgraph::storage

}  // namespace memgraph::storage
