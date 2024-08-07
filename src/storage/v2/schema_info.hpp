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
#include <unordered_set>

#include "mgp.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
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

  template <typename TCallback>
  inline void ApplyDeltasForRead2(const Delta *delta, uint64_t current_commit_timestamp, const TCallback &callback) {
    while (true) {
      // This delta must be applied, call the callback.
      callback(*delta);
      // Move to the next delta in this transaction.
      auto *older = delta->next.load(std::memory_order_acquire);
      if (older == nullptr || older->timestamp->load(std::memory_order_acquire) != current_commit_timestamp) break;
      delta = older;
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
          --tracking_[prev_edge_identifier].n;
          ++tracking_[next_edge_identifier].n;
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

        // Below is not true
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

  void ProcessTransaction(Transaction &transaction, bool properties_on_edges) {
    std::unordered_set<Vertex *> apply_v;

    std::unordered_set<Tracking::EdgeType, Tracking::EdgeType::hasher> remove_edge;
    std::unordered_set<Tracking::EdgeType, Tracking::EdgeType::hasher> add_edge;

    std::unordered_set<Gid> handled_edges;

    for (const auto &delta : transaction.deltas) {
      auto prev = delta.prev.Get();
      DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) continue;

      const auto &vertex = *prev.vertex;
      // We are at the head
      // Calculate the difference
      // pre labels <- easy
      // post labels <- easy
      // pre properties <- easy
      // post properties <- easy

      // pre edges <- easy
      // pre edges other vertex <- don't need global order
      // post edges <- easy
      // post edges other vertex <- don't need global order

      // -+ for each label property change <- easy
      // -+ for each pre edge label change

      // Changed out edges
      // Changed in edges

      auto guard = std::shared_lock{vertex.lock};

      auto pre_labels = vertex.labels;
      const auto post_labels = vertex.labels;
      auto pre_properties = vertex.properties.Properties();
      const auto post_properties = vertex.properties.Properties();

      auto pre_in_edges = vertex.in_edges;
      const auto post_in_edges = vertex.in_edges;
      auto pre_out_edges = vertex.out_edges;
      const auto post_out_edges = vertex.out_edges;

      ApplyDeltasForRead2(&delta, transaction.commit_timestamp->load(std::memory_order_acquire),
                          [&pre_labels, &pre_properties, &pre_in_edges, &pre_out_edges](const Delta &delta) {
                            // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Properties_ActionMethod(pre_properties),
            Labels_ActionMethod(pre_labels),
            Edges_ActionMethod<EdgeDirection::OUT>(pre_out_edges, {/* all edge types */}, {/* all destinations */}),
            Edges_ActionMethod<EdgeDirection::IN>(pre_in_edges, {/* all edge types */}, {/* all destinations */}),
          });
                            // clang-format on
                          });

      // DELETE_DESERIALIZED_OBJECT: <
      // DELETE_OBJECT:   <- (adds) add vertex as defined at commit
      // RECREATE_OBJECT: <- (removes) remove vertex as defined at start
      // SET_PROPERTY:    <- (both adds and removes) remove property-label at start add property-label at commit
      // ADD_LABEL:       <
      // REMOVE_LABEL:    <- update v labels and (if any edges at start) also edges
      // ADD_OUT_EDGE:    <- (removes) remove edge as defined at start
      // REMOVE_OUT_EDGE: <- (adds)  add edges as defined at commit

      bool label_update = false;
      bool label_edge_update = false;

      std::unordered_map<PropertyId, PropertyValue> remove_property;
      std::unordered_map<PropertyId, PropertyValue> add_property;
      bool vertex_added = false;
      struct EdgeInfo {
        Vertex *from;
        Vertex *to;
        EdgeTypeId type;

        struct hasher {
          size_t operator()(const EdgeInfo &et) const {
            size_t combined_hash = 0;
            auto element_hash = [&combined_hash](const auto &elem) {
              size_t element_hash = std::hash<std::decay_t<decltype(elem)>>{}(elem);
              combined_hash ^= element_hash + 0x9e3779b9 + (combined_hash << 6) + (combined_hash >> 2);
            };

            element_hash(et.type);
            element_hash(et.from);
            element_hash(et.to);
            return combined_hash;
          }
        };

        bool operator==(const EdgeInfo &other) const {
          return type == other.type && from == other.from && to == other.to;
        }
      };

      // std::unordered_set<EdgeInfo, EdgeInfo::hasher> remove_edge;
      // std::unordered_set<EdgeInfo, EdgeInfo::hasher> add_edge;

      auto *current_delta = &delta;
      auto current_commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);

      bool remove_labels = false;
      bool add_labels = false;

      while (true) {
        switch (current_delta->action) {
          case Delta::Action::DELETE_DESERIALIZED_OBJECT:
          case Delta::Action::DELETE_OBJECT: {
            // This is an empty object without any labels or properties
            label_update = true;
            vertex_added = true;
            break;
          }
          case Delta::Action::RECREATE_OBJECT: {
            // This is an empty object without any labels or properties
            label_update = true;
            break;
          }
          case Delta::Action::SET_PROPERTY: {
            if (pre_properties.contains(current_delta->property.key)) {  // already present value
              remove_property.try_emplace(current_delta->property.key, *current_delta->property.value);
            }
            if (vertex.properties.HasProperty(current_delta->property.key)) {
              add_property.try_emplace(current_delta->property.key,
                                       vertex.properties.GetProperty(current_delta->property.key));
            }
            break;
          }
          case Delta::Action::ADD_LABEL:
          case Delta::Action::REMOVE_LABEL: {
            label_update = true;
            label_edge_update |= !pre_in_edges.empty() || !pre_out_edges.empty();
            break;
          }
          case Delta::Action::ADD_OUT_EDGE: {
            // This delta covers both vertices; we can update count in place
            if (std::find(pre_out_edges.begin(), pre_out_edges.end(),
                          std::tuple<EdgeTypeId, Vertex *, EdgeRef>{
                              current_delta->vertex_edge.edge_type, current_delta->vertex_edge.vertex,
                              current_delta->vertex_edge.edge}) != pre_out_edges.end()) {
              // Removed an already existing edge
              // remove_edge.emplace(current_delta->vertex_edge.edge_type, vertex, current_delta->vertex_edge.vertex);
              const auto *other_vertex = current_delta->vertex_edge.vertex;
              auto pre_other_labels = other_vertex->labels;
              if (other_vertex->delta) {
                ApplyDeltasForRead2(&delta, transaction.commit_timestamp->load(std::memory_order_acquire),
                                    [&pre_other_labels](const Delta &delta) {
                                      // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Labels_ActionMethod(pre_other_labels)
          });
                                      // clang-format on
                                    });
              }
              --tracking_[Tracking::EdgeType{current_delta->vertex_edge.edge_type, pre_labels, pre_other_labels}].n;
            } else {
              // Remove from temporary edges
              --tracking_[{current_delta->vertex_edge.edge_type, post_labels,
                           current_delta->vertex_edge.vertex->labels}]
                    .n;
            }
            break;
          }
          case Delta::Action::REMOVE_OUT_EDGE: {
            // This delta covers both vertices; we can update count in place
            // New edge; add to the final labels
            ++tracking_[{current_delta->vertex_edge.edge_type, post_labels, current_delta->vertex_edge.vertex->labels}]
                  .n;
            break;
          }
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
            // These actions are already encoded in the *_OUT_EDGE actions. This
            // function should never be called for this type of deltas.
            // LOG_FATAL("Invalid delta action!");
        }

        // Advance along the chain
        auto *older = current_delta->next.load(std::memory_order_acquire);
        if (older == nullptr || older->timestamp->load(std::memory_order_acquire) != current_commit_timestamp) break;
        current_delta = older;
      }

      if (label_update) {
        // Move all info as is at the start
        // Other deltas will take care of updating the end result
        if (!vertex_added) {
          --tracking_[pre_labels].n;
          remove_property.insert(pre_properties.begin(), pre_properties.end());
        }
        if (!vertex.deleted) {
          ++tracking_[post_labels].n;
          add_property.insert(post_properties.begin(), post_properties.end());
        }
      }

      if (label_edge_update) {
        // There could be multiple label changes or changes from both ends; so make a list of unique edges to update
        // Move edges to the new labels
        for (const auto &out_edge : pre_out_edges) {
          // Label deltas could move edges multiple times, from both ends
          // We are only interested in the start and end state
          // That means we only need to move the edge once, irrelevant of how many deltas could influence it
          // Labels can only move, creation or deletion is handled via other deltas
          Gid edge_gid;
          if (properties_on_edges) {
            edge_gid = delta.vertex_edge.edge.ptr->gid;
          } else {
            edge_gid = delta.vertex_edge.edge.gid;
          }

          const auto [_, emplaced] = handled_edges.emplace(edge_gid);
          if (emplaced) continue;

          const auto *other_vertex = std::get<1>(out_edge);
          auto pre_other_labels = other_vertex->labels;
          const auto post_other_labels = other_vertex->labels;
          if (other_vertex->delta) {
            ApplyDeltasForRead2(&delta, transaction.commit_timestamp->load(std::memory_order_acquire),
                                [&pre_other_labels](const Delta &delta) {
                                  // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Labels_ActionMethod(pre_other_labels)
          });
                                  // clang-format on
                                });
          }

          --tracking_[{std::get<0>(out_edge), pre_labels, pre_other_labels}].n;
          ++tracking_[{std::get<0>(out_edge), post_labels, post_other_labels}].n;
        }
        for (const auto &in_edge : pre_in_edges) {
          // Label deltas could move edges multiple times, from both ends
          // We are only interested in the start and end state
          // That means we only need to move the edge once, irrelevant of how many deltas could influence it
          // Labels can only move, creation or deletion is handled via other deltas
          Gid edge_gid;
          if (properties_on_edges) {
            edge_gid = delta.vertex_edge.edge.ptr->gid;
          } else {
            edge_gid = delta.vertex_edge.edge.gid;
          }

          const auto [_, emplaced] = handled_edges.emplace(edge_gid);
          if (emplaced) continue;

          const auto *other_vertex = std::get<1>(in_edge);
          auto pre_other_labels = other_vertex->labels;
          const auto post_other_labels = other_vertex->labels;
          if (other_vertex->delta) {
            ApplyDeltasForRead2(&delta, transaction.commit_timestamp->load(std::memory_order_acquire),
                                [&pre_other_labels](const Delta &delta) {
                                  // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Labels_ActionMethod(pre_other_labels)
          });
                                  // clang-format on
                                });
          }

          --tracking_[{std::get<0>(in_edge), pre_other_labels, pre_labels}].n;
          ++tracking_[{std::get<0>(in_edge), post_other_labels, post_labels}].n;
        }
      }

      // We are only removing properties that have existed before transaction
      for (const auto &[id, val] : remove_property) {
        --tracking_[pre_labels].properties[id].n;
        --tracking_[pre_labels].properties[id].types[val.type()];
      }
      // Add the new (or updated) properties
      for (const auto &[id, val] : add_property) {
        ++tracking_[post_labels].properties[id].n;
        ++tracking_[post_labels].properties[id].types[val.type()];
      }
    }
  }

  void CleanUp() { tracking_.CleanUp(); }

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
        if (emplaced) {
          last_page = page;
        }
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
