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
#include <unordered_map>

#include "mgp.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
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
    utils::small_vector<PropertyId> properties;
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

  UniqueIdentifier ApplyDeltas(const Delta *delta, const Delta *delta_fin, const Vertex &vertex) {
    UniqueIdentifier res;
    res.labels = vertex.labels;
    std::map<PropertyId, PropertyValue> properties = vertex.properties.Properties();
    ApplyDeltasForRead(delta, delta_fin, [&res, &properties](const Delta &delta) {
      // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Properties_ActionMethod(properties),
            Labels_ActionMethod(res.labels)
          });
      // clang-format on
    });
    auto keys = std::views::keys(properties);
    res.properties = utils::small_vector<PropertyId>(keys.begin(), keys.end());
    return res;
  }

  void ProcessDelta(const Delta &delta, const Vertex &vertex, NameIdMapper &name_id_mapper, uint64_t timestamp) {
    auto guard = std::shared_lock{vertex.lock};
    switch (delta.action) {
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT:
        // We created a empty vertex

        ++tracking_[{/* empty vertex; no labels or properties */}].n;
        break;
      case Delta::Action::RECREATE_OBJECT: {
        // We deleted the object; so we need to clean up its labels/properties as well
        // Vertex holds on the labels/properties; just the deleted gets set

        // Do we need to apply deltas?
        auto &v = tracking_[vertex.labels];
        --v.n;
        for (const auto &[pid, _] : vertex.properties.Properties()) --v.properties[pid];
        break;
      }
      case Delta::Action::SET_PROPERTY: {
        // This delta is both for setting and deleting properties
        // When we delete, we store the previous property value; and null when defining
        // Property defined via ID delta.property.key.AsUint()

        // If value NULL this means that we added the label;
        // How to figure out if we deleted the value or just set another one? <- look at the vertex? could lead to
        // multiple --

        const auto prev_info = ApplyDeltas(vertex.delta, delta.next, vertex);
        const auto next_info = ApplyDeltas(vertex.delta, &delta, vertex);
        auto &v = tracking_[prev_info.labels];  // Labels at this point

        // Decrement from current property
        for (const auto pid : prev_info.properties) --v.properties[pid];
        // Increment to next property
        for (const auto pid : next_info.properties) ++v.properties[pid];
        break;
      }
      case Delta::Action::ADD_LABEL:
        // Remove label identified via ID delta.label.value.AsUint()
        // For now fallthrough; but not optimal
      case Delta::Action::REMOVE_LABEL: {
        // Add label identified via ID delta.label.value.AsUint()

        const auto prev_info = ApplyDeltas(vertex.delta, delta.next, vertex);
        const auto next_info = ApplyDeltas(vertex.delta, &delta, vertex);

        auto &tracking_now = tracking_[prev_info.labels];
        auto &tracking_next = tracking_[next_info.labels];

        // Decrement from current labels
        --tracking_now.n;
        for (const auto pid : prev_info.properties) --tracking_now.properties[pid];
        // Increment to next labels
        ++tracking_next.n;
        for (const auto pid : next_info.properties) ++tracking_next.properties[pid];
        break;
      }
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        // Do we need to handle edges here?

        // if (properties_on_edges) {
        //   delta.vertex_edge.edge.ptr->gid.AsUint();
        // } else {
        //   delta.vertex_edge.edge.gid.AsUint();
        // }
        // name_id_mapper.IdToName(delta.vertex_edge.edge_type.AsUint());
        // vertex.gid.AsUint();
        // delta.vertex_edge.vertex->gid.AsUint();
        break;
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
        // These actions are already encoded in the *_OUT_EDGE actions. This
        // function should never be called for this type of deltas.
        LOG_FATAL("Invalid delta action!");
    }
  }

  void CleanUp() { tracking_.CleanUp(); }

  void Print(NameIdMapper &name_id_mapper) {
    std::cout << "\n\nSCHEMA INFO\n";
    for (const auto &[labels, info] : tracking_) {
      std::cout << "[";
      for (const auto l : labels) {
        std::cout << name_id_mapper.IdToName(l.AsUint()) << ", ";
      }
      std::cout << "]:\n";
      std::cout << "\t" << info.n << "\n";
      for (const auto [p, n] : info.properties) {
        std::cout << "\t" << name_id_mapper.IdToName(p.AsUint()) << ": " << n << "\n";
      }
      std::cout << "\n";
    }
    std::cout << "\n\n";
  }

  struct Info {
    int n;
    std::unordered_map<PropertyId, int> properties;
  };

  struct Tracking {
    // We want the key to be independent of the vector order.
    // Could create a hash and equals function that is independent
    // Difficult to make them performant and low collision
    // Stick to sorting keys for now
    Info &operator[](const utils::small_vector<LabelId> &key) {
      if (!std::is_sorted(key.begin(), key.end())) {
        auto sorted_key = key;
        std::sort(sorted_key.begin(), sorted_key.end());
        return state_[sorted_key];
      }
      return state_[key];
    }

    void CleanUp() {
      // Erase all elements that don't have any vertices associated
      std::erase_if(state_, [](auto &elem) { return elem.second.n <= 0; });
      for (auto &[_, val] : state_) {
        std::erase_if(val.properties, [](auto &elem) { return elem.second <= 0; });
      }
    }

    std::unordered_map<utils::small_vector<LabelId>, Info> state_;

    decltype(state_)::const_iterator begin() const { return state_.begin(); }
    decltype(state_)::const_iterator end() const { return state_.end(); }
  };

  Tracking tracking_;
};

}  // namespace memgraph::storage
