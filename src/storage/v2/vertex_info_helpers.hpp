// Copyright 2026 Memgraph Ltd.
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
#include <tuple>
#include <vector>

#include "storage/v2/delta.hpp"
#include "storage/v2/delta_action.hpp"
#include "storage/v2/delta_action_methods.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/vertex_info_cache.hpp"
#include "utils/small_vector.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::storage {

inline auto HasLabel_ActionMethod(bool &has_label, LabelId label) {
  using enum DeltaAction;
  // clang-format off
  return utils::Overloaded{
      ActionMethod<REMOVE_LABEL>([&, label](Delta const &delta) {
        if (delta.label.value == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
        }
      }),
      ActionMethod<ADD_LABEL>([&, label](Delta const &delta) {
        if (delta.label.value == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
        }
      })
  };
  // clang-format on
}

template <typename TSmallVector>
inline auto Labels_ActionMethod(TSmallVector &labels) {
  using enum DeltaAction;
  // clang-format off
  return utils::Overloaded{
      ActionMethod<REMOVE_LABEL>([&](Delta const &delta) {
        auto it = std::ranges::find(labels, delta.label.value);
        DMG_ASSERT(it != labels.end(), "Invalid database state!");
        *it = labels.back();
        labels.pop_back();
      }),
      ActionMethod<ADD_LABEL>([&](Delta const &delta) {
        DMG_ASSERT(!std::ranges::contains(labels, delta.label.value), "Invalid database state!");
        labels.emplace_back(delta.label.value);
      })
  };
  // clang-format on
}

template <EdgeDirection dir, typename TSmallVector>
auto Edges_ActionMethod(TSmallVector &edges, std::vector<EdgeTypeId> const &edge_types, Vertex const *destination) {
  auto const predicate = [&, destination](Delta const &delta) {
    if (destination && delta.vertex_edge.vertex.Get() != destination) return false;
    if (!edge_types.empty() && !std::ranges::contains(edge_types, delta.vertex_edge.edge_type)) return false;
    return true;
  };

  // clang-format off
  using enum DeltaAction;
  return utils::Overloaded{
      ActionMethod <(dir == EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
          [&, predicate](Delta const &delta) {
              if (!predicate(delta)) return;
              // Add the edge because we don't see the removal.
              auto link = std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex.Get(), delta.vertex_edge.edge};
              /// NOTE: For in_memory_storage, link should never exist but for on_disk storage it is possible that
              /// after edge deletion, in the same txn, user requests loading from disk. Then edge will already exist
              /// in out_edges struct.
              auto link_exists = std::ranges::contains(edges, link);
              if (!link_exists) {
                edges.push_back(link);
              }
          }
      ),
      ActionMethod <(dir == EdgeDirection::IN) ? REMOVE_IN_EDGE : REMOVE_OUT_EDGE> (
          [&, predicate](Delta const &delta) {
              if (!predicate(delta)) return;
              // Remove the label because we don't see the addition.
              auto it = std::ranges::find(edges,
                            std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex.Get(), delta.vertex_edge.edge});
              DMG_ASSERT(it != edges.end(), "Invalid database state!");
              *it = edges.back();
              edges.pop_back();
          }
      )
  };
  // clang-format on
}

template <EdgeDirection dir, typename TSmallVector>
auto Edges_ActionMethod(TSmallVector &edges, EdgeTypeId edge_type) {
  auto const predicate = [edge_type](Delta const &delta) { return delta.vertex_edge.edge_type == edge_type; };

  // clang-format off
  using enum DeltaAction;
  return utils::Overloaded{
      ActionMethod <(dir == EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
          [&, predicate](Delta const &delta) {
              if (!predicate(delta)) return;
              // Add the edge because we don't see the removal.
              auto link = std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex.Get(), delta.vertex_edge.edge};
              /// NOTE: For in_memory_storage, link should never exist but for on_disk storage it is possible that
              /// after edge deletion, in the same txn, user requests loading from disk. Then edge will already exist
              /// in out_edges struct.
              auto link_exists = std::ranges::contains(edges, link);
              if (!link_exists) {
                edges.push_back(link);
              }
          }
      ),
      ActionMethod <(dir == EdgeDirection::IN) ? REMOVE_IN_EDGE : REMOVE_OUT_EDGE> (
          [&, predicate](Delta const &delta) {
              if (!predicate(delta)) return;
              // Remove the label because we don't see the addition.
              auto it = std::ranges::find(edges,
                            std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex.Get(), delta.vertex_edge.edge});
              DMG_ASSERT(it != edges.end(), "Invalid database state!");
              *it = edges.back();
              edges.pop_back();
          }
      )
  };
  // clang-format on
}

template <EdgeDirection dir, typename TSmallVector>
auto Edges_ActionMethod(TSmallVector &edges) {
  // clang-format off
  using enum DeltaAction;
  return utils::Overloaded{
      ActionMethod <(dir == EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
          [&](Delta const &delta) {
              // Add the edge because we don't see the removal.
              auto link = std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex.Get(), delta.vertex_edge.edge};
              /// NOTE: For in_memory_storage, link should never exist but for on_disk storage it is possible that
              /// after edge deletion, in the same txn, user requests loading from disk. Then edge will already exist
              /// in out_edges struct.
              auto link_exists = std::ranges::contains(edges, link);
              if (!link_exists) {
                edges.push_back(link);
              }
          }
      ),
      ActionMethod <(dir == EdgeDirection::IN) ? REMOVE_IN_EDGE : REMOVE_OUT_EDGE> (
          [&](Delta const &delta) {
              // Remove the label because we don't see the addition.
              auto it = std::ranges::find(edges,
                            std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex.Get(), delta.vertex_edge.edge});
              DMG_ASSERT(it != edges.end(), "Invalid database state!");
              *it = edges.back();
              edges.pop_back();
          }
      )
  };
  // clang-format on
}

template <EdgeDirection dir>
auto Degree_ActionMethod(size_t &degree) {
  using enum DeltaAction;
  // clang-format off
  return utils::Overloaded{
    ActionMethod <(dir == EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
      [&](Delta const &/*unused*/) { ++degree; }
    ),
    ActionMethod <(dir == EdgeDirection::IN) ? REMOVE_IN_EDGE : REMOVE_OUT_EDGE> (
      [&](Delta const &/*unused*/) { --degree; }
    ),
  };
  // clang-format on
}

inline auto HasError(View view, VertexInfoCache const &cache, Vertex const *vertex, bool for_deleted)
    -> std::optional<Error> {
  if (auto resExists = cache.GetExists(view, vertex); resExists && !resExists.value()) return Error::NONEXISTENT_OBJECT;
  if (!for_deleted) {
    if (auto resDeleted = cache.GetDeleted(view, vertex); resDeleted && resDeleted.value())
      return Error::DELETED_OBJECT;
  }
  return std::nullopt;
}

}  // namespace memgraph::storage
