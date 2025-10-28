// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/delta_action.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/vertex_info_cache.hpp"
#include "utils/small_vector.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::storage {

template <DeltaAction>
struct DeltaAction_tag {};

template <DeltaAction A, typename Method>
struct ActionMethodImpl : Method {
  // Uses tag dispatch to ensure method is only called for the correct action
  void operator()(DeltaAction_tag<A> /*unused*/, Delta const &delta) { Method::operator()(delta); }
};

template <DeltaAction A, typename Method>
auto ActionMethod(Method &&func) {
  return ActionMethodImpl<A, Method>{std::forward<Method>(func)};
}

/// Converts runtime DeltaAction into compile time tag, this allows us to dispatch to the correct overload
template <typename Func>
void DeltaDispatch(Delta const &delta, Func &&func) {
  // clang-format off
#define dispatch(E) case E: return func(DeltaAction_tag<E>{}, delta); // NOLINT
  // clang-format on
  switch (delta.action) {
    using enum DeltaAction;
    dispatch(DELETE_DESERIALIZED_OBJECT);
    dispatch(DELETE_OBJECT);
    dispatch(RECREATE_OBJECT);
    dispatch(SET_PROPERTY);
    dispatch(SET_VECTOR_PROPERTY);
    dispatch(ADD_LABEL);
    dispatch(REMOVE_LABEL);
    dispatch(ADD_IN_EDGE);
    dispatch(ADD_OUT_EDGE);
    dispatch(REMOVE_IN_EDGE);
    dispatch(REMOVE_OUT_EDGE);
  }
#undef dispatch
}

inline auto Exists_ActionMethod(bool &exists) {
  using enum DeltaAction;
  // clang-format off
  return utils::Overloaded{
      ActionMethod<DELETE_DESERIALIZED_OBJECT>([&](Delta const & /*unused*/) { exists = false; }),
      ActionMethod<DELETE_OBJECT>([&](Delta const & /*unused*/) { exists = false; })
  };
  // clang-format on
}

inline auto Deleted_ActionMethod(bool &deleted) {
  using enum DeltaAction;
  return ActionMethod<RECREATE_OBJECT>([&](Delta const & /*unused*/) { deleted = false; });
}

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

inline auto Labels_ActionMethod(utils::small_vector<LabelId> &labels) {
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
        DMG_ASSERT(std::find(labels.begin(), labels.end(), delta.label.value) == labels.end(), "Invalid database state!");
        labels.emplace_back(delta.label.value);
      })
  };
  // clang-format on
}

inline auto PropertyValue_ActionMethod(PropertyValue &value, PropertyId property) {
  using enum DeltaAction;
  return ActionMethod<SET_PROPERTY>([&, property](Delta const &delta) {
    if (delta.property.key == property) {
      value = *delta.property.value;
    }
  });
}

inline auto PropertyValueMatch_ActionMethod(bool &match, PropertyId property, PropertyValue const &value) {
  using enum DeltaAction;
  return ActionMethod<SET_PROPERTY>([&, property](Delta const &delta) {
    if (delta.property.key == property) match = (value == *delta.property.value);
  });
}

inline auto Properties_ActionMethod(std::map<PropertyId, PropertyValue> &properties) {
  using enum DeltaAction;
  return ActionMethod<SET_PROPERTY>([&](Delta const &delta) {
    auto it = properties.find(delta.property.key);
    if (it != properties.end()) {
      if (delta.property.value->IsNull()) {
        // remove the property
        properties.erase(it);
      } else {
        // set the value
        it->second = *delta.property.value;
      }
    } else if (!delta.property.value->IsNull()) {
      properties.emplace(delta.property.key, *delta.property.value);
    }
  });
}

template <EdgeDirection dir>
auto Edges_ActionMethod(utils::small_vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> &edges,
                        std::vector<EdgeTypeId> const &edge_types, Vertex const *destination) {
  auto const predicate = [&, destination](Delta const &delta) {
    if (destination && delta.vertex_edge.vertex != destination) return false;
    if (!edge_types.empty() && std::ranges::find(edge_types, delta.vertex_edge.edge_type) == edge_types.end())
      return false;
    return true;
  };

  // clang-format off
  using enum DeltaAction;
  return utils::Overloaded{
      ActionMethod <(dir == EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
          [&, predicate](Delta const &delta) {
              if (!predicate(delta)) return;
              // Add the edge because we don't see the removal.
              auto link = std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
              /// NOTE: For in_memory_storage, link should never exist but for on_disk storage it is possible that
              /// after edge deletion, in the same txn, user requests loading from disk. Then edge will already exist
              /// in out_edges struct.
              auto link_exists = std::ranges::find(edges, link) != edges.end();
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
                            std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge});
              DMG_ASSERT(it != edges.end(), "Invalid database state!");
              *it = edges.back();
              edges.pop_back();
          }
      )
  };
  // clang-format on
}

template <EdgeDirection dir>
auto Edges_ActionMethod(utils::small_vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> &edges, EdgeTypeId edge_type) {
  auto const predicate = [edge_type](Delta const &delta) { return delta.vertex_edge.edge_type == edge_type; };

  // clang-format off
  using enum DeltaAction;
  return utils::Overloaded{
      ActionMethod <(dir == EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
          [&, predicate](Delta const &delta) {
              if (!predicate(delta)) return;
              // Add the edge because we don't see the removal.
              auto link = std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
              /// NOTE: For in_memory_storage, link should never exist but for on_disk storage it is possible that
              /// after edge deletion, in the same txn, user requests loading from disk. Then edge will already exist
              /// in out_edges struct.
              auto link_exists = std::ranges::find(edges, link) != edges.end();
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
                            std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge});
              DMG_ASSERT(it != edges.end(), "Invalid database state!");
              *it = edges.back();
              edges.pop_back();
          }
      )
  };
  // clang-format on
}

template <EdgeDirection dir>
auto Edges_ActionMethod(utils::small_vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> &edges) {
  // clang-format off
  using enum DeltaAction;
  return utils::Overloaded{
      ActionMethod <(dir == EdgeDirection::IN) ? ADD_IN_EDGE : ADD_OUT_EDGE> (
          [&](Delta const &delta) {
              // Add the edge because we don't see the removal.
              auto link = std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
              /// NOTE: For in_memory_storage, link should never exist but for on_disk storage it is possible that
              /// after edge deletion, in the same txn, user requests loading from disk. Then edge will already exist
              /// in out_edges struct.
              auto link_exists = std::ranges::find(edges, link) != edges.end();
              if (!link_exists) {
                edges.push_back(link);
              }
          }
      ),
      ActionMethod <(dir == EdgeDirection::IN) ? REMOVE_IN_EDGE : REMOVE_OUT_EDGE> (
          [&](Delta const &delta) {
              // Remove the label because we don't see the addition.
              auto it = std::ranges::find(edges,
                            std::tuple{delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge});
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
