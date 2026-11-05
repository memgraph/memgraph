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
#include <functional>
#include <map>
#include <span>

#include "storage/v2/delta.hpp"
#include "storage/v2/delta_action.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/variant_helpers.hpp"

/// The half of a delta replay that means the same for a vertex as for an edge: the dispatch from a
/// runtime action to a compile-time tag, and the property and existence methods built on it.
/// Whatever holds labels or edges is particular to a vertex and lives with it.
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
        properties.erase(it);
      } else {
        it->second = *delta.property.value;
      }
    } else if (!delta.property.value->IsNull()) {
      properties.emplace(delta.property.key, *delta.property.value);
    }
  });
}

/// As above, for a fixed set of properties with one slot each. A property the deltas remove leaves
/// Null, which is what one the record never carried reads as.
///
/// Every path must name a single property: a slot is found by its leaf, and a nested path's leaf
/// orders with nothing, so it would match no slot and leave a stale value. Callers check first.
inline auto PropertyValues_ActionMethod(std::span<PropertyPath const> ordered_properties,
                                        std::span<PropertyValue> values) {
  using enum DeltaAction;
  return ActionMethod<SET_PROPERTY>([ordered_properties, values](Delta const &delta) {
    auto const it = std::ranges::lower_bound(
        ordered_properties, delta.property.key, std::less<>{}, [](PropertyPath const &path) { return path.back(); });
    if (it == ordered_properties.end() || it->back() != delta.property.key) return;
    values[static_cast<std::size_t>(std::distance(ordered_properties.begin(), it))] = *delta.property.value;
  });
}

}  // namespace memgraph::storage
