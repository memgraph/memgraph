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

#include "query/relations/equivalence.hpp"

#include <algorithm>
#include <cmath>

#include "utils/temporal.hpp"

import memgraph.utils.fnv;

namespace memgraph::query::relations::equivalence {

bool EquivalentOfLists(TypedValue::TVector const &a, TypedValue::TVector const &b) {
  return std::ranges::equal(a, b, [](TypedValue const &x, TypedValue const &y) { return Equivalent(x, y); });
}

bool EquivalentOfMaps(TypedValue::TMap const &a, TypedValue::TMap const &b) {
  if (a.size() != b.size()) return false;
  return std::ranges::all_of(a, [&b](auto const &entry) {
    auto const found = b.find(entry.first);
    return found != b.end() && Equivalent(entry.second, found->second);
  });
}

bool EquivalentOfContainersHoldingANull(const TypedValue &a, const TypedValue &b) {
  DMG_ASSERT(a.type() == b.type(), "Equality answers Null only over a pair holding the same type");
  switch (a.type()) {
    case TypedValue::Type::List:
      return EquivalentOfLists(a.UnsafeValueList(), b.UnsafeValueList());
    case TypedValue::Type::Map:
      return EquivalentOfMaps(a.UnsafeValueMap(), b.UnsafeValueMap());
    default:
      LOG_FATAL("Equality answered Null for a pair holding no Null");
  }
}

size_t Hash(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return 31;
    case TypedValue::Type::Bool:
      return std::hash<bool>{}(value.ValueBool());
    case TypedValue::Type::Int:
      return std::hash<int64_t>{}(value.ValueInt());
    case TypedValue::Type::Double: {
      // Store whole number doubles as int hashes to be consistent with
      // TypedValue equality in which (2.0 == 2) returns true
      const double double_value = std::trunc(value.ValueDouble());
      double whole_value = 0.0;
      if (std::modf(double_value, &whole_value) == 0.0) {
        return std::hash<int64_t>{}(static_cast<int64_t>(whole_value));
      }
      return std::hash<double>{}(double_value);
    }
    case TypedValue::Type::String:
      return std::hash<std::string_view>{}(value.ValueString());
    case TypedValue::Type::List: {
      return utils::FnvCollection<TypedValue::TVector, TypedValue, TypedValue::Hash>{}(value.ValueList());
    }
    case TypedValue::Type::Map: {
      size_t hash = 6'543'457;
      for (const auto &kv : value.ValueMap()) {
        hash ^= std::hash<std::string_view>{}(kv.first);
        hash ^= Hash(kv.second);
      }
      return hash;
    }
    case TypedValue::Type::Vertex:
      return value.ValueVertex().Gid().AsUint();
    case TypedValue::Type::Edge:
      return value.ValueEdge().Gid().AsUint();
    case TypedValue::Type::VirtualEdge:
      return value.ValueVirtualEdge().Gid().AsUint();
    case TypedValue::Type::VirtualNode:
      return value.ValueVirtualNode().Gid().AsUint();
    case TypedValue::Type::Path: {
      const auto &vertices = value.ValuePath().vertices();
      const auto &edges = value.ValuePath().edges();
      return utils::FnvCollection<decltype(vertices), VertexAccessor>{}(vertices) ^
             utils::FnvCollection<decltype(edges), EdgeAccessor>{}(edges);
    }
    case TypedValue::Type::Date:
      return utils::DateHash{}(value.ValueDate());
    case TypedValue::Type::LocalTime:
      return utils::LocalTimeHash{}(value.ValueLocalTime());
    case TypedValue::Type::LocalDateTime:
      return utils::LocalDateTimeHash{}(value.ValueLocalDateTime());
    case TypedValue::Type::ZonedDateTime:
      return utils::ZonedDateTimeHash{}(value.ValueZonedDateTime());
    case TypedValue::Type::Duration:
      return utils::DurationHash{}(value.ValueDuration());
    case TypedValue::Type::Enum:
      return std::hash<storage::Enum>{}(value.ValueEnum());
    case TypedValue::Type::Point2d:
      return std::hash<storage::Point2d>{}(value.ValuePoint2d());
    case TypedValue::Type::Point3d:
      return std::hash<storage::Point3d>{}(value.ValuePoint3d());
    case TypedValue::Type::Function:
      throw TypedValueException("Unsupported hash function for Function");
    case TypedValue::Type::Graph:
      throw TypedValueException("Unsupported hash function for Graph");
    case TypedValue::Type::VirtualGraph:
      throw TypedValueException("Unsupported hash function for VirtualGraph");
  }
  LOG_FATAL("Unhandled TypedValue.type() in hash function");
}

}  // namespace memgraph::query::relations::equivalence
