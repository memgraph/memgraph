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

#include <limits>

// typed_value.hpp only forward-declares these two, and the hash reads the
// identity out of each.
#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"

import memgraph.utils.fnv;

namespace memgraph::query::relations::equivalence {

namespace {
/// Hash as a function object, which is what a collection hasher needs.
struct HashOf {
  size_t operator()(const TypedValue &value) const { return Hash(value); }
};
}  // namespace

size_t Hash(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return 31;
    case TypedValue::Type::Bool:
      return std::hash<bool>{}(value.ValueBool());
    case TypedValue::Type::Int:
      return std::hash<int64_t>{}(value.ValueInt());
    case TypedValue::Type::Double: {
      // Every NaN is equivalent to every other, whatever sign and payload it
      // carries, so all of them have to reach one bucket. A hash of the value
      // would separate them and a set would then hold what the relation calls
      // one value twice.
      if (std::isnan(value.ValueDouble())) return 2'147'483'647;
      // A double holding a whole number is equivalent to that integer, so the
      // two have to reach one bucket. Asking for the fractional part of the
      // value itself is what decides that; asking it of the value already
      // truncated answers zero every time, which would give every double the
      // bucket of the integer below it.
      auto const double_value = value.ValueDouble();
      double whole_value = 0.0;
      // Outside the integers an int64_t can hold there is no integer to share a
      // bucket with, and the conversion itself would not be defined.
      static constexpr auto kBelowSmallest = static_cast<double>(std::numeric_limits<int64_t>::min());
      static constexpr auto kAboveLargest = static_cast<double>(std::numeric_limits<int64_t>::max());
      if (std::modf(double_value, &whole_value) == 0.0 && whole_value >= kBelowSmallest &&
          whole_value < kAboveLargest) {
        return std::hash<int64_t>{}(static_cast<int64_t>(whole_value));
      }
      return std::hash<double>{}(double_value);
    }
    case TypedValue::Type::String:
      return std::hash<std::string_view>{}(value.ValueString());
    case TypedValue::Type::List: {
      return utils::FnvCollection<TypedValue::TVector, TypedValue, HashOf>{}(value.ValueList());
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
    // A coordinate that is a NaN is one value whatever it holds, so every point
    // carrying one has to reach the bucket the others do.
    case TypedValue::Type::Point2d: {
      auto const &point = value.ValuePoint2d();
      if (std::isnan(point.x()) || std::isnan(point.y())) return 1'000'000'007;
      return std::hash<storage::Point2d>{}(point);
    }
    case TypedValue::Type::Point3d: {
      auto const &point = value.ValuePoint3d();
      if (std::isnan(point.x()) || std::isnan(point.y()) || std::isnan(point.z())) return 1'000'000'009;
      return std::hash<storage::Point3d>{}(point);
    }
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
