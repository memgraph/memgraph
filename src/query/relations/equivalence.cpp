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
#include <limits>

#include "utils/temporal.hpp"
#include "value_order/numbers.hpp"

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

namespace {

/// Whether two coordinates are the same coordinate, counting two NaNs as one.
bool SameCoordinate(double a, double b) { return (std::isnan(a) && std::isnan(b)) || a == b; }

/// A coordinate with every NaN replaced by one value, so that a pair of points
/// this relation holds alike hashes alike. Any double would do; this one is the
/// smallest, so a point carrying it hashes as some real point does, which costs
/// a collision and no correctness.
double WithoutANaN(double coordinate) {
  return std::isnan(coordinate) ? std::numeric_limits<double>::lowest() : coordinate;
}

}  // namespace

bool EquivalentOfPoints(const TypedValue &a, const TypedValue &b) {
  if (a.type() != b.type()) return false;

  if (a.type() == TypedValue::Type::Point2d) {
    auto const &left = a.UnsafeValuePoint2d();
    auto const &right = b.UnsafeValuePoint2d();
    return left.crs() == right.crs() && SameCoordinate(left.x(), right.x()) && SameCoordinate(left.y(), right.y());
  }

  auto const &left = a.UnsafeValuePoint3d();
  auto const &right = b.UnsafeValuePoint3d();
  return left.crs() == right.crs() && SameCoordinate(left.x(), right.x()) && SameCoordinate(left.y(), right.y()) &&
         SameCoordinate(left.z(), right.z());
}

bool EquivalentOfContainers(const TypedValue &a, const TypedValue &b) {
  // A container is equivalent only to a container of its own kind, so a pair of
  // unlike types is settled without walking either.
  if (a.type() != b.type()) return false;

  switch (a.type()) {
    case TypedValue::Type::List:
      return EquivalentOfLists(a.UnsafeValueList(), b.UnsafeValueList());
    case TypedValue::Type::Map:
      return EquivalentOfMaps(a.UnsafeValueMap(), b.UnsafeValueMap());
    default:
      LOG_FATAL("Asked of a pair that is not a pair of containers");
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
      // Every NaN is equivalent to every other, and more than one bit pattern
      // spells one, so a hash over the bits would send two of them to different
      // buckets and the lookup would never reach the comparison.
      auto const held = value.UnsafeValueDouble();
      if (std::isnan(held)) return 1'214'729'715;

      // A whole double hashes as the integer it equals, since equality holds
      // the two equal and a hash container has to find one key for both. What
      // a double carries past the point reaches the hash as itself: sending
      // every double between two integers to the lower one would file a whole
      // run of distinct keys in one bucket.
      double whole = 0.0;
      if (std::modf(held, &whole) == 0.0 && value_order::AnIntegerCanHold(held)) {
        return std::hash<int64_t>{}(static_cast<int64_t>(whole));
      }
      return std::hash<double>{}(held);
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
    case TypedValue::Type::Point2d: {
      // A NaN coordinate is replaced rather than hashed, for the reason a NaN
      // itself is: more than one bit pattern spells one, and this relation holds
      // them alike.
      auto const &point = value.ValuePoint2d();
      return std::hash<storage::Point2d>{}(
          storage::Point2d{point.crs(), WithoutANaN(point.x()), WithoutANaN(point.y())});
    }
    case TypedValue::Type::Point3d: {
      auto const &point = value.ValuePoint3d();
      return std::hash<storage::Point3d>{}(
          storage::Point3d{point.crs(), WithoutANaN(point.x()), WithoutANaN(point.y()), WithoutANaN(point.z())});
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
