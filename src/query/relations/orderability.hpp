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

/// @file
/// Orderability: one of the four relations openCypher defines over values, the
/// one a sort reads.
///
/// It answers for pairs comparability refuses, so that `ORDER BY` has somewhere
/// to put every row: a null sorts after everything, and two values of unlike
/// type are placed rather than reported as unknown.
#pragma once

#include <algorithm>
#include <compare>
#include <vector>

#include "query/exceptions.hpp"
#include "query/fmt.hpp"
#include "query/typed_value.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::relations::orderability {

/// Orders two lists element by element, the shorter one first where they agree.
///
/// Out of line so that Compare does not call itself. A compiler will not inline
/// a function that recurses, and every sort reaches Compare through the caller.
std::partial_ordering CompareOfLists(TypedValue const &a, TypedValue const &b);

/// Where `a` falls relative to `b`.
///
/// @throw QueryRuntimeException for a pair this relation does not place.
inline std::partial_ordering Compare(TypedValue const &a, TypedValue const &b) {
  // First assume typical same type comparisons
  if (a.type() == b.type()) {
    switch (a.type()) {
      case TypedValue::Type::Bool:
        return a.UnsafeValueBool() <=> b.UnsafeValueBool();
      case TypedValue::Type::Int:
        return a.UnsafeValueInt() <=> b.UnsafeValueInt();
      case TypedValue::Type::Double:
        return a.UnsafeValueDouble() <=> b.UnsafeValueDouble();
      case TypedValue::Type::String:
        return a.UnsafeValueString() <=> b.UnsafeValueString();
      case TypedValue::Type::Date:
        return a.UnsafeValueDate() <=> b.UnsafeValueDate();
      case TypedValue::Type::LocalTime:
        return a.UnsafeValueLocalTime() <=> b.UnsafeValueLocalTime();
      case TypedValue::Type::LocalDateTime:
        return a.UnsafeValueLocalDateTime() <=> b.UnsafeValueLocalDateTime();
      case TypedValue::Type::ZonedDateTime:
        return a.UnsafeValueZonedDateTime() <=> b.UnsafeValueZonedDateTime();
      case TypedValue::Type::Duration:
        return a.UnsafeValueDuration() <=> b.UnsafeValueDuration();
      case TypedValue::Type::Null:
        return std::partial_ordering::equivalent;
      case TypedValue::Type::Enum:
        return a.UnsafeValueEnum() <=> b.UnsafeValueEnum();
      case TypedValue::Type::Point2d:
        return a.UnsafeValuePoint2d() <=> b.UnsafeValuePoint2d();
      case TypedValue::Type::Point3d:
        return a.UnsafeValuePoint3d() <=> b.UnsafeValuePoint3d();
      case TypedValue::Type::List:
        return CompareOfLists(a, b);
      case TypedValue::Type::VectorRef: {
        // Lazy embedding: reconstruct both operands into reused thread-local float buffers (never the
        // query's monotonic arena) and compare the floats, so a full ORDER BY holds only references.
        thread_local std::vector<float> la;
        thread_local std::vector<float> lb;
        a.MaterializeVectorRefInto(la);
        b.MaterializeVectorRefInto(lb);
        return std::lexicographical_compare_three_way(la.begin(), la.end(), lb.begin(), lb.end());
      }
      case TypedValue::Type::Map:
      case TypedValue::Type::Vertex:
      case TypedValue::Type::Edge:
      case TypedValue::Type::VirtualEdge:
      case TypedValue::Type::VirtualNode:
      case TypedValue::Type::Path:
      case TypedValue::Type::Graph:
      case TypedValue::Type::VirtualGraph:
      case TypedValue::Type::Function:
        throw QueryRuntimeException("Comparison is not defined for values of type {}.", a.type());
    }
  } else {
    // from this point legal only between values of
    // int+float combinations or against null

    // in ordering null comes after everything else
    // at the same time Null is not less that null
    // first deal with Null < Whatever case
    if (a.IsNull()) return std::partial_ordering::greater;
    // now deal with NotNull < Null case
    if (b.IsNull()) return std::partial_ordering::less;

    if (!(a.IsNumeric() && b.IsNumeric())) [[unlikely]]
      throw QueryRuntimeException("Can't compare value of type {} to value of type {}.", a.type(), b.type());

    switch (a.type()) {
      case TypedValue::Type::Int:
        return a.UnsafeValueInt() <=> b.ValueDouble();
      case TypedValue::Type::Double:
        return a.UnsafeValueDouble() <=> b.ValueInt();
      case TypedValue::Type::Bool:
      case TypedValue::Type::Null:
      case TypedValue::Type::String:
      case TypedValue::Type::List:
      case TypedValue::Type::Map:
      case TypedValue::Type::Vertex:
      case TypedValue::Type::Edge:
      case TypedValue::Type::VirtualEdge:
      case TypedValue::Type::VirtualNode:
      case TypedValue::Type::Path:
      case TypedValue::Type::Date:
      case TypedValue::Type::LocalTime:
      case TypedValue::Type::LocalDateTime:
      case TypedValue::Type::ZonedDateTime:
      case TypedValue::Type::Duration:
      case TypedValue::Type::Enum:
      case TypedValue::Type::Point2d:
      case TypedValue::Type::Point3d:
      case TypedValue::Type::Graph:
      case TypedValue::Type::VirtualGraph:
      case TypedValue::Type::Function:
      case TypedValue::Type::VectorRef:  // unreachable: not numeric, rejected by the IsNumeric guard above
        LOG_FATAL("Invalid type");
    }
  }
}

}  // namespace memgraph::query::relations::orderability
