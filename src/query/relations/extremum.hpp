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
/// Which columns have two ends to report.
///
/// `min` and `max` answer for a whole column what `ORDER BY` answers for its
/// rows: the value a sort puts first, and the value it puts last. They read
/// orderability for that, so the only columns they cannot answer for are the
/// ones a sort cannot place either.
#pragma once

#include <optional>

#include "query/typed_value.hpp"

namespace memgraph::query::relations::extremum {

/// Whether a sort has an order for two values of this type.
constexpr bool ASortOrdersTwoOfThese(TypedValue::Type type) {
  switch (type) {
    using enum TypedValue::Type;
    case Map:
    case Vertex:
    case Edge:
    case VirtualEdge:
    case VirtualNode:
    case Path:
    case Graph:
    case VirtualGraph:
    case Function:
      return false;
    case Null:
    case Bool:
    case Int:
    case Double:
    case String:
    case List:
    case Date:
    case LocalTime:
    case LocalDateTime:
    case ZonedDateTime:
    case Duration:
    case Enum:
    case Point2d:
    case Point3d:
      return true;
  }
  return false;
}

/// The type within @p value that a sort has no order for, if it holds one.
///
/// Orderability places any two values of unlike type, and refuses a pair whose
/// one type carries no order of its own. A column of such values has no first
/// and no last to report, and reading the whole value up front is what keeps
/// the answer from depending on how many rows arrived: a refusal reached only
/// once a second row turns up would answer a one-row column and decline a
/// longer one holding the same value.
///
/// A list is ordered by what it holds, so it is read through. That declines a
/// column a sort could place, since a pair of lists parting at an earlier
/// element never reaches the one carrying no order, but whether it parts there
/// is a fact about the pair rather than about the column.
inline std::optional<TypedValue::Type> ATypeNoSortOrders(TypedValue const &value) {
  if (!ASortOrdersTwoOfThese(value.type())) return value.type();
  if (!value.IsList()) return std::nullopt;

  for (auto const &element : value.ValueList()) {
    if (auto const unordered = ATypeNoSortOrders(element)) return unordered;
  }
  return std::nullopt;
}

}  // namespace memgraph::query::relations::extremum
