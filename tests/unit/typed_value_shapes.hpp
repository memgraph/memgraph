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

#include <array>
#include <initializer_list>
#include <vector>

#include "query/db_accessor.hpp"
#include "query/typed_value.hpp"

namespace memgraph::test::shapes {

/// The query-side counterpart of the stored shapes. The types the two have in
/// common draw the same values, so the doubles a comparison cannot order by
/// magnitude and the strings a store lays out differently are written once.

/// Every type, in the order the enum declares them.
inline constexpr std::array kEveryTypedValueType{
    query::TypedValue::Type::Null,          query::TypedValue::Type::Bool,          query::TypedValue::Type::Int,
    query::TypedValue::Type::Double,        query::TypedValue::Type::String,        query::TypedValue::Type::List,
    query::TypedValue::Type::Map,           query::TypedValue::Type::Vertex,        query::TypedValue::Type::Edge,
    query::TypedValue::Type::Path,          query::TypedValue::Type::Date,          query::TypedValue::Type::LocalTime,
    query::TypedValue::Type::LocalDateTime, query::TypedValue::Type::ZonedDateTime, query::TypedValue::Type::Duration,
    query::TypedValue::Type::Graph,         query::TypedValue::Type::VirtualGraph,  query::TypedValue::Type::Function,
    query::TypedValue::Type::Enum,          query::TypedValue::Type::Point2d,       query::TypedValue::Type::Point3d,
    query::TypedValue::Type::VirtualEdge,   query::TypedValue::Type::VirtualNode,
};

/// The types no shape is made of. A function is bound to a call rather than
/// held as a value a test can write down, and the three virtual types are made
/// by a projection out of values it has already produced.
inline constexpr std::array kUnshapedTypedValueTypes{
    query::TypedValue::Type::Function,
    query::TypedValue::Type::VirtualGraph,
    query::TypedValue::Type::VirtualEdge,
    query::TypedValue::Type::VirtualNode,
};

/// The types whose values hold a piece of the graph, so an accessor has to make
/// them and a test with no database reaches every type but these.
inline constexpr std::array kGraphTypedValueTypes{
    query::TypedValue::Type::Vertex,
    query::TypedValue::Type::Edge,
    query::TypedValue::Type::Path,
    query::TypedValue::Type::Graph,
};

/// Whether a NaN sits anywhere in the value, at the top or below it. A test
/// that checks a copy is the same value needs one a relation can decide against
/// itself, and neither equality nor equivalence decides a NaN.
bool HoldsANaN(query::TypedValue const &value);

/// The shapes of one type. A vertex, an edge, a path and a graph are made
/// through the accessor, so without one those types have no shapes.
auto ShapesOfType(query::TypedValue::Type type, query::DbAccessor *dba) -> std::vector<query::TypedValue>;

/// Every type's shapes, in the order `kEveryTypedValueType` gives.
auto EveryTypedValueShape(query::DbAccessor *dba) -> std::vector<query::TypedValue>;

/// Every type's shapes but those of the named types.
auto EveryTypedValueShapeExcept(query::DbAccessor *dba, std::initializer_list<query::TypedValue::Type> types)
    -> std::vector<query::TypedValue>;

}  // namespace memgraph::test::shapes
