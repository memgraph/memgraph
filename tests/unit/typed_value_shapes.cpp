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

#include "tests/unit/typed_value_shapes.hpp"

#include <algorithm>
#include <chrono>

#include "query/graph.hpp"
#include "query/path.hpp"
#include "tests/unit/value_shapes.hpp"
#include "utils/memory.hpp"
#include "utils/temporal.hpp"

namespace memgraph::test::shapes {

namespace {

using query::TypedValue;
using storage::PropertyValueType;

/// The scalars a stored value and a query value both hold, unwrapped from the
/// stored shapes so that each interesting one is written once.
template <typename Unwrap>
auto FromStored(PropertyValueType type, Unwrap unwrap) -> std::vector<TypedValue> {
  auto result = std::vector<TypedValue>{};
  for (auto const &value : ShapesOfType(type)) result.emplace_back(unwrap(value));
  return result;
}

auto Lists() -> std::vector<TypedValue> {
  auto const boxed = [](std::vector<TypedValue> elements) { return TypedValue(std::move(elements)); };
  return {
      boxed({}),
      boxed({TypedValue(std::int64_t{1})}),
      boxed({TypedValue(std::int64_t{1}), TypedValue(std::int64_t{2})}),
      boxed({TypedValue(std::int64_t{2})}),
      boxed({TypedValue(1.0), TypedValue(kNaN)}),
      boxed({TypedValue("a")}),
      boxed({TypedValue()}),
      boxed({TypedValue(), TypedValue(std::int64_t{1})}),
      boxed({boxed({TypedValue(kNaN)})}),
  };
}

auto Maps() -> std::vector<TypedValue> {
  using Map = std::map<std::string, TypedValue>;
  return {
      TypedValue(Map{}),
      TypedValue(Map{{"a", TypedValue(std::int64_t{1})}}),
      TypedValue(Map{{"a", TypedValue(std::int64_t{2})}}),
      TypedValue(Map{{"b", TypedValue(std::int64_t{1})}}),
      TypedValue(Map{{"a", TypedValue(kNaN)}}),
      TypedValue(Map{{"a", TypedValue()}}),
      TypedValue(Map{{"a", TypedValue(std::int64_t{1})}, {"b", TypedValue("a")}}),
  };
}

auto Vertices(query::DbAccessor *dba) -> std::vector<TypedValue> {
  if (dba == nullptr) return {};
  return {TypedValue(dba->InsertVertex()), TypedValue(dba->InsertVertex())};
}

auto Edges(query::DbAccessor *dba) -> std::vector<TypedValue> {
  if (dba == nullptr) return {};
  auto from = dba->InsertVertex();
  auto to = dba->InsertVertex();
  auto const type = dba->NameToEdgeType("et");
  auto self = dba->InsertEdge(&from, &from, type);
  auto between = dba->InsertEdge(&from, &to, type);
  return {TypedValue(*self), TypedValue(*between)};
}

auto Paths(query::DbAccessor *dba) -> std::vector<TypedValue> {
  if (dba == nullptr) return {};
  auto from = dba->InsertVertex();
  auto to = dba->InsertVertex();
  auto const edge = dba->InsertEdge(&from, &to, dba->NameToEdgeType("et"));

  auto lone = query::Path(from);
  auto walked = query::Path(from);
  walked.Expand(*edge);
  walked.Expand(to);
  return {TypedValue(std::move(lone)), TypedValue(std::move(walked))};
}

auto Graphs(query::DbAccessor *dba) -> std::vector<TypedValue> {
  if (dba == nullptr) return {};
  auto from = dba->InsertVertex();
  auto to = dba->InsertVertex();
  auto const edge = dba->InsertEdge(&from, &to, dba->NameToEdgeType("et"));

  auto empty = query::Graph(utils::NewDeleteResource());
  auto filled = query::Graph(utils::NewDeleteResource());
  filled.InsertVertex(from);
  filled.InsertVertex(to);
  filled.InsertEdge(*edge);
  return {TypedValue(std::move(empty)), TypedValue(std::move(filled))};
}

auto Dates() -> std::vector<TypedValue> {
  return {
      TypedValue(utils::Date(utils::DateParameters{0, 1, 1})),
      TypedValue(utils::Date(utils::DateParameters{1970, 1, 1})),
      TypedValue(utils::Date(utils::DateParameters{2026, 9, 15})),
      TypedValue(utils::Date(utils::DateParameters{9999, 12, 31})),
  };
}

auto LocalTimes() -> std::vector<TypedValue> {
  return {
      TypedValue(utils::LocalTime(utils::LocalTimeParameters{0, 0, 0})),
      TypedValue(utils::LocalTime(utils::LocalTimeParameters{12, 30, 15})),
      TypedValue(utils::LocalTime(utils::LocalTimeParameters{23, 59, 59, 999, 999})),
  };
}

auto LocalDateTimes() -> std::vector<TypedValue> {
  return {
      TypedValue(utils::LocalDateTime(utils::DateParameters{1970, 1, 1}, utils::LocalTimeParameters{0, 0, 0})),
      TypedValue(utils::LocalDateTime(utils::DateParameters{2026, 9, 15}, utils::LocalTimeParameters{12, 30, 15})),
  };
}

auto ZonedDateTimes() -> std::vector<TypedValue> {
  // A zone is kept either as a name or as an offset, and one instant carries
  // both readings.
  auto const instant = std::chrono::sys_time<std::chrono::microseconds>{std::chrono::microseconds{0}};
  return {
      TypedValue(utils::ZonedDateTime(instant, utils::Timezone("Etc/UTC"))),
      TypedValue(utils::ZonedDateTime(instant, utils::Timezone("America/Los_Angeles"))),
      TypedValue(utils::ZonedDateTime(instant, utils::Timezone(std::chrono::minutes{-330}))),
  };
}

auto Durations() -> std::vector<TypedValue> {
  return {
      TypedValue(utils::Duration(0)),
      TypedValue(utils::Duration(1)),
      TypedValue(utils::Duration(-1)),
  };
}

}  // namespace

auto ShapesOfType(TypedValue::Type type, query::DbAccessor *dba) -> std::vector<TypedValue> {
  switch (type) {
    case TypedValue::Type::Null:
      return {TypedValue()};
    case TypedValue::Type::Bool:
      return FromStored(PropertyValueType::Bool, [](auto const &v) { return TypedValue(v.ValueBool()); });
    case TypedValue::Type::Int:
      return FromStored(PropertyValueType::Int, [](auto const &v) { return TypedValue(v.ValueInt()); });
    case TypedValue::Type::Double:
      return FromStored(PropertyValueType::Double, [](auto const &v) { return TypedValue(v.ValueDouble()); });
    case TypedValue::Type::String:
      return FromStored(PropertyValueType::String, [](auto const &v) { return TypedValue(v.ValueString()); });
    case TypedValue::Type::List:
      return Lists();
    case TypedValue::Type::Map:
      return Maps();
    case TypedValue::Type::Vertex:
      return Vertices(dba);
    case TypedValue::Type::Edge:
      return Edges(dba);
    case TypedValue::Type::Path:
      return Paths(dba);
    case TypedValue::Type::Date:
      return Dates();
    case TypedValue::Type::LocalTime:
      return LocalTimes();
    case TypedValue::Type::LocalDateTime:
      return LocalDateTimes();
    case TypedValue::Type::ZonedDateTime:
      return ZonedDateTimes();
    case TypedValue::Type::Duration:
      return Durations();
    case TypedValue::Type::Graph:
      return Graphs(dba);
    case TypedValue::Type::Enum:
      return FromStored(PropertyValueType::Enum, [](auto const &v) { return TypedValue(v.ValueEnum()); });
    case TypedValue::Type::Point2d:
      return FromStored(PropertyValueType::Point2d, [](auto const &v) { return TypedValue(v.ValuePoint2d()); });
    case TypedValue::Type::Point3d:
      return FromStored(PropertyValueType::Point3d, [](auto const &v) { return TypedValue(v.ValuePoint3d()); });
    case TypedValue::Type::Function:
    case TypedValue::Type::VirtualGraph:
    case TypedValue::Type::VirtualEdge:
    case TypedValue::Type::VirtualNode:
      return {};
  }
}

auto EveryTypedValueShape(query::DbAccessor *dba) -> std::vector<TypedValue> {
  return EveryTypedValueShapeExcept(dba, {});
}

auto EveryTypedValueShapeExcept(query::DbAccessor *dba, std::initializer_list<TypedValue::Type> types)
    -> std::vector<TypedValue> {
  auto result = std::vector<TypedValue>{};
  for (auto const type : kEveryTypedValueType) {
    if (std::ranges::find(types, type) != types.end()) continue;
    auto group = ShapesOfType(type, dba);
    result.insert(result.end(), std::make_move_iterator(group.begin()), std::make_move_iterator(group.end()));
  }
  return result;
}

}  // namespace memgraph::test::shapes
