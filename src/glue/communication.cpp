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

#include "glue/communication.hpp"

#include <map>
#include <string>
#include <vector>

#include "communication/bolt/v1/mg_types.hpp"
#include "communication/bolt/v1/value.hpp"
#include "query/graph.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/temporal.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/temporal.hpp"

using memgraph::communication::bolt::kMgTypeEnum;
using memgraph::communication::bolt::kMgTypeType;
using memgraph::communication::bolt::kMgTypeValue;
using memgraph::communication::bolt::MgType;
using memgraph::communication::bolt::Value;
using bolt_map_t = memgraph::communication::bolt::map_t;
using namespace std::string_view_literals;

namespace memgraph::glue {

auto BoltMapToMgType(bolt_map_t const &value, storage::Storage const *storage)
    -> std::optional<storage::ExternalPropertyValue> {
  auto info = BoltMapToMgTypeInfo(value);
  if (!info) return std::nullopt;

  auto const &[type, _, mg_value] = *info;
  switch (type) {
    case MgType::Enum: {
      if (!storage) return std::nullopt;
      auto enum_val = storage->enum_store_.ToEnum(mg_value);
      if (!enum_val) return std::nullopt;
      return storage::ExternalPropertyValue(*enum_val);
    }
  }
  return std::nullopt;
}

query::TypedValue ToTypedValue(const Value &value, storage::Storage const *storage) {
  switch (value.type()) {
    case Value::Type::Null:
      return {};
    case Value::Type::Bool:
      return query::TypedValue(value.ValueBool());
    case Value::Type::Int:
      return query::TypedValue(value.ValueInt());
    case Value::Type::Double:
      return query::TypedValue(value.ValueDouble());
    case Value::Type::String:
      return query::TypedValue(value.ValueString());
    case Value::Type::List: {
      std::vector<query::TypedValue> list;
      list.reserve(value.ValueList().size());
      for (const auto &v : value.ValueList()) list.push_back(ToTypedValue(v, storage));
      return query::TypedValue(std::move(list));
    }
    case Value::Type::Map: {
      auto const &valueMap = value.ValueMap();
      auto mg_type = BoltMapToMgType(valueMap, storage);
      if (mg_type) return query::TypedValue{*mg_type};

      std::map<std::string, query::TypedValue> map;
      for (const auto &kv : valueMap) map.emplace(kv.first, ToTypedValue(kv.second, storage));
      return query::TypedValue(std::move(map));
    }
    case Value::Type::Vertex:
    case Value::Type::Edge:
    case Value::Type::UnboundedEdge:
    case Value::Type::Path:
      throw communication::bolt::ValueException("Unsupported conversion from Value to TypedValue");
    case Value::Type::Date:
      return query::TypedValue(value.ValueDate());
    case Value::Type::LocalTime:
      return query::TypedValue(value.ValueLocalTime());
    case Value::Type::LocalDateTime:
      return query::TypedValue(value.ValueLocalDateTime());
    case Value::Type::Duration:
      return query::TypedValue(value.ValueDuration());
    case Value::Type::ZonedDateTime:
      return query::TypedValue(value.ValueZonedDateTime());
    case Value::Type::Point2d: {
      return query::TypedValue{value.ValuePoint2d()};
    }
    case Value::Type::Point3d: {
      return query::TypedValue{value.ValuePoint3d()};
    }
  }
}

storage::Result<communication::bolt::Vertex> ToBoltVertex(const query::VertexAccessor &vertex,
                                                          const storage::Storage &db, storage::View view) {
  return ToBoltVertex(vertex.impl_, db, view);
}

storage::Result<communication::bolt::Edge> ToBoltEdge(const query::EdgeAccessor &edge, const storage::Storage &db,
                                                      storage::View view) {
  return ToBoltEdge(edge.impl_, db, view);
}

storage::Result<Value> ToBoltValue(const query::TypedValue &value, const storage::Storage *db, storage::View view) {
  auto check_db = [db]() {
    if (db == nullptr) [[unlikely]]
      throw communication::bolt::ValueException("Database needed for TypeValue conversion.");
  };

  switch (value.type()) {
    // No database needed
    case query::TypedValue::Type::Null:
      return storage::Result<Value>{std::in_place};
    case query::TypedValue::Type::Bool:
      return storage::Result<Value>{std::in_place, value.ValueBool()};
    case query::TypedValue::Type::Int:
      return storage::Result<Value>{std::in_place, value.ValueInt()};
    case query::TypedValue::Type::Double:
      return storage::Result<Value>{std::in_place, value.ValueDouble()};
    case query::TypedValue::Type::String:
      return storage::Result<Value>{std::in_place, std::string_view(value.ValueString())};
    case query::TypedValue::Type::Date:
      return storage::Result<Value>{std::in_place, value.ValueDate()};
    case query::TypedValue::Type::LocalTime:
      return storage::Result<Value>{std::in_place, value.ValueLocalTime()};
    case query::TypedValue::Type::LocalDateTime:
      return storage::Result<Value>{std::in_place, value.ValueLocalDateTime()};
    case query::TypedValue::Type::Duration:
      return storage::Result<Value>{std::in_place, value.ValueDuration()};
    case query::TypedValue::Type::ZonedDateTime:
      return storage::Result<Value>{std::in_place, value.ValueZonedDateTime()};

    // Database potentially not required
    case query::TypedValue::Type::Map: {
      bolt_map_t map;
      for (const auto &kv : value.ValueMap()) {
        auto maybe_value = ToBoltValue(kv.second, db, view);
        if (!maybe_value) return std::unexpected{maybe_value.error()};
        map.emplace(kv.first, std::move(*maybe_value));
      }
      return storage::Result<Value>{std::in_place, std::move(map)};
    }

    // Database is required
    case query::TypedValue::Type::List: {
      check_db();
      std::vector<Value> values;
      values.reserve(value.ValueList().size());
      for (const auto &v : value.ValueList()) {
        auto maybe_value = ToBoltValue(v, db, view);
        if (!maybe_value) return std::unexpected{maybe_value.error()};
        values.emplace_back(std::move(*maybe_value));
      }
      return storage::Result<Value>{std::in_place, std::move(values)};
    }
    case query::TypedValue::Type::Vertex: {
      check_db();
      auto maybe_vertex = ToBoltVertex(value.ValueVertex(), *db, view);
      if (!maybe_vertex) return std::unexpected{maybe_vertex.error()};
      return storage::Result<Value>{std::in_place, std::move(*maybe_vertex)};
    }
    case query::TypedValue::Type::Edge: {
      check_db();
      auto maybe_edge = ToBoltEdge(value.ValueEdge(), *db, view);
      if (!maybe_edge) return std::unexpected{maybe_edge.error()};
      return storage::Result<Value>{std::in_place, std::move(*maybe_edge)};
    }
    case query::TypedValue::Type::Path: {
      check_db();
      auto maybe_path = ToBoltPath(value.ValuePath(), *db, view);
      if (!maybe_path) return std::unexpected{maybe_path.error()};
      return storage::Result<Value>{std::in_place, std::move(*maybe_path)};
    }
    case query::TypedValue::Type::Graph: {
      check_db();
      auto maybe_graph = ToBoltGraph(value.ValueGraph(), *db, view);
      if (!maybe_graph) return std::unexpected{maybe_graph.error()};
      return storage::Result<Value>{std::in_place, std::move(*maybe_graph)};
    }
    case query::TypedValue::Type::Enum: {
      check_db();
      auto maybe_enum_value_str = db->enum_store_.ToString(value.ValueEnum());
      if (!maybe_enum_value_str) [[unlikely]] {
        throw communication::bolt::ValueException("Enum not registered in the database");
      }
      auto map = bolt_map_t{};
      map.emplace(kMgTypeType, memgraph::communication::bolt::kMgTypeEnum);
      map.emplace(kMgTypeValue, *std::move(maybe_enum_value_str));
      return storage::Result<Value>{std::in_place, std::move(map)};
    }
    case query::TypedValue::Type::Point2d: {
      return storage::Result<Value>{std::in_place, value.ValuePoint2d()};
    }
    case query::TypedValue::Type::Point3d: {
      return storage::Result<Value>{std::in_place, value.ValuePoint3d()};
    }

    // Unsupported conversions
    case query::TypedValue::Type::Function: {
      throw communication::bolt::ValueException("Unsupported conversion from TypedValue::Function to Value");
    }
  }
}

storage::Result<communication::bolt::Vertex> ToBoltVertex(const storage::VertexAccessor &vertex,
                                                          const storage::Storage &db, storage::View view) {
  auto id = communication::bolt::Id::FromUint(vertex.Gid().AsUint());
  auto maybe_labels = vertex.Labels(view);
  if (!maybe_labels) return std::unexpected{maybe_labels.error()};
  std::vector<std::string> labels;
  labels.reserve(maybe_labels->size());
  for (const auto &label : *maybe_labels) {
    labels.push_back(db.LabelToName(label));
  }
  auto maybe_properties = vertex.Properties(view);
  if (!maybe_properties) return std::unexpected{maybe_properties.error()};
  bolt_map_t properties;
  for (const auto &prop : *maybe_properties) {
    properties[db.PropertyToName(prop.first)] = ToBoltValue(prop.second, db);
  }
  // Introduced in Bolt v5 (for now just send the ID)
  auto element_id = std::to_string(id.AsInt());
  return communication::bolt::Vertex{id, std::move(labels), std::move(properties), std::move(element_id)};
}

storage::Result<communication::bolt::Edge> ToBoltEdge(const storage::EdgeAccessor &edge, const storage::Storage &db,
                                                      storage::View view) {
  auto id = communication::bolt::Id::FromUint(edge.Gid().AsUint());
  auto from = communication::bolt::Id::FromUint(edge.FromVertex().Gid().AsUint());
  auto to = communication::bolt::Id::FromUint(edge.ToVertex().Gid().AsUint());
  auto type = db.EdgeTypeToName(edge.EdgeType());
  auto maybe_properties = edge.Properties(view);
  if (!maybe_properties) return std::unexpected{maybe_properties.error()};
  bolt_map_t properties;
  for (const auto &prop : *maybe_properties) {
    properties[db.PropertyToName(prop.first)] = ToBoltValue(prop.second, db);
  }
  // Introduced in Bolt v5 (for now just send the ID)
  const auto element_id = std::to_string(id.AsInt());
  const auto from_element_id = std::to_string(from.AsInt());
  const auto to_element_id = std::to_string(to.AsInt());
  return communication::bolt::Edge{
      id, from, to, std::move(type), std::move(properties), element_id, from_element_id, to_element_id};
}

storage::Result<communication::bolt::Path> ToBoltPath(const query::Path &path, const storage::Storage &db,
                                                      storage::View view) {
  std::vector<communication::bolt::Vertex> vertices;
  vertices.reserve(path.vertices().size());
  for (const auto &v : path.vertices()) {
    auto maybe_vertex = ToBoltVertex(v, db, view);
    if (!maybe_vertex) return std::unexpected{maybe_vertex.error()};
    vertices.emplace_back(std::move(*maybe_vertex));
  }
  std::vector<communication::bolt::Edge> edges;
  edges.reserve(path.edges().size());
  for (const auto &e : path.edges()) {
    auto maybe_edge = ToBoltEdge(e, db, view);
    if (!maybe_edge) return std::unexpected{maybe_edge.error()};
    edges.emplace_back(std::move(*maybe_edge));
  }
  return communication::bolt::Path(vertices, edges);
}

storage::Result<bolt_map_t> ToBoltGraph(const query::Graph &graph, const storage::Storage &db, storage::View view) {
  bolt_map_t map;
  std::vector<Value> vertices;
  vertices.reserve(graph.vertices().size());
  for (const auto &v : graph.vertices()) {
    auto maybe_vertex = ToBoltVertex(v, db, view);
    if (!maybe_vertex) return std::unexpected{maybe_vertex.error()};
    vertices.emplace_back(std::move(*maybe_vertex));
  }
  map.emplace("nodes", Value(vertices));

  std::vector<Value> edges;
  edges.reserve(graph.edges().size());
  for (const auto &e : graph.edges()) {
    auto maybe_edge = ToBoltEdge(e, db, view);
    if (!maybe_edge) return std::unexpected{maybe_edge.error()};
    edges.emplace_back(std::move(*maybe_edge));
  }
  map.emplace("edges", Value(edges));

  return std::move(map);
}

storage::ExternalPropertyValue ToExternalPropertyValue(communication::bolt::Value const &value,
                                                       storage::Storage const *storage) {
  switch (value.type()) {
    case Value::Type::Null:
      return storage::ExternalPropertyValue();
    case Value::Type::Bool:
      return storage::ExternalPropertyValue(value.ValueBool());
    case Value::Type::Int:
      return storage::ExternalPropertyValue(value.ValueInt());
    case Value::Type::Double:
      return storage::ExternalPropertyValue(value.ValueDouble());
    case Value::Type::String:
      return storage::ExternalPropertyValue(value.ValueString());
    case Value::Type::List: {
      std::vector<storage::ExternalPropertyValue> vec;
      vec.reserve(value.ValueList().size());
      for (const auto &value : value.ValueList()) vec.emplace_back(ToExternalPropertyValue(value, storage));
      return storage::ExternalPropertyValue(std::move(vec));
    }
    case Value::Type::Map: {
      auto const &valueMap = value.ValueMap();
      auto mg_type = BoltMapToMgType(valueMap, storage);
      if (mg_type) return *mg_type;

      auto map = storage::ExternalPropertyValue::map_t{};
      do_reserve(map, valueMap.size());
      for (const auto &[k, v] : valueMap) {
        map.try_emplace(k, ToExternalPropertyValue(v, storage));
      }
      return storage::ExternalPropertyValue(std::move(map));
    }
    case Value::Type::Vertex:
    case Value::Type::Edge:
    case Value::Type::UnboundedEdge:
    case Value::Type::Path:
      throw communication::bolt::ValueException("Unsupported conversion from Value to ExternalPropertyValue");
    case Value::Type::Date:
      return storage::ExternalPropertyValue(
          storage::TemporalData(storage::TemporalType::Date, value.ValueDate().MicrosecondsSinceEpoch()));
    case Value::Type::LocalTime:
      return storage::ExternalPropertyValue(
          storage::TemporalData(storage::TemporalType::LocalTime, value.ValueLocalTime().MicrosecondsSinceEpoch()));
    case Value::Type::LocalDateTime:
      // Bolt uses time since epoch without timezone (as if in UTC)
      return storage::ExternalPropertyValue(storage::TemporalData(
          storage::TemporalType::LocalDateTime, value.ValueLocalDateTime().SysMicrosecondsSinceEpoch()));
    case Value::Type::Duration:
      return storage::ExternalPropertyValue(
          storage::TemporalData(storage::TemporalType::Duration, value.ValueDuration().microseconds));
    case Value::Type::ZonedDateTime: {
      const auto &temp_value = value.ValueZonedDateTime();
      return storage::ExternalPropertyValue(storage::ZonedTemporalData(
          storage::ZonedTemporalType::ZonedDateTime, temp_value.SysTimeSinceEpoch(), temp_value.GetTimezone()));
    }
    case Value::Type::Point2d: {
      return storage::ExternalPropertyValue(value.ValuePoint2d());
    }
    case Value::Type::Point3d: {
      return storage::ExternalPropertyValue(value.ValuePoint3d());
    }
  }
}

Value ToBoltValue(const storage::PropertyValue &value, const storage::Storage &storage) {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      return Value();
    case storage::PropertyValue::Type::Bool:
      return Value(value.ValueBool());
    case storage::PropertyValue::Type::Int:
      return Value(value.ValueInt());
      break;
    case storage::PropertyValue::Type::Double:
      return Value(value.ValueDouble());
    case storage::PropertyValue::Type::String:
      return Value(value.ValueString());
    case storage::PropertyValue::Type::List: {
      const auto &values = value.ValueList();
      std::vector<Value> vec;
      vec.reserve(values.size());
      for (const auto &v : values) {
        vec.push_back(ToBoltValue(v, storage));
      }
      return vec;
    }
    case storage::PropertyValue::Type::NumericList: {
      const auto &values = value.ValueNumericList();
      std::vector<Value> vec;
      vec.reserve(values.size());
      for (const auto &v : values) {
        if (std::holds_alternative<int>(v)) {
          vec.emplace_back(std::get<int>(v));
        } else {
          vec.emplace_back(std::get<double>(v));
        }
      }
      return vec;
    }
    case storage::PropertyValue::Type::IntList: {
      const auto &values = value.ValueIntList();
      std::vector<Value> vec;
      vec.reserve(values.size());
      for (const auto &v : values) {
        vec.emplace_back(v);
      }
      return vec;
    }
    case storage::PropertyValue::Type::DoubleList: {
      const auto &values = value.ValueDoubleList();
      std::vector<Value> vec;
      vec.reserve(values.size());
      for (const auto &v : values) {
        vec.emplace_back(v);
      }
      return vec;
    }
    case storage::PropertyValue::Type::Map: {
      const auto &map = value.ValueMap();
      bolt_map_t dv_map;
      for (const auto &kv : map) {
        dv_map.emplace(storage.PropertyToName(kv.first), ToBoltValue(kv.second, storage));
      }
      return Value(std::move(dv_map));
    }
    case storage::PropertyValue::Type::TemporalData: {
      const auto &type = value.ValueTemporalData();
      switch (type.type) {
        case storage::TemporalType::Date:
          return {utils::Date{std::chrono::microseconds{type.microseconds}}};
        case storage::TemporalType::LocalTime:
          return Value(utils::LocalTime(type.microseconds));
        case storage::TemporalType::LocalDateTime:
          return Value(utils::LocalDateTime(type.microseconds));
        case storage::TemporalType::Duration:
          return Value(utils::Duration(type.microseconds));
      }
    }
    case storage::PropertyValue::Type::ZonedTemporalData: {
      const auto &type = value.ValueZonedTemporalData();
      switch (type.type) {
        case storage::ZonedTemporalType::ZonedDateTime:
          return {utils::ZonedDateTime(type.microseconds, type.timezone)};
      }
    }
    case storage::PropertyValue::Type::Enum: {
      auto maybe_enum_value_str = storage.enum_store_.ToString(value.ValueEnum());
      if (!maybe_enum_value_str) [[unlikely]] {
        throw communication::bolt::ValueException("Enum not registered in the database");
      }
      // Bolt does not know about enums, encode as map type instead
      auto map = bolt_map_t{};
      map.emplace(kMgTypeType, kMgTypeEnum);
      map.emplace(kMgTypeValue, *std::move(maybe_enum_value_str));
      return {std::move(map)};
    }
    case storage::PropertyValue::Type::Point2d: {
      return {value.ValuePoint2d()};
    }
    case storage::PropertyValue::Type::Point3d: {
      return {value.ValuePoint3d()};
    }
  }
}

}  // namespace memgraph::glue
