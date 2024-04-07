// Copyright 2024 Memgraph Ltd.
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

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/temporal.hpp"

using memgraph::communication::bolt::Value;

namespace memgraph::glue {

query::TypedValue ToTypedValue(const Value &value) {
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
      for (const auto &v : value.ValueList()) list.push_back(ToTypedValue(v));
      return query::TypedValue(std::move(list));
    }
    case Value::Type::Map: {
      std::map<std::string, query::TypedValue> map;
      for (const auto &kv : value.ValueMap()) map.emplace(kv.first, ToTypedValue(kv.second));
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
      return Value();
    case query::TypedValue::Type::Bool:
      return Value(value.ValueBool());
    case query::TypedValue::Type::Int:
      return Value(value.ValueInt());
    case query::TypedValue::Type::Double:
      return Value(value.ValueDouble());
    case query::TypedValue::Type::String:
      return Value(std::string(value.ValueString()));
    case query::TypedValue::Type::Date:
      return Value(value.ValueDate());
    case query::TypedValue::Type::LocalTime:
      return Value(value.ValueLocalTime());
    case query::TypedValue::Type::LocalDateTime:
      return Value(value.ValueLocalDateTime());
    case query::TypedValue::Type::Duration:
      return Value(value.ValueDuration());
    case query::TypedValue::Type::ZonedDateTime:
      return Value(value.ValueZonedDateTime());

    // Database potentially not required
    case query::TypedValue::Type::Map: {
      std::map<std::string, Value> map;
      for (const auto &kv : value.ValueMap()) {
        auto maybe_value = ToBoltValue(kv.second, db, view);
        if (maybe_value.HasError()) return maybe_value.GetError();
        map.emplace(kv.first, std::move(*maybe_value));
      }
      return Value(std::move(map));
    }

    // Database is required
    case query::TypedValue::Type::List: {
      check_db();
      std::vector<Value> values;
      values.reserve(value.ValueList().size());
      for (const auto &v : value.ValueList()) {
        auto maybe_value = ToBoltValue(v, db, view);
        if (maybe_value.HasError()) return maybe_value.GetError();
        values.emplace_back(std::move(*maybe_value));
      }
      return Value(std::move(values));
    }
    case query::TypedValue::Type::Vertex: {
      check_db();
      auto maybe_vertex = ToBoltVertex(value.ValueVertex(), *db, view);
      if (maybe_vertex.HasError()) return maybe_vertex.GetError();
      return Value(std::move(*maybe_vertex));
    }
    case query::TypedValue::Type::Edge: {
      check_db();
      auto maybe_edge = ToBoltEdge(value.ValueEdge(), *db, view);
      if (maybe_edge.HasError()) return maybe_edge.GetError();
      return Value(std::move(*maybe_edge));
    }
    case query::TypedValue::Type::Path: {
      check_db();
      auto maybe_path = ToBoltPath(value.ValuePath(), *db, view);
      if (maybe_path.HasError()) return maybe_path.GetError();
      return Value(std::move(*maybe_path));
    }
    case query::TypedValue::Type::Graph: {
      check_db();
      auto maybe_graph = ToBoltGraph(value.ValueGraph(), *db, view);
      if (maybe_graph.HasError()) return maybe_graph.GetError();
      return Value(std::move(*maybe_graph));
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
  if (maybe_labels.HasError()) return maybe_labels.GetError();
  std::vector<std::string> labels;
  labels.reserve(maybe_labels->size());
  for (const auto &label : *maybe_labels) {
    labels.push_back(db.LabelToName(label));
  }
  auto maybe_properties = vertex.Properties(view);
  if (maybe_properties.HasError()) return maybe_properties.GetError();
  std::map<std::string, Value> properties;
  for (const auto &prop : *maybe_properties) {
    properties[db.PropertyToName(prop.first)] = ToBoltValue(prop.second);
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
  if (maybe_properties.HasError()) return maybe_properties.GetError();
  std::map<std::string, Value> properties;
  for (const auto &prop : *maybe_properties) {
    properties[db.PropertyToName(prop.first)] = ToBoltValue(prop.second);
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
    if (maybe_vertex.HasError()) return maybe_vertex.GetError();
    vertices.emplace_back(std::move(*maybe_vertex));
  }
  std::vector<communication::bolt::Edge> edges;
  edges.reserve(path.edges().size());
  for (const auto &e : path.edges()) {
    auto maybe_edge = ToBoltEdge(e, db, view);
    if (maybe_edge.HasError()) return maybe_edge.GetError();
    edges.emplace_back(std::move(*maybe_edge));
  }
  return communication::bolt::Path(vertices, edges);
}

storage::Result<std::map<std::string, Value>> ToBoltGraph(const query::Graph &graph, const storage::Storage &db,
                                                          storage::View view) {
  std::map<std::string, Value> map;
  std::vector<Value> vertices;
  vertices.reserve(graph.vertices().size());
  for (const auto &v : graph.vertices()) {
    auto maybe_vertex = ToBoltVertex(v, db, view);
    if (maybe_vertex.HasError()) return maybe_vertex.GetError();
    vertices.emplace_back(std::move(*maybe_vertex));
  }
  map.emplace("nodes", Value(vertices));

  std::vector<Value> edges;
  edges.reserve(graph.edges().size());
  for (const auto &e : graph.edges()) {
    auto maybe_edge = ToBoltEdge(e, db, view);
    if (maybe_edge.HasError()) return maybe_edge.GetError();
    edges.emplace_back(std::move(*maybe_edge));
  }
  map.emplace("edges", Value(edges));

  return std::move(map);
}

storage::PropertyValue ToPropertyValue(const Value &value) {
  switch (value.type()) {
    case Value::Type::Null:
      return storage::PropertyValue();
    case Value::Type::Bool:
      return storage::PropertyValue(value.ValueBool());
    case Value::Type::Int:
      return storage::PropertyValue(value.ValueInt());
    case Value::Type::Double:
      return storage::PropertyValue(value.ValueDouble());
    case Value::Type::String:
      return storage::PropertyValue(value.ValueString());
    case Value::Type::List: {
      std::vector<storage::PropertyValue> vec;
      vec.reserve(value.ValueList().size());
      for (const auto &value : value.ValueList()) vec.emplace_back(ToPropertyValue(value));
      return storage::PropertyValue(std::move(vec));
    }
    case Value::Type::Map: {
      std::map<std::string, storage::PropertyValue> map;
      for (const auto &kv : value.ValueMap()) map.emplace(kv.first, ToPropertyValue(kv.second));
      return storage::PropertyValue(std::move(map));
    }
    case Value::Type::Vertex:
    case Value::Type::Edge:
    case Value::Type::UnboundedEdge:
    case Value::Type::Path:
      throw communication::bolt::ValueException("Unsupported conversion from Value to PropertyValue");
    case Value::Type::Date:
      return storage::PropertyValue(
          storage::TemporalData(storage::TemporalType::Date, value.ValueDate().MicrosecondsSinceEpoch()));
    case Value::Type::LocalTime:
      return storage::PropertyValue(
          storage::TemporalData(storage::TemporalType::LocalTime, value.ValueLocalTime().MicrosecondsSinceEpoch()));
    case Value::Type::LocalDateTime:
      return storage::PropertyValue(storage::TemporalData(storage::TemporalType::LocalDateTime,
                                                          value.ValueLocalDateTime().MicrosecondsSinceEpoch()));
    case Value::Type::Duration:
      return storage::PropertyValue(
          storage::TemporalData(storage::TemporalType::Duration, value.ValueDuration().microseconds));
    case Value::Type::ZonedDateTime: {
      const auto &temp_value = value.ValueZonedDateTime();
      return storage::PropertyValue(storage::ZonedTemporalData(
          storage::ZonedTemporalType::ZonedDateTime, temp_value.MicrosecondsSinceEpoch(), temp_value.GetTimezone()));
    }
  }
}

Value ToBoltValue(const storage::PropertyValue &value) {
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
        vec.push_back(ToBoltValue(v));
      }
      return Value(std::move(vec));
    }
    case storage::PropertyValue::Type::Map: {
      const auto &map = value.ValueMap();
      std::map<std::string, Value> dv_map;
      for (const auto &kv : map) {
        dv_map.emplace(kv.first, ToBoltValue(kv.second));
      }
      return Value(std::move(dv_map));
    }
    case storage::PropertyValue::Type::TemporalData: {
      const auto &type = value.ValueTemporalData();
      switch (type.type) {
        case storage::TemporalType::Date:
          return Value(utils::Date(type.microseconds));
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
          return Value(utils::ZonedDateTime(type.microseconds, type.timezone));
      }
    }
  }
}

}  // namespace memgraph::glue
