// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "glue/v2/communication.hpp"

#include <map>
#include <string>
#include <vector>

#include "common/errors.hpp"
#include "coordinator/shard_map.hpp"
#include "query/v2/accessors.hpp"
#include "query/v2/request_runtime.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/edge_accessor.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage/v3/view.hpp"
#include "utils/exceptions.hpp"
#include "utils/temporal.hpp"

using memgraph::communication::bolt::Value;

namespace memgraph::glue::v2 {

query::v2::TypedValue ToTypedValue(const Value &value) {
  switch (value.type()) {
    case Value::Type::Null:
      return {};
    case Value::Type::Bool:
      return query::v2::TypedValue(value.ValueBool());
    case Value::Type::Int:
      return query::v2::TypedValue(value.ValueInt());
    case Value::Type::Double:
      return query::v2::TypedValue(value.ValueDouble());
    case Value::Type::String:
      return query::v2::TypedValue(value.ValueString());
    case Value::Type::List: {
      std::vector<query::v2::TypedValue> list;
      list.reserve(value.ValueList().size());
      for (const auto &v : value.ValueList()) list.push_back(ToTypedValue(v));
      return query::v2::TypedValue(std::move(list));
    }
    case Value::Type::Map: {
      std::map<std::string, query::v2::TypedValue> map;
      for (const auto &kv : value.ValueMap()) map.emplace(kv.first, ToTypedValue(kv.second));
      return query::v2::TypedValue(std::move(map));
    }
    case Value::Type::Vertex:
    case Value::Type::Edge:
    case Value::Type::UnboundedEdge:
    case Value::Type::Path:
      throw communication::bolt::ValueException("Unsupported conversion from Value to TypedValue");
    case Value::Type::Date:
      return query::v2::TypedValue(value.ValueDate());
    case Value::Type::LocalTime:
      return query::v2::TypedValue(value.ValueLocalTime());
    case Value::Type::LocalDateTime:
      return query::v2::TypedValue(value.ValueLocalDateTime());
    case Value::Type::Duration:
      return query::v2::TypedValue(value.ValueDuration());
  }
}

communication::bolt::Vertex ToBoltVertex(const query::v2::accessors::VertexAccessor &vertex,
                                         const query::v2::RequestRuntimeInterface *request_runtime,
                                         storage::v3::View /*view*/) {
  auto id = communication::bolt::Id::FromUint(0);

  auto labels = vertex.Labels();
  std::vector<std::string> new_labels;
  new_labels.reserve(labels.size());
  for (const auto &label : labels) {
    new_labels.push_back(request_runtime->LabelToName(label.id));
  }

  auto properties = vertex.Properties();
  std::map<std::string, Value> new_properties;
  for (const auto &[prop, property_value] : properties) {
    new_properties[request_runtime->PropertyToName(prop)] = ToBoltValue(property_value);
  }
  return communication::bolt::Vertex{id, new_labels, new_properties};
}

communication::bolt::Edge ToBoltEdge(const query::v2::accessors::EdgeAccessor &edge,
                                     const query::v2::RequestRuntimeInterface *request_runtime,
                                     storage::v3::View /*view*/) {
  // TODO(jbajic) Fix bolt communication
  auto id = communication::bolt::Id::FromUint(0);
  auto from = communication::bolt::Id::FromUint(0);
  auto to = communication::bolt::Id::FromUint(0);
  const auto &type = request_runtime->EdgeTypeToName(edge.EdgeType());

  auto properties = edge.Properties();
  std::map<std::string, Value> new_properties;
  for (const auto &[prop, property_value] : properties) {
    new_properties[request_runtime->PropertyToName(prop)] = ToBoltValue(property_value);
  }
  return communication::bolt::Edge{id, from, to, type, new_properties};
}

communication::bolt::Path ToBoltPath(const query::v2::accessors::Path & /*edge*/,
                                     const query::v2::RequestRuntimeInterface * /*request_runtime*/,
                                     storage::v3::View /*view*/) {
  // TODO(jbajic) Fix bolt communication
  MG_ASSERT(false, "Path is unimplemented!");
  return {};
}

Value ToBoltValue(const query::v2::TypedValue &value, const query::v2::RequestRuntimeInterface *request_runtime,
                  storage::v3::View view) {
  switch (value.type()) {
    case query::v2::TypedValue::Type::Null:
      return {};
    case query::v2::TypedValue::Type::Bool:
      return {value.ValueBool()};
    case query::v2::TypedValue::Type::Int:
      return {value.ValueInt()};
    case query::v2::TypedValue::Type::Double:
      return {value.ValueDouble()};
    case query::v2::TypedValue::Type::String:
      return {std::string(value.ValueString())};
    case query::v2::TypedValue::Type::List: {
      std::vector<Value> values;
      values.reserve(value.ValueList().size());
      for (const auto &v : value.ValueList()) {
        auto value = ToBoltValue(v, request_runtime, view);
        values.emplace_back(std::move(value));
      }
      return {std::move(values)};
    }
    case query::v2::TypedValue::Type::Map: {
      std::map<std::string, Value> map;
      for (const auto &kv : value.ValueMap()) {
        auto value = ToBoltValue(kv.second, request_runtime, view);
        map.emplace(kv.first, std::move(value));
      }
      return {std::move(map)};
    }
    case query::v2::TypedValue::Type::Vertex: {
      auto vertex = ToBoltVertex(value.ValueVertex(), request_runtime, view);
      return {std::move(vertex)};
    }
    case query::v2::TypedValue::Type::Edge: {
      auto edge = ToBoltEdge(value.ValueEdge(), request_runtime, view);
      return {std::move(edge)};
    }
    case query::v2::TypedValue::Type::Path: {
      auto path = ToBoltPath(value.ValuePath(), request_runtime, view);
      return {std::move(path)};
    }
    case query::v2::TypedValue::Type::Date:
      return {value.ValueDate()};
    case query::v2::TypedValue::Type::LocalTime:
      return {value.ValueLocalTime()};
    case query::v2::TypedValue::Type::LocalDateTime:
      return {value.ValueLocalDateTime()};
    case query::v2::TypedValue::Type::Duration:
      return {value.ValueDuration()};
  }
}

Value ToBoltValue(msgs::Value value) {
  switch (value.type) {
    case msgs::Value::Type::Null:
      return {};
    case msgs::Value::Type::Bool:
      return {value.bool_v};
    case msgs::Value::Type::Int64:
      return {value.int_v};
    case msgs::Value::Type::Double:
      return {value.double_v};
    case msgs::Value::Type::String:
      return {std::string(value.string_v)};
    case msgs::Value::Type::List: {
      std::vector<Value> values;
      values.reserve(value.list_v.size());
      for (const auto &v : value.list_v) {
        auto maybe_value = ToBoltValue(v);
        values.emplace_back(std::move(maybe_value));
      }
      return Value{std::move(values)};
    }
    case msgs::Value::Type::Map: {
      std::map<std::string, Value> map;
      for (const auto &kv : value.map_v) {
        auto maybe_value = ToBoltValue(kv.second);
        map.emplace(kv.first, std::move(maybe_value));
      }
      return Value{std::move(map)};
    }
    case msgs::Value::Type::Vertex:
    case msgs::Value::Type::Edge: {
      throw utils::BasicException("Vertex and Edge not supported!");
    }
      // TODO Value to Date types not supported
  }
}

storage::v3::PropertyValue ToPropertyValue(const Value &value) {
  switch (value.type()) {
    case Value::Type::Null:
      return {};
    case Value::Type::Bool:
      return storage::v3::PropertyValue(value.ValueBool());
    case Value::Type::Int:
      return storage::v3::PropertyValue(value.ValueInt());
    case Value::Type::Double:
      return storage::v3::PropertyValue(value.ValueDouble());
    case Value::Type::String:
      return storage::v3::PropertyValue(value.ValueString());
    case Value::Type::List: {
      std::vector<storage::v3::PropertyValue> vec;
      vec.reserve(value.ValueList().size());
      for (const auto &value : value.ValueList()) vec.emplace_back(ToPropertyValue(value));
      return storage::v3::PropertyValue(std::move(vec));
    }
    case Value::Type::Map: {
      std::map<std::string, storage::v3::PropertyValue> map;
      for (const auto &kv : value.ValueMap()) map.emplace(kv.first, ToPropertyValue(kv.second));
      return storage::v3::PropertyValue(std::move(map));
    }
    case Value::Type::Vertex:
    case Value::Type::Edge:
    case Value::Type::UnboundedEdge:
    case Value::Type::Path:
      throw communication::bolt::ValueException("Unsupported conversion from Value to PropertyValue");
    case Value::Type::Date:
      return storage::v3::PropertyValue(
          storage::v3::TemporalData(storage::v3::TemporalType::Date, value.ValueDate().MicrosecondsSinceEpoch()));
    case Value::Type::LocalTime:
      return storage::v3::PropertyValue(storage::v3::TemporalData(storage::v3::TemporalType::LocalTime,
                                                                  value.ValueLocalTime().MicrosecondsSinceEpoch()));
    case Value::Type::LocalDateTime:
      return storage::v3::PropertyValue(storage::v3::TemporalData(storage::v3::TemporalType::LocalDateTime,
                                                                  value.ValueLocalDateTime().MicrosecondsSinceEpoch()));
    case Value::Type::Duration:
      return storage::v3::PropertyValue(
          storage::v3::TemporalData(storage::v3::TemporalType::Duration, value.ValueDuration().microseconds));
  }
}

Value ToBoltValue(const storage::v3::PropertyValue &value) {
  switch (value.type()) {
    case storage::v3::PropertyValue::Type::Null:
      return {};
    case storage::v3::PropertyValue::Type::Bool:
      return {value.ValueBool()};
    case storage::v3::PropertyValue::Type::Int:
      return {value.ValueInt()};
      break;
    case storage::v3::PropertyValue::Type::Double:
      return {value.ValueDouble()};
    case storage::v3::PropertyValue::Type::String:
      return {value.ValueString()};
    case storage::v3::PropertyValue::Type::List: {
      const auto &values = value.ValueList();
      std::vector<Value> vec;
      vec.reserve(values.size());
      for (const auto &v : values) {
        vec.push_back(ToBoltValue(v));
      }
      return {std::move(vec)};
    }
    case storage::v3::PropertyValue::Type::Map: {
      const auto &map = value.ValueMap();
      std::map<std::string, Value> dv_map;
      for (const auto &kv : map) {
        dv_map.emplace(kv.first, ToBoltValue(kv.second));
      }
      return {std::move(dv_map)};
    }
    case storage::v3::PropertyValue::Type::TemporalData:
      const auto &type = value.ValueTemporalData();
      switch (type.type) {
        case storage::v3::TemporalType::Date:
          return {utils::Date(type.microseconds)};
        case storage::v3::TemporalType::LocalTime:
          return {utils::LocalTime(type.microseconds)};
        case storage::v3::TemporalType::LocalDateTime:
          return {utils::LocalDateTime(type.microseconds)};
        case storage::v3::TemporalType::Duration:
          return {utils::Duration(type.microseconds)};
      }
  }
}

}  // namespace memgraph::glue::v2
