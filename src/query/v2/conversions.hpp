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

#pragma once
#include "bindings/typed_value.hpp"
#include "query/v2/accessors.hpp"
#include "query/v2/requests.hpp"

namespace memgraph::query::v2 {

inline TypedValue ValueToTypedValue(const msgs::Value &value) {
  using Value = msgs::Value;
  switch (value.type) {
    case Value::Type::Null:
      return {};
    case Value::Type::Bool:
      return TypedValue(value.bool_v);
    case Value::Type::Int64:
      return TypedValue(value.int_v);
    case Value::Type::Double:
      return TypedValue(value.double_v);
    case Value::Type::String:
      return TypedValue(value.string_v);
    case Value::Type::List: {
      const auto &lst = value.list_v;
      std::vector<TypedValue> dst;
      dst.reserve(lst.size());
      for (const auto &elem : lst) {
        dst.push_back(ValueToTypedValue(elem));
      }
      return TypedValue(std::move(dst));
    }
    case Value::Type::Map: {
      const auto &value_map = value.map_v;
      std::map<std::string, TypedValue> dst;
      for (const auto &[key, val] : value_map) {
        dst[key] = ValueToTypedValue(val);
      }
      return TypedValue(std::move(dst));
    }
    case Value::Type::Vertex:
      return TypedValue(accessors::VertexAccessor(value.vertex_v, {}));
    case Value::Type::Edge:
      return TypedValue(accessors::EdgeAccessor(value.edge_v, {}));
    case Value::Type::Path:
      break;
  }
  throw std::runtime_error("Incorrect type in conversion");
}

inline msgs::Value TypedValueToValue(const TypedValue &value) {
  using Value = msgs::Value;
  switch (value.type()) {
    case TypedValue::Type::Null:
      return {};
    case TypedValue::Type::Bool:
      return Value(value.ValueBool());
    case TypedValue::Type::Int:
      return Value(value.ValueInt());
    case TypedValue::Type::Double:
      return Value(value.ValueDouble());
    case TypedValue::Type::String:
      return Value(std::string(value.ValueString()));
    case TypedValue::Type::List: {
      const auto &lst = value.ValueList();
      std::vector<Value> dst;
      dst.reserve(lst.size());
      for (const auto &elem : lst) {
        dst.push_back(TypedValueToValue(elem));
      }
      return Value(std::move(dst));
    }
    case TypedValue::Type::Map: {
      const auto &value_map = value.ValueMap();
      std::map<std::string, Value> dst;
      for (const auto &[key, val] : value_map) {
        dst[std::string(key)] = TypedValueToValue(val);
      }
      return Value(std::move(dst));
    }
    case TypedValue::Type::Vertex:
      return Value(value.ValueVertex().GetVertex());
    case TypedValue::Type::Edge:
      return Value(value.ValueEdge().GetEdge());
    case TypedValue::Type::Path:
    default:
      break;
  }
  throw std::runtime_error("Incorrect type in conversion");
}

}  // namespace memgraph::query::v2
