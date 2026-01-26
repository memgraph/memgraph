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

#include "query/serialization/typed_value.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::query::serialization {

nlohmann::json SerializeTypedValue(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return {};
    case TypedValue::Type::Bool:
      return value.ValueBool();
    case TypedValue::Type::Int:
      return value.ValueInt();
    case TypedValue::Type::Double:
      return value.ValueDouble();
    case TypedValue::Type::String:
      return value.ValueString();
    case TypedValue::Type::List: {
      nlohmann::json arr = nlohmann::json::array();
      for (const auto &item : value.ValueList()) {
        arr.push_back(SerializeTypedValue(item));
      }
      return arr;
    }
    case TypedValue::Type::Map: {
      nlohmann::json obj = nlohmann::json::object();
      for (const auto &[key, val] : value.ValueMap()) {
        obj.emplace(key, SerializeTypedValue(val));
      }
      return obj;
    }
    default:
      throw utils::BasicException("Unsupported TypedValue type for JSON serialization");
  }
}

TypedValue DeserializeTypedValue(const nlohmann::json &data) {
  if (data.is_null()) {
    return {};
  }

  if (data.is_boolean()) {
    return TypedValue(data.get<bool>());
  }

  if (data.is_number_integer()) {
    return TypedValue(data.get<int64_t>());
  }

  if (data.is_number_float()) {
    return TypedValue(data.get<double>());
  }

  if (data.is_string()) {
    return TypedValue(data.get<std::string>());
  }

  if (data.is_array()) {
    return TypedValue(DeserializeTypedValueList(data.get<nlohmann::json::array_t>()));
  }

  if (data.is_object()) {
    return TypedValue(DeserializeTypedValueMap(data.get<nlohmann::json::object_t>()));
  }

  throw utils::BasicException("Unsupported JSON type for TypedValue deserialization");
}

std::vector<TypedValue> DeserializeTypedValueList(const nlohmann::json::array_t &data) {
  std::vector<TypedValue> typed_values;
  typed_values.reserve(data.size());
  for (const auto &value : data) {
    typed_values.emplace_back(DeserializeTypedValue(value));
  }
  return typed_values;
}

std::map<std::string, TypedValue> DeserializeTypedValueMap(const nlohmann::json::object_t &data) {
  std::map<std::string, TypedValue> typed_map;
  for (const auto &[key, value] : data) {
    typed_map.emplace(key, DeserializeTypedValue(value));
  }
  return typed_map;
}

}  // namespace memgraph::query::serialization
