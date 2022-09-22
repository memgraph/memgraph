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
#include "query/v2/requests.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/logging.hpp"

#include <map>
#include <string>
#include <utility>
#include <vector>

#pragma once

// TODO(kostasrim) Think about long term sustainability

// This should not be put under v3 because ADL will mess that up.
namespace memgraph::storage::conversions {

using memgraph::msgs::PropertyId;
using memgraph::msgs::Value;
using memgraph::msgs::VertexId;

// TODO(gvolfing use come algorithm instead of explicit for loops)
inline memgraph::storage::v3::PropertyValue ToPropertyValue(Value value) {
  using PV = memgraph::storage::v3::PropertyValue;
  PV ret;
  switch (value.type) {
    case Value::Type::Null:
      return PV{};
    case Value::Type::Bool:
      return PV(value.bool_v);
    case Value::Type::Int64:
      return PV(static_cast<int64_t>(value.int_v));
    case Value::Type::Double:
      return PV(value.double_v);
    case Value::Type::String:
      return PV(value.string_v);
    case Value::Type::List: {
      std::vector<PV> list;
      for (auto &elem : value.list_v) {
        list.emplace_back(ToPropertyValue(std::move(elem)));
      }
      return PV(list);
    }
    case Value::Type::Map: {
      std::map<std::string, PV> map;
      for (auto &[key, value] : value.map_v) {
        map.emplace(std::make_pair(key, ToPropertyValue(std::move(value))));
      }
      return PV(map);
    }
    // These are not PropertyValues
    case Value::Type::Vertex:
    case Value::Type::Edge:
    case Value::Type::Path:
      MG_ASSERT(false, "Not PropertyValue");
  }
  return ret;
}

inline Value ToValue(const memgraph::storage::v3::PropertyValue &pv) {
  using memgraph::storage::v3::PropertyValue;

  switch (pv.type()) {
    case PropertyValue::Type::Bool:
      return Value(pv.ValueBool());
    case PropertyValue::Type::Double:
      return Value(pv.ValueDouble());
    case PropertyValue::Type::Int:
      return Value(pv.ValueInt());
    case PropertyValue::Type::List: {
      std::vector<Value> list;
      list.reserve(pv.ValueList().size());
      for (const auto &elem : pv.ValueList()) {
        list.emplace_back(ToValue(elem));
      }

      return Value(list);
    }
    case PropertyValue::Type::Map: {
      std::map<std::string, Value> map;
      for (const auto &[key, val] : pv.ValueMap()) {
        // maybe use std::make_pair once the && issue is resolved.
        map.emplace(key, ToValue(val));
      }

      return Value(map);
    }
    case PropertyValue::Type::Null:
      return Value{};
    case PropertyValue::Type::String:
      return Value(pv.ValueString());
    case PropertyValue::Type::TemporalData: {
      // TBD -> we need to specify this in the messages, not a priority.
      MG_ASSERT(false, "Temporal datatypes are not yet implemented on Value!");
      return Value{};
    }
  }
}

inline std::vector<memgraph::storage::v3::PropertyValue> ConvertPropertyVector(std::vector<Value> vec) {
  std::vector<memgraph::storage::v3::PropertyValue> ret;
  ret.reserve(vec.size());

  for (auto &elem : vec) {
    ret.push_back(ToPropertyValue(std::move(elem)));
  }

  return ret;
}

inline std::vector<Value> ConvertValueVector(const std::vector<memgraph::storage::v3::PropertyValue> &vec) {
  std::vector<Value> ret;
  ret.reserve(vec.size());

  for (const auto &elem : vec) {
    ret.push_back(ToValue(elem));
  }

  return ret;
}

}  // namespace memgraph::storage::conversions
