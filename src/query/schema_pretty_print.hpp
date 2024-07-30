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

#pragma once

#include <json/json.hpp>
#include <vector>

#include "query/typed_value.hpp"
#include "storage/v2/schema_info.hpp"

namespace memgraph::query {

using nlohmann::json;

namespace {

json ToJson(const std::unordered_map<SchemaPropertyType, int64_t> &property_types) {
  json obj;
  for (const auto &[key, value] : property_types) {
    json obj;
    obj["key"] = key;
    obj["value"] = value;
    obj.emplace_back(obj);
  }
  return obj;
}

json ToJson(const storage::NodesInfo::LabelsSet &labels) {
  json json;
  for (const auto &label : labels) {
    json.emplace_back(label);
  }
  return json;
}

json ToJson(const std::unordered_map<std::string, storage::PropertyInfo> &properties) {
  json obj;
  for (const auto &[key, propertyInfo] : properties) {
    json propertyInfoJson;
    propertyInfoJson["key"] = key;
    propertyInfoJson["count"] = propertyInfo.number_of_property_occurrences;
    propertyInfoJson["type"] = ToJson(propertyInfo.property_types);
    obj.emplace_back(propertyInfoJson);
  }
  return obj;
}

json ToJson(const storage::NodesInfo &nodes) {
  json obj;
  uint64_t node_id = 0;
  for (const auto &[labels, labelsInfo] : nodes.node_types_properties) {
    json nodeInfo;
    nodeInfo["id"] = node_id++;
    nodeInfo["labels"] = ToJson(labels);
    nodeInfo["count"] = labelsInfo.number_of_label_occurrences;
    nodeInfo["properties"] = ToJson(labelsInfo.properties);
    obj.emplace_back(nodeInfo);
  }
  // json["node_types_properties"] = ToJson(nodes.node_types_properties);
  return obj;
}

}  // namespace

std::vector<std::vector<TypedValue>> SchemaInfoToJson(const storage::SchemaInfo &schemaInfo) {
  json obj;
  obj["nodes"] = ToJson(schemaInfo.nodes);
  return obj;
}

}  // namespace memgraph::query
