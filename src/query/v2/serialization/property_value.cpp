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

#include "query/v2/serialization/property_value.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::v2::serialization {

namespace {
enum class ObjectType : uint8_t { MAP, TEMPORAL_DATA };
}  // namespace

nlohmann::json SerializePropertyValue(const storage::v3::PropertyValue &property_value) {
  using Type = storage::v3::PropertyValue::Type;
  switch (property_value.type()) {
    case Type::Null:
      return {};
    case Type::Bool:
      return property_value.ValueBool();
    case Type::Int:
      return property_value.ValueInt();
    case Type::Double:
      return property_value.ValueDouble();
    case Type::String:
      return property_value.ValueString();
    case Type::List:
      return SerializePropertyValueVector(property_value.ValueList());
    case Type::Map:
      return SerializePropertyValueMap(property_value.ValueMap());
    case Type::TemporalData:
      const auto temporal_data = property_value.ValueTemporalData();
      auto data = nlohmann::json::object();
      data.emplace("type", static_cast<uint64_t>(ObjectType::TEMPORAL_DATA));
      data.emplace("value", nlohmann::json::object({{"type", static_cast<uint64_t>(temporal_data.type)},
                                                    {"microseconds", temporal_data.microseconds}}));
      return data;
  }
}

nlohmann::json SerializePropertyValueVector(const std::vector<storage::v3::PropertyValue> &values) {
  nlohmann::json array = nlohmann::json::array();
  for (const auto &value : values) {
    array.push_back(SerializePropertyValue(value));
  }
  return array;
}

nlohmann::json SerializePropertyValueMap(const std::map<std::string, storage::v3::PropertyValue> &parameters) {
  nlohmann::json data = nlohmann::json::object();
  data.emplace("type", static_cast<uint64_t>(ObjectType::MAP));
  data.emplace("value", nlohmann::json::object());

  for (const auto &[key, value] : parameters) {
    data["value"][key] = SerializePropertyValue(value);
  }

  return data;
};

storage::v3::PropertyValue DeserializePropertyValue(const nlohmann::json &data) {
  if (data.is_null()) {
    return storage::v3::PropertyValue();
  }

  if (data.is_boolean()) {
    return storage::v3::PropertyValue(data.get<bool>());
  }

  if (data.is_number_integer()) {
    return storage::v3::PropertyValue(data.get<int64_t>());
  }

  if (data.is_number_float()) {
    return storage::v3::PropertyValue(data.get<double>());
  }

  if (data.is_string()) {
    return storage::v3::PropertyValue(data.get<std::string>());
  }

  if (data.is_array()) {
    return storage::v3::PropertyValue(DeserializePropertyValueList(data));
  }

  MG_ASSERT(data.is_object(), "Unknown type found in the trigger storage");

  switch (data["type"].get<ObjectType>()) {
    case ObjectType::MAP:
      return storage::v3::PropertyValue(DeserializePropertyValueMap(data));
    case ObjectType::TEMPORAL_DATA:
      return storage::v3::PropertyValue(storage::v3::TemporalData{
          data["value"]["type"].get<storage::v3::TemporalType>(), data["value"]["microseconds"].get<int64_t>()});
  }
}

std::vector<storage::v3::PropertyValue> DeserializePropertyValueList(const nlohmann::json::array_t &data) {
  std::vector<storage::v3::PropertyValue> property_values;
  property_values.reserve(data.size());
  for (const auto &value : data) {
    property_values.emplace_back(DeserializePropertyValue(value));
  }

  return property_values;
}

std::map<std::string, storage::v3::PropertyValue> DeserializePropertyValueMap(const nlohmann::json::object_t &data) {
  MG_ASSERT(data.at("type").get<ObjectType>() == ObjectType::MAP, "Invalid map serialization");
  std::map<std::string, storage::v3::PropertyValue> property_values;

  const nlohmann::json::object_t &values = data.at("value");
  for (const auto &[key, value] : values) {
    property_values.emplace(key, DeserializePropertyValue(value));
  }

  return property_values;
}

}  // namespace memgraph::query::v2::serialization
