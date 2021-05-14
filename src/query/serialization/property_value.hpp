#pragma once

#include <json/json.hpp>

#include "storage/v2/property_value.hpp"

namespace query::serialization {

nlohmann::json SerializePropertyValue(const storage::PropertyValue &property_value);

nlohmann::json SerializePropertyValueVector(const std::vector<storage::PropertyValue> &values);

nlohmann::json SerializePropertyValueMap(const std::map<std::string, storage::PropertyValue> &parameters);

storage::PropertyValue DeserializePropertyValue(const nlohmann::json &data);

std::vector<storage::PropertyValue> DeserializePropertyValueList(const nlohmann::json::array_t &data);

std::map<std::string, storage::PropertyValue> DeserializePropertyValueMap(const nlohmann::json::object_t &data);

}  // namespace query::serialization
