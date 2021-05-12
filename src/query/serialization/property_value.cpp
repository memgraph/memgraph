#include "query/serialization/property_value.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"

namespace query::serialization {

nlohmann::json SerializePropertyValue(const storage::PropertyValue &property_value) {
  using Type = storage::PropertyValue::Type;
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
  }
}

nlohmann::json SerializePropertyValueVector(const std::vector<storage::PropertyValue> &values) {
  nlohmann::json array = nlohmann::json::array();
  for (const auto &value : values) {
    array.push_back(SerializePropertyValue(value));
  }
  return array;
}

nlohmann::json SerializePropertyValueMap(const std::map<std::string, storage::PropertyValue> &parameters) {
  nlohmann::json data = nlohmann::json::object();

  for (const auto &[key, value] : parameters) {
    data[key] = SerializePropertyValue(value);
  }

  return data;
};

storage::PropertyValue DeserializePropertyValue(const nlohmann::json &data) {
  if (data.is_null()) {
    return storage::PropertyValue();
  }

  if (data.is_boolean()) {
    return storage::PropertyValue(static_cast<bool>(data));
  }

  if (data.is_number_integer()) {
    return storage::PropertyValue(static_cast<int64_t>(data));
  }

  if (data.is_number_float()) {
    return storage::PropertyValue(static_cast<double>(data));
  }

  if (data.is_string()) {
    return storage::PropertyValue(std::string{data});
  }

  if (data.is_array()) {
    return storage::PropertyValue(DeserializePropertyValueList(data));
  }

  MG_ASSERT(data.is_object(), "Unknown type found in the trigger storage");
  return storage::PropertyValue(DeserializePropertyValueMap(data));
}

std::vector<storage::PropertyValue> DeserializePropertyValueList(const nlohmann::json::array_t &data) {
  std::vector<storage::PropertyValue> property_values;
  property_values.reserve(data.size());
  for (const auto &value : data) {
    property_values.emplace_back(DeserializePropertyValue(value));
  }

  return property_values;
}

std::map<std::string, storage::PropertyValue> DeserializePropertyValueMap(const nlohmann::json::object_t &data) {
  std::map<std::string, storage::PropertyValue> property_values;

  for (const auto &[key, value] : data) {
    property_values.emplace(key, DeserializePropertyValue(value));
  }

  return property_values;
}

}  // namespace query::serialization
