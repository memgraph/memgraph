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

#include <chrono>
#include <cstdint>
#include <string>

#include <nlohmann/json.hpp>

#include "query/serialization/property_value.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

namespace memgraph::query::serialization {

namespace {
enum class ObjectType : uint8_t {
  MAP,
  TEMPORAL_DATA,
  ZONED_TEMPORAL_DATA,
  OFFSET_ZONED_TEMPORAL_DATA,
  ENUM,
  POINT_2D,
  POINT_3D,
};
}  // namespace

nlohmann::json SerializeExternalPropertyValue(const storage::ExternalPropertyValue &property_value,
                                              memgraph::storage::Storage::Accessor *storage_acc) {
  using Type = storage::ExternalPropertyValue::Type;
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
      return SerializeExternalPropertyValueVector(property_value.ValueList(), storage_acc);
    case Type::Map:
      return SerializeExternalPropertyValueMap(property_value.ValueMap(), storage_acc);
    case Type::TemporalData: {
      const auto temporal_data = property_value.ValueTemporalData();
      auto data = nlohmann::json::object();
      data.emplace("type", static_cast<uint64_t>(ObjectType::TEMPORAL_DATA));
      data.emplace("value", nlohmann::json::object({{"type", static_cast<uint64_t>(temporal_data.type)},
                                                    {"microseconds", temporal_data.microseconds}}));
      return data;
    }
    case Type::ZonedTemporalData: {
      const auto zoned_temporal_data = property_value.ValueZonedTemporalData();
      auto data = nlohmann::json::object();
      auto properties = nlohmann::json::object({{"type", static_cast<uint64_t>(zoned_temporal_data.type)},
                                                {"microseconds", zoned_temporal_data.IntMicroseconds()}});
      if (zoned_temporal_data.timezone.InTzDatabase()) {
        data.emplace("type", static_cast<uint64_t>(ObjectType::ZONED_TEMPORAL_DATA));
        properties.emplace("timezone", zoned_temporal_data.timezone.TimezoneName());
      } else {
        data.emplace("type", static_cast<uint64_t>(ObjectType::OFFSET_ZONED_TEMPORAL_DATA));
        properties.emplace("timezone", zoned_temporal_data.timezone.DefiningOffset());
      }
      data.emplace("value", properties);
      return data;
    }
    case storage::ExternalPropertyValue::Type::Enum: {
      nlohmann::json data = nlohmann::json::object();
      data.emplace("type", static_cast<uint64_t>(ObjectType::ENUM));
      auto enum_val = property_value.ValueEnum();
      auto enum_str = storage_acc->GetEnumStoreShared().ToString(enum_val);
      MG_ASSERT(enum_str.HasValue(), "Unknown enum");
      data.emplace("value", *std::move(enum_str));
      return data;
    }
    case storage::ExternalPropertyValue::Type::Point2d: {
      nlohmann::json data = nlohmann::json::object();
      data.emplace("type", static_cast<uint64_t>(ObjectType::POINT_2D));
      auto const &point_2d = property_value.ValuePoint2d();
      data.emplace("srid", storage::CrsToSrid(point_2d.crs()).value_of());
      data.emplace("x", point_2d.x());
      data.emplace("y", point_2d.y());
      return data;
    }
    case storage::ExternalPropertyValue::Type::Point3d: {
      nlohmann::json data = nlohmann::json::object();
      data.emplace("type", static_cast<uint64_t>(ObjectType::POINT_3D));
      auto const &point_3d = property_value.ValuePoint3d();
      data.emplace("srid", storage::CrsToSrid(point_3d.crs()).value_of());
      data.emplace("x", point_3d.x());
      data.emplace("y", point_3d.y());
      data.emplace("z", point_3d.z());
      return data;
    }
  }
}

nlohmann::json SerializeExternalPropertyValueVector(const std::vector<storage::ExternalPropertyValue> &values,
                                                    storage::Storage::Accessor *storage_acc) {
  nlohmann::json array = nlohmann::json::array();
  for (const auto &value : values) {
    array.push_back(SerializeExternalPropertyValue(value, storage_acc));
  }
  return array;
}

nlohmann::json SerializeExternalPropertyValueMap(storage::ExternalPropertyValue::map_t const &map,
                                                 storage::Storage::Accessor *storage_acc) {
  nlohmann::json data = nlohmann::json::object();
  data.emplace("type", static_cast<uint64_t>(ObjectType::MAP));
  data.emplace("value", nlohmann::json::object());

  for (const auto &[key, value] : map) {
    data["value"][key] = SerializeExternalPropertyValue(value, storage_acc);
  }

  return data;
}

storage::ExternalPropertyValue DeserializeExternalPropertyValue(const nlohmann::json &data,
                                                                storage::Storage::Accessor *storage_acc) {
  if (data.is_null()) {
    return storage::ExternalPropertyValue();
  }

  if (data.is_boolean()) {
    return storage::ExternalPropertyValue(data.get<bool>());
  }

  if (data.is_number_integer()) {
    return storage::ExternalPropertyValue(data.get<int64_t>());
  }

  if (data.is_number_float()) {
    return storage::ExternalPropertyValue(data.get<double>());
  }

  if (data.is_string()) {
    return storage::ExternalPropertyValue(data.get<std::string>());
  }

  if (data.is_array()) {
    return storage::ExternalPropertyValue(DeserializeExternalPropertyValueList(data, storage_acc));
  }

  MG_ASSERT(data.is_object(), "Unknown type found in the trigger storage");

  switch (data["type"].get<ObjectType>()) {
    case ObjectType::MAP:
      return storage::ExternalPropertyValue(DeserializeExternalPropertyValueMap(data, storage_acc));
    case ObjectType::TEMPORAL_DATA:
      return storage::ExternalPropertyValue(storage::TemporalData{data["value"]["type"].get<storage::TemporalType>(),
                                                                  data["value"]["microseconds"].get<int64_t>()});
    case ObjectType::ZONED_TEMPORAL_DATA:
      return storage::ExternalPropertyValue(
          storage::ZonedTemporalData{data["value"]["type"].get<storage::ZonedTemporalType>(),
                                     utils::AsSysTime(data["value"]["microseconds"].get<int64_t>()),
                                     utils::Timezone(data["value"]["timezone"].get<std::string>())});
    case ObjectType::OFFSET_ZONED_TEMPORAL_DATA:
      return storage::ExternalPropertyValue(
          storage::ZonedTemporalData{data["value"]["type"].get<storage::ZonedTemporalType>(),
                                     utils::AsSysTime(data["value"]["microseconds"].get<int64_t>()),
                                     utils::Timezone(std::chrono::minutes{data["value"]["timezone"].get<int64_t>()})});
    case ObjectType::ENUM: {
      auto enum_val = storage_acc->GetEnumValue(data["value"].get<std::string>());
      MG_ASSERT(enum_val.HasValue(), "Unknown enum found in the trigger storage");
      return storage::ExternalPropertyValue(*enum_val);
    }
    case ObjectType::POINT_2D: {
      auto crs_opt = storage::SridToCrs(storage::Srid{data["srid"].get<uint16_t>()});
      MG_ASSERT(crs_opt.has_value(), "Unknown srid");
      return storage::ExternalPropertyValue(
          storage::Point2d{*crs_opt, data["x"].get<double>(), data["y"].get<double>()});
    }
    case ObjectType::POINT_3D: {
      auto crs_opt = storage::SridToCrs(storage::Srid{data["srid"].get<uint16_t>()});
      MG_ASSERT(crs_opt.has_value(), "Unknown srid");
      return storage::ExternalPropertyValue(
          storage::Point3d{*crs_opt, data["x"].get<double>(), data["y"].get<double>(), data["z"].get<double>()});
    }
  }
}

std::vector<storage::ExternalPropertyValue> DeserializeExternalPropertyValueList(
    const nlohmann::json::array_t &data, storage::Storage::Accessor *storage_acc) {
  std::vector<storage::ExternalPropertyValue> property_values;
  property_values.reserve(data.size());
  for (const auto &value : data) {
    property_values.emplace_back(DeserializeExternalPropertyValue(value, storage_acc));
  }

  return property_values;
}

storage::ExternalPropertyValue::map_t DeserializeExternalPropertyValueMap(nlohmann::json::object_t const &data,
                                                                          storage::Storage::Accessor *storage_acc) {
  MG_ASSERT(data.at("type").get<ObjectType>() == ObjectType::MAP, "Invalid map serialization");
  const nlohmann::json::object_t &values = data.at("value");

  auto property_values = storage::ExternalPropertyValue::map_t{};
  do_reserve(property_values, values.size());
  for (const auto &[key, value] : values) {
    property_values.emplace(key, DeserializeExternalPropertyValue(value, storage_acc));
  }

  return property_values;
}

}  // namespace memgraph::query::serialization
