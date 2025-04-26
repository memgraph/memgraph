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

#include "nlohmann/detail/input/parser.hpp"
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

nlohmann::json SerializeIntermediatePropertyValue(const storage::IntermediatePropertyValue &property_value,
                                                  memgraph::storage::Storage::Accessor *storage_acc) {
  using Type = storage::IntermediatePropertyValue::Type;
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
      return SerializeIntermediatePropertyValue(property_value.ValueList(), storage_acc);
    case Type::Map:
      return SerializeIntermediatePropertyValue(property_value.ValueMap(), storage_acc);
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
    case storage::IntermediatePropertyValue::Type::Enum: {
      nlohmann::json data = nlohmann::json::object();
      data.emplace("type", static_cast<uint64_t>(ObjectType::ENUM));
      auto enum_val = property_value.ValueEnum();
      auto enum_str = storage_acc->GetEnumStoreShared().ToString(enum_val);
      MG_ASSERT(enum_str.HasValue(), "Unknown enum");
      data.emplace("value", *std::move(enum_str));
      return data;
    }
    case storage::IntermediatePropertyValue::Type::Point2d: {
      nlohmann::json data = nlohmann::json::object();
      data.emplace("type", static_cast<uint64_t>(ObjectType::POINT_2D));
      auto const &point_2d = property_value.ValuePoint2d();
      data.emplace("srid", storage::CrsToSrid(point_2d.crs()).value_of());
      data.emplace("x", point_2d.x());
      data.emplace("y", point_2d.y());
      return data;
    }
    case storage::IntermediatePropertyValue::Type::Point3d: {
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

nlohmann::json SerializeIntermediatePropertyValueVector(const std::vector<storage::IntermediatePropertyValue> &values,
                                                        memgraph::storage::Storage::Accessor *storage_acc) {
  nlohmann::json array = nlohmann::json::array();
  for (const auto &value : values) {
    array.push_back(SerializeIntermediatePropertyValue(value, storage_acc));
  }
  return array;
}

nlohmann::json SerializeIntermediatePropertyValueMap(storage::IntermediatePropertyValue::map_t const &map,
                                                     memgraph::storage::Storage::Accessor *storage_acc) {
  nlohmann::json data = nlohmann::json::object();
  data.emplace("type", static_cast<uint64_t>(ObjectType::MAP));
  data.emplace("value", nlohmann::json::object());

  for (const auto &[key, value] : map) {
    data["value"][key] = SerializeIntermediatePropertyValue(value, storage_acc);
  }

  return data;
}

storage::IntermediatePropertyValue DeserializeIntermediatePropertyValue(const nlohmann::json &data,
                                                                        storage::Storage::Accessor *storage_acc) {
  if (data.is_null()) {
    return storage::IntermediatePropertyValue();
  }

  if (data.is_boolean()) {
    return storage::IntermediatePropertyValue(data.get<bool>());
  }

  if (data.is_number_integer()) {
    return storage::IntermediatePropertyValue(data.get<int64_t>());
  }

  if (data.is_number_float()) {
    return storage::IntermediatePropertyValue(data.get<double>());
  }

  if (data.is_string()) {
    return storage::IntermediatePropertyValue(data.get<std::string>());
  }

  if (data.is_array()) {
    return storage::IntermediatePropertyValue(DeserializeIntermediatePropertyValue(data, storage_acc));
  }

  MG_ASSERT(data.is_object(), "Unknown type found in the trigger storage");

  switch (data["type"].get<ObjectType>()) {
    case ObjectType::MAP:
      return storage::IntermediatePropertyValue(DeserializeIntermediatePropertyValueMap(data, storage_acc));
    case ObjectType::TEMPORAL_DATA:
      return storage::IntermediatePropertyValue(storage::TemporalData{
          data["value"]["type"].get<storage::TemporalType>(), data["value"]["microseconds"].get<int64_t>()});
    case ObjectType::ZONED_TEMPORAL_DATA:
      return storage::IntermediatePropertyValue(
          storage::ZonedTemporalData{data["value"]["type"].get<storage::ZonedTemporalType>(),
                                     utils::AsSysTime(data["value"]["microseconds"].get<int64_t>()),
                                     utils::Timezone(data["value"]["timezone"].get<std::string>())});
    case ObjectType::OFFSET_ZONED_TEMPORAL_DATA:
      return storage::IntermediatePropertyValue(
          storage::ZonedTemporalData{data["value"]["type"].get<storage::ZonedTemporalType>(),
                                     utils::AsSysTime(data["value"]["microseconds"].get<int64_t>()),
                                     utils::Timezone(std::chrono::minutes{data["value"]["timezone"].get<int64_t>()})});
    case ObjectType::ENUM: {
      auto enum_val = storage_acc->GetEnumValue(data["value"].get<std::string>());
      MG_ASSERT(enum_val.HasValue(), "Unknown enum found in the trigger storage");
      return storage::IntermediatePropertyValue(*enum_val);
    }
    case ObjectType::POINT_2D: {
      auto crs_opt = storage::SridToCrs(storage::Srid{data["srid"].get<uint16_t>()});
      MG_ASSERT(crs_opt.has_value(), "Unknown srid");
      return storage::IntermediatePropertyValue(
          storage::Point2d{*crs_opt, data["x"].get<double>(), data["y"].get<double>()});
    }
    case ObjectType::POINT_3D: {
      auto crs_opt = storage::SridToCrs(storage::Srid{data["srid"].get<uint16_t>()});
      MG_ASSERT(crs_opt.has_value(), "Unknown srid");
      return storage::IntermediatePropertyValue(
          storage::Point3d{*crs_opt, data["x"].get<double>(), data["y"].get<double>(), data["z"].get<double>()});
    }
  }
}

std::vector<storage::IntermediatePropertyValue> DeserializeIntermediatePropertyValueList(
    const nlohmann::json::array_t &data, storage::Storage::Accessor *storage_acc) {
  std::vector<storage::IntermediatePropertyValue> property_values;
  property_values.reserve(data.size());
  for (const auto &value : data) {
    property_values.emplace_back(DeserializeIntermediatePropertyValue(value, storage_acc));
  }

  return property_values;
}

storage::IntermediatePropertyValue::map_t DeserializeIntermediatePropertyValueMap(
    nlohmann::json::object_t const &data, storage::Storage::Accessor *storage_acc) {
  MG_ASSERT(data.at("type").get<ObjectType>() == ObjectType::MAP, "Invalid map serialization");
  const nlohmann::json::object_t &values = data.at("value");

  auto property_values = storage::IntermediatePropertyValue::map_t{};
  property_values.reserve(values.size());
  for (const auto &[key, value] : values) {
    property_values.emplace(key, DeserializeIntermediatePropertyValue(value, storage_acc));
  }

  return property_values;
}

// // Serialization
// nlohmann::json SerialiazeIntermediateIntermediatePropertyValue(const storage::IntermediateIntermediatePropertyValue
// &value) {
//   using Type = typename storage::IntermediateIntermediatePropertyValue::Type;

//   switch(value.type()) {
//     case Type::Null:
//       return nullptr;
//     case Type::Bool:
//       return value.ValueBool();
//     case Type::Int:
//       return value.ValueInt();
//     case Type::Double:
//       return value.ValueDouble();
//     case Type::String:
//       return value.ValueString();
//     case Type::List: {
//       nlohmann::json arr = nlohmann::json::array();
//       for(const auto& elem : value.ValueList()) {
//         arr.push_back(SerialiazeIntermediateIntermediatePropertyValue(elem));
//       }
//       return arr;
//     }
//     case Type::Map: {
//       nlohmann::json obj = nlohmann::json::object();
//       for(const auto& [key, val] : value.ValueMap()) {
//         obj[key] = SerialiazeIntermediateIntermediatePropertyValue(val);
//       }
//       return obj;
//     }
//     case Type::TemporalData: {
//       const auto& td = value.ValueTemporalData();
//       return nlohmann::json{
//         {"type", "TEMPORAL_DATA"},
//         {"temporal_type", td.type},
//         {"microseconds", td.microseconds}
//       };
//     }
//     case Type::ZonedTemporalData: {
//       const auto& ztd = value.ValueZonedTemporalData();
//       return nlohmann::json{
//         {"type", "ZONED_TEMPORAL_DATA"},
//         {"temporal_type", ztd.type},
//         {"microseconds", ztd.microseconds},
//         {"timezone", ztd.timezone}
//       };
//     }
//     case Type::Enum:
//       return nlohmann::json{
//         {"type", "ENUM"},
//         {"value", value.ValueEnum()}
//       };
//     case Type::Point2d: {
//       const auto& p = value.ValuePoint2d();
//       return nlohmann::json{
//         {"type", "POINT2D"},
//         {"x", p.x()},
//         {"y", p.y()},
//         {"srid", p.srid()}
//       };
//     }
//     case Type::Point3d: {
//       const auto& p = value.ValuePoint3d();
//       return nlohmann::json{
//         {"type", "POINT3D"},
//         {"x", p.x()},
//         {"y", p.y()},
//         {"z", p.z()},
//         {"srid", p.srid()}
//       };
//     }
//     default:
//       throw std::runtime_error("Unknown storage::IntermediateIntermediatePropertyValue type");
//   }
// }

// // Deserialization
// template <typename Alloc>
// storage::IntermediateIntermediatePropertyValue DeIntermediateIntermediatePropertyValue(const nlohmann::json& j,
//                                                                       const Alloc& alloc = Alloc{}) {
//   if(j.is_null()) {
//     return storage::IntermediateIntermediatePropertyValue(alloc);
//   }
//   if(j.is_boolean()) {
//     return storage::IntermediateIntermediatePropertyValue(j.get<bool>(), alloc);
//   }
//   if(j.is_number_integer()) {
//     return storage::IntermediateIntermediatePropertyValue(j.get<int64_t>(), alloc);
//   }
//   if(j.is_number_float()) {
//     return storage::IntermediateIntermediatePropertyValue(j.get<double>(), alloc);
//   }
//   if(j.is_string()) {
//     return storage::IntermediateIntermediatePropertyValue(j.get<std::string>(), alloc);
//   }
//   if(j.is_array()) {
//     typename storage::IntermediateIntermediatePropertyValue::list_t list(alloc);
//     for(const auto& elem : j) {
//       list.emplace_back(DeIntermediateIntermediatePropertyValue(elem, alloc));
//     }
//     return storage::IntermediateIntermediatePropertyValue(std::move(list), alloc);
//   }
//   if(j.is_object()) {
//     // Handle special types
//     if(j.contains("type")) {
//       const std::string type = j["type"];

//       if(type == "TEMPORAL_DATA") {
//         return storage::IntermediateIntermediatePropertyValue(
//           TemporalData{
//             j["temporal_type"].get<TemporalType>(),
//             j["microseconds"].get<int64_t>()
//           },
//           alloc
//         );
//       }
//       if(type == "ZONED_TEMPORAL_DATA") {
//         return storage::IntermediateIntermediatePropertyValue(
//           ZonedTemporalData{
//             j["temporal_type"].get<ZonedTemporalType>(),
//             j["microseconds"].get<int64_t>(),
//             j["timezone"].get<std::string>()
//           },
//           alloc
//         );
//       }
//       if(type == "ENUM") {
//         return storage::IntermediateIntermediatePropertyValue(
//           j["value"].get<Enum>(),
//           alloc
//         );
//       }
//       if(type == "POINT2D") {
//         return storage::IntermediateIntermediatePropertyValue(
//           Point2d{
//             j["srid"].get<uint16_t>(),
//             j["x"].get<double>(),
//             j["y"].get<double>()
//           },
//           alloc
//         );
//       }
//       if(type == "POINT3D") {
//         return storage::IntermediateIntermediatePropertyValue(
//           Point3d{
//             j["srid"].get<uint16_t>(),
//             j["x"].get<double>(),
//             j["y"].get<double>(),
//             j["z"].get<double>()
//           },
//           alloc
//         );
//       }
//     }

//     // Handle regular map
//     typename storage::IntermediateIntermediatePropertyValue::map_t map(alloc);
//     for(auto& [key, value] : j.items()) {
//       map.emplace(key, DeIntermediateIntermediatePropertyValue(value, alloc));
//     }
//     return storage::IntermediateIntermediatePropertyValue(std::move(map), alloc);
//   }

//   throw std::runtime_error("Unknown JSON type during storage::IntermediateIntermediatePropertyValue
//   deserialization");
// }

}  // namespace memgraph::query::serialization
