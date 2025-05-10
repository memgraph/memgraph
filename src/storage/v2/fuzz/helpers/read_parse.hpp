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

#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

namespace fuzz::helper {

template <typename T>
bool ReadPrimitive(const uint8_t *&data, size_t &size, T &out) {
  if (size < sizeof(T)) return false;
  std::memcpy(&out, data, sizeof(T));
  data += sizeof(T);
  size -= sizeof(T);
  return true;
}

bool ReadString(const uint8_t *&data, size_t &size, std::string &out) {
  uint8_t len = 0;
  if (!ReadPrimitive(data, size, len)) return false;
  len = std::min<uint8_t>(len, 40);  // don't make strings too big
  if (size < len) return false;

  out.assign(reinterpret_cast<const char *>(data), len);
  data += len;
  size -= len;
  return true;
}

auto ParsePropertyValue(const uint8_t *&data, size_t &size) -> std::optional<memgraph::storage::PropertyValue> {
  using namespace memgraph::storage;
  using namespace memgraph::utils;
  uint8_t type;
  if (!ReadPrimitive(data, size, type)) return std::nullopt;

  switch (type % 12) {
    case 0: {  // INT
      int64_t val;
      if (!ReadPrimitive(data, size, val)) return std::nullopt;
      return PropertyValue(val);
    }
    case 1: {  // DOUBLE
      double val;
      if (!ReadPrimitive(data, size, val)) return std::nullopt;
      return PropertyValue(val);
    }
    case 2: {  // BOOL
      uint8_t val;
      if (!ReadPrimitive(data, size, val)) return std::nullopt;
      return PropertyValue(static_cast<bool>(val & 1));
    }
    case 3: {  // STRING
      std::string str;
      if (!ReadString(data, size, str)) return std::nullopt;
      return PropertyValue(std::string_view{str});
    }
    case 4: {  // LIST
      uint8_t count = 0;
      if (!ReadPrimitive(data, size, count)) return std::nullopt;
      count = std::min<uint8_t>(count, 10);  // don't make lists too big
      PropertyValue::list_t list;
      for (uint8_t i = 0; i < count; ++i) {
        auto val = ParsePropertyValue(data, size);
        if (!val) return std::nullopt;
        list.emplace_back(std::move(*val));
      }
      return PropertyValue(std::move(list));
    }
    case 5: {  // MAP
      uint8_t count = 0;
      if (!ReadPrimitive(data, size, count)) return std::nullopt;
      count = std::min<uint8_t>(count, 10);  // don't make lists too big
      PropertyValue::map_t map;
      for (uint8_t i = 0; i < count; ++i) {
        std::string key;
        if (!ReadString(data, size, key)) return std::nullopt;

        auto val = ParsePropertyValue(data, size);
        if (!val) return std::nullopt;

        map.emplace(std::move(key), std::move(*val));
      }
      return PropertyValue(std::move(map));
    }
    case 6: {  // ENUM
      EnumTypeId enum_type;
      if (!ReadPrimitive(data, size, enum_type)) return std::nullopt;
      EnumValueId enum_val;
      if (!ReadPrimitive(data, size, enum_val)) return std::nullopt;
      return PropertyValue(Enum{enum_type, enum_val});
    }
    case 7: {  // POINT2D
      double x, y;
      if (!ReadPrimitive(data, size, x) || !ReadPrimitive(data, size, y)) return std::nullopt;
      uint8_t crs_raw;
      if (!ReadPrimitive(data, size, crs_raw)) return std::nullopt;
      CoordinateReferenceSystem crs = std::invoke([&] {
        switch (crs_raw % 2) {
          case 0:
            return CoordinateReferenceSystem::Cartesian_2d;
          case 1:
          default:
            return CoordinateReferenceSystem::WGS84_2d;
        }
      });
      return PropertyValue(Point2d{crs, x, y});
    }
    case 8: {  // POINT3D
      double x, y, z;
      if (!ReadPrimitive(data, size, x) || !ReadPrimitive(data, size, y) || !ReadPrimitive(data, size, z))
        return std::nullopt;
      uint8_t crs_raw;
      if (!ReadPrimitive(data, size, crs_raw)) return std::nullopt;
      CoordinateReferenceSystem crs = std::invoke([&] {
        switch (crs_raw % 2) {
          case 0:
            return CoordinateReferenceSystem::Cartesian_3d;
          case 1:
          default:
            return CoordinateReferenceSystem::WGS84_3d;
        }
      });
      return PropertyValue(Point3d{crs, x, y, z});
    }
    case 9: {  // TEMPORAL DATA
      uint8_t temporal_type_raw;
      if (!ReadPrimitive(data, size, temporal_type_raw)) return std::nullopt;
      auto temporal_type = static_cast<TemporalType>(temporal_type_raw % 4);  // 4 known types

      int64_t micros;
      if (!ReadPrimitive(data, size, micros)) return std::nullopt;

      return PropertyValue(TemporalData(temporal_type, micros));
    }

    case 10: {  // ZONED TEMPORAL DATA
      int64_t micros;
      if (!ReadPrimitive(data, size, micros)) return std::nullopt;

      std::string tz;
      if (!ReadString(data, size, tz)) return std::nullopt;

      auto time = memgraph::utils::AsSysTime(micros);
      auto timezone = Timezone{std::chrono::current_zone()};
      return PropertyValue(ZonedTemporalData(ZonedTemporalType::ZonedDateTime, time, timezone));
    }
    case 11:  // NULL
    default:
      return PropertyValue();  // Null
  }
}

auto ParsePropertyId(const uint8_t *&data, size_t &size) -> std::optional<memgraph::storage::PropertyId> {
  using namespace memgraph::storage;
  PropertyId id;
  if (!ReadPrimitive(data, size, id)) return std::nullopt;
  return id;
}

auto ParsePropertyMap(const uint8_t *data, size_t size)
    -> std::optional<std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue>> {
  using namespace memgraph::storage;
  std::map<PropertyId, PropertyValue> props;

  uint8_t count = 0;
  if (!ReadPrimitive(data, size, count)) return std::nullopt;

  for (uint8_t i = 0; i < count; ++i) {
    auto id = ParsePropertyId(data, size);
    if (!id) break;

    auto value = ParsePropertyValue(data, size);
    if (!value) break;
    props[*id] = *std::move(value);
  }

  return props;
}

}  // namespace fuzz::helper
