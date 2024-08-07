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

#include "storage/v2/replication/slk.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <type_traits>

#include "slk/serialization.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/cast.hpp"
#include "utils/temporal.hpp"

namespace memgraph::slk {

void Save(const storage::Enum &enum_val, slk::Builder *builder) {
  slk::Save(enum_val.type_id().value_of(), builder);
  slk::Save(enum_val.value_id().value_of(), builder);
}

void Load(storage::Enum *enum_val, slk::Reader *reader) {
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  strong::underlying_type_t<storage::EnumTypeId> etype;
  slk::Load(&etype, reader);
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  strong::underlying_type_t<storage::EnumValueId> evalue;
  slk::Load(&evalue, reader);
  *enum_val = storage::Enum{storage::EnumTypeId{etype}, storage::EnumValueId{evalue}};
}

void Save(const storage::Point2d &point2d_val, slk::Builder *builder) {
  slk::Save(point2d_val.crs(), builder);
  slk::Save(point2d_val.x(), builder);
  slk::Save(point2d_val.y(), builder);
}

void Load(storage::Point2d *point2d_val, slk::Reader *reader) {
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  storage::CoordinateReferenceSystem crs;
  slk::Load(&crs, reader);
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  double x;
  slk::Load(&x, reader);
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  double y;
  slk::Load(&y, reader);
  *point2d_val = storage::Point2d{crs, x, y};
}

void Save(const storage::Point3d &point3d_val, slk::Builder *builder) {
  slk::Save(point3d_val.crs(), builder);
  slk::Save(point3d_val.x(), builder);
  slk::Save(point3d_val.y(), builder);
  slk::Save(point3d_val.z(), builder);
}

void Load(storage::Point3d *point3d_val, slk::Reader *reader) {
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  storage::CoordinateReferenceSystem crs;
  slk::Load(&crs, reader);
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  double x;
  slk::Load(&x, reader);
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  double y;
  slk::Load(&y, reader);
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  double z;
  slk::Load(&z, reader);
  *point3d_val = storage::Point3d{crs, x, y, z};
}

void Save(const storage::Gid &gid, slk::Builder *builder) { slk::Save(gid.AsUint(), builder); }

void Load(storage::Gid *gid, slk::Reader *reader) {
  uint64_t value;
  slk::Load(&value, reader);
  *gid = storage::Gid::FromUint(value);
}

void Load(storage::PropertyValue::Type *type, slk::Reader *reader) {
  using PVTypeUnderlyingType = std::underlying_type_t<storage::PropertyValue::Type>;
  PVTypeUnderlyingType value{};
  slk::Load(&value, reader);
  bool valid;
  switch (value) {
    case utils::UnderlyingCast(storage::PropertyValue::Type::Null):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Bool):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Int):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Double):
    case utils::UnderlyingCast(storage::PropertyValue::Type::String):
    case utils::UnderlyingCast(storage::PropertyValue::Type::List):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Map):
    case utils::UnderlyingCast(storage::PropertyValue::Type::TemporalData):
    case utils::UnderlyingCast(storage::PropertyValue::Type::ZonedTemporalData):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Enum):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Point2d):
    case utils::UnderlyingCast(storage::PropertyValue::Type::Point3d):
      valid = true;
      break;
    default:
      valid = false;
      break;
  }
  if (!valid) throw slk::SlkDecodeException("Trying to load unknown storage::PropertyValue!");
  *type = static_cast<storage::PropertyValue::Type>(value);
}

void Save(const storage::PropertyValue &value, slk::Builder *builder) {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      slk::Save(storage::PropertyValue::Type::Null, builder);
      return;
    case storage::PropertyValue::Type::Bool:
      slk::Save(storage::PropertyValue::Type::Bool, builder);
      slk::Save(value.ValueBool(), builder);
      return;
    case storage::PropertyValue::Type::Int:
      slk::Save(storage::PropertyValue::Type::Int, builder);
      slk::Save(value.ValueInt(), builder);
      return;
    case storage::PropertyValue::Type::Double:
      slk::Save(storage::PropertyValue::Type::Double, builder);
      slk::Save(value.ValueDouble(), builder);
      return;
    case storage::PropertyValue::Type::String:
      slk::Save(storage::PropertyValue::Type::String, builder);
      slk::Save(value.ValueString(), builder);
      return;
    case storage::PropertyValue::Type::List: {
      slk::Save(storage::PropertyValue::Type::List, builder);
      const auto &values = value.ValueList();
      size_t size = values.size();
      slk::Save(size, builder);
      for (const auto &v : values) {
        slk::Save(v, builder);
      }
      return;
    }
    case storage::PropertyValue::Type::Map: {
      slk::Save(storage::PropertyValue::Type::Map, builder);
      const auto &map = value.ValueMap();
      size_t size = map.size();
      slk::Save(size, builder);
      for (const auto &kv : map) {
        slk::Save(kv, builder);
      }
      return;
    }
    case storage::PropertyValue::Type::TemporalData: {
      slk::Save(storage::PropertyValue::Type::TemporalData, builder);
      const auto temporal_data = value.ValueTemporalData();
      slk::Save(temporal_data.type, builder);
      slk::Save(temporal_data.microseconds, builder);
      return;
    }
    case storage::PropertyValue::Type::ZonedTemporalData: {
      slk::Save(storage::PropertyValue::Type::ZonedTemporalData, builder);
      const auto zoned_temporal_data = value.ValueZonedTemporalData();
      slk::Save(zoned_temporal_data.type, builder);
      slk::Save(zoned_temporal_data.IntMicroseconds(), builder);
      if (zoned_temporal_data.timezone.InTzDatabase()) {
        slk::Save(storage::PropertyValue::Type::String, builder);
        slk::Save(zoned_temporal_data.timezone.TimezoneName(), builder);
      } else {
        slk::Save(storage::PropertyValue::Type::Int, builder);
        slk::Save(zoned_temporal_data.timezone.DefiningOffset(), builder);
      }
      return;
    }
    case storage::PropertyValue::Type::Enum: {
      slk::Save(storage::PropertyValue::Type::Enum, builder);
      slk::Save(value.ValueEnum(), builder);
      return;
    }
    case storage::PropertyValue::Type::Point2d: {
      slk::Save(storage::PropertyValue::Type::Point2d, builder);
      slk::Save(value.ValuePoint2d(), builder);
      return;
    }
    case storage::PropertyValue::Type::Point3d: {
      slk::Save(storage::PropertyValue::Type::Point3d, builder);
      slk::Save(value.ValuePoint3d(), builder);
      return;
    }
  }
}

void Load(storage::PropertyValue *value, slk::Reader *reader) {
  storage::PropertyValue::Type type{};
  slk::Load(&type, reader);
  switch (type) {
    case storage::PropertyValue::Type::Null:
      *value = storage::PropertyValue();
      return;
    case storage::PropertyValue::Type::Bool: {
      bool v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(v);
      return;
    }
    case storage::PropertyValue::Type::Int: {
      int64_t v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(v);
      return;
    }
    case storage::PropertyValue::Type::Double: {
      double v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(v);
      return;
    }
    case storage::PropertyValue::Type::String: {
      std::string v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(std::move(v));
      return;
    }
    case storage::PropertyValue::Type::List: {
      size_t size;
      slk::Load(&size, reader);
      std::vector<storage::PropertyValue> list(size);
      for (size_t i = 0; i < size; ++i) {
        slk::Load(&list[i], reader);
      }
      *value = storage::PropertyValue(std::move(list));
      return;
    }
    case storage::PropertyValue::Type::Map: {
      size_t size;
      slk::Load(&size, reader);
      auto map = storage::PropertyValue::map_t{};
      map.reserve(size);
      for (size_t i = 0; i < size; ++i) {
        std::pair<std::string, storage::PropertyValue> kv;
        slk::Load(&kv, reader);
        map.insert(kv);
      }
      *value = storage::PropertyValue(std::move(map));
      return;
    }
    case storage::PropertyValue::Type::TemporalData: {
      storage::TemporalType temporal_type{};
      slk::Load(&temporal_type, reader);
      int64_t microseconds{0};
      slk::Load(&microseconds, reader);
      *value = storage::PropertyValue(storage::TemporalData{temporal_type, microseconds});
      return;
    }
    case storage::PropertyValue::Type::ZonedTemporalData: {
      storage::ZonedTemporalType temporal_type{};
      slk::Load(&temporal_type, reader);
      int64_t microseconds{0};
      slk::Load(&microseconds, reader);
      storage::PropertyValue::Type timezone_representation_type{};
      slk::Load(&timezone_representation_type, reader);
      switch (timezone_representation_type) {
        case storage::PropertyValue::Type::String: {
          std::string timezone_name;
          slk::Load(&timezone_name, reader);
          *value = storage::PropertyValue(storage::ZonedTemporalData{temporal_type, utils::AsSysTime(microseconds),
                                                                     utils::Timezone(timezone_name)});
          return;
        }
        case storage::PropertyValue::Type::Int: {
          int64_t offset_minutes{0};
          slk::Load(&offset_minutes, reader);
          *value = storage::PropertyValue(storage::ZonedTemporalData{
              temporal_type, utils::AsSysTime(microseconds), utils::Timezone(std::chrono::minutes{offset_minutes})});
          return;
        }
        default:
          throw slk::SlkDecodeException("Trying to load ZonedTemporalData with invalid timezone representation!");
      }
      return;
    }
    case storage::PropertyValue::Type::Enum: {
      storage::Enum v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(v);
      return;
    }
    case storage::PropertyValue::Type::Point2d: {
      storage::Point2d v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(v);
      return;
    }
    case storage::PropertyValue::Type::Point3d: {
      storage::Point3d v;
      slk::Load(&v, reader);
      *value = storage::PropertyValue(v);
      return;
    }
  }
}

}  // namespace memgraph::slk
