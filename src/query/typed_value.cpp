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

#include "query/typed_value.hpp"

#include <fmt/format.h>
#include <chrono>
#include <cmath>
#include <iosfwd>
#include <memory>
#include <nlohmann/json.hpp>
#include <string_view>
#include <utility>

#include "query/fmt.hpp"
#include "query/graph.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/temporal.hpp"

import memgraph.utils.fnv;

namespace memgraph::query {

TypedValue::TypedValue(std::vector<TypedValue> &&other, allocator_type alloc)
    : alloc_{alloc},
      list_v{
          std::make_move_iterator(other.begin()),
          std::make_move_iterator(other.end()),
          alloc_,
      },
      type_(Type::List) {}

TypedValue::TypedValue(TMap &&other) : TypedValue(std::move(other), other.get_allocator()) {}

TypedValue::TypedValue(std::map<std::string, TypedValue> &&other, allocator_type alloc)
    : alloc_{alloc},
      map_v{std::make_move_iterator(other.begin()), std::make_move_iterator(other.end()), alloc_},
      type_(Type::Map) {}

TypedValue::TypedValue(TMap &&other, allocator_type alloc)
    : alloc_{alloc}, map_v{std::move(other), alloc_}, type_(Type::Map) {}

TypedValue::TypedValue(Path &&path) : TypedValue(std::move(path), path.get_allocator()) {}

TypedValue::TypedValue(Path &&path, allocator_type alloc) : alloc_{alloc}, type_(Type::Path) {
  auto *path_ptr = utils::Allocator<Path>(alloc_).new_object<Path>(std::move(path));
  alloc_trait::construct(alloc_, &path_v, path_ptr);
}

TypedValue::TypedValue(Graph &&graph) : TypedValue(std::move(graph), graph.get_allocator()) {}

TypedValue::TypedValue(Graph &&graph, allocator_type alloc) : alloc_{alloc}, type_(Type::Graph) {
  auto *graph_ptr = utils::Allocator<Graph>(alloc_).new_object<Graph>(std::move(graph));
  alloc_trait::construct(alloc_, &graph_v, graph_ptr);
}

TypedValue::TypedValue(const storage::PropertyValue &value, storage::NameIdMapper *name_id_mapper)
    // TODO: MemoryResource in storage::PropertyValue
    : TypedValue(value, name_id_mapper, utils::NewDeleteResource()) {}

TypedValue::TypedValue(const storage::PropertyValue &value, storage::NameIdMapper *name_id_mapper, allocator_type alloc)
    : alloc_{alloc} {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      type_ = Type::Null;
      return;
    case storage::PropertyValue::Type::Bool:
      type_ = Type::Bool;
      bool_v = value.ValueBool();
      return;
    case storage::PropertyValue::Type::Int:
      type_ = Type::Int;
      int_v = value.ValueInt();
      return;
    case storage::PropertyValue::Type::Double:
      type_ = Type::Double;
      double_v = value.ValueDouble();
      return;
    case storage::PropertyValue::Type::String:
      type_ = Type::String;
      alloc_trait::construct(alloc_, &string_v, value.ValueString());
      return;
    case storage::PropertyValue::Type::List: {
      type_ = Type::List;
      const auto &vec = value.ValueList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v, name_id_mapper);
      }
      return;
    }
    case storage::PropertyValue::Type::NumericList: {
      type_ = Type::List;
      const auto &vec = value.ValueNumericList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        if (std::holds_alternative<int>(v)) {
          list_v.emplace_back(std::get<int>(v));
        } else {
          list_v.emplace_back(std::get<double>(v));
        }
      }
      return;
    }
    case storage::PropertyValue::Type::IntList: {
      type_ = Type::List;
      const auto &vec = value.ValueIntList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v);
      }
      return;
    }
    case storage::PropertyValue::Type::DoubleList: {
      type_ = Type::List;
      const auto &vec = value.ValueDoubleList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v);
      }
      return;
    }
    case storage::PropertyValue::Type::Map: {
      if (!name_id_mapper) {
        throw std::runtime_error("NameIdMapper is required for TypedValue::Map");
      }
      type_ = Type::Map;
      const auto &map = value.ValueMap();
      alloc_trait::construct(alloc, &map_v);
      for (const auto &kv : map) {
        auto key = name_id_mapper->IdToName(kv.first.AsUint());
        map_v.emplace(TString(key, alloc), TypedValue(kv.second, name_id_mapper, alloc));
      }
      return;
    }
    case storage::PropertyValue::Type::TemporalData: {
      const auto &temporal_data = value.ValueTemporalData();
      switch (temporal_data.type) {
        case storage::TemporalType::Date: {
          type_ = Type::Date;
          alloc_trait::construct(alloc_, &date_v, std::chrono::microseconds{temporal_data.microseconds});
          break;
        }
        case storage::TemporalType::LocalTime: {
          type_ = Type::LocalTime;
          alloc_trait::construct(alloc_, &local_time_v, temporal_data.microseconds);
          break;
        }
        case storage::TemporalType::LocalDateTime: {
          type_ = Type::LocalDateTime;
          alloc_trait::construct(alloc_, &local_date_time_v, temporal_data.microseconds);
          break;
        }
        case storage::TemporalType::Duration: {
          type_ = Type::Duration;
          alloc_trait::construct(alloc_, &duration_v, temporal_data.microseconds);
          break;
        }
      }
      return;
    }
    case storage::PropertyValue::Type::ZonedTemporalData: {
      const auto &zoned_temporal_data = value.ValueZonedTemporalData();
      switch (zoned_temporal_data.type) {
        case storage::ZonedTemporalType::ZonedDateTime: {
          type_ = Type::ZonedDateTime;
          alloc_trait::construct(
              alloc_, &zoned_date_time_v, zoned_temporal_data.microseconds, zoned_temporal_data.timezone);
          break;
        }
      }
      return;
    }
    case storage::PropertyValue::Type::Enum: {
      type_ = Type::Enum;
      alloc_trait::construct(alloc_, &enum_v, value.ValueEnum());
      return;
    }
    case storage::PropertyValue::Type::Point2d: {
      type_ = Type::Point2d;
      alloc_trait::construct(alloc_, &point_2d_v, value.ValuePoint2d());
      return;
    }
    case storage::PropertyValue::Type::Point3d: {
      type_ = Type::Point3d;
      alloc_trait::construct(alloc_, &point_3d_v, value.ValuePoint3d());
      return;
    }
    case storage::PropertyValue::Type::VectorIndexId: {
      const auto &vec = value.ValueVectorIndexList();
      type_ = Type::List;
      alloc_trait::construct(alloc_, &list_v, vec.cbegin(), vec.cend());
      return;
    }
  }
  LOG_FATAL("Unsupported type");
}

TypedValue::TypedValue(storage::PropertyValue &&other, storage::NameIdMapper *name_id_mapper) /* noexcept */
    // TODO: MemoryResource in storage::PropertyValue, so this can be noexcept
    : TypedValue(std::move(other), name_id_mapper, utils::NewDeleteResource()) {}

TypedValue::TypedValue(storage::PropertyValue &&other, storage::NameIdMapper *name_id_mapper, allocator_type alloc)
    : alloc_{alloc} {
  switch (other.type()) {
    case storage::PropertyValue::Type::Null:
      type_ = Type::Null;
      break;
    case storage::PropertyValue::Type::Bool:
      type_ = Type::Bool;
      bool_v = other.ValueBool();
      break;
    case storage::PropertyValue::Type::Int:
      type_ = Type::Int;
      int_v = other.ValueInt();
      break;
    case storage::PropertyValue::Type::Double:
      type_ = Type::Double;
      double_v = other.ValueDouble();
      break;
    case storage::PropertyValue::Type::String:
      type_ = Type::String;
      // PropertyValue uses std::allocator, hence copy here
      alloc_trait::construct(alloc_, &string_v, other.ValueString());
      break;
    case storage::PropertyValue::Type::List: {
      type_ = Type::List;
      auto &vec = other.ValueList();
      // PropertyValue uses std::allocator, hence copy here
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v, name_id_mapper);
      }
      break;
    }
    case storage::PropertyValue::Type::NumericList: {
      type_ = Type::List;
      auto &vec = other.ValueNumericList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        if (std::holds_alternative<int>(v)) {
          list_v.emplace_back(std::get<int>(v));
        } else {
          list_v.emplace_back(std::get<double>(v));
        }
      }
      break;
    }
    case storage::PropertyValue::Type::IntList: {
      type_ = Type::List;
      auto &vec = other.ValueIntList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v);
      }
      break;
    }
    case storage::PropertyValue::Type::DoubleList: {
      type_ = Type::List;
      auto &vec = other.ValueDoubleList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v);
      }
      break;
    }
    case storage::PropertyValue::Type::Map: {
      if (!name_id_mapper) {
        throw TypedValueException("NameIdMapper is required for TypedValue::Map");
      }
      type_ = Type::Map;
      auto &map = other.ValueMap();
      alloc_trait::construct(alloc_, &map_v);
      // PropertyValue uses std::allocator, hence copy here
      for (const auto &kv : map) {
        auto key = name_id_mapper->IdToName(kv.first.AsUint());
        map_v.emplace(TString(key, alloc), TypedValue(kv.second, name_id_mapper, alloc));
      }
      break;
    }
    case storage::PropertyValue::Type::TemporalData: {
      const auto &temporal_data = other.ValueTemporalData();
      switch (temporal_data.type) {
        case storage::TemporalType::Date: {
          type_ = Type::Date;
          alloc_trait::construct(alloc_, &date_v, std::chrono::microseconds{temporal_data.microseconds});
          break;
        }
        case storage::TemporalType::LocalTime: {
          type_ = Type::LocalTime;
          alloc_trait::construct(alloc_, &local_time_v, temporal_data.microseconds);
          break;
        }
        case storage::TemporalType::LocalDateTime: {
          type_ = Type::LocalDateTime;
          alloc_trait::construct(alloc_, &local_date_time_v, temporal_data.microseconds);
          break;
        }
        case storage::TemporalType::Duration: {
          type_ = Type::Duration;
          alloc_trait::construct(alloc_, &duration_v, temporal_data.microseconds);
          break;
        }
      }
      break;
    }
    case storage::PropertyValue::Type::ZonedTemporalData: {
      const auto &zoned_temporal_data = other.ValueZonedTemporalData();
      switch (zoned_temporal_data.type) {
        case storage::ZonedTemporalType::ZonedDateTime: {
          type_ = Type::ZonedDateTime;
          alloc_trait::construct(
              alloc_, &zoned_date_time_v, zoned_temporal_data.microseconds, zoned_temporal_data.timezone);
          break;
        }
      }
      break;
    }
    case storage::PropertyValue::Type::Enum: {
      type_ = Type::Enum;
      alloc_trait::construct(alloc_, &enum_v, other.ValueEnum());
      break;
    }
    case storage::PropertyValue::Type::Point2d: {
      type_ = Type::Point2d;
      alloc_trait::construct(alloc_, &point_2d_v, other.ValuePoint2d());
      break;
    }
    case storage::PropertyValue::Type::Point3d: {
      type_ = Type::Point3d;
      alloc_trait::construct(alloc_, &point_3d_v, other.ValuePoint3d());
      break;
    }
    case storage::PropertyValue::Type::VectorIndexId: {
      const auto &vec = other.ValueVectorIndexList();
      type_ = Type::List;
      alloc_trait::construct(alloc_, &list_v, vec.cbegin(), vec.cend());
      break;
    }
  }

  other = storage::PropertyValue();
}

TypedValue::TypedValue(const storage::ExternalPropertyValue &value)
    // TODO: MemoryResource in storage::ExternalPropertyValue
    : TypedValue(value, utils::NewDeleteResource()) {}

TypedValue::TypedValue(const storage::ExternalPropertyValue &value, allocator_type alloc) : alloc_{alloc} {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      type_ = Type::Null;
      return;
    case storage::PropertyValue::Type::Bool:
      type_ = Type::Bool;
      bool_v = value.ValueBool();
      return;
    case storage::PropertyValue::Type::Int:
      type_ = Type::Int;
      int_v = value.ValueInt();
      return;
    case storage::PropertyValue::Type::Double:
      type_ = Type::Double;
      double_v = value.ValueDouble();
      return;
    case storage::PropertyValue::Type::String:
      type_ = Type::String;
      alloc_trait::construct(alloc_, &string_v, value.ValueString());
      return;
    case storage::PropertyValue::Type::List: {
      type_ = Type::List;
      const auto &vec = value.ValueList();
      alloc_trait::construct(alloc_, &list_v, vec.cbegin(), vec.cend());
      return;
    }
    case storage::PropertyValue::Type::NumericList: {
      type_ = Type::List;
      const auto &vec = value.ValueNumericList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        if (std::holds_alternative<int>(v)) {
          list_v.emplace_back(std::get<int>(v));
        } else {
          list_v.emplace_back(std::get<double>(v));
        }
      }
      return;
    }
    case storage::PropertyValue::Type::IntList: {
      type_ = Type::List;
      const auto &vec = value.ValueIntList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v);
      }
      return;
    }
    case storage::PropertyValue::Type::DoubleList: {
      type_ = Type::List;
      const auto &vec = value.ValueDoubleList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v);
      }
      return;
    }
    case storage::PropertyValue::Type::Map: {
      type_ = Type::Map;
      const auto &map = value.ValueMap();
      alloc_trait::construct(alloc_, &map_v, map.cbegin(), map.cend());
      return;
    }
    case storage::PropertyValue::Type::TemporalData: {
      const auto &temporal_data = value.ValueTemporalData();
      switch (temporal_data.type) {
        case storage::TemporalType::Date: {
          type_ = Type::Date;
          alloc_trait::construct(alloc_, &date_v, std::chrono::microseconds{temporal_data.microseconds});
          break;
        }
        case storage::TemporalType::LocalTime: {
          type_ = Type::LocalTime;
          alloc_trait::construct(alloc_, &local_time_v, temporal_data.microseconds);
          break;
        }
        case storage::TemporalType::LocalDateTime: {
          type_ = Type::LocalDateTime;
          alloc_trait::construct(alloc_, &local_date_time_v, temporal_data.microseconds);
          break;
        }
        case storage::TemporalType::Duration: {
          type_ = Type::Duration;
          alloc_trait::construct(alloc_, &duration_v, temporal_data.microseconds);
          break;
        }
      }
      return;
    }
    case storage::PropertyValue::Type::ZonedTemporalData: {
      const auto &zoned_temporal_data = value.ValueZonedTemporalData();
      switch (zoned_temporal_data.type) {
        case storage::ZonedTemporalType::ZonedDateTime: {
          type_ = Type::ZonedDateTime;
          alloc_trait::construct(
              alloc_, &zoned_date_time_v, zoned_temporal_data.microseconds, zoned_temporal_data.timezone);
          break;
        }
      }
      return;
    }
    case storage::PropertyValue::Type::Enum: {
      type_ = Type::Enum;
      alloc_trait::construct(alloc_, &enum_v, value.ValueEnum());
      return;
    }
    case storage::PropertyValue::Type::Point2d: {
      type_ = Type::Point2d;
      alloc_trait::construct(alloc_, &point_2d_v, value.ValuePoint2d());
      return;
    }
    case storage::PropertyValue::Type::Point3d: {
      type_ = Type::Point3d;
      alloc_trait::construct(alloc_, &point_3d_v, value.ValuePoint3d());
      return;
    }
    case storage::PropertyValue::Type::VectorIndexId: {
      const auto &vec = value.ValueVectorIndexList();
      type_ = Type::List;
      alloc_trait::construct(alloc_, &list_v, vec.cbegin(), vec.cend());
      return;
    }
  }
  LOG_FATAL("Unsupported type");
}

TypedValue::TypedValue(storage::ExternalPropertyValue &&other) /* noexcept */
    // TODO: MemoryResource in storage::ExternalPropertyValue, so this can be noexcept
    : TypedValue(std::move(other), utils::NewDeleteResource()) {}

TypedValue::TypedValue(storage::ExternalPropertyValue &&other, allocator_type alloc) : alloc_{alloc} {
  switch (other.type()) {
    case storage::PropertyValue::Type::Null:
      type_ = Type::Null;
      break;
    case storage::PropertyValue::Type::Bool:
      type_ = Type::Bool;
      bool_v = other.ValueBool();
      break;
    case storage::PropertyValue::Type::Int:
      type_ = Type::Int;
      int_v = other.ValueInt();
      break;
    case storage::PropertyValue::Type::Double:
      type_ = Type::Double;
      double_v = other.ValueDouble();
      break;
    case storage::PropertyValue::Type::String:
      type_ = Type::String;
      // PropertyValue uses std::allocator, hence copy here
      alloc_trait::construct(alloc_, &string_v, other.ValueString());
      break;
    case storage::PropertyValue::Type::List: {
      type_ = Type::List;
      auto &vec = other.ValueList();
      // PropertyValue uses std::allocator, hence copy here
      alloc_trait::construct(alloc_, &list_v, vec.cbegin(), vec.cend());
      break;
    }
    case storage::PropertyValue::Type::NumericList: {
      type_ = Type::List;
      auto &vec = other.ValueNumericList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        if (std::holds_alternative<int>(v)) {
          list_v.emplace_back(std::get<int>(v));
        } else {
          list_v.emplace_back(std::get<double>(v));
        }
      }
      break;
    }
    case storage::PropertyValue::Type::IntList: {
      type_ = Type::List;
      auto &vec = other.ValueIntList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v);
      }
      break;
    }
    case storage::PropertyValue::Type::DoubleList: {
      type_ = Type::List;
      auto &vec = other.ValueDoubleList();
      alloc_trait::construct(alloc_, &list_v);
      for (const auto &v : vec) {
        list_v.emplace_back(v);
      }
      break;
    }
    case storage::PropertyValue::Type::Map: {
      type_ = Type::Map;
      auto &map = other.ValueMap();
      // PropertyValue uses std::allocator, hence copy here
      alloc_trait::construct(alloc_, &map_v, map.cbegin(), map.cend());
      break;
    }
    case storage::PropertyValue::Type::TemporalData: {
      const auto &temporal_data = other.ValueTemporalData();
      switch (temporal_data.type) {
        case storage::TemporalType::Date: {
          type_ = Type::Date;
          alloc_trait::construct(alloc_, &date_v, std::chrono::microseconds{temporal_data.microseconds});
          break;
        }
        case storage::TemporalType::LocalTime: {
          type_ = Type::LocalTime;
          alloc_trait::construct(alloc_, &local_time_v, temporal_data.microseconds);
          break;
        }
        case storage::TemporalType::LocalDateTime: {
          type_ = Type::LocalDateTime;
          alloc_trait::construct(alloc_, &local_date_time_v, temporal_data.microseconds);
          break;
        }
        case storage::TemporalType::Duration: {
          type_ = Type::Duration;
          alloc_trait::construct(alloc_, &duration_v, temporal_data.microseconds);
          break;
        }
      }
      break;
    }
    case storage::PropertyValue::Type::ZonedTemporalData: {
      const auto &zoned_temporal_data = other.ValueZonedTemporalData();
      switch (zoned_temporal_data.type) {
        case storage::ZonedTemporalType::ZonedDateTime: {
          type_ = Type::ZonedDateTime;
          alloc_trait::construct(
              alloc_, &zoned_date_time_v, zoned_temporal_data.microseconds, zoned_temporal_data.timezone);
          break;
        }
      }
      break;
    }
    case storage::PropertyValue::Type::Enum: {
      type_ = Type::Enum;
      alloc_trait::construct(alloc_, &enum_v, other.ValueEnum());
      break;
    }
    case storage::PropertyValue::Type::Point2d: {
      type_ = Type::Point2d;
      alloc_trait::construct(alloc_, &point_2d_v, other.ValuePoint2d());
      break;
    }
    case storage::PropertyValue::Type::Point3d: {
      type_ = Type::Point3d;
      alloc_trait::construct(alloc_, &point_3d_v, other.ValuePoint3d());
      break;
    }
    case storage::PropertyValue::Type::VectorIndexId: {
      const auto &vec = other.ValueVectorIndexList();
      type_ = Type::List;
      alloc_trait::construct(alloc_, &list_v, vec.cbegin(), vec.cend());
      break;
    }
  }

  other = storage::ExternalPropertyValue();
}

TypedValue::operator storage::ExternalPropertyValue() const {
  switch (type_) {
    case TypedValue::Type::Null:
      return storage::ExternalPropertyValue();
    case TypedValue::Type::Bool:
      return storage::ExternalPropertyValue(bool_v);
    case TypedValue::Type::Int:
      return storage::ExternalPropertyValue(int_v);
    case TypedValue::Type::Double:
      return storage::ExternalPropertyValue(double_v);
    case TypedValue::Type::String:
      return storage::ExternalPropertyValue(std::string(string_v));
    case TypedValue::Type::List:
      return storage::ExternalPropertyValue(std::vector<storage::ExternalPropertyValue>(list_v.begin(), list_v.end()));
    case TypedValue::Type::Map: {
      storage::ExternalPropertyValue::map_t map;
      for (const auto &kv : map_v) map.emplace(kv.first, kv.second);
      return storage::ExternalPropertyValue(std::move(map));
    }
    case Type::Date:
      return storage::ExternalPropertyValue(
          storage::TemporalData{storage::TemporalType::Date, date_v.MicrosecondsSinceEpoch()});
    case Type::LocalTime:
      return storage::ExternalPropertyValue(
          storage::TemporalData{storage::TemporalType::LocalTime, local_time_v.MicrosecondsSinceEpoch()});
    case Type::LocalDateTime:
      // Use generic system time (UTC)
      return storage::ExternalPropertyValue(
          storage::TemporalData{storage::TemporalType::LocalDateTime, local_date_time_v.SysMicrosecondsSinceEpoch()});
    case Type::ZonedDateTime:
      return storage::ExternalPropertyValue(storage::ZonedTemporalData{storage::ZonedTemporalType::ZonedDateTime,
                                                                       zoned_date_time_v.SysTimeSinceEpoch(),
                                                                       zoned_date_time_v.GetTimezone()});
    case Type::Duration:
      return storage::ExternalPropertyValue(
          storage::TemporalData{storage::TemporalType::Duration, duration_v.microseconds});
    case TypedValue::Type::Enum:
      return storage::ExternalPropertyValue(enum_v);
    case TypedValue::Type::Point2d:
      return storage::ExternalPropertyValue(point_2d_v);
    case TypedValue::Type::Point3d:
      return storage::ExternalPropertyValue(point_3d_v);
    case Type::Vertex:
    case Type::Edge:
    case Type::Path:
    case Type::Graph:
    case Type::Function:
      throw TypedValueException("Unsupported conversion from TypedValue to PropertyValue");
  }
}

TypedValue::TypedValue(const TypedValue &other, allocator_type alloc) : alloc_{alloc}, type_(other.type_) {
  switch (other.type_) {
    case TypedValue::Type::Null:
      return;
    case TypedValue::Type::Bool:
      this->bool_v = other.bool_v;
      return;
    case Type::Int:
      this->int_v = other.int_v;
      return;
    case Type::Double:
      this->double_v = other.double_v;
      return;
    case TypedValue::Type::String:
      alloc_trait::construct(alloc_, &string_v, other.string_v);
      return;
    case Type::List:
      alloc_trait::construct(alloc_, &list_v, other.list_v);
      return;
    case Type::Map: {
      alloc_trait::construct(alloc_, &map_v, other.map_v);
      return;
    }
    case Type::Vertex:
      alloc_trait::construct(alloc_, &vertex_v, other.vertex_v);
      return;
    case Type::Edge:
      alloc_trait::construct(alloc_, &edge_v, other.edge_v);
      return;
    case Type::Path: {
      auto *path_ptr = utils::Allocator<Path>(alloc_).new_object<Path>(*other.path_v);
      alloc_trait::construct(alloc_, &path_v, path_ptr);
      return;
    }
    case Type::Date:
      alloc_trait::construct(alloc_, &date_v, other.date_v);
      return;
    case Type::LocalTime:
      alloc_trait::construct(alloc_, &local_time_v, other.local_time_v);
      return;
    case Type::LocalDateTime:
      alloc_trait::construct(alloc_, &local_date_time_v, other.local_date_time_v);
      return;
    case Type::ZonedDateTime:
      alloc_trait::construct(alloc_, &zoned_date_time_v, other.zoned_date_time_v);
      return;
    case Type::Duration:
      alloc_trait::construct(alloc_, &duration_v, other.duration_v);
      return;
    case Type::Enum:
      alloc_trait::construct(alloc_, &enum_v, other.enum_v);
      return;
    case Type::Point2d:
      alloc_trait::construct(alloc_, &point_2d_v, other.point_2d_v);
      return;
    case Type::Point3d:
      alloc_trait::construct(alloc_, &point_3d_v, other.point_3d_v);
      return;
    case Type::Function:
      alloc_trait::construct(alloc_, &function_v, other.function_v);
      return;
    case Type::Graph:
      auto *graph_ptr = utils::Allocator<Graph>(alloc_).new_object<Graph>(*other.graph_v);
      alloc_trait::construct(alloc_, &graph_v, graph_ptr);
      return;
  }
  LOG_FATAL("Unsupported TypedValue::Type");
}

TypedValue::TypedValue(TypedValue &&other) noexcept : TypedValue(std::move(other), other.alloc_) {}

TypedValue::TypedValue(TypedValue &&other, allocator_type alloc) : alloc_{alloc}, type_(other.type_) {
  switch (other.type_) {
    case TypedValue::Type::Null:
      break;
    case TypedValue::Type::Bool:
      alloc_trait::construct(alloc_, &bool_v, other.bool_v);
      break;
    case Type::Int:
      alloc_trait::construct(alloc_, &int_v, other.int_v);
      break;
    case Type::Double:
      alloc_trait::construct(alloc_, &double_v, other.double_v);
      break;
    case TypedValue::Type::String:
      alloc_trait::construct(alloc_, &string_v, std::move(other.string_v));
      break;
    case Type::List:
      alloc_trait::construct(alloc_, &list_v, std::move(other.list_v));
      break;
    case Type::Map: {
      alloc_trait::construct(alloc_, &map_v, std::move(other.map_v));
      break;
    }
    case Type::Vertex:
      alloc_trait::construct(alloc_, &vertex_v, other.vertex_v);
      break;
    case Type::Edge:
      alloc_trait::construct(alloc_, &edge_v, other.edge_v);
      break;
    case Type::Path: {
      if (other.alloc_ == alloc_) {
        alloc_trait::construct(alloc_, &path_v, std::move(other.path_v));
      } else {
        auto *path_ptr = utils::Allocator<Path>(alloc_).new_object<Path>(std::move(*other.path_v));
        alloc_trait::construct(alloc_, &path_v, path_ptr);
      }
      break;
    }
    case Type::Date:
      alloc_trait::construct(alloc_, &date_v, other.date_v);
      break;
    case Type::LocalTime:
      alloc_trait::construct(alloc_, &local_time_v, other.local_time_v);
      break;
    case Type::LocalDateTime:
      alloc_trait::construct(alloc_, &local_date_time_v, other.local_date_time_v);
      break;
    case Type::ZonedDateTime:
      alloc_trait::construct(alloc_, &zoned_date_time_v, other.zoned_date_time_v);
      break;
    case Type::Duration:
      alloc_trait::construct(alloc_, &duration_v, other.duration_v);
      break;
    case Type::Enum:
      alloc_trait::construct(alloc_, &enum_v, other.enum_v);
      break;
    case Type::Point2d:
      alloc_trait::construct(alloc_, &point_2d_v, other.point_2d_v);
      break;
    case Type::Point3d:
      alloc_trait::construct(alloc_, &point_3d_v, other.point_3d_v);
      break;
    case Type::Function:
      alloc_trait::construct(alloc_, &function_v, std::move(other.function_v));
      break;
    case Type::Graph:
      if (other.alloc_ == alloc_) {
        alloc_trait::construct(alloc_, &graph_v, std::move(other.graph_v));
      } else {
        auto *graph_ptr = utils::Allocator<Graph>(alloc_).new_object<Graph>(std::move(*other.graph_v));
        alloc_trait::construct(alloc_, &graph_v, graph_ptr);
      }
  }
}

storage::PropertyValue TypedValue::ToPropertyValue(storage::NameIdMapper *name_id_mapper) const {
  switch (type_) {
    case TypedValue::Type::Null:
      return storage::PropertyValue();
    case TypedValue::Type::Bool:
      return storage::PropertyValue(bool_v);
    case TypedValue::Type::Int:
      return storage::PropertyValue(int_v);
    case TypedValue::Type::Double:
      return storage::PropertyValue(double_v);
    case TypedValue::Type::String:
      return storage::PropertyValue(std::string(string_v));
    case TypedValue::Type::List: {
      storage::PropertyValue::list_t list;
      bool all_ints = true;
      bool all_doubles = true;
      bool all_numeric = true;
      list.reserve(list_v.size());
      for (const auto &v : list_v) {
        all_ints &= v.IsInt();
        all_doubles &= v.IsDouble();
        all_numeric &= v.IsInt() || v.IsDouble();
        list.emplace_back(v.ToPropertyValue(name_id_mapper));
      }
      if (all_ints) {
        return storage::PropertyValue(storage::IntListTag{}, std::move(list));
      }
      if (all_doubles) {
        return storage::PropertyValue(storage::DoubleListTag{}, std::move(list));
      }
      if (all_numeric) {
        return storage::PropertyValue(storage::NumericListTag{}, std::move(list));
      }
      return storage::PropertyValue(std::move(list));
    }
    case TypedValue::Type::Map: {
      if (!name_id_mapper) {
        throw TypedValueException("NameIdMapper is required for TypedValue::Map");
      }
      storage::PropertyValue::map_t map;
      for (const auto &kv : map_v) {
        map.emplace(storage::PropertyId::FromUint(name_id_mapper->NameToId(kv.first)),
                    kv.second.ToPropertyValue(name_id_mapper));
      }
      return storage::PropertyValue(std::move(map));
    }
    case Type::Date:
      return storage::PropertyValue(
          storage::TemporalData{storage::TemporalType::Date, date_v.MicrosecondsSinceEpoch()});
    case Type::LocalTime:
      return storage::PropertyValue(
          storage::TemporalData{storage::TemporalType::LocalTime, local_time_v.MicrosecondsSinceEpoch()});
    case Type::LocalDateTime:
      // Use generic system time (UTC)
      return storage::PropertyValue(
          storage::TemporalData{storage::TemporalType::LocalDateTime, local_date_time_v.SysMicrosecondsSinceEpoch()});
    case Type::ZonedDateTime:
      return storage::PropertyValue(storage::ZonedTemporalData{storage::ZonedTemporalType::ZonedDateTime,
                                                               zoned_date_time_v.SysTimeSinceEpoch(),
                                                               zoned_date_time_v.GetTimezone()});
    case Type::Duration:
      return storage::PropertyValue(storage::TemporalData{storage::TemporalType::Duration, duration_v.microseconds});
    case TypedValue::Type::Enum:
      return storage::PropertyValue(enum_v);
    case TypedValue::Type::Point2d:
      return storage::PropertyValue(point_2d_v);
    case TypedValue::Type::Point3d:
      return storage::PropertyValue(point_3d_v);
    case Type::Vertex:
    case Type::Edge:
    case Type::Path:
    case Type::Graph:
    case Type::Function:
      throw TypedValueException("Unsupported conversion from TypedValue to PropertyValue");
  }
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define DEFINE_VALUE_AND_TYPE_GETTERS_PRIMITIVE(type_param, type_enum, field)                    \
  type_param &TypedValue::Value##type_enum() {                                                   \
    if (type_ != Type::type_enum) [[unlikely]]                                                   \
      throw TypedValueException("TypedValue is of type '{}', not '{}'", type_, Type::type_enum); \
    return field;                                                                                \
  }                                                                                              \
  type_param TypedValue::Value##type_enum() const {                                              \
    if (type_ != Type::type_enum) [[unlikely]]                                                   \
      throw TypedValueException("TypedValue is of type '{}', not '{}'", type_, Type::type_enum); \
    return field;                                                                                \
  }                                                                                              \
  bool TypedValue::Is##type_enum() const { return type_ == Type::type_enum; }

#define DEFINE_VALUE_AND_TYPE_GETTERS(type_param, type_enum, field)                              \
  type_param &TypedValue::Value##type_enum() {                                                   \
    if (type_ != Type::type_enum) [[unlikely]]                                                   \
      throw TypedValueException("TypedValue is of type '{}', not '{}'", type_, Type::type_enum); \
    return field;                                                                                \
  }                                                                                              \
  const type_param &TypedValue::Value##type_enum() const {                                       \
    if (type_ != Type::type_enum) [[unlikely]]                                                   \
      throw TypedValueException("TypedValue is of type '{}', not '{}'", type_, Type::type_enum); \
    return field;                                                                                \
  }                                                                                              \
  bool TypedValue::Is##type_enum() const { return type_ == Type::type_enum; }

DEFINE_VALUE_AND_TYPE_GETTERS_PRIMITIVE(bool, Bool, bool_v)
DEFINE_VALUE_AND_TYPE_GETTERS_PRIMITIVE(int64_t, Int, int_v)
DEFINE_VALUE_AND_TYPE_GETTERS_PRIMITIVE(double, Double, double_v)
DEFINE_VALUE_AND_TYPE_GETTERS(TypedValue::TString, String, string_v)
DEFINE_VALUE_AND_TYPE_GETTERS(TypedValue::TVector, List, list_v)
DEFINE_VALUE_AND_TYPE_GETTERS(TypedValue::TMap, Map, map_v)
DEFINE_VALUE_AND_TYPE_GETTERS(VertexAccessor, Vertex, vertex_v)
DEFINE_VALUE_AND_TYPE_GETTERS(EdgeAccessor, Edge, edge_v)
DEFINE_VALUE_AND_TYPE_GETTERS(Path, Path, *path_v)
DEFINE_VALUE_AND_TYPE_GETTERS(utils::Date, Date, date_v)
DEFINE_VALUE_AND_TYPE_GETTERS(utils::LocalTime, LocalTime, local_time_v)
DEFINE_VALUE_AND_TYPE_GETTERS(utils::LocalDateTime, LocalDateTime, local_date_time_v)
DEFINE_VALUE_AND_TYPE_GETTERS(utils::ZonedDateTime, ZonedDateTime, zoned_date_time_v)
DEFINE_VALUE_AND_TYPE_GETTERS(utils::Duration, Duration, duration_v)
DEFINE_VALUE_AND_TYPE_GETTERS(storage::Enum, Enum, enum_v)
DEFINE_VALUE_AND_TYPE_GETTERS(storage::Point2d, Point2d, point_2d_v)
DEFINE_VALUE_AND_TYPE_GETTERS(storage::Point3d, Point3d, point_3d_v)
DEFINE_VALUE_AND_TYPE_GETTERS(std::function<void(TypedValue *)>, Function, function_v)
DEFINE_VALUE_AND_TYPE_GETTERS(Graph, Graph, *graph_v)

#undef DEFINE_VALUE_AND_TYPE_GETTERS
#undef DEFINE_VALUE_AND_TYPE_GETTERS_PRIMITIVE

bool TypedValue::ContainsDeleted() const {
  switch (type_) {
    // Value types
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
    case Type::String:
    case Type::Date:
    case Type::LocalTime:
    case Type::LocalDateTime:
    case Type::ZonedDateTime:
    case Type::Duration:
    case Type::Enum:
    case Type::Point2d:
    case Type::Point3d:
      return false;
    // Reference types
    case Type::List:
      return std::ranges::any_of(list_v, [](const auto &elem) { return elem.ContainsDeleted(); });
    case Type::Map:
      return std::ranges::any_of(map_v, [](const auto &item) { return item.second.ContainsDeleted(); });
    case Type::Vertex:
      return vertex_v.impl_.vertex_->deleted();
    case Type::Edge:
      return edge_v.IsDeleted();
    case Type::Path:
      return std::ranges::any_of(path_v->vertices(),
                                 [](auto &vertex_acc) { return vertex_acc.impl_.vertex_->deleted(); }) ||
             std::ranges::any_of(path_v->edges(), [](auto &edge_acc) { return edge_acc.IsDeleted(); });
    case Type::Graph:
    case Type::Function:
      throw TypedValueException("Value of unknown type");
  }
  return false;
}

bool TypedValue::IsNumeric() const { return IsInt() || IsDouble(); }

bool TypedValue::IsPropertyValue() const {
  switch (type_) {
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
    case Type::String:
    case Type::List:
    case Type::Map:
    case Type::Date:
    case Type::LocalTime:
    case Type::LocalDateTime:
    case Type::ZonedDateTime:
    case Type::Duration:
    case Type::Enum:
    case Type::Point2d:
    case Type::Point3d:
      return true;
    case Type::Vertex:
    case Type::Edge:
    case Type::Path:
    case Type::Graph:
    case Type::Function:
      return false;
  }
}

std::ostream &operator<<(std::ostream &os, const TypedValue::Type &type) {
  switch (type) {
    case TypedValue::Type::Null:
      return os << "null";
    case TypedValue::Type::Bool:
      return os << "bool";
    case TypedValue::Type::Int:
      return os << "int";
    case TypedValue::Type::Double:
      return os << "double";
    case TypedValue::Type::String:
      return os << "string";
    case TypedValue::Type::List:
      return os << "list";
    case TypedValue::Type::Map:
      return os << "map";
    case TypedValue::Type::Vertex:
      return os << "vertex";
    case TypedValue::Type::Edge:
      return os << "edge";
    case TypedValue::Type::Path:
      return os << "path";
    case TypedValue::Type::Date:
      return os << "date";
    case TypedValue::Type::LocalTime:
      return os << "local_time";
    case TypedValue::Type::LocalDateTime:
      return os << "local_date_time";
    case TypedValue::Type::ZonedDateTime:
      return os << "zoned_date_time";
    case TypedValue::Type::Duration:
      return os << "duration";
    case TypedValue::Type::Enum:
      return os << "enum";
    case TypedValue::Type::Point2d:
    case TypedValue::Type::Point3d:
      return os << "point";
    case TypedValue::Type::Graph:
      return os << "graph";
    case TypedValue::Type::Function:
      return os << "function";
  }
  LOG_FATAL("Unsupported TypedValue::Type");
}

#define DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(type_param, typed_value_type, member) \
  TypedValue &TypedValue::operator=(type_param other) {                          \
    if (this->type_ == TypedValue::Type::typed_value_type) {                     \
      this->member = other;                                                      \
    } else {                                                                     \
      *this = TypedValue(other, alloc_);                                         \
    }                                                                            \
                                                                                 \
    return *this;                                                                \
  }

DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const char *, String, string_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(int, Int, int_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(bool, Bool, bool_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(int64_t, Int, int_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(double, Double, double_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const std::string_view, String, string_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const TypedValue::TVector &, List, list_v)

TypedValue &TypedValue::operator=(const std::vector<TypedValue> &other) {
  if (type_ == Type::List) {
    list_v.reserve(other.size());
    list_v.assign(other.begin(), other.end());
  } else {
    *this = TypedValue(other, alloc_);
  }
  return *this;
}

DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const TypedValue::TMap &, Map, map_v)

TypedValue &TypedValue::operator=(const std::map<std::string, TypedValue> &other) {
  if (type_ == Type::Map) {
    map_v.clear();
    for (const auto &kv : other) map_v.emplace(TString(kv.first, alloc_), kv.second);
  } else {
    *this = TypedValue(other, alloc_);
  }
  return *this;
}

DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const VertexAccessor &, Vertex, vertex_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const EdgeAccessor &, Edge, edge_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const utils::Date &, Date, date_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const utils::LocalTime &, LocalTime, local_time_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const utils::LocalDateTime &, LocalDateTime, local_date_time_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const utils::ZonedDateTime &, ZonedDateTime, zoned_date_time_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const utils::Duration &, Duration, duration_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const storage::Enum &, Enum, enum_v)

TypedValue &TypedValue::operator=(const Path &other) {
  if (type_ == Type::Path) {
    auto path = path_v.release();
    if (path) {
      utils::Allocator<Path>(alloc_).delete_object(path);
    }
    auto *path_ptr = utils::Allocator<Path>(alloc_).new_object<Path>(other);
    path_v = std::unique_ptr<Path>(path_ptr);
  } else {
    *this = TypedValue(other, alloc_);
  }
  return *this;
}

#undef DEFINE_TYPED_VALUE_COPY_ASSIGNMENT

#define DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(type_param, typed_value_type, member) \
  TypedValue &TypedValue::operator=(type_param &&other) {                        \
    if (this->type_ == TypedValue::Type::typed_value_type) {                     \
      this->member = std::move(other);                                           \
    } else {                                                                     \
      *this = TypedValue(std::move(other), alloc_);                              \
    }                                                                            \
    return *this;                                                                \
  }

DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TypedValue::TString, String, string_v)
DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TypedValue::TVector, List, list_v)

TypedValue &TypedValue::operator=(std::vector<TypedValue> &&other) {
  if (type_ == Type::List) {
    list_v.clear();
    list_v.reserve(other.size());
    for (auto &elem : other) {
      list_v.emplace_back(std::move(elem));
    }
  } else {
    *this = TypedValue(std::move(other), alloc_);
  }
  return *this;
}

DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TMap, Map, map_v)

TypedValue &TypedValue::operator=(std::map<std::string, TypedValue> &&other) {
  if (type_ == Type::Map) {
    map_v.clear();
    for (auto &[key, value] : other) {
      map_v.emplace(TString(key, alloc_), TypedValue(std::move(value), alloc_));
    }
  } else {
    *this = TypedValue(std::move(other), alloc_);
  }
  return *this;
}

TypedValue &TypedValue::operator=(Path &&other) {
  if (type_ == Type::Path) {
    auto path = path_v.release();
    if (path) {
      utils::Allocator<Path>(alloc_).delete_object(path);
    }
    path_v = std::make_unique<Path>(std::move(other), alloc_);
  } else {
    *this = TypedValue(std::move(other), alloc_);
  }
  return *this;
}

#undef DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT

TypedValue &TypedValue::operator=(const TypedValue &other) {
  static_assert(!alloc_trait::propagate_on_container_copy_assignment::value, "Allocator propagation not implemented");
  if (this != &other) {
    if (type_ == other.type_ && alloc_ == other.alloc_) {
      // same type, copy assign value
      switch (type_) {
        case Type::Null:
          break;
        case Type::Bool:
          bool_v = other.bool_v;
          break;
        case Type::Int:
          int_v = other.int_v;
          break;
        case Type::Double:
          double_v = other.double_v;
          break;
        case Type::String:
          string_v = other.string_v;
          break;
        case Type::List:
          list_v = other.list_v;
          break;
        case Type::Map: {
          map_v = other.map_v;
          break;
        }
        case Type::Vertex:
          vertex_v = other.vertex_v;
          break;
        case Type::Edge:
          edge_v = other.edge_v;
          break;
        case Type::Path: {
          auto *path = path_v.release();
          if (path) {
            utils::Allocator<Path>(alloc_).delete_object(path);
          }
          if (other.path_v) {
            auto *path_ptr = utils::Allocator<Path>(alloc_).new_object<Path>(*other.path_v);
            path_v = std::unique_ptr<Path>(path_ptr);
          }
          break;
        }
        case Type::Date:
          date_v = other.date_v;
          break;
        case Type::LocalTime:
          local_time_v = other.local_time_v;
          break;
        case Type::LocalDateTime:
          local_date_time_v = other.local_date_time_v;
          break;
        case Type::ZonedDateTime:
          zoned_date_time_v = other.zoned_date_time_v;
          break;
        case Type::Duration:
          duration_v = other.duration_v;
          break;
        case Type::Graph: {
          auto *graph = graph_v.release();
          if (graph) {
            utils::Allocator<Graph>(alloc_).delete_object(graph);
          }
          if (other.graph_v) {
            auto *graph_ptr = utils::Allocator<Graph>(alloc_).new_object<Graph>(*other.graph_v);
            graph_v = std::unique_ptr<Graph>(graph_ptr);
          }
          break;
        }
        case Type::Function:
          function_v = other.function_v;
          break;
        case Type::Enum:
          enum_v = other.enum_v;
          break;
        case Type::Point2d:
          point_2d_v = other.point_2d_v;
          break;
        case Type::Point3d:
          point_3d_v = other.point_3d_v;
          break;
      }
      return *this;
    }
    // destroy + construct
    auto alloc = alloc_;
    alloc_trait::destroy(alloc, this);
    alloc_trait::construct(alloc, std::launder(this), other);
    // NOLINTNEXTLINE(cppcoreguidelines-c-copy-assignment-signature,misc-unconventional-assign-operator)
    return *std::launder(this);
  }
  return *this;
}

TypedValue &TypedValue::operator=(TypedValue &&other) noexcept(false) {
  static_assert(!std::allocator_traits<utils::Allocator<TypedValue>>::propagate_on_container_move_assignment::value,
                "Allocator propagation not implemented");
  if (this != &other) {
    if (type_ == other.type_ && alloc_ == other.alloc_) {
      // same type, move assign value
      switch (type_) {
        case Type::Null:
          break;
        case Type::Bool:
          bool_v = other.bool_v;
          break;
        case Type::Int:
          int_v = other.int_v;
          break;
        case Type::Double:
          double_v = other.double_v;
          break;
        case Type::String:
          string_v = std::move(other.string_v);
          break;
        case Type::List:
          list_v = std::move(other.list_v);
          break;
        case Type::Map: {
          map_v = std::move(other.map_v);
          break;
        }
        case Type::Vertex:
          vertex_v = other.vertex_v;
          break;
        case Type::Edge:
          edge_v = other.edge_v;
          break;
        case Type::Path: {
          auto *path = path_v.release();
          if (path) {
            utils::Allocator<Path>(alloc_).delete_object(path);
          }
          path_v = std::move(other.path_v);
          break;
        }
        case Type::Date:
          date_v = other.date_v;
          break;
        case Type::LocalTime:
          local_time_v = other.local_time_v;
          break;
        case Type::LocalDateTime:
          local_date_time_v = other.local_date_time_v;
          break;
        case Type::ZonedDateTime:
          zoned_date_time_v = other.zoned_date_time_v;
          break;
        case Type::Duration:
          duration_v = other.duration_v;
          break;
        case Type::Graph: {
          auto *graph = graph_v.release();
          if (graph) {
            utils::Allocator<Graph>(alloc_).delete_object(graph);
          }
          graph_v = std::move(other.graph_v);
          break;
        }
        case Type::Function:
          function_v = std::move(other.function_v);
          break;
        case Type::Enum:
          enum_v = other.enum_v;
          break;
        case Type::Point2d:
          point_2d_v = other.point_2d_v;
          break;
        case Type::Point3d:
          point_3d_v = other.point_3d_v;
          break;
      }

      return *this;
    }
    // destroy + construct
    auto orig_mem = alloc_;
    std::destroy_at(this);
    // NOLINTNEXTLINE(cppcoreguidelines-c-copy-assignment-signature,misc-unconventional-assign-operator)
    return *std::construct_at(std::launder(this), std::move(other), orig_mem);
  }
  return *this;
}

TypedValue::~TypedValue() {
  switch (type_) {
      // destructor for primitive types does nothing
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
      break;

      // we need to call destructors for non primitive types since we used
      // placement new
    case Type::String:
      std::destroy_at(&string_v);
      break;
    case Type::List:
      std::destroy_at(&list_v);
      break;
    case Type::Map: {
      std::destroy_at(&map_v);
      break;
    }
    case Type::Vertex:
      std::destroy_at(&vertex_v);
      break;
    case Type::Edge:
      std::destroy_at(&edge_v);
      break;
    case Type::Path: {
      auto *path = path_v.release();
      std::destroy_at(&path_v);
      if (path) {
        utils::Allocator<Path>(alloc_).delete_object(path);
      }
      break;
    }
    case Type::Date:
    case Type::LocalTime:
    case Type::LocalDateTime:
    case Type::Duration:
    case Type::Enum:
    case Type::Point2d:
    case Type::Point3d:
    case Type::ZonedDateTime:
      // Do nothing: std::chrono::time_zone* pointers reference immutable values from the external tz DB
      break;
    case Type::Function:
      std::destroy_at(&function_v);
      break;
    case Type::Graph: {
      auto *graph = graph_v.release();
      std::destroy_at(&graph_v);
      if (graph) {
        utils::Allocator<Graph>(alloc_).delete_object(graph);
      }
      break;
    }
  }
}

/**
 * Returns the double value of a value.
 * The value MUST be either Double or Int.
 *
 * @param value
 * @return
 */
double ToDouble(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Int:
      return (double)value.ValueInt();
    case TypedValue::Type::Double:
      return value.ValueDouble();
    default:
      throw TypedValueException("Unsupported TypedValue::Type conversion to double");
  }
}

namespace {

// Place NaN as the largest finite/infinite double so that ordering is total.
// Two NaNs compare equivalent; NaN > any non-NaN; non-NaN < NaN.
std::partial_ordering CompareDoubles(double a, double b) {
  const bool a_nan = std::isnan(a);
  const bool b_nan = std::isnan(b);
  if (a_nan && b_nan) return std::partial_ordering::equivalent;
  if (a_nan) return std::partial_ordering::greater;
  if (b_nan) return std::partial_ordering::less;
  return a <=> b;
}

// Lexicographic Gid-walk shared between the orderability comparator
// (OrderCompare) and the Cypher `<` operator. Paths contain only
// VertexAccessor/EdgeAccessor handles, so the walk is pure strong_ordering.
std::strong_ordering ComparePaths(const Path &path_a, const Path &path_b) {
  const auto &verts_a = path_a.vertices();
  const auto &verts_b = path_b.vertices();
  const auto &edges_a = path_a.edges();
  const auto &edges_b = path_b.edges();
  const auto min_edges = std::min(edges_a.size(), edges_b.size());
  for (size_t i = 0; i < min_edges; ++i) {
    if (auto v = verts_a[i].Gid() <=> verts_b[i].Gid(); v != 0) return v;
    if (auto e = edges_a[i].Gid() <=> edges_b[i].Gid(); e != 0) return e;
  }
  if (min_edges < verts_a.size() && min_edges < verts_b.size()) {
    if (auto v = verts_a[min_edges].Gid() <=> verts_b[min_edges].Gid(); v != 0) return v;
  }
  return edges_a.size() <=> edges_b.size();
}

// Helper for the Cypher `<` operator on lists. Walks element-wise via
// `==`/`<`, propagating null per equality semantics. This DIFFERS from
// orderability (OrderCompare's List branch) which never returns null —
// they cannot share an implementation: the element relation differs.
TypedValue ListLess(const TypedValue::TVector &list_a, const TypedValue::TVector &list_b,
                    utils::MemoryResource *alloc) {
  const auto min_size = std::min(list_a.size(), list_b.size());
  for (size_t i = 0; i < min_size; ++i) {
    TypedValue eq = list_a[i] == list_b[i];
    if (!eq.IsBool() || !eq.ValueBool()) return list_a[i] < list_b[i];
  }
  return TypedValue(list_a.size() < list_b.size(), alloc);
}

// Helper for the Cypher `<` operator on maps; null-propagating, see ListLess.
TypedValue MapLess(const TypedValue::TMap &map_a, const TypedValue::TMap &map_b, utils::MemoryResource *alloc) {
  auto it_a = map_a.begin();
  auto it_b = map_b.begin();
  const auto min_size = std::min(map_a.size(), map_b.size());
  for (size_t i = 0; i < min_size; ++i) {
    auto key_cmp = it_a->first <=> it_b->first;
    if (key_cmp < 0) return TypedValue(true, alloc);
    if (key_cmp > 0) return TypedValue(false, alloc);
    TypedValue eq = it_a->second == it_b->second;
    if (!eq.IsBool() || !eq.ValueBool()) return it_a->second < it_b->second;
    ++it_a;
    ++it_b;
  }
  return TypedValue(map_a.size() < map_b.size(), alloc);
}

}  // namespace

std::partial_ordering OrderCompare(TypedValue const &a, TypedValue const &b) {
  const auto type_a = a.type();
  const auto type_b = b.type();

  if (type_a == type_b) [[likely]] {
    switch (type_a) {
      case TypedValue::Type::Null:
        return std::partial_ordering::equivalent;
      case TypedValue::Type::Bool:
        return a.UnsafeValueBool() <=> b.UnsafeValueBool();
      case TypedValue::Type::Int:
        return a.UnsafeValueInt() <=> b.UnsafeValueInt();
      case TypedValue::Type::Double:
        return CompareDoubles(a.UnsafeValueDouble(), b.UnsafeValueDouble());
      case TypedValue::Type::String:
        return a.UnsafeValueString() <=> b.UnsafeValueString();
      case TypedValue::Type::Date:
        return a.UnsafeValueDate() <=> b.UnsafeValueDate();
      case TypedValue::Type::LocalTime:
        return a.UnsafeValueLocalTime() <=> b.UnsafeValueLocalTime();
      case TypedValue::Type::LocalDateTime:
        return a.UnsafeValueLocalDateTime() <=> b.UnsafeValueLocalDateTime();
      case TypedValue::Type::ZonedDateTime:
        return a.UnsafeValueZonedDateTime() <=> b.UnsafeValueZonedDateTime();
      case TypedValue::Type::Duration:
        return a.UnsafeValueDuration() <=> b.UnsafeValueDuration();
      case TypedValue::Type::Enum:
        return a.UnsafeValueEnum() <=> b.UnsafeValueEnum();
      case TypedValue::Type::Point2d:
        return a.UnsafeValuePoint2d() <=> b.UnsafeValuePoint2d();
      case TypedValue::Type::Point3d:
        return a.UnsafeValuePoint3d() <=> b.UnsafeValuePoint3d();
      case TypedValue::Type::List:
        return std::lexicographical_compare_three_way(a.UnsafeValueList().begin(),
                                                      a.UnsafeValueList().end(),
                                                      b.UnsafeValueList().begin(),
                                                      b.UnsafeValueList().end(),
                                                      OrderCompare);
      case TypedValue::Type::Map: {
        // Maps ordering: 1) by size, 2) by keys alphabetically, 3) by values
        const auto &map_a = a.UnsafeValueMap();
        const auto &map_b = b.UnsafeValueMap();
        if (map_a.size() != map_b.size()) {
          return map_a.size() <=> map_b.size();
        }
        auto it_a = map_a.begin();
        auto it_b = map_b.begin();
        while (it_a != map_a.end()) {
          if (auto key_cmp = it_a->first <=> it_b->first; key_cmp != std::strong_ordering::equal) {
            return key_cmp;
          }
          ++it_a;
          ++it_b;
        }
        it_a = map_a.begin();
        it_b = map_b.begin();
        while (it_a != map_a.end()) {
          if (auto val_cmp = OrderCompare(it_a->second, it_b->second); val_cmp != std::partial_ordering::equivalent) {
            return val_cmp;
          }
          ++it_a;
          ++it_b;
        }
        return std::partial_ordering::equivalent;
      }
      case TypedValue::Type::Vertex:
        return a.UnsafeValueVertex().Gid() <=> b.UnsafeValueVertex().Gid();
      case TypedValue::Type::Edge:
        return a.UnsafeValueEdge().Gid() <=> b.UnsafeValueEdge().Gid();
      case TypedValue::Type::Path:
        return ComparePaths(a.ValuePath(), b.ValuePath());
      case TypedValue::Type::Graph:
      case TypedValue::Type::Function:
        return std::partial_ordering::equivalent;
    }
  }

  // Different types: handle common cases before computing ranks.
  if (type_a == TypedValue::Type::Int && type_b == TypedValue::Type::Double) {
    return CompareDoubles(static_cast<double>(a.UnsafeValueInt()), b.UnsafeValueDouble());
  }
  if (type_a == TypedValue::Type::Double && type_b == TypedValue::Type::Int) {
    return CompareDoubles(a.UnsafeValueDouble(), static_cast<double>(b.UnsafeValueInt()));
  }

  // Point2d vs Point3d (same rank, compare by SRID)
  if (type_a == TypedValue::Type::Point2d && type_b == TypedValue::Type::Point3d) {
    return storage::CrsToSrid(a.UnsafeValuePoint2d().crs()) <=> storage::CrsToSrid(b.UnsafeValuePoint3d().crs());
  }
  if (type_a == TypedValue::Type::Point3d && type_b == TypedValue::Type::Point2d) {
    return storage::CrsToSrid(a.UnsafeValuePoint3d().crs()) <=> storage::CrsToSrid(b.UnsafeValuePoint2d().crs());
  }

  return TypeOrderRank(type_a) <=> TypeOrderRank(type_b);
}

TypedValue operator<(const TypedValue &a, const TypedValue &b) {
  // null is incomparable with any value (including other nulls)
  if (a.IsNull() || b.IsNull()) {
    return TypedValue(a.alloc_);
  }

  const auto type_a = a.type();
  const auto type_b = b.type();

  // Numbers can be compared across Int/Double
  if (a.IsNumeric() && b.IsNumeric()) {
    if (a.IsDouble() || b.IsDouble()) {
      double da = a.IsDouble() ? a.ValueDouble() : static_cast<double>(a.ValueInt());
      double db = b.IsDouble() ? b.ValueDouble() : static_cast<double>(b.ValueInt());
      // NaN is incomparable
      if (std::isnan(da) || std::isnan(db)) {
        return TypedValue(a.alloc_);
      }
      return TypedValue(da < db, a.alloc_);
    }
    return TypedValue(a.ValueInt() < b.ValueInt(), a.alloc_);
  }

  // All other types are only comparable within their own type
  if (type_a != type_b) {
    return TypedValue(a.alloc_);  // Incomparable - different types
  }

  switch (type_a) {
    case TypedValue::Type::Bool:
      // false < true
      return TypedValue(!a.ValueBool() && b.ValueBool(), a.alloc_);

    case TypedValue::Type::String:
      return TypedValue(a.ValueString() < b.ValueString(), a.alloc_);

    case TypedValue::Type::List:
      return ListLess(a.ValueList(), b.ValueList(), a.alloc_.resource());

    case TypedValue::Type::Map:
      return MapLess(a.ValueMap(), b.ValueMap(), a.alloc_.resource());

    case TypedValue::Type::Vertex:
      return TypedValue(a.ValueVertex().Gid() < b.ValueVertex().Gid(), a.alloc_);

    case TypedValue::Type::Edge:
      return TypedValue(a.ValueEdge().Gid() < b.ValueEdge().Gid(), a.alloc_);

    case TypedValue::Type::Path:
      return TypedValue(ComparePaths(a.ValuePath(), b.ValuePath()) < 0, a.alloc_);

    case TypedValue::Type::Date:
      return TypedValue(a.ValueDate() < b.ValueDate(), a.alloc_);

    case TypedValue::Type::LocalTime:
      return TypedValue(a.ValueLocalTime() < b.ValueLocalTime(), a.alloc_);

    case TypedValue::Type::LocalDateTime:
      return TypedValue(a.ValueLocalDateTime() < b.ValueLocalDateTime(), a.alloc_);

    case TypedValue::Type::ZonedDateTime:
      return TypedValue(a.ValueZonedDateTime() < b.ValueZonedDateTime(), a.alloc_);

    case TypedValue::Type::Duration:
      // Duration comparison is incomparable per spec (returns null)
      return TypedValue(a.alloc_);

    case TypedValue::Type::Enum:
      return TypedValue(a.ValueEnum() < b.ValueEnum(), a.alloc_);

    case TypedValue::Type::Point2d:
    case TypedValue::Type::Point3d:
    case TypedValue::Type::Graph:
    case TypedValue::Type::Function:
    case TypedValue::Type::Null:
    case TypedValue::Type::Int:
    case TypedValue::Type::Double:
      // These cases are handled above or should not reach here
      return TypedValue(a.alloc_);
  }
}

TypedValue operator==(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.alloc_);

  // Different types are incomparable (return null), except numbers
  if (a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric())) {
    return TypedValue(a.alloc_);  // Incomparable - different types
  }

  switch (a.type()) {
    case TypedValue::Type::Bool:
      return TypedValue(a.ValueBool() == b.ValueBool(), a.alloc_);
    case TypedValue::Type::Int:
      if (b.IsDouble())
        return TypedValue(ToDouble(a) == ToDouble(b), a.alloc_);
      else
        return TypedValue(a.ValueInt() == b.ValueInt(), a.alloc_);
    case TypedValue::Type::Double:
      return TypedValue(ToDouble(a) == ToDouble(b), a.alloc_);
    case TypedValue::Type::String:
      return TypedValue(a.ValueString() == b.ValueString(), a.alloc_);
    case TypedValue::Type::Vertex:
      return TypedValue(a.ValueVertex() == b.ValueVertex(), a.alloc_);
    case TypedValue::Type::Edge:
      return TypedValue(a.ValueEdge() == b.ValueEdge(), a.alloc_);
    case TypedValue::Type::List: {
      const auto &list_a = a.ValueList();
      const auto &list_b = b.ValueList();
      if (list_a.size() != list_b.size()) return TypedValue(false, a.alloc_);
      // Per openCypher 9: list equality is the conjunction of element-wise `=`.
      // In three-valued logic `false ∧ null = false`, so a definite mismatch
      // dominates over null; only return null when no element compared false.
      bool seen_null = false;
      for (size_t i = 0; i < list_a.size(); ++i) {
        TypedValue eq = list_a[i] == list_b[i];
        if (eq.IsNull()) {
          seen_null = true;
          continue;
        }
        if (!eq.ValueBool()) return TypedValue(false, a.alloc_);
      }
      if (seen_null) return TypedValue(a.alloc_);
      return TypedValue(true, a.alloc_);
    }
    case TypedValue::Type::Map: {
      const auto &map_a = a.ValueMap();
      const auto &map_b = b.ValueMap();
      if (map_a.size() != map_b.size()) return TypedValue(false, a.alloc_);
      // Per openCypher 9: map equality is the conjunction of `m1.k = m2.k` for every key.
      // Since `null = null` is null, propagate null when any nested comparison is null
      // (matching the List branch above).
      bool seen_null = false;
      for (const auto &kv_a : map_a) {
        auto found_b_it = map_b.find(kv_a.first);
        if (found_b_it == map_b.end()) return TypedValue(false, a.alloc_);
        TypedValue eq = kv_a.second == found_b_it->second;
        if (eq.IsNull()) {
          seen_null = true;
          continue;
        }
        if (!eq.ValueBool()) return TypedValue(false, a.alloc_);
      }
      if (seen_null) return TypedValue(a.alloc_);
      return TypedValue(true, a.alloc_);
    }
    case TypedValue::Type::Path:
      return TypedValue(a.ValuePath() == b.ValuePath(), a.alloc_);
    case TypedValue::Type::Date:
      return TypedValue(a.ValueDate() == b.ValueDate(), a.alloc_);
    case TypedValue::Type::LocalTime:
      return TypedValue(a.ValueLocalTime() == b.ValueLocalTime(), a.alloc_);
    case TypedValue::Type::LocalDateTime:
      return TypedValue(a.ValueLocalDateTime() == b.ValueLocalDateTime(), a.alloc_);
    case TypedValue::Type::ZonedDateTime:
      return TypedValue(a.ValueZonedDateTime() == b.ValueZonedDateTime(), a.alloc_);
    case TypedValue::Type::Duration:
      return TypedValue(a.ValueDuration() == b.ValueDuration(), a.alloc_);
    case TypedValue::Type::Enum:
      return TypedValue(a.ValueEnum() == b.ValueEnum(), a.alloc_);
    case TypedValue::Type::Point2d:
      return TypedValue(a.ValuePoint2d() == b.ValuePoint2d(), a.alloc_);
    case TypedValue::Type::Point3d:
      return TypedValue(a.ValuePoint3d() == b.ValuePoint3d(), a.alloc_);
    case TypedValue::Type::Graph:
      throw TypedValueException("Unsupported comparison operator");
    case TypedValue::Type::Function:
    case TypedValue::Type::Null:
      LOG_FATAL("Unhandled comparison for types");
  }
}

TypedValue operator!(const TypedValue &a) {
  if (a.IsNull()) return TypedValue(a.alloc_);
  if (a.IsBool()) return TypedValue(!a.ValueBool(), a.alloc_);
  throw TypedValueException("Invalid logical not operand type (!{})", a.type());
}

/**
 * Turns a numeric or string value into a string.
 *
 * @param value a value.
 * @return A string.
 */
std::string ValueToString(const TypedValue &value) {
  // TODO: Should this allocate a string through value.alloc_?
  if (value.IsString()) return std::string(value.ValueString());
  if (value.IsInt()) return std::to_string(value.ValueInt());
  if (value.IsDouble()) return fmt::format("{}", value.ValueDouble());
  // unsupported situations
  throw TypedValueException("Unsupported TypedValue::Type conversion to string");
}

TypedValue operator-(const TypedValue &a) {
  if (a.IsNull()) return TypedValue(a.alloc_);
  if (a.IsInt()) return TypedValue(-a.ValueInt(), a.alloc_);
  if (a.IsDouble()) return TypedValue(-a.ValueDouble(), a.alloc_);
  if (a.IsDuration()) return TypedValue(-a.ValueDuration(), a.alloc_);
  throw TypedValueException("Invalid unary minus operand type (-{})", a.type());
}

TypedValue operator+(const TypedValue &a) {
  if (a.IsNull()) return TypedValue(a.alloc_);
  if (a.IsInt()) return TypedValue(+a.ValueInt(), a.alloc_);
  if (a.IsDouble()) return TypedValue(+a.ValueDouble(), a.alloc_);
  throw TypedValueException("Invalid unary plus operand type (+{})", a.type());
}

/**
 * Raises a TypedValueException if the given values do not support arithmetic
 * operations. If they do, nothing happens.
 *
 * @param a First value.
 * @param b Second value.
 * @param string_ok If or not for the given operation it's valid to work with
 *  String values (typically it's OK only for sum).
 *  @param op_name Name of the operation, used only for exception description,
 *  if raised.
 */
inline void EnsureArithmeticallyOk(const TypedValue &a, const TypedValue &b, bool string_ok,
                                   const std::string &op_name) {
  auto is_legal = [string_ok](const TypedValue &value) {
    return value.IsNumeric() || (string_ok && value.type() == TypedValue::Type::String);
  };

  // Note that List and Null can also be valid in arithmetic ops. They are not
  // checked here because they are handled before this check is performed in
  // arithmetic op implementations.

  if (!is_legal(a) || !is_legal(b)) {
    throw TypedValueException("Invalid {} operand types {}, {}", op_name, a.type(), b.type());
  }
}

namespace {

std::optional<TypedValue> MaybeDoTemporalTypeAddition(const TypedValue &a, const TypedValue &b) {
  // Duration
  if (a.IsDuration() && b.IsDuration()) {
    return TypedValue(a.ValueDuration() + b.ValueDuration(), a.get_allocator());
  }
  // Date
  if (a.IsDate() && b.IsDuration()) {
    return TypedValue(a.ValueDate() + b.ValueDuration(), a.get_allocator());
  }
  if (a.IsDuration() && b.IsDate()) {
    return TypedValue(a.ValueDuration() + b.ValueDate(), a.get_allocator());
  }
  // LocalTime
  if (a.IsLocalTime() && b.IsDuration()) {
    return TypedValue(a.ValueLocalTime() + b.ValueDuration(), a.get_allocator());
  }
  if (a.IsDuration() && b.IsLocalTime()) {
    return TypedValue(a.ValueDuration() + b.ValueLocalTime(), a.get_allocator());
  }
  // LocalDateTime
  if (a.IsLocalDateTime() && b.IsDuration()) {
    return TypedValue(a.ValueLocalDateTime() + b.ValueDuration(), a.get_allocator());
  }
  if (a.IsDuration() && b.IsLocalDateTime()) {
    return TypedValue(a.ValueDuration() + b.ValueLocalDateTime(), a.get_allocator());
  }
  // ZonedDateTime
  if (a.IsZonedDateTime() && b.IsDuration()) {
    return TypedValue(a.ValueZonedDateTime() + b.ValueDuration(), a.get_allocator());
  }
  if (a.IsDuration() && b.IsZonedDateTime()) {
    return TypedValue(a.ValueDuration() + b.ValueZonedDateTime(), a.get_allocator());
  }
  return std::nullopt;
}

std::optional<TypedValue> MaybeDoTemporalTypeSubtraction(const TypedValue &a, const TypedValue &b) {
  // Duration
  if (a.IsDuration() && b.IsDuration()) {
    return TypedValue(a.ValueDuration() - b.ValueDuration());
  }
  // Date
  if (a.IsDate() && b.IsDuration()) {
    return TypedValue(a.ValueDate() - b.ValueDuration());
  }
  if (a.IsDate() && b.IsDate()) {
    return TypedValue(a.ValueDate() - b.ValueDate());
  }
  // LocalTime
  if (a.IsLocalTime() && b.IsDuration()) {
    return TypedValue(a.ValueLocalTime() - b.ValueDuration());
  }
  if (a.IsLocalTime() && b.IsLocalTime()) {
    return TypedValue(a.ValueLocalTime() - b.ValueLocalTime());
  }
  // LocalDateTime
  if (a.IsLocalDateTime() && b.IsDuration()) {
    return TypedValue(a.ValueLocalDateTime() - b.ValueDuration());
  }
  if (a.IsLocalDateTime() && b.IsLocalDateTime()) {
    return TypedValue(a.ValueLocalDateTime() - b.ValueLocalDateTime());
  }
  // ZonedDateTime
  if (a.IsZonedDateTime() && b.IsDuration()) {
    return TypedValue(a.ValueZonedDateTime() - b.ValueDuration());
  }
  if (a.IsZonedDateTime() && b.IsZonedDateTime()) {
    return TypedValue(a.ValueZonedDateTime() - b.ValueZonedDateTime());
  }
  return std::nullopt;
}
}  // namespace

// TODO: move to switch
TypedValue operator+(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.alloc_);

  if (a.IsList() || b.IsList()) {
    TypedValue::TVector list(a.alloc_);

    size_t const new_list_size{(a.IsList() ? a.ValueList().size() : 1) + (b.IsList() ? b.ValueList().size() : 1)};
    list.reserve(new_list_size);

    auto append_list = [&list](const TypedValue &v) {
      if (v.IsList()) {
        auto list2 = v.ValueList();
        list.insert(list.end(), list2.begin(), list2.end());
      } else {
        list.push_back(v);
      }
    };
    append_list(a);
    append_list(b);
    return TypedValue(std::move(list), a.alloc_);
  }

  if (const auto maybe_add = MaybeDoTemporalTypeAddition(a, b); maybe_add) {
    return *maybe_add;
  }

  EnsureArithmeticallyOk(a, b, true, "addition");
  // no more Bool nor Null, summing works on anything from here onward

  if (a.IsString() || b.IsString()) return TypedValue(ValueToString(a) + ValueToString(b), a.alloc_);

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) + ToDouble(b), a.alloc_);
  }
  return TypedValue(a.ValueInt() + b.ValueInt(), a.alloc_);
}

TypedValue operator-(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.alloc_);
  if (const auto maybe_sub = MaybeDoTemporalTypeSubtraction(a, b); maybe_sub) {
    return *maybe_sub;
  }
  EnsureArithmeticallyOk(a, b, true, "subraction");
  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) - ToDouble(b), a.alloc_);
  }
  return TypedValue(a.ValueInt() - b.ValueInt(), a.alloc_);
}

TypedValue operator/(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.alloc_);
  EnsureArithmeticallyOk(a, b, false, "division");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) / ToDouble(b), a.alloc_);
  } else {
    if (b.ValueInt() == 0LL) throw TypedValueException("Division by zero");
    return TypedValue(a.ValueInt() / b.ValueInt(), a.alloc_);
  }
}

TypedValue operator*(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.alloc_);
  EnsureArithmeticallyOk(a, b, false, "multiplication");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) * ToDouble(b), a.alloc_);
  } else {
    return TypedValue(a.ValueInt() * b.ValueInt(), a.alloc_);
  }
}

TypedValue operator%(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.alloc_);
  EnsureArithmeticallyOk(a, b, false, "modulo");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(static_cast<double>(fmod(ToDouble(a), ToDouble(b))), a.alloc_);
  } else {
    if (b.ValueInt() == 0LL) throw TypedValueException("Mod with zero");
    return TypedValue(a.ValueInt() % b.ValueInt(), a.alloc_);
  }
}

TypedValue pow(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.alloc_);
  EnsureArithmeticallyOk(a, b, false, "^");

  return TypedValue(std::pow(ToDouble(a), ToDouble(b)), a.alloc_);
}

inline void EnsureLogicallyOk(const TypedValue &a, const TypedValue &b, const std::string &op_name) {
  if (!((a.IsBool() || a.IsNull()) && (b.IsBool() || b.IsNull()))) {
    throw TypedValueException("Invalid {} operand types({} && {})", op_name, a.type(), b.type());
  }
}

TypedValue operator&&(const TypedValue &a, const TypedValue &b) {
  EnsureLogicallyOk(a, b, "logical AND");
  // at this point we only have null and bool
  // if either operand is false, the result is false
  if (a.IsBool() && !a.ValueBool()) return TypedValue(false, a.alloc_);
  if (b.IsBool() && !b.ValueBool()) return TypedValue(false, a.alloc_);
  if (a.IsNull() || b.IsNull()) return TypedValue(a.alloc_);
  // neither is false, neither is null, thus both are true
  return TypedValue(true, a.alloc_);
}

TypedValue operator||(const TypedValue &a, const TypedValue &b) {
  EnsureLogicallyOk(a, b, "logical OR");
  // at this point we only have null and bool
  // if either operand is true, the result is true
  if (a.IsBool() && a.ValueBool()) return TypedValue(true, a.alloc_);
  if (b.IsBool() && b.ValueBool()) return TypedValue(true, a.alloc_);
  if (a.IsNull() || b.IsNull()) return TypedValue(a.alloc_);
  // neither is true, neither is null, thus both are false
  return TypedValue(false, a.alloc_);
}

TypedValue operator^(const TypedValue &a, const TypedValue &b) {
  EnsureLogicallyOk(a, b, "logical XOR");
  // at this point we only have null and bool
  if (a.IsNull() || b.IsNull())
    return TypedValue(a.alloc_);
  else
    return TypedValue(static_cast<bool>(a.ValueBool() ^ b.ValueBool()), a.alloc_);
}

bool TypedValue::Equivalent::operator()(const TypedValue &lhs, const TypedValue &rhs) const {
  // Equivalence per openCypher 9 CIP "Define comparability and equality as well as
  // orderability and equivalence": identical to equality except that the relation
  // is reflexive — any two nulls are equivalent (recursively into containers),
  // and NaN is equivalent to NaN even though IEEE-754 says NaN == NaN is false.
  // Used by DISTINCT, GROUP BY and hash-based set membership where null and NaN
  // grouping keys must collapse together. Hot path — compare leaf values directly
  // instead of round-tripping through `TypedValue operator==` (which builds an
  // allocator-aware TypedValue per call).
  using Type = TypedValue::Type;
  const auto type_a = lhs.type();
  const auto type_b = rhs.type();

  // Null reflexivity (recursive into containers via the List/Map branches below).
  if (type_a == Type::Null || type_b == Type::Null) return type_a == type_b;

  // Int/Int fast path — the common DISTINCT/GROUP BY key case. Skips double
  // conversion since Int can never be NaN.
  if (type_a == Type::Int && type_b == Type::Int) {
    return lhs.UnsafeValueInt() == rhs.UnsafeValueInt();
  }

  // Numeric coercion (Int <-> Double) with NaN reflexivity. NaN check only kicks
  // in when at least one side is Double; required to keep Hash and Equivalent
  // consistent so DISTINCT collapses NaN keys that hash to the same bucket.
  const bool a_double = type_a == Type::Double;
  const bool b_double = type_b == Type::Double;
  if ((a_double || type_a == Type::Int) && (b_double || type_b == Type::Int)) {
    const double da = a_double ? lhs.UnsafeValueDouble() : static_cast<double>(lhs.UnsafeValueInt());
    const double db = b_double ? rhs.UnsafeValueDouble() : static_cast<double>(rhs.UnsafeValueInt());
    if (da == db) return true;
    return std::isnan(da) && std::isnan(db);
  }

  if (type_a != type_b) return false;

  switch (type_a) {
    case Type::Bool:
      return lhs.UnsafeValueBool() == rhs.UnsafeValueBool();
    case Type::String:
      return lhs.UnsafeValueString() == rhs.UnsafeValueString();
    case Type::Vertex:
      return lhs.UnsafeValueVertex() == rhs.UnsafeValueVertex();
    case Type::Edge:
      return lhs.UnsafeValueEdge() == rhs.UnsafeValueEdge();
    case Type::Path:
      return lhs.UnsafeValuePath() == rhs.UnsafeValuePath();
    case Type::Date:
      return lhs.UnsafeValueDate() == rhs.UnsafeValueDate();
    case Type::LocalTime:
      return lhs.UnsafeValueLocalTime() == rhs.UnsafeValueLocalTime();
    case Type::LocalDateTime:
      return lhs.UnsafeValueLocalDateTime() == rhs.UnsafeValueLocalDateTime();
    case Type::ZonedDateTime:
      return lhs.UnsafeValueZonedDateTime() == rhs.UnsafeValueZonedDateTime();
    case Type::Duration:
      return lhs.UnsafeValueDuration() == rhs.UnsafeValueDuration();
    case Type::Enum:
      return lhs.UnsafeValueEnum() == rhs.UnsafeValueEnum();
    case Type::Point2d:
      return lhs.UnsafeValuePoint2d() == rhs.UnsafeValuePoint2d();
    case Type::Point3d:
      return lhs.UnsafeValuePoint3d() == rhs.UnsafeValuePoint3d();
    case Type::List: {
      const auto &la = lhs.UnsafeValueList();
      const auto &lb = rhs.UnsafeValueList();
      if (la.size() != lb.size()) return false;
      for (size_t i = 0; i < la.size(); ++i) {
        if (!(*this)(la[i], lb[i])) return false;
      }
      return true;
    }
    case Type::Map: {
      const auto &ma = lhs.UnsafeValueMap();
      const auto &mb = rhs.UnsafeValueMap();
      if (ma.size() != mb.size()) return false;
      for (const auto &kv : ma) {
        auto it = mb.find(kv.first);
        if (it == mb.end()) return false;
        if (!(*this)(kv.second, it->second)) return false;
      }
      return true;
    }
    case Type::Int:
    case Type::Double:
    case Type::Null:
      // Int/Double handled by the numeric branch above; Null handled at entry.
      std::unreachable();
    case Type::Graph:
    case Type::Function:
      // Preserve the original diagnostics for these unreachable-in-practice types.
      TypedValue eq = lhs == rhs;
      DMG_ASSERT(eq.IsBool() || eq.IsNull(), "Equality between two TypedValues must result in either Null or Bool");
      return eq.IsBool() && eq.ValueBool();
  }
  std::unreachable();
}

size_t TypedValue::Hash::operator()(const TypedValue &value) const {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return 31;
    case TypedValue::Type::Bool:
      return std::hash<bool>{}(value.ValueBool());
    case TypedValue::Type::Int:
      return std::hash<int64_t>{}(value.ValueInt());
    case TypedValue::Type::Double: {
      // NaN reflexivity (see Equivalent): every NaN must hash to the same bucket
      // so DISTINCT/GROUP BY can collapse them. std::hash<double> is unspecified
      // for NaN and may differ between bit patterns.
      const double v = value.ValueDouble();
      if (std::isnan(v)) return 0x7ff8000000000000ULL;  // sentinel for NaN equivalence class
      // Store whole number doubles as int hashes to be consistent with
      // TypedValue equality in which (2.0 == 2) returns true
      const double double_value = std::trunc(v);
      double whole_value = 0.0;
      if (std::modf(double_value, &whole_value) == 0.0) {
        return std::hash<int64_t>{}(static_cast<int64_t>(whole_value));
      }
      return std::hash<double>{}(double_value);
    }
    case TypedValue::Type::String:
      return std::hash<std::string_view>{}(value.ValueString());
    case TypedValue::Type::List: {
      return utils::FnvCollection<TypedValue::TVector, TypedValue, Hash>{}(value.ValueList());
    }
    case TypedValue::Type::Map: {
      size_t hash = 6'543'457;
      for (const auto &kv : value.ValueMap()) {
        hash ^= std::hash<std::string_view>{}(kv.first);
        hash ^= this->operator()(kv.second);
      }
      return hash;
    }
    case TypedValue::Type::Vertex:
      return value.ValueVertex().Gid().AsUint();
    case TypedValue::Type::Edge:
      return value.ValueEdge().Gid().AsUint();
    case TypedValue::Type::Path: {
      const auto &vertices = value.ValuePath().vertices();
      const auto &edges = value.ValuePath().edges();
      return utils::FnvCollection<decltype(vertices), VertexAccessor>{}(vertices) ^
             utils::FnvCollection<decltype(edges), EdgeAccessor>{}(edges);
    }
    case TypedValue::Type::Date:
      return utils::DateHash{}(value.ValueDate());
    case TypedValue::Type::LocalTime:
      return utils::LocalTimeHash{}(value.ValueLocalTime());
    case TypedValue::Type::LocalDateTime:
      return utils::LocalDateTimeHash{}(value.ValueLocalDateTime());
    case TypedValue::Type::ZonedDateTime:
      return utils::ZonedDateTimeHash{}(value.ValueZonedDateTime());
    case TypedValue::Type::Duration:
      return utils::DurationHash{}(value.ValueDuration());
    case TypedValue::Type::Enum:
      return std::hash<storage::Enum>{}(value.ValueEnum());
    case TypedValue::Type::Point2d:
      return std::hash<storage::Point2d>{}(value.ValuePoint2d());
    case TypedValue::Type::Point3d:
      return std::hash<storage::Point3d>{}(value.ValuePoint3d());
    case TypedValue::Type::Function:
      throw TypedValueException("Unsupported hash function for Function");
    case TypedValue::Type::Graph:
      throw TypedValueException("Unsupported hash function for Graph");
  }
  LOG_FATAL("Unhandled TypedValue.type() in hash function");
}

auto GetCRS(TypedValue const &tv) -> std::optional<storage::CoordinateReferenceSystem> {
  switch (tv.type()) {
    using enum TypedValue::Type;
    case Point2d: {
      return tv.point_2d_v.crs();
    }
    case Point3d: {
      return tv.point_3d_v.crs();
    }
    default: {
      return std::nullopt;
    }
  }
}

void to_json(nlohmann::json &j, TypedValue::TVector const &value) {
  j = nlohmann::json::array();
  for (const auto &item : value) {
    nlohmann::json elem;
    to_json(elem, item);
    j.push_back(std::move(elem));
  }
}

void to_json(nlohmann::json &j, TypedValue::TMap const &value) {
  j = nlohmann::json::object();
  for (const auto &[key, val] : value) {
    nlohmann::json elem;
    to_json(elem, val);
    j.emplace(std::string(key), std::move(elem));
  }
}

void to_json(nlohmann::json &j, TypedValue const &value) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      j = nullptr;
      break;
    case TypedValue::Type::Bool:
      j = value.ValueBool();
      break;
    case TypedValue::Type::Int:
      j = value.ValueInt();
      break;
    case TypedValue::Type::Double:
      j = value.ValueDouble();
      break;
    case TypedValue::Type::String:
      j = value.ValueString();
      break;
    case TypedValue::Type::List:
      j = value.ValueList();
      break;
    case TypedValue::Type::Map:
      j = value.ValueMap();
      break;
    default:
      throw utils::BasicException("Unsupported TypedValue type for JSON serialization");
  }
}

void from_json(nlohmann::json const &j, TypedValue &value) {
  if (j.is_null()) {
    value = TypedValue{};
    return;
  }

  if (j.is_boolean()) {
    value = TypedValue(j.get<bool>());
    return;
  }

  if (j.is_number_integer()) {
    value = TypedValue(j.get<int64_t>());
    return;
  }

  if (j.is_number_float()) {
    value = TypedValue(j.get<double>());
    return;
  }

  if (j.is_string()) {
    value = TypedValue(j.get<std::string>());
    return;
  }

  if (j.is_array()) {
    value = TypedValue{j.get<std::vector<TypedValue>>()};
    return;
  }

  if (j.is_object()) {
    value = TypedValue{j.get<std::map<std::string, TypedValue>>()};
    return;
  }

  throw utils::BasicException("Unsupported JSON type for TypedValue deserialization");
}

}  // namespace memgraph::query
