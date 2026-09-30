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

#include "storage/v2/schema_info_types.hpp"

#include <algorithm>
#include <sstream>
#include <unordered_map>
#include <utility>

#include <nlohmann/json.hpp>

namespace memgraph::storage {

template <template <class...> class TContainer>
nlohmann::json PropertyInfo<TContainer>::ToJson(const EnumStore &enum_store, std::string_view key,
                                                uint32_t max_count) const {
  nlohmann::json::object_t property_info;
  property_info.emplace("key", key);
  const auto num = n.load();
  property_info.emplace("count", num);
  property_info.emplace("filling_factor", (100.0 * num) / max_count);
  const auto &[types_itr, _] = property_info.emplace("types", nlohmann::json::array_t{});
  for (const auto &type : types) {
    nlohmann::json::object_t type_info;
    std::stringstream ss;
    if (type.first.type == PropertyValueType::TemporalData) {
      ss << type.first.temporal_type;
    } else if (type.first.type == PropertyValueType::Enum) {
      ss << "Enum::" << *enum_store.ToTypeString(type.first.enum_type);
    } else {
      // Unify formatting
      switch (type.first.type) {
        break;
        case PropertyValueType::Null:
          ss << "Null";
          break;
        case PropertyValueType::Bool:
          ss << "Boolean";
          break;
        case PropertyValueType::Int:
          ss << "Integer";
          break;
        case PropertyValueType::Double:
          ss << "Float";
          break;
        case PropertyValueType::String:
          ss << "String";
          break;
        case PropertyValueType::List:
        case PropertyValueType::IntList:
        case PropertyValueType::DoubleList:
        case PropertyValueType::NumericList:
        case PropertyValueType::VectorIndexId:
          ss << "List";
          break;
        case PropertyValueType::Map:
          ss << "Map";
          break;
        case PropertyValueType::TemporalData:
          ss << "TemporalData";
          break;
        case PropertyValueType::ZonedTemporalData:
          ss << "ZonedDateTime";
          break;
        case PropertyValueType::Enum:
          ss << "Enum";
          break;
        case PropertyValueType::Point2d:
          ss << "Point2D";
          break;
        case PropertyValueType::Point3d:
          ss << "Point3D";
          break;
      }
    }
    type_info.emplace("type", ss.str());
    type_info.emplace("count", type.second.load());
    types_itr->second.emplace_back(std::move(type_info));
  }
  return property_info;
}

template <template <class...> class TContainer>
nlohmann::json TrackingInfo<TContainer>::ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const {
  return ToJson(name_id_mapper, enum_store, [](PropertyId) { return true; });
}

template <template <class...> class TContainer>
nlohmann::json TrackingInfo<TContainer>::ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store,
                                                const std::function<bool(PropertyId)> &property_predicate) const {
  nlohmann::json::object_t tracking_info;
  tracking_info.emplace("count", n.load());
  const auto &[prop_itr, _] = tracking_info.emplace("properties", nlohmann::json::array_t{});
  for (const auto &[p, info] : properties) {
    if (!property_predicate(p)) continue;
    prop_itr->second.emplace_back(info.ToJson(enum_store, name_id_mapper.IdToName(p.AsUint()), std::max(n.load(), 1)));
  }
  return tracking_info;
}

}  // namespace memgraph::storage

// A schema tracking instantiated with a third container needs its instantiation added here.
template struct memgraph::storage::PropertyInfo<std::unordered_map>;
template struct memgraph::storage::PropertyInfo<memgraph::utils::ConcurrentUnorderedMap>;
template struct memgraph::storage::TrackingInfo<std::unordered_map>;
template struct memgraph::storage::TrackingInfo<memgraph::utils::ConcurrentUnorderedMap>;
