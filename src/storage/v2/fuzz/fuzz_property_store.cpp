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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>

#include "helpers/read_parse.hpp"
#include "storage/v2/property_store.hpp"

using namespace memgraph::storage;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyValue;
using memgraph::utils::Timezone;

namespace {

bool shouldBeEqual(PropertyValue const &val) {
  switch (val.type()) {
    case PropertyValueType::Double: {
      return !isnanl(val.ValueDouble());
    }
    case PropertyValueType::List: {
      auto const &l = val.ValueList();
      return std::all_of(l.begin(), l.end(), [](auto const &v) { return shouldBeEqual(v); });
    }
    case PropertyValueType::Map: {
      auto const &m = val.ValueMap();
      return std::all_of(m.begin(), m.end(), [](auto const &v) { return shouldBeEqual(v.second); });
    }
    case PropertyValueType::Null:
    case PropertyValueType::Bool:
    case PropertyValueType::Int:
    case PropertyValueType::String:
    case PropertyValueType::TemporalData:
    case PropertyValueType::ZonedTemporalData:
    case PropertyValueType::Enum: {
      return true;
    }
    case PropertyValueType::Point2d: {
      auto const &p = val.ValuePoint2d();
      return !isnanl(p.x()) && !isnanl(p.y());
    }
    case PropertyValueType::Point3d: {
      auto const &p = val.ValuePoint3d();
      return !isnanl(p.x()) && !isnanl(p.y()) && !isnanl(p.z());
    }
  }
};

bool Validate(PropertyStore const &store, std::map<PropertyId, PropertyValue> const &properties) {
  std::vector<PropertyId> property_ids;
  for (const auto &[id, value] : properties) {
    if (value.IsNull()) {
      // should not store nulls
      if (store.HasProperty(id)) {
        return false;
      }
      if (store.PropertySize(id) != 0) {
        return false;
      }
    } else {
      property_ids.emplace_back(id);
      // otherwise value should exist within store
      if (!store.HasProperty(id)) {
        return false;
      }
      if (store.PropertySize(id) == 0) {
        return false;
      }
    }

    auto retrievedValue = store.GetProperty(id);
    if (shouldBeEqual(retrievedValue)) {
      if (retrievedValue != value) {
        return false;
      }
      if (!store.IsPropertyEqual(id, value)) {
        return false;
      }
    } else {
      if (retrievedValue == value) {
        return false;
      }
      if (store.IsPropertyEqual(id, value)) {
        return false;
      }
    }
  }

  // For all stored ids
  if (!store.HasAllProperties(std::set(property_ids.begin(), property_ids.end()))) {
    return false;
  }
  if (store.ExtractPropertyIds() != property_ids) {
    return false;
  }

  return true;
}

bool ApplyCommand(PropertyStore &store, std::map<PropertyId, PropertyValue> &properties, const uint8_t *&data,
                  size_t &size) {
  using namespace fuzz::helper;
  uint8_t cmd;
  if (!ReadPrimitive(data, size, cmd)) return false;

  enum Command : uint8_t {
    CMD_SET = 0,
    CMD_REMOVE = 1,
    CMD_UPDATE = 2,
    CMD_COMPRESSION_TOGGLE = 3,
  };

  switch (cmd % 4) {
    case CMD_SET: {
      auto id = ParsePropertyId(data, size);
      if (!id) return false;
      auto value = ParsePropertyValue(data, size);
      if (!value) return false;
      store.SetProperty(*id, *value);
      properties[*id] = *std::move(value);
      return true;
    }
    case CMD_REMOVE: {
      auto id = ParsePropertyId(data, size);
      if (!id) return false;
      store.SetProperty(*id, {});
      properties.erase(*id);
      return true;
    }
    case CMD_UPDATE: {
      auto new_properties = ParsePropertyMap(data, size);
      if (!new_properties) return false;
      store.UpdateProperties(*new_properties);
      for (auto &[id, value] : *new_properties) {
        if (value.IsNull()) {
          properties.erase(id);
        } else {
          properties[id] = value;
        }
      }
      return true;
    }
    case CMD_COMPRESSION_TOGGLE: {
      FLAGS_storage_property_store_compression_enabled = !FLAGS_storage_property_store_compression_enabled;
      return true;
    }
    default: {
      return false;
    }
  }
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(std::uint8_t const *data, std::size_t size) {
  auto store = PropertyStore{};
  auto properties = std::map<PropertyId, PropertyValue>{};
  while (true) {
    if (!ApplyCommand(store, properties, data, size)) return 0;
    if (!Validate(store, properties)) {
      abort();
    }
  }
}
