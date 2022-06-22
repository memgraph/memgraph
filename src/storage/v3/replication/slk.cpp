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

#include "storage/v3/replication/slk.hpp"

#include <type_traits>

#include "storage/v3/property_value.hpp"
#include "storage/v3/temporal.hpp"
#include "utils/cast.hpp"

namespace memgraph::slk {

void Save(const storage::v3::Gid &gid, slk::Builder *builder) { slk::Save(gid.AsUint(), builder); }

void Load(storage::v3::Gid *gid, slk::Reader *reader) {
  uint64_t value;
  slk::Load(&value, reader);
  *gid = storage::v3::Gid::FromUint(value);
}

void Load(storage::v3::PropertyValue::Type *type, slk::Reader *reader) {
  using PVTypeUnderlyingType = std::underlying_type_t<storage::v3::PropertyValue::Type>;
  PVTypeUnderlyingType value{};
  slk::Load(&value, reader);
  bool valid;
  switch (value) {
    case utils::UnderlyingCast(storage::v3::PropertyValue::Type::Null):
    case utils::UnderlyingCast(storage::v3::PropertyValue::Type::Bool):
    case utils::UnderlyingCast(storage::v3::PropertyValue::Type::Int):
    case utils::UnderlyingCast(storage::v3::PropertyValue::Type::Double):
    case utils::UnderlyingCast(storage::v3::PropertyValue::Type::String):
    case utils::UnderlyingCast(storage::v3::PropertyValue::Type::List):
    case utils::UnderlyingCast(storage::v3::PropertyValue::Type::Map):
    case utils::UnderlyingCast(storage::v3::PropertyValue::Type::TemporalData):
      valid = true;
      break;
    default:
      valid = false;
      break;
  }
  if (!valid) throw slk::SlkDecodeException("Trying to load unknown storage::v3::PropertyValue!");
  *type = static_cast<storage::v3::PropertyValue::Type>(value);
}

void Save(const storage::v3::PropertyValue &value, slk::Builder *builder) {
  switch (value.type()) {
    case storage::v3::PropertyValue::Type::Null:
      slk::Save(storage::v3::PropertyValue::Type::Null, builder);
      return;
    case storage::v3::PropertyValue::Type::Bool:
      slk::Save(storage::v3::PropertyValue::Type::Bool, builder);
      slk::Save(value.ValueBool(), builder);
      return;
    case storage::v3::PropertyValue::Type::Int:
      slk::Save(storage::v3::PropertyValue::Type::Int, builder);
      slk::Save(value.ValueInt(), builder);
      return;
    case storage::v3::PropertyValue::Type::Double:
      slk::Save(storage::v3::PropertyValue::Type::Double, builder);
      slk::Save(value.ValueDouble(), builder);
      return;
    case storage::v3::PropertyValue::Type::String:
      slk::Save(storage::v3::PropertyValue::Type::String, builder);
      slk::Save(value.ValueString(), builder);
      return;
    case storage::v3::PropertyValue::Type::List: {
      slk::Save(storage::v3::PropertyValue::Type::List, builder);
      const auto &values = value.ValueList();
      size_t size = values.size();
      slk::Save(size, builder);
      for (const auto &v : values) {
        slk::Save(v, builder);
      }
      return;
    }
    case storage::v3::PropertyValue::Type::Map: {
      slk::Save(storage::v3::PropertyValue::Type::Map, builder);
      const auto &map = value.ValueMap();
      size_t size = map.size();
      slk::Save(size, builder);
      for (const auto &kv : map) {
        slk::Save(kv, builder);
      }
      return;
    }
    case storage::v3::PropertyValue::Type::TemporalData: {
      slk::Save(storage::v3::PropertyValue::Type::TemporalData, builder);
      const auto temporal_data = value.ValueTemporalData();
      slk::Save(temporal_data.type, builder);
      slk::Save(temporal_data.microseconds, builder);
      return;
    }
  }
}

void Load(storage::v3::PropertyValue *value, slk::Reader *reader) {
  storage::v3::PropertyValue::Type type{};
  slk::Load(&type, reader);
  switch (type) {
    case storage::v3::PropertyValue::Type::Null:
      *value = storage::v3::PropertyValue();
      return;
    case storage::v3::PropertyValue::Type::Bool: {
      bool v;
      slk::Load(&v, reader);
      *value = storage::v3::PropertyValue(v);
      return;
    }
    case storage::v3::PropertyValue::Type::Int: {
      int64_t v;
      slk::Load(&v, reader);
      *value = storage::v3::PropertyValue(v);
      return;
    }
    case storage::v3::PropertyValue::Type::Double: {
      double v;
      slk::Load(&v, reader);
      *value = storage::v3::PropertyValue(v);
      return;
    }
    case storage::v3::PropertyValue::Type::String: {
      std::string v;
      slk::Load(&v, reader);
      *value = storage::v3::PropertyValue(std::move(v));
      return;
    }
    case storage::v3::PropertyValue::Type::List: {
      size_t size;
      slk::Load(&size, reader);
      std::vector<storage::v3::PropertyValue> list(size);
      for (size_t i = 0; i < size; ++i) {
        slk::Load(&list[i], reader);
      }
      *value = storage::v3::PropertyValue(std::move(list));
      return;
    }
    case storage::v3::PropertyValue::Type::Map: {
      size_t size;
      slk::Load(&size, reader);
      std::map<std::string, storage::v3::PropertyValue> map;
      for (size_t i = 0; i < size; ++i) {
        std::pair<std::string, storage::v3::PropertyValue> kv;
        slk::Load(&kv, reader);
        map.insert(kv);
      }
      *value = storage::v3::PropertyValue(std::move(map));
      return;
    }
    case storage::v3::PropertyValue::Type::TemporalData: {
      storage::v3::TemporalType temporal_type{};
      slk::Load(&temporal_type, reader);
      int64_t microseconds{0};
      slk::Load(&microseconds, reader);
      *value = storage::v3::PropertyValue(storage::v3::TemporalData{temporal_type, microseconds});
      return;
    }
  }
}

}  // namespace memgraph::slk
