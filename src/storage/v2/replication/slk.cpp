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

#include "storage/v2/replication/slk.hpp"

#include <type_traits>

#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/cast.hpp"

namespace memgraph::slk {

void Save(const memgraph::storage::Gid &gid, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(gid.AsUint(), builder);
}

void Load(memgraph::storage::Gid *gid, memgraph::slk::Reader *reader) {
  uint64_t value;
  memgraph::slk::Load(&value, reader);
  *gid = memgraph::storage::Gid::FromUint(value);
}

void Load(memgraph::storage::PropertyValue::Type *type, memgraph::slk::Reader *reader) {
  using PVTypeUnderlyingType = std::underlying_type_t<memgraph::storage::PropertyValue::Type>;
  PVTypeUnderlyingType value{};
  memgraph::slk::Load(&value, reader);
  bool valid;
  switch (value) {
    case utils::UnderlyingCast(memgraph::storage::PropertyValue::Type::Null):
    case utils::UnderlyingCast(memgraph::storage::PropertyValue::Type::Bool):
    case utils::UnderlyingCast(memgraph::storage::PropertyValue::Type::Int):
    case utils::UnderlyingCast(memgraph::storage::PropertyValue::Type::Double):
    case utils::UnderlyingCast(memgraph::storage::PropertyValue::Type::String):
    case utils::UnderlyingCast(memgraph::storage::PropertyValue::Type::List):
    case utils::UnderlyingCast(memgraph::storage::PropertyValue::Type::Map):
    case utils::UnderlyingCast(memgraph::storage::PropertyValue::Type::TemporalData):
      valid = true;
      break;
    default:
      valid = false;
      break;
  }
  if (!valid) throw slk::SlkDecodeException("Trying to load unknown memgraph::storage::PropertyValue!");
  *type = static_cast<memgraph::storage::PropertyValue::Type>(value);
}

void Save(const memgraph::storage::PropertyValue &value, memgraph::slk::Builder *builder) {
  switch (value.type()) {
    case memgraph::storage::PropertyValue::Type::Null:
      memgraph::slk::Save(memgraph::storage::PropertyValue::Type::Null, builder);
      return;
    case memgraph::storage::PropertyValue::Type::Bool:
      memgraph::slk::Save(memgraph::storage::PropertyValue::Type::Bool, builder);
      memgraph::slk::Save(value.ValueBool(), builder);
      return;
    case memgraph::storage::PropertyValue::Type::Int:
      memgraph::slk::Save(memgraph::storage::PropertyValue::Type::Int, builder);
      memgraph::slk::Save(value.ValueInt(), builder);
      return;
    case memgraph::storage::PropertyValue::Type::Double:
      memgraph::slk::Save(memgraph::storage::PropertyValue::Type::Double, builder);
      memgraph::slk::Save(value.ValueDouble(), builder);
      return;
    case memgraph::storage::PropertyValue::Type::String:
      memgraph::slk::Save(memgraph::storage::PropertyValue::Type::String, builder);
      memgraph::slk::Save(value.ValueString(), builder);
      return;
    case memgraph::storage::PropertyValue::Type::List: {
      memgraph::slk::Save(memgraph::storage::PropertyValue::Type::List, builder);
      const auto &values = value.ValueList();
      size_t size = values.size();
      memgraph::slk::Save(size, builder);
      for (const auto &v : values) {
        memgraph::slk::Save(v, builder);
      }
      return;
    }
    case memgraph::storage::PropertyValue::Type::Map: {
      memgraph::slk::Save(memgraph::storage::PropertyValue::Type::Map, builder);
      const auto &map = value.ValueMap();
      size_t size = map.size();
      memgraph::slk::Save(size, builder);
      for (const auto &kv : map) {
        memgraph::slk::Save(kv, builder);
      }
      return;
    }
    case memgraph::storage::PropertyValue::Type::TemporalData: {
      memgraph::slk::Save(memgraph::storage::PropertyValue::Type::TemporalData, builder);
      const auto temporal_data = value.ValueTemporalData();
      memgraph::slk::Save(temporal_data.type, builder);
      memgraph::slk::Save(temporal_data.microseconds, builder);
      return;
    }
  }
}

void Load(memgraph::storage::PropertyValue *value, memgraph::slk::Reader *reader) {
  memgraph::storage::PropertyValue::Type type{};
  memgraph::slk::Load(&type, reader);
  switch (type) {
    case memgraph::storage::PropertyValue::Type::Null:
      *value = memgraph::storage::PropertyValue();
      return;
    case memgraph::storage::PropertyValue::Type::Bool: {
      bool v;
      memgraph::slk::Load(&v, reader);
      *value = memgraph::storage::PropertyValue(v);
      return;
    }
    case memgraph::storage::PropertyValue::Type::Int: {
      int64_t v;
      memgraph::slk::Load(&v, reader);
      *value = memgraph::storage::PropertyValue(v);
      return;
    }
    case memgraph::storage::PropertyValue::Type::Double: {
      double v;
      memgraph::slk::Load(&v, reader);
      *value = memgraph::storage::PropertyValue(v);
      return;
    }
    case memgraph::storage::PropertyValue::Type::String: {
      std::string v;
      memgraph::slk::Load(&v, reader);
      *value = memgraph::storage::PropertyValue(std::move(v));
      return;
    }
    case memgraph::storage::PropertyValue::Type::List: {
      size_t size;
      memgraph::slk::Load(&size, reader);
      std::vector<memgraph::storage::PropertyValue> list(size);
      for (size_t i = 0; i < size; ++i) {
        memgraph::slk::Load(&list[i], reader);
      }
      *value = memgraph::storage::PropertyValue(std::move(list));
      return;
    }
    case memgraph::storage::PropertyValue::Type::Map: {
      size_t size;
      memgraph::slk::Load(&size, reader);
      std::map<std::string, memgraph::storage::PropertyValue> map;
      for (size_t i = 0; i < size; ++i) {
        std::pair<std::string, memgraph::storage::PropertyValue> kv;
        memgraph::slk::Load(&kv, reader);
        map.insert(kv);
      }
      *value = memgraph::storage::PropertyValue(std::move(map));
      return;
    }
    case memgraph::storage::PropertyValue::Type::TemporalData: {
      memgraph::storage::TemporalType temporal_type{};
      memgraph::slk::Load(&temporal_type, reader);
      int64_t microseconds{0};
      memgraph::slk::Load(&microseconds, reader);
      *value = memgraph::storage::PropertyValue(memgraph::storage::TemporalData{temporal_type, microseconds});
      return;
    }
  }
}

}  // namespace memgraph::slk
