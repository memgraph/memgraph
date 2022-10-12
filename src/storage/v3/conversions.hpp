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

#include "query/v2/requests.hpp"
#include "storage/v3/bindings/typed_value.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/memory.hpp"

#pragma once

namespace memgraph::storage::v3 {

template <typename TTypedValue>
TTypedValue PropertyToTypedValue(const PropertyValue &value) {
  switch (value.type()) {
    case storage::v3::PropertyValue::Type::Null:
      return TTypedValue();
    case storage::v3::PropertyValue::Type::Bool:
      return TTypedValue(value.ValueBool());
    case storage::v3::PropertyValue::Type::Int:
      return TTypedValue(value.ValueInt());
    case storage::v3::PropertyValue::Type::Double:
      return TTypedValue(value.ValueDouble());
    case storage::v3::PropertyValue::Type::String:
      return TTypedValue(value.ValueString());
    case storage::v3::PropertyValue::Type::List: {
      const auto &src = value.ValueList();
      std::vector<TTypedValue> dst;
      dst.reserve(src.size());
      for (const auto &elem : src) {
        dst.push_back(PropertyToTypedValue<TTypedValue>(elem));
      }
      return TTypedValue(std::move(dst));
    }
    case storage::v3::PropertyValue::Type::Map: {
      const auto &src = value.ValueMap();
      std::map<std::string, TTypedValue> dst;
      for (const auto &elem : src) {
        dst.insert({std::string(elem.first), PropertyToTypedValue<TTypedValue>(elem.second)});
      }
      return TTypedValue(std::move(dst));
    }
    case storage::v3::PropertyValue::Type::TemporalData: {
      const auto &temporal_data = value.ValueTemporalData();
      switch (temporal_data.type) {
        case storage::v3::TemporalType::Date: {
          return TTypedValue(utils::Date(temporal_data.microseconds));
        }
        case storage::v3::TemporalType::LocalTime: {
          return TTypedValue(utils::LocalTime(temporal_data.microseconds));
        }
        case storage::v3::TemporalType::LocalDateTime: {
          return TTypedValue(utils::LocalDateTime(temporal_data.microseconds));
        }
        case storage::v3::TemporalType::Duration: {
          return TTypedValue(utils::Duration(temporal_data.microseconds));
        }
      }
    }
  }
  LOG_FATAL("Unsupported type");
}

template <typename TTypedValue>
TTypedValue PropertyToTypedValue(const PropertyValue &value, utils::MemoryResource *mem) {
  switch (value.type()) {
    case storage::v3::PropertyValue::Type::Null:
      return TTypedValue(mem);
    case storage::v3::PropertyValue::Type::Bool:
      return TTypedValue(value.ValueBool(), mem);
    case storage::v3::PropertyValue::Type::Int:
      return TTypedValue(value.ValueInt(), mem);
    case storage::v3::PropertyValue::Type::Double:
      return TTypedValue(value.ValueDouble(), mem);
    case storage::v3::PropertyValue::Type::String:
      return TTypedValue(value.ValueString(), mem);
    case storage::v3::PropertyValue::Type::List: {
      const auto &src = value.ValueList();
      std::vector<TTypedValue> dst;
      dst.reserve(src.size());
      for (const auto &elem : src) {
        dst.push_back(PropertyToTypedValue<TTypedValue>(elem, mem));
      }
      return TTypedValue(std::move(dst), mem);
    }
    case storage::v3::PropertyValue::Type::Map: {
      const auto &src = value.ValueMap();
      std::map<std::string, TTypedValue> dst;
      for (const auto &elem : src) {
        dst.insert({std::string(elem.first), PropertyToTypedValue<TTypedValue>(elem.second, mem)});
      }
      return TTypedValue(std::move(dst), mem);
    }
    case storage::v3::PropertyValue::Type::TemporalData: {
      const auto &temporal_data = value.ValueTemporalData();
      switch (temporal_data.type) {
        case storage::v3::TemporalType::Date: {
          return TTypedValue(utils::Date(temporal_data.microseconds), mem);
        }
        case storage::v3::TemporalType::LocalTime: {
          return TTypedValue(utils::LocalTime(temporal_data.microseconds), mem);
        }
        case storage::v3::TemporalType::LocalDateTime: {
          return TTypedValue(utils::LocalDateTime(temporal_data.microseconds), mem);
        }
        case storage::v3::TemporalType::Duration: {
          return TTypedValue(utils::Duration(temporal_data.microseconds), mem);
        }
      }
    }
  }
  LOG_FATAL("Unsupported type");
}

template <typename TTypedValue>
storage::v3::PropertyValue TypedToPropertyValue(const TTypedValue &value) {
  switch (value.type()) {
    case TTypedValue::Type::Null:
      return storage::v3::PropertyValue{};
    case TTypedValue::Type::Bool:
      return storage::v3::PropertyValue(value.ValueBool());
    case TTypedValue::Type::Int:
      return storage::v3::PropertyValue(value.ValueInt());
    case TTypedValue::Type::Double:
      return storage::v3::PropertyValue(value.ValueDouble());
    case TTypedValue::Type::String:
      return storage::v3::PropertyValue(std::string(value.ValueString()));
    case TTypedValue::Type::List: {
      const auto &src = value.ValueList();
      std::vector<storage::v3::PropertyValue> dst;
      dst.reserve(src.size());
      std::transform(src.begin(), src.end(), std::back_inserter(dst),
                     [](const auto &val) { return TypedToPropertyValue(val); });
      return storage::v3::PropertyValue(std::move(dst));
    }
    case TTypedValue::Type::Map: {
      const auto &src = value.ValueMap();
      std::map<std::string, storage::v3::PropertyValue> dst;
      for (const auto &elem : src) {
        dst.insert({std::string(elem.first), TypedToPropertyValue(elem.second)});
      }
      return storage::v3::PropertyValue(std::move(dst));
    }
    case TTypedValue::Type::Date:
      return storage::v3::PropertyValue(
          storage::v3::TemporalData{storage::v3::TemporalType::Date, value.ValueDate().MicrosecondsSinceEpoch()});
    case TTypedValue::Type::LocalTime:
      return storage::v3::PropertyValue(storage::v3::TemporalData{storage::v3::TemporalType::LocalTime,
                                                                  value.ValueLocalTime().MicrosecondsSinceEpoch()});
    case TTypedValue::Type::LocalDateTime:
      return storage::v3::PropertyValue(storage::v3::TemporalData{storage::v3::TemporalType::LocalDateTime,
                                                                  value.ValueLocalDateTime().MicrosecondsSinceEpoch()});
    case TTypedValue::Type::Duration:
      return storage::v3::PropertyValue(
          storage::v3::TemporalData{storage::v3::TemporalType::Duration, value.ValueDuration().microseconds});
    default:
      break;
  }
  throw expr::TypedValueException("Unsupported conversion from TTypedValue to PropertyValue");
}

template <typename TTypedValue>
msgs::Value TypedValueToValue(const TTypedValue &value) {
  using Value = msgs::Value;
  switch (value.type()) {
    case TTypedValue::Type::Null:
      return {};
    case TTypedValue::Type::Bool:
      return Value(value.ValueBool());
    case TTypedValue::Type::Int:
      return Value(value.ValueInt());
    case TTypedValue::Type::Double:
      return Value(value.ValueDouble());
    case TTypedValue::Type::String:
      return Value(std::string(value.ValueString()));
    case TTypedValue::Type::List: {
      const auto &src = value.ValueList();
      std::vector<msgs::Value> dst;
      dst.reserve(src.size());
      std::transform(src.begin(), src.end(), std::back_inserter(dst),
                     [](const auto &val) { return TypedValueToValue(val); });
      return Value(std::move(dst));
    }
    case TTypedValue::Type::Map: {
      const auto &src = value.ValueMap();
      std::map<std::string, Value> dst;
      for (const auto &elem : src) {
        dst.insert({std::string(elem.first), TypedValueToValue(elem.second)});
      }
      return Value(std::move(dst));
    }
    default:
      break;
  }
  throw expr::TypedValueException("Unsupported conversion from TTypedValue to PropertyValue");
}
}  // namespace memgraph::storage::v3
