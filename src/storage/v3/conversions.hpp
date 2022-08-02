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

#include "storage/v3/property_value.hpp"

#pragma once

namespace memgraph::storage::v3 {

template <typename TypedValue>
TypedValue PropertyToTypedValue(const PropertyValue &value) {
  switch (value.type()) {
    case storage::v3::PropertyValue::Type::Null:
      return TypedValue();
    case storage::v3::PropertyValue::Type::Bool:
      return TypedValue(value.ValueBool());
    case storage::v3::PropertyValue::Type::Int:
      return TypedValue(value.ValueInt());
    case storage::v3::PropertyValue::Type::Double:
      return TypedValue(value.ValueDouble());
    case storage::v3::PropertyValue::Type::String:
      return TypedValue(value.ValueString());
    case storage::v3::PropertyValue::Type::List: {
      const auto &src = value.ValueList();
      std::vector<TypedValue> dst;
      dst.reserve(src.size());
      // std::transform(src.begin(), src.end(), std::back_inserter(dst), [](const auto& elem) { return
      // PropertyToTypedValue(elem); });
      for (const auto &elem : src) {
        dst.push_back(PropertyToTypedValue<TypedValue>(elem));
        //      std::transform(src.begin(), src.end(), std::inserter(dst, dst.end()), [](const auto &pr) {
        //        return std::pair(pr.first, TypedToPropertyValue(pr.second));
        //      });
      }
      return TypedValue(std::move(dst));
    }
    case storage::v3::PropertyValue::Type::Map: {
      const auto &src = value.ValueMap();
      std::map<std::string, TypedValue> dst;
      for (const auto &elem : src) {
        dst.insert({std::string(elem.first), PropertyToTypedValue<TypedValue>(elem.second)});
        //      std::transform(src.begin(), src.end(), std::inserter(dst, dst.end()), [](const auto &pr) {
        //        return std::pair(pr.first, TypedToPropertyValue(pr.second));
        //      });
      }
      return TypedValue(std::move(dst));
    }
    case storage::v3::PropertyValue::Type::TemporalData: {
      const auto &temporal_data = value.ValueTemporalData();
      switch (temporal_data.type) {
        case storage::v3::TemporalType::Date: {
          return TypedValue(utils::Date(temporal_data.microseconds));
        }
        case storage::v3::TemporalType::LocalTime: {
          return TypedValue(utils::LocalTime(temporal_data.microseconds));
        }
        case storage::v3::TemporalType::LocalDateTime: {
          return TypedValue(utils::LocalDateTime(temporal_data.microseconds));
        }
        case storage::v3::TemporalType::Duration: {
          return TypedValue(utils::Duration(temporal_data.microseconds));
        }
      }
    }
  }
  LOG_FATAL("Unsupported type");
}

template <typename TypedValue>
storage::v3::PropertyValue TypedToPropertyValue(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return storage::v3::PropertyValue();
    case TypedValue::Type::Bool:
      return storage::v3::PropertyValue(value.ValueBool());
    case TypedValue::Type::Int:
      return storage::v3::PropertyValue(value.ValueInt());
    case TypedValue::Type::Double:
      return storage::v3::PropertyValue(value.ValueDouble());
    case TypedValue::Type::String:
      return storage::v3::PropertyValue(std::string(value.ValueString()));
    case TypedValue::Type::List: {
      const auto &src = value.ValueList();
      std::vector<storage::v3::PropertyValue> dst;
      dst.reserve(src.size());
      std::transform(src.begin(), src.end(), std::back_inserter(dst),
                     [](const auto &val) { return TypedToPropertyValue(val); });
      return storage::v3::PropertyValue(std::move(dst));
    }
    case TypedValue::Type::Map: {
      const auto &src = value.ValueMap();
      std::map<std::string, storage::v3::PropertyValue> dst;
      for (const auto &elem : src) {
        dst.insert({std::string(elem.first), TypedToPropertyValue(elem.second)});
        //      std::transform(src.begin(), src.end(), std::inserter(dst, dst.end()), [](const auto &pr) {
        //        return std::pair(pr.first, TypedToPropertyValue(pr.second));
        //      });
      }
      return storage::v3::PropertyValue(std::move(dst));
    }
    case TypedValue::Type::Date:
      return storage::v3::PropertyValue(
          storage::v3::TemporalData{storage::v3::TemporalType::Date, value.ValueDate().MicrosecondsSinceEpoch()});
    case TypedValue::Type::LocalTime:
      return storage::v3::PropertyValue(storage::v3::TemporalData{storage::v3::TemporalType::LocalTime,
                                                                  value.ValueLocalTime().MicrosecondsSinceEpoch()});
    case TypedValue::Type::LocalDateTime:
      return storage::v3::PropertyValue(storage::v3::TemporalData{storage::v3::TemporalType::LocalDateTime,
                                                                  value.ValueLocalDateTime().MicrosecondsSinceEpoch()});
    case TypedValue::Type::Duration:
      return storage::v3::PropertyValue(
          storage::v3::TemporalData{storage::v3::TemporalType::Duration, value.ValueDuration().microseconds});
    default:
      break;
  }
  throw expr::TypedValueException("Unsupported conversion from TypedValue to PropertyValue");
}
}  // namespace memgraph::storage::v3
