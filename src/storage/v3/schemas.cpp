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

#include "storage/v3/schemas.hpp"

#include <unordered_map>
#include <vector>

#include "storage/v3/property_value.hpp"

namespace memgraph::storage::v3 {

bool operator==(const SchemaProperty &lhs, const SchemaProperty &rhs) {
  return lhs.property_id == rhs.property_id && lhs.type == rhs.type;
}

Schemas::SchemasList Schemas::ListSchemas() const {
  Schemas::SchemasList ret;
  ret.reserve(schemas_.size());
  std::transform(schemas_.begin(), schemas_.end(), std::back_inserter(ret),
                 [](const auto &schema_property_type) { return schema_property_type; });
  return ret;
}

const Schemas::Schema *Schemas::GetSchema(const LabelId primary_label) const {
  if (auto schema_map = schemas_.find(primary_label); schema_map != schemas_.end()) {
    return &*schema_map;
  }
  return nullptr;
}

bool Schemas::CreateSchema(const LabelId primary_label, const std::vector<SchemaProperty> &schemas_types) {
  if (schemas_.contains(primary_label)) {
    return false;
  }
  schemas_.emplace(primary_label, schemas_types);
  return true;
}

bool Schemas::DropSchema(const LabelId primary_label) { return schemas_.erase(primary_label); }

std::optional<common::SchemaType> PropertyTypeToSchemaType(const PropertyValue &property_value) {
  switch (property_value.type()) {
    case PropertyValue::Type::Bool: {
      return common::SchemaType::BOOL;
    }
    case PropertyValue::Type::Int: {
      return common::SchemaType::INT;
    }
    case PropertyValue::Type::String: {
      return common::SchemaType::STRING;
    }
    case PropertyValue::Type::TemporalData: {
      switch (property_value.ValueTemporalData().type) {
        case TemporalType::Date: {
          return common::SchemaType::DATE;
        }
        case TemporalType::LocalDateTime: {
          return common::SchemaType::LOCALDATETIME;
        }
        case TemporalType::LocalTime: {
          return common::SchemaType::LOCALTIME;
        }
        case TemporalType::Duration: {
          return common::SchemaType::DURATION;
        }
      }
    }
    case PropertyValue::Type::Double:
    case PropertyValue::Type::Null:
    case PropertyValue::Type::Map:
    case PropertyValue::Type::List: {
      return std::nullopt;
    }
  }
}

std::string SchemaTypeToString(const common::SchemaType type) {
  switch (type) {
    case common::SchemaType::BOOL: {
      return "Bool";
    }
    case common::SchemaType::INT: {
      return "Integer";
    }
    case common::SchemaType::STRING: {
      return "String";
    }
    case common::SchemaType::DATE: {
      return "Date";
    }
    case common::SchemaType::LOCALTIME: {
      return "LocalTime";
    }
    case common::SchemaType::LOCALDATETIME: {
      return "LocalDateTime";
    }
    case common::SchemaType::DURATION: {
      return "Duration";
    }
  }
}

}  // namespace memgraph::storage::v3
