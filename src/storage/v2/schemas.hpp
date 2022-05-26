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

#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/result.hpp"

namespace memgraph::storage {

class SchemaViolationException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

struct SchemaType {
  enum class Type : uint8_t { Bool, Int, Double, String, List, Map, Date, LocalTime, LocalDateTime, Duration };

  Type type;
  PropertyId property_id;
  std::shared_ptr<SchemaType> nested_value;
};

struct SchemaViolation {
  enum class ValidationStatus : uint8_t {
    VERTEX_HAS_NO_PRIMARY_LABEL,
    VERTEX_HAS_NO_PROPERTY,
    NO_SCHEMA_DEFINED_FOR_LABEL,
    VERTEX_PROPERTY_WRONG_TYPE
  };

  SchemaViolation(ValidationStatus status, LabelId label);

  SchemaViolation(ValidationStatus status, LabelId label, SchemaType violated_type);

  SchemaViolation(ValidationStatus status, LabelId label, SchemaType violated_type,
                  PropertyValue violated_property_value);

  ValidationStatus status;
  LabelId label;
  std::optional<SchemaType> violated_type;
  std::optional<PropertyValue> violated_property_value;
};

/// Structure that represents a collection of schemas
/// Schema can be mapped under only one label => primary label
class Schemas {
 public:
  using SchemasStructure = std::unordered_map<LabelId, std::vector<SchemaType>>;
  using SchemasList = std::vector<std::pair<LabelId, std::vector<SchemaType>>>;

  Schemas() = default;
  Schemas(const Schemas &) = delete;
  Schemas(Schemas &&) = delete;
  Schemas &operator=(const Schemas &) = delete;
  Schemas &operator=(Schemas &&) = delete;
  ~Schemas() = default;

  enum class CreationStatus : uint8_t { SUCCESS, FAIL };
  enum class DeletionStatus : uint8_t { SUCCESS, FAIL };

  [[nodiscard]] CreationStatus CreateSchema(LabelId label, const std::vector<SchemaType> &schemas_types);

  [[nodiscard]] DeletionStatus DeleteSchema(LabelId label);

  [[nodiscard]] std::optional<SchemaViolation> ValidateVertex(LabelId primary_label, const Vertex &vertex);

  [[nodiscard]] SchemasList ListSchemas() const;

 private:
  SchemasStructure schemas_;
};

inline std::optional<SchemaType::Type> PropertyValueTypeToSchemaType(const PropertyValue &property_value) {
  switch (property_value.type()) {
    case PropertyValue::Type::Bool: {
      return SchemaType::Type::Bool;
    }
    case PropertyValue::Type::Int: {
      return SchemaType::Type::Int;
    }
    case PropertyValue::Type::Double: {
      return SchemaType::Type::Double;
    }
    case PropertyValue::Type::String: {
      return SchemaType::Type::String;
    }
    case PropertyValue::Type::List: {
      return SchemaType::Type::List;
    }
    case PropertyValue::Type::Map: {
      return SchemaType::Type::Map;
    }
    case PropertyValue::Type::TemporalData: {
      switch (property_value.ValueTemporalData().type) {
        case TemporalType::Date: {
          return SchemaType::Type::Date;
        }
        case TemporalType::LocalDateTime: {
          return SchemaType::Type::LocalDateTime;
        }
        case TemporalType::LocalTime: {
          return SchemaType::Type::LocalTime;
        }
        case TemporalType::Duration: {
          return SchemaType::Type::Duration;
        }
      }
    }
    default: {
      return std::nullopt;
    }
  }
}

inline std::string SchemaTypeToString(const SchemaType::Type type) {
  switch (type) {
    case SchemaType::Type::Bool: {
      return "Bool";
    }
    case SchemaType::Type::Int: {
      return "Integer";
    }
    case SchemaType::Type::Double: {
      return "Double";
    }
    case SchemaType::Type::String: {
      return "String";
    }
    case SchemaType::Type::List: {
      return "List";
    }
    case SchemaType::Type::Map: {
      return "Map";
    }
    case SchemaType::Type::Date: {
      return "Date";
    }
    case SchemaType::Type::LocalTime: {
      return "LocalTime";
    }
    case SchemaType::Type::LocalDateTime: {
      return "LocalDateTime";
    }
    case SchemaType::Type::Duration: {
      return "Duration";
    }
  }
}

inline std::string PropertyTypeToString(const PropertyValue &property_value) {
  if (const auto schema_type = PropertyValueTypeToSchemaType(property_value); schema_type) {
    return SchemaTypeToString(*schema_type);
  }
  return "Unknown";
}

}  // namespace memgraph::storage
