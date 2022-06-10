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

struct SchemaProperty {
  enum class Type : uint8_t { Bool, Int, Double, String, Date, LocalTime, LocalDateTime, Duration };

  Type type;
  PropertyId property_id;
};

struct SchemaViolation {
  enum class ValidationStatus : uint8_t {
    VERTEX_HAS_NO_PRIMARY_LABEL,
    VERTEX_HAS_NO_PROPERTY,
    NO_SCHEMA_DEFINED_FOR_LABEL,
    VERTEX_PROPERTY_WRONG_TYPE
  };

  SchemaViolation(ValidationStatus status, LabelId label);

  SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_type);

  SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_type,
                  PropertyValue violated_property_value);

  ValidationStatus status;
  LabelId label;
  std::optional<SchemaProperty> violated_type;
  std::optional<PropertyValue> violated_property_value;
};

/// Structure that represents a collection of schemas
/// Schema can be mapped under only one label => primary label
class Schemas {
 public:
  using Schema = std::pair<LabelId, std::vector<SchemaProperty>>;
  using SchemasMap = std::unordered_map<LabelId, std::vector<SchemaProperty>>;
  using SchemasList = std::vector<Schema>;

  Schemas() = default;
  Schemas(const Schemas &) = delete;
  Schemas(Schemas &&) = delete;
  Schemas &operator=(const Schemas &) = delete;
  Schemas &operator=(Schemas &&) = delete;
  ~Schemas() = default;

  [[nodiscard]] SchemasList ListSchemas() const;

  [[nodiscard]] SchemasList GetSchema(LabelId primary_label) const;

  [[nodiscard]] bool CreateSchema(LabelId label, const std::vector<SchemaProperty> &schemas_types);

  [[nodiscard]] bool DeleteSchema(LabelId label);

  [[nodiscard]] std::optional<SchemaViolation> ValidateVertex(LabelId primary_label, const Vertex &vertex);

 private:
  SchemasMap schemas_;
};

inline std::optional<SchemaProperty::Type> PropertyValueTypeToSchemaProperty(const PropertyValue &property_value) {
  switch (property_value.type()) {
    case PropertyValue::Type::Bool: {
      return SchemaProperty::Type::Bool;
    }
    case PropertyValue::Type::Int: {
      return SchemaProperty::Type::Int;
    }
    case PropertyValue::Type::Double: {
      return SchemaProperty::Type::Double;
    }
    case PropertyValue::Type::String: {
      return SchemaProperty::Type::String;
    }
    case PropertyValue::Type::TemporalData: {
      switch (property_value.ValueTemporalData().type) {
        case TemporalType::Date: {
          return SchemaProperty::Type::Date;
        }
        case TemporalType::LocalDateTime: {
          return SchemaProperty::Type::LocalDateTime;
        }
        case TemporalType::LocalTime: {
          return SchemaProperty::Type::LocalTime;
        }
        case TemporalType::Duration: {
          return SchemaProperty::Type::Duration;
        }
      }
    }
    case PropertyValue::Type::Null:
    case PropertyValue::Type::Map:
    case PropertyValue::Type::List: {
      return std::nullopt;
    }
  }
}

inline std::string SchemaPropertyToString(const SchemaProperty::Type type) {
  switch (type) {
    case SchemaProperty::Type::Bool: {
      return "Bool";
    }
    case SchemaProperty::Type::Int: {
      return "Integer";
    }
    case SchemaProperty::Type::Double: {
      return "Double";
    }
    case SchemaProperty::Type::String: {
      return "String";
    }
    case SchemaProperty::Type::Date: {
      return "Date";
    }
    case SchemaProperty::Type::LocalTime: {
      return "LocalTime";
    }
    case SchemaProperty::Type::LocalDateTime: {
      return "LocalDateTime";
    }
    case SchemaProperty::Type::Duration: {
      return "Duration";
    }
  }
}

}  // namespace memgraph::storage
