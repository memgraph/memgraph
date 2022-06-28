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

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/types.hpp"
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

struct SchemaPropertyType {
  common::SchemaType type;
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

  SchemaViolation(ValidationStatus status, LabelId label, SchemaPropertyType violated_type);

  SchemaViolation(ValidationStatus status, LabelId label, SchemaPropertyType violated_type,
                  PropertyValue violated_property_value);

  ValidationStatus status;
  LabelId label;
  std::optional<SchemaPropertyType> violated_type;
  std::optional<PropertyValue> violated_property_value;
};

/// Structure that represents a collection of schemas
/// Schema can be mapped under only one label => primary label
class Schemas {
 public:
  using Schema = std::pair<LabelId, std::vector<SchemaPropertyType>>;
  using SchemasMap = std::unordered_map<LabelId, std::vector<SchemaPropertyType>>;
  using SchemasList = std::vector<Schema>;

  Schemas() = default;
  Schemas(const Schemas &) = delete;
  Schemas(Schemas &&) = delete;
  Schemas &operator=(const Schemas &) = delete;
  Schemas &operator=(Schemas &&) = delete;
  ~Schemas() = default;

  [[nodiscard]] SchemasList ListSchemas() const;

  [[nodiscard]] SchemasList GetSchema(LabelId primary_label) const;

  // Returns true if it was successfully created or false if the schema
  // already exists
  [[nodiscard]] bool CreateSchema(LabelId label, const std::vector<SchemaPropertyType> &schemas_types);

  // Returns true if it was successfully dropped or false if the schema
  // does not exist
  [[nodiscard]] bool DropSchema(LabelId label);

  [[nodiscard]] std::optional<SchemaViolation> ValidateVertex(LabelId primary_label, const Vertex &vertex);

 private:
  SchemasMap schemas_;
};

inline std::optional<common::SchemaType> PropertyTypeToSchemaType(const PropertyValue &property_value) {
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

inline std::string SchemaTypeToString(const common::SchemaType type) {
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

}  // namespace memgraph::storage
