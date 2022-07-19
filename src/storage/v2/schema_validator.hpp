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

#include <optional>
#include <variant>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/schemas.hpp"

namespace memgraph::storage {

struct SchemaViolation {
  enum class ValidationStatus : uint8_t {
    VERTEX_HAS_NO_PROPERTY,
    NO_SCHEMA_DEFINED_FOR_LABEL,
    VERTEX_PROPERTY_WRONG_TYPE,
    VERTEX_UPDATE_PRIMARY_KEY,
    VERTEX_ALREADY_HAS_PRIMARY_LABEL,
    VERTEX_SECONDARY_LABEL_IS_PRIMARY,
  };

  SchemaViolation(ValidationStatus status, LabelId label);

  SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_schema_property);

  SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_schema_property,
                  PropertyValue violated_property_value);

  ValidationStatus status;
  LabelId label;
  std::optional<SchemaProperty> violated_schema_property;
  std::optional<PropertyValue> violated_property_value;
};

bool operator==(const SchemaViolation &lhs, const SchemaViolation &rhs);

class SchemaValidator {
 public:
  explicit SchemaValidator(Schemas &schemas);

  [[nodiscard]] std::optional<SchemaViolation> ValidateVertexCreate(
      LabelId primary_label, const std::vector<LabelId> &labels,
      const std::vector<std::pair<PropertyId, PropertyValue>> &properties) const;

  [[nodiscard]] std::optional<SchemaViolation> ValidateVertexUpdate(LabelId primary_label,
                                                                    PropertyId property_id) const;

  [[nodiscard]] std::optional<SchemaViolation> ValidateLabelUpdate(LabelId label) const;

 private:
  storage::Schemas &schemas_;
};

template <typename TValue>
using ResultSchema = utils::BasicResult<std::variant<SchemaViolation, Error>, TValue>;

}  // namespace memgraph::storage
