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

#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schemas.hpp"

namespace memgraph::storage::v3 {

struct SchemaViolation {
  enum class ValidationStatus : uint8_t {
    NO_SCHEMA_DEFINED_FOR_LABEL,
    VERTEX_PROPERTY_WRONG_TYPE,
    VERTEX_UPDATE_PRIMARY_KEY,
    VERTEX_UPDATE_PRIMARY_LABEL,
    VERTEX_SECONDARY_LABEL_IS_PRIMARY,
    VERTEX_PRIMARY_PROPERTIES_UNDEFINED,
  };

  SchemaViolation(ValidationStatus status, LabelId label);

  SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_schema_property);

  SchemaViolation(ValidationStatus status, LabelId label, SchemaProperty violated_schema_property,
                  PropertyValue violated_property_value);

  friend bool operator==(const SchemaViolation &lhs, const SchemaViolation &rhs);

  ValidationStatus status;
  LabelId label;
  std::optional<SchemaProperty> violated_schema_property;
  std::optional<PropertyValue> violated_property_value;
};

class SchemaValidator {
 public:
  explicit SchemaValidator(Schemas &schemas);

  [[nodiscard]] std::optional<SchemaViolation> ValidateVertexCreate(
      LabelId primary_label, const std::vector<LabelId> &labels,
      const std::vector<PropertyValue> &primary_properties) const;

  [[nodiscard]] std::optional<SchemaViolation> ValidatePropertyUpdate(LabelId primary_label,
                                                                      PropertyId property_id) const;

  [[nodiscard]] std::optional<SchemaViolation> ValidateLabelUpdate(LabelId label) const;

  const Schemas::Schema *GetSchema(LabelId label) const;

 private:
  Schemas &schemas_;
};

struct VertexValidator {
  explicit VertexValidator(const SchemaValidator &schema_validator, LabelId primary_label);

  [[nodiscard]] std::optional<SchemaViolation> ValidatePropertyUpdate(PropertyId property_id) const;

  [[nodiscard]] std::optional<SchemaViolation> ValidateAddLabel(LabelId label) const;

  [[nodiscard]] std::optional<SchemaViolation> ValidateRemoveLabel(LabelId label) const;

  const SchemaValidator *schema_validator;

  LabelId primary_label_;
};

template <typename TValue>
using ResultSchema = utils::BasicResult<std::variant<SchemaViolation, Error>, TValue>;

}  // namespace memgraph::storage::v3
