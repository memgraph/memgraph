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

#include "storage/v2/result.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schemas.hpp"

namespace memgraph::storage::v3 {

class SchemaValidator {
 public:
  explicit SchemaValidator(Schemas &schemas, const NameIdMapper &name_id_mapper);

  [[nodiscard]] std::optional<ShardError> ValidateVertexCreate(
      LabelId primary_label, const std::vector<LabelId> &labels,
      const std::vector<PropertyValue> &primary_properties) const;

  [[nodiscard]] std::optional<ShardError> ValidatePropertyUpdate(LabelId primary_label, PropertyId property_id) const;

  [[nodiscard]] std::optional<ShardError> ValidateLabelUpdate(LabelId label) const;

  const Schemas::Schema *GetSchema(LabelId label) const;

 private:
  Schemas *schemas_;
  const NameIdMapper *name_id_mapper_;
};

struct VertexValidator {
  explicit VertexValidator(const SchemaValidator &schema_validator, LabelId primary_label);

  [[nodiscard]] std::optional<ShardError> ValidatePropertyUpdate(PropertyId property_id) const;

  [[nodiscard]] std::optional<ShardError> ValidateAddLabel(LabelId label) const;

  [[nodiscard]] std::optional<ShardError> ValidateRemoveLabel(LabelId label) const;

  const SchemaValidator *schema_validator;

  LabelId primary_label_;
};

}  // namespace memgraph::storage::v3
