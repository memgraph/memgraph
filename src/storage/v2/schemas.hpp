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

#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/v2/indices.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/result.hpp"

namespace memgraph::storage {

///
/// Structure that represents a collection of schemas
/// Schema can be mapped under only one label => primary label
class Schemas {
 public:
  using SchemasStructure = std::unordered_map<LabelId, std::vector<std::pair<PropertyId, PropertyValue::Type>>>;
  using SchemasList = std::vector<std::pair<LabelId, std::vector<std::pair<PropertyId, PropertyValue::Type>>>>;

  Schemas() = default;
  Schemas(const Schemas &) = delete;
  Schemas(Schemas &&) = delete;
  Schemas &operator=(const Schemas &) = delete;
  Schemas &operator=(Schemas &&) = delete;
  ~Schemas() = default;

  enum class CreationStatus : uint8_t { SUCCESS, FAIL };
  enum class DeletionStatus : uint8_t { SUCCESS, FAIL };
  enum class ValidationStatus : uint8_t {
    SUCCESS,
    VERTEX_DELETED,
    VERTEX_HAS_NO_PRIMARY_LABEL,
    VERTEX_HAS_NO_PROPERTY,
    NO_SCHEMA_DEFINED_FOR_LABEL,
    VERTEX_PROPERTY_WRONG_TYPE
  };

  CreationStatus CreateSchema(LabelId label,
                              const std::vector<std::pair<PropertyId, PropertyValue::Type>> &property_types);

  DeletionStatus DeleteSchema(LabelId label);

  ValidationStatus ValidateVertex(LabelId primary_label, const Vertex &vertex);

  SchemasList ListSchemas() const;

 private:
  SchemasStructure schemas_;
};

}  // namespace memgraph::storage
