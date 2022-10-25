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

#include "common/types.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/temporal.hpp"
#include "utils/result.hpp"

namespace memgraph::storage::v3 {

std::string SchemaTypeToString(common::SchemaType type);

struct SchemaProperty {
  PropertyId property_id;
  common::SchemaType type;

  friend bool operator==(const SchemaProperty &lhs, const SchemaProperty &rhs);

  friend std::ostream &operator<<(std::ostream &in, const SchemaProperty &schema_property) {
    in << "SchemaProperty { property_id: ", in << schema_property.property_id.AsUint();
    in << ", type: ";
    in << SchemaTypeToString(schema_property.type);
    in << " }";

    return in;
  }
};

/// Structure that represents a collection of schemas
/// Schema can be mapped under only one label => primary label
class Schemas {
 public:
  using SchemasMap = std::unordered_map<LabelId, std::vector<SchemaProperty>>;
  using Schema = SchemasMap::value_type;
  using SchemasList = std::vector<Schema>;

  Schemas() = default;
  Schemas(const Schemas &) = delete;
  Schemas(Schemas &&) = delete;
  Schemas &operator=(const Schemas &) = delete;
  Schemas &operator=(Schemas &&) = delete;
  ~Schemas() = default;

  [[nodiscard]] SchemasList ListSchemas() const;

  [[nodiscard]] const Schema *GetSchema(LabelId primary_label) const;

  // Returns true if it was successfully created or false if the schema
  // already exists
  [[nodiscard]] bool CreateSchema(LabelId primary_label, const std::vector<SchemaProperty> &schemas_types);

  // Returns true if it was successfully dropped or false if the schema
  // does not exist
  [[nodiscard]] bool DropSchema(LabelId primary_label);

  // Returns true if property is part of schema defined
  // by primary label
  [[nodiscard]] bool IsPropertyKey(LabelId primary_label, PropertyId property_id) const;

 private:
  SchemasMap schemas_;
};

std::optional<common::SchemaType> PropertyTypeToSchemaType(const PropertyValue &property_value);

}  // namespace memgraph::storage::v3
