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
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/result.hpp"

namespace memgraph::storage {

struct SchemaProperty {
  PropertyId property_id;
  common::SchemaType type;
};

bool operator==(const SchemaProperty &lhs, const SchemaProperty &rhs);

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

  [[nodiscard]] std::optional<Schemas::Schema> GetSchema(LabelId primary_label) const;

  // Returns true if it was successfully created or false if the schema
  // already exists
  [[nodiscard]] bool CreateSchema(LabelId label, const std::vector<SchemaProperty> &schemas_types);

  // Returns true if it was successfully dropped or false if the schema
  // does not exist
  [[nodiscard]] bool DropSchema(LabelId label);

 private:
  SchemasMap schemas_;
};

std::optional<common::SchemaType> PropertyTypeToSchemaType(const PropertyValue &property_value);

std::string SchemaTypeToString(common::SchemaType type);

}  // namespace memgraph::storage
