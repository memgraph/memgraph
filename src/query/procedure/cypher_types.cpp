// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/procedure/cypher_types.hpp"

#include "query/procedure/mg_procedure_impl.hpp"

namespace memgraph::query::procedure {

bool AnyType::SatisfiesType(const mgp_value &value) const { return value.type != MGP_VALUE_TYPE_NULL; }
bool BoolType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_BOOL; }
bool StringType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_STRING; }
bool IntType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_INT; }
bool FloatType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_DOUBLE; }
bool NumberType::SatisfiesType(const mgp_value &value) const {
  return value.type == MGP_VALUE_TYPE_INT || value.type == MGP_VALUE_TYPE_DOUBLE;
}
bool NodeType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_VERTEX; }
bool RelationshipType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_EDGE; }
bool PathType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_PATH; }
bool MapType::SatisfiesType(const mgp_value &value) const {
  return value.type == MGP_VALUE_TYPE_MAP || value.type == MGP_VALUE_TYPE_VERTEX || value.type == MGP_VALUE_TYPE_EDGE;
}
bool DateType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_DATE; }
bool LocalTimeType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_LOCAL_TIME; }
bool LocalDateTimeType::SatisfiesType(const mgp_value &value) const {
  return value.type == MGP_VALUE_TYPE_LOCAL_DATE_TIME;
}
bool DurationType::SatisfiesType(const mgp_value &value) const { return value.type == MGP_VALUE_TYPE_DURATION; }
bool ListType::SatisfiesType(const mgp_value &value) const {
  if (value.type != MGP_VALUE_TYPE_LIST) {
    return false;
  }
  auto *list = value.list_v;
  const auto list_size = list->elems.size();
  for (size_t i = 0; i < list_size; ++i) {
    if (!element_type_->SatisfiesType(list->elems[i])) {
      return false;
    };
  }
  return true;
}
bool NullableType::SatisfiesType(const mgp_value &value) const {
  return value.type == MGP_VALUE_TYPE_NULL || type_->SatisfiesType(value);
}
}  // namespace memgraph::query::procedure
