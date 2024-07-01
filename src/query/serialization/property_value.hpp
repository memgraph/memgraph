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

#pragma once

#include <json/json.hpp>

#include "query/db_accessor.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query::serialization {

nlohmann::json SerializePropertyValue(const storage::PropertyValue &property_value,
                                      memgraph::query::DbAccessor *db_accessor);

nlohmann::json SerializePropertyValueVector(const std::vector<storage::PropertyValue> &values,
                                            memgraph::query::DbAccessor *db_accessor);

nlohmann::json SerializePropertyValueMap(storage::PropertyValue::map_t const &parameters,
                                         memgraph::query::DbAccessor *db_accessor);

storage::PropertyValue DeserializePropertyValue(const nlohmann::json &data, DbAccessor *db_accessor);

std::vector<storage::PropertyValue> DeserializePropertyValueList(const nlohmann::json::array_t &data,
                                                                 DbAccessor *db_accessor);

storage::PropertyValue::map_t DeserializePropertyValueMap(nlohmann::json::object_t const &data,
                                                          DbAccessor *db_accessor);

}  // namespace memgraph::query::serialization
