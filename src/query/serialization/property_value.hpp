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

#include "storage/v2/property_value.hpp"

namespace memgraph::query::serialization {

nlohmann::json SerializePropertyValue(const storage::PropertyValue &property_value);

nlohmann::json SerializePropertyValueVector(const std::vector<storage::PropertyValue> &values);

nlohmann::json SerializePropertyValueMap(const storage::PropertyValue::map_t &parameters);

storage::PropertyValue DeserializePropertyValue(const nlohmann::json &data);

std::vector<storage::PropertyValue> DeserializePropertyValueList(const nlohmann::json::array_t &data);

storage::PropertyValue::map_t DeserializePropertyValueMap(const nlohmann::json::object_t &data);

}  // namespace memgraph::query::serialization
