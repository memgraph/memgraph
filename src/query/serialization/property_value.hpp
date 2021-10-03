// Copyright 2021 Memgraph Ltd.
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

namespace query::serialization {

nlohmann::json SerializePropertyValue(const storage::PropertyValue &property_value);

nlohmann::json SerializePropertyValueVector(const std::vector<storage::PropertyValue> &values);

nlohmann::json SerializePropertyValueMap(const std::map<std::string, storage::PropertyValue> &parameters);

storage::PropertyValue DeserializePropertyValue(const nlohmann::json &data);

std::vector<storage::PropertyValue> DeserializePropertyValueList(const nlohmann::json::array_t &data);

std::map<std::string, storage::PropertyValue> DeserializePropertyValueMap(const nlohmann::json::object_t &data);

}  // namespace query::serialization
