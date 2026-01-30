// Copyright 2026 Memgraph Ltd.
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

#include <nlohmann/json.hpp>
#include "query/typed_value.hpp"

namespace memgraph::query::serialization {

/**
 * @brief Serialize a TypedValue to JSON.
 * @param value The TypedValue to serialize
 * @return JSON representation of the TypedValue
 */
nlohmann::json SerializeTypedValue(const TypedValue &value);

/**
 * @brief Deserialize a TypedValue from JSON.
 * @param data The JSON data to deserialize
 * @return TypedValue representation of the JSON data
 */
TypedValue DeserializeTypedValue(const nlohmann::json &data);

}  // namespace memgraph::query::serialization
