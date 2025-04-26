// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"

// TODO: this can move to storage/v2

namespace memgraph::query::serialization {

nlohmann::json SerializeIntermediatePropertyValue(const storage::IntermediatePropertyValue &property_value,
                                                  memgraph::storage::Storage::Accessor *storage_acc);

nlohmann::json SerializeIntermediatePropertyValueVector(const std::vector<storage::IntermediatePropertyValue> &values,
                                                        memgraph::storage::Storage::Accessor *storage_acc);

nlohmann::json SerializeIntermediatePropertyValueMap(storage::IntermediatePropertyValue::map_t const &map,
                                                     memgraph::storage::Storage::Accessor *storage_acc);

storage::IntermediatePropertyValue DeserializeIntermediatePropertyValue(const nlohmann::json &data,
                                                                        storage::Storage::Accessor *storage_acc);

std::vector<storage::IntermediatePropertyValue> DeserializeIntermediatePropertyValueList(
    const nlohmann::json::array_t &data, storage::Storage::Accessor *storage_acc);

storage::IntermediatePropertyValue::map_t DeserializeIntermediatePropertyValueMap(
    nlohmann::json::object_t const &data, storage::Storage::Accessor *storage_acc);

}  // namespace memgraph::query::serialization
