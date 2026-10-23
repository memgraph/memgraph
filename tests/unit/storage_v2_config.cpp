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

#include "storage/v2/config.hpp"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

using memgraph::storage::SalientConfig;

TEST(StorageV2Config, Default) {
  SalientConfig::Items items{};
  EXPECT_FALSE(items.storage_light_edge);
}

TEST(StorageV2Config, RoundTrip) {
  // Every Items field is set away from its struct default, so a field missing from either direction
  // of the JSON pair is caught by the whole-struct comparison. gh5190: delta_on_identical_property_update
  // was absent from both to_json and from_json, so it silently reset to its default on read-back.
  SalientConfig::Items items{};
  items.properties_on_edges = false;
  items.enable_edges_metadata = true;
  items.enable_schema_metadata = true;
  items.enable_schema_info = true;
  items.enable_label_index_auto_creation = true;
  items.enable_edge_type_index_auto_creation = true;
  items.storage_light_edge = true;
  items.delta_on_identical_property_update = false;
  items.property_store_compression_enabled = true;

  nlohmann::json j;
  to_json(j, items);

  SalientConfig::Items result{};
  from_json(j, result);

  EXPECT_EQ(result, items) << "the JSON round-trip must preserve every Items field";
}

TEST(StorageV2Config, BackCompatMissingKey) {
  SalientConfig::Items items{};

  nlohmann::json j;
  to_json(j, items);
  j.erase("storage_light_edge");

  SalientConfig::Items result{};
  EXPECT_NO_THROW(from_json(j, result));
  EXPECT_FALSE(result.storage_light_edge);
}
