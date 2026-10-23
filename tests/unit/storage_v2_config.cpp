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
  // Every Items field is set away from its struct default, so a field missing from either direction of
  // the JSON pair is caught by the whole-struct comparison.
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

TEST(StorageV2Config, SalientConfigRoundTrip) {
  // Regression: the hand-written pair serialized SalientConfig as a JSON array (initializer list with
  // a bare trailing key/value) and dropped delta_on_identical_property_update from both directions.
  SalientConfig config;
  config.name = "tenant";
  config.uuid = memgraph::utils::UUID{};
  config.storage_mode = memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL;
  config.property_store_compression_level = memgraph::utils::CompressionLevel::HIGH;
  config.items.properties_on_edges = false;
  config.items.enable_edges_metadata = true;
  config.items.enable_schema_metadata = true;
  config.items.enable_schema_info = true;
  config.items.enable_label_index_auto_creation = true;
  config.items.enable_edge_type_index_auto_creation = true;
  config.items.storage_light_edge = true;
  config.items.delta_on_identical_property_update = false;
  config.items.property_store_compression_enabled = true;

  nlohmann::json j;
  to_json(j, config);
  ASSERT_TRUE(j.is_object()) << "the SalientConfig JSON pair must emit an object of the 13 fields";

  for (auto const *key : {"name", "uuid", "storage_mode", "property_store_compression_level"}) {
    EXPECT_TRUE(j.contains(key)) << "the SalientConfig JSON object must carry the top-level field " << key;
  }

  SalientConfig result{};
  from_json(j, result);

  EXPECT_EQ(result, config) << "the JSON round-trip must preserve every SalientConfig field";
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
