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
  SalientConfig::Items items{};
  items.storage_light_edge = true;

  nlohmann::json j;
  to_json(j, items);

  SalientConfig::Items result{};
  from_json(j, result);

  EXPECT_TRUE(result.storage_light_edge);
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
