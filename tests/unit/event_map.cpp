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

#include "utils/event_map.hpp"
#include "gtest/gtest.h"
#include "nlohmann/json.hpp"

using memgraph::metrics::DecrementCounter;
using memgraph::metrics::EventMap;
using memgraph::metrics::global_counters_map;
using memgraph::metrics::IncrementCounter;

TEST(EventMapTest, SimpleTest) {
  IncrementCounter("ReplicationException1");
  IncrementCounter("ReplicationException1");
  IncrementCounter("ReplicationException2");
  ASSERT_EQ(global_counters_map.num_free_counters_, EventMap::kMaxCounters - 2);
  ASSERT_EQ(global_counters_map.name_to_id_[0], "ReplicationException1");
  ASSERT_EQ(global_counters_map.name_to_id_[1], "ReplicationException2");

  auto const expected_json = nlohmann::json::array({nlohmann::json{{"name", "ReplicationException1"}, {"count", 2}},
                                                    nlohmann::json{{"name", "ReplicationException2"}, {"count", 1}}});

  ASSERT_EQ(global_counters_map.ToJson(), expected_json);
}
