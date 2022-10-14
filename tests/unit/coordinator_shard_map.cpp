// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <sstream>
#include <string>

#include "coordinator/shard_map.hpp"
#include "gtest/gtest.h"

namespace memgraph::coordinator::tests {
TEST(ShardMap, Parse) {
  std::string input = R"(4
property_1
property_2
property_3
property_4
3
edge_type_1
edge_type_2
edge_type_3
2
label_1
1
primary_property_name_1
string
label_2
3
property_1
string
property_2
int
primary_property_name_2
InT
)";

  std::stringstream stream(input);
  auto shard_map = ShardMap::Parse(stream);
  EXPECT_EQ(shard_map.properties.size(), 6);
  EXPECT_EQ(shard_map.edge_types.size(), 3);
  EXPECT_EQ(shard_map.label_spaces.size(), 2);
  EXPECT_EQ(shard_map.schemas.size(), 2);
}
}  // namespace memgraph::coordinator::tests
