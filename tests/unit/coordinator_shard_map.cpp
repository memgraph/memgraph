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

#include "common/types.hpp"
#include "coordinator/shard_map.hpp"
#include "gtest/gtest.h"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"

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
4
[asdasd]
[qweqwe]
[bnm]
[tryuryturtyur]
label_2
3
property_1
string
property_2
int
primary_property_name_2
InT
2
[first,1 ,2]
[     second   ,-1,  -9223372036854775808]
)";

  std::stringstream stream(input);
  auto shard_map = ShardMap::Parse(stream);
  EXPECT_EQ(shard_map.properties.size(), 6);
  EXPECT_EQ(shard_map.edge_types.size(), 3);
  EXPECT_EQ(shard_map.label_spaces.size(), 2);
  EXPECT_EQ(shard_map.schemas.size(), 2);

  auto check_label = [&shard_map](const std::string &label_name, const std::vector<SchemaProperty> &expected_schema,
                                  const std::vector<PrimaryKey> &expected_split_points) {
    ASSERT_TRUE(shard_map.labels.contains(label_name));
    const auto label_id = shard_map.labels.at(label_name);
    const auto &schema = shard_map.schemas.at(label_id);
    ASSERT_EQ(schema.size(), expected_schema.size());
    for (auto pp_index = 0; pp_index < schema.size(); ++pp_index) {
      EXPECT_EQ(schema[pp_index].property_id, expected_schema[pp_index].property_id);
      EXPECT_EQ(schema[pp_index].type, expected_schema[pp_index].type);
    }

    const auto &label_space = shard_map.label_spaces.at(label_id);

    ASSERT_EQ(label_space.shards.size(), expected_split_points.size());
    for (const auto &split_point : expected_split_points) {
      EXPECT_TRUE(label_space.shards.contains(split_point)) << split_point[0];
    }
  };

  check_label("label_1",
              {SchemaProperty{shard_map.properties.at("primary_property_name_1"), common::SchemaType::STRING}},
              std::vector<PrimaryKey>{
                  PrimaryKey{PropertyValue{""}},
                  PrimaryKey{PropertyValue{"asdasd"}},
                  PrimaryKey{PropertyValue{"qweqwe"}},
                  PrimaryKey{PropertyValue{"bnm"}},
                  PrimaryKey{PropertyValue{"tryuryturtyur"}},
              });

  static constexpr int64_t kMinInt = std::numeric_limits<int64_t>::min();
  check_label("label_2",
              {SchemaProperty{shard_map.properties.at("property_1"), common::SchemaType::STRING},
               SchemaProperty{shard_map.properties.at("property_2"), common::SchemaType::INT},
               SchemaProperty{shard_map.properties.at("primary_property_name_2"), common::SchemaType::INT}},
              std::vector<PrimaryKey>{
                  PrimaryKey{PropertyValue{""}, PropertyValue{kMinInt}, PropertyValue{kMinInt}},
                  PrimaryKey{PropertyValue{"first"}, PropertyValue{1}, PropertyValue{2}},
                  PrimaryKey{PropertyValue{"     second   "}, PropertyValue{-1},
                             PropertyValue{int64_t{-9223372036854775807LL - 1LL}}},
              });
}
}  // namespace memgraph::coordinator::tests
