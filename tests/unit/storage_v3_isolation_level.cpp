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

#include <gtest/gtest.h>

#include "storage/v3/isolation_level.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/storage.hpp"

namespace memgraph::storage::v3::tests {
int64_t VerticesCount(Shard::Accessor &accessor) {
  int64_t count{0};
  for ([[maybe_unused]] const auto &vertex : accessor.Vertices(View::NEW)) {
    ++count;
  }

  return count;
}

inline constexpr std::array isolation_levels{IsolationLevel::SNAPSHOT_ISOLATION, IsolationLevel::READ_COMMITTED,
                                             IsolationLevel::READ_UNCOMMITTED};

std::string_view IsolationLevelToString(const IsolationLevel isolation_level) {
  switch (isolation_level) {
    case IsolationLevel::SNAPSHOT_ISOLATION:
      return "SNAPSHOT_ISOLATION";
    case IsolationLevel::READ_COMMITTED:
      return "READ_COMMITTED";
    case IsolationLevel::READ_UNCOMMITTED:
      return "READ_UNCOMMITTED";
  }
}

class StorageIsolationLevelTest : public ::testing::TestWithParam<IsolationLevel> {
 protected:
  [[nodiscard]] LabelId NameToLabelId(std::string_view label_name) {
    return LabelId::FromUint(id_mapper.NameToId(label_name));
  }

  [[nodiscard]] PropertyId NameToPropertyId(std::string_view property_name) {
    return PropertyId::FromUint(id_mapper.NameToId(property_name));
  }

  [[nodiscard]] EdgeTypeId NameToEdgeTypeId(std::string_view edge_type_name) {
    return EdgeTypeId::FromUint(id_mapper.NameToId(edge_type_name));
  }

  [[nodiscard]] coordinator::Hlc GetNextHlc() {
    ++last_hlc.logical_id;
    last_hlc.coordinator_wall_clock += std::chrono::seconds(10);
    return last_hlc;
  }

  NameIdMapper id_mapper;
  const std::vector<PropertyValue> min_pk{PropertyValue{0}};
  const std::vector<PropertyValue> max_pk{PropertyValue{10000}};
  const PropertyId primary_property{NameToPropertyId("property")};
  std::vector<storage::v3::SchemaProperty> schema_property_vector = {
      storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}};
  const LabelId primary_label{NameToLabelId("label")};

  coordinator::Hlc last_hlc{0, io::Time{}};

 public:
  struct PrintToStringParamName {
    std::string operator()(const testing::TestParamInfo<IsolationLevel> &info) {
      return std::string(IsolationLevelToString(static_cast<IsolationLevel>(info.param)));
    }
  };
};

TEST_P(StorageIsolationLevelTest, Visibility) {
  const auto default_isolation_level = GetParam();

  for (auto override_isolation_level_index{0U}; override_isolation_level_index < isolation_levels.size();
       ++override_isolation_level_index) {
    Shard store{primary_label, min_pk, max_pk, schema_property_vector,
                Config{.transaction = {.isolation_level = default_isolation_level}}};
    const auto override_isolation_level = isolation_levels[override_isolation_level_index];
    auto creator = store.Access(GetNextHlc());
    auto default_isolation_level_reader = store.Access(GetNextHlc());
    auto override_isolation_level_reader = store.Access(GetNextHlc(), override_isolation_level);

    ASSERT_EQ(VerticesCount(default_isolation_level_reader), 0);
    ASSERT_EQ(VerticesCount(override_isolation_level_reader), 0);

    static constexpr auto iteration_count = 10;
    {
      SCOPED_TRACE(fmt::format(
          "Visibility while the creator transaction is active "
          "(default isolation level = {}, override isolation level = {})",
          IsolationLevelToString(default_isolation_level), IsolationLevelToString(override_isolation_level)));
      for (auto i{1}; i <= iteration_count; ++i) {
        ASSERT_TRUE(
            creator.CreateVertexAndValidate(primary_label, {}, {{primary_property, PropertyValue{i}}}).HasValue());

        const auto check_vertices_count = [i](auto &accessor, const auto isolation_level) {
          const auto expected_count = isolation_level == IsolationLevel::READ_UNCOMMITTED ? i : 0;
          EXPECT_EQ(VerticesCount(accessor), expected_count);
        };
        check_vertices_count(default_isolation_level_reader, default_isolation_level);
        check_vertices_count(override_isolation_level_reader, override_isolation_level);
      }
    }

    creator.Commit(GetNextHlc());
    {
      SCOPED_TRACE(fmt::format(
          "Visibility after the creator transaction is committed "
          "(default isolation level = {}, override isolation level = {})",
          IsolationLevelToString(default_isolation_level), IsolationLevelToString(override_isolation_level)));
      const auto check_vertices_count = [](auto &accessor, const auto isolation_level) {
        const auto expected_count = isolation_level == IsolationLevel::SNAPSHOT_ISOLATION ? 0 : iteration_count;
        ASSERT_EQ(VerticesCount(accessor), expected_count);
      };

      check_vertices_count(default_isolation_level_reader, default_isolation_level);
      check_vertices_count(override_isolation_level_reader, override_isolation_level);
    }

    default_isolation_level_reader.Commit(GetNextHlc());
    override_isolation_level_reader.Commit(GetNextHlc());

    SCOPED_TRACE("Visibility after a new transaction is started");
    auto verifier = store.Access(GetNextHlc());
    ASSERT_EQ(VerticesCount(verifier), iteration_count);
    verifier.Commit(GetNextHlc());
  }
}

INSTANTIATE_TEST_CASE_P(ParameterizedStorageIsolationLevelTests, StorageIsolationLevelTest,
                        ::testing::ValuesIn(isolation_levels), StorageIsolationLevelTest::PrintToStringParamName());
}  // namespace memgraph::storage::v3::tests
