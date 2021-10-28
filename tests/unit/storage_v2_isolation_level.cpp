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

#include <gtest/gtest.h>

#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"

namespace {
int64_t VerticesCount(storage::Storage::Accessor &accessor) {
  int64_t count{0};
  for ([[maybe_unused]] const auto &vertex : accessor.Vertices(storage::View::NEW)) {
    ++count;
  }

  return count;
}

constexpr std::array isolation_levels{storage::IsolationLevel::SNAPSHOT_ISOLATION,
                                      storage::IsolationLevel::READ_COMMITTED,
                                      storage::IsolationLevel::READ_UNCOMMITTED};

std::string_view IsolationLevelToString(const storage::IsolationLevel isolation_level) {
  switch (isolation_level) {
    case storage::IsolationLevel::SNAPSHOT_ISOLATION:
      return "SNAPSHOT_ISOLATION";
    case storage::IsolationLevel::READ_COMMITTED:
      return "READ_COMMITTED";
    case storage::IsolationLevel::READ_UNCOMMITTED:
      return "READ_UNCOMMITTED";
  }
}
}  // namespace

class StorageIsolationLevelTest : public ::testing::TestWithParam<storage::IsolationLevel> {
 public:
  struct PrintToStringParamName {
    std::string operator()(const testing::TestParamInfo<storage::IsolationLevel> &info) {
      return std::string(IsolationLevelToString(static_cast<storage::IsolationLevel>(info.param)));
    }
  };
};

TEST_P(StorageIsolationLevelTest, Visibility) {
  const auto default_isolation_level = GetParam();

  for (const auto override_isolation_level : isolation_levels) {
    storage::Storage storage{storage::Config{.transaction = {.isolation_level = default_isolation_level}}};
    auto creator = storage.Access();
    auto default_isolation_level_reader = storage.Access();
    auto override_isolation_level_reader = storage.Access(override_isolation_level);

    ASSERT_EQ(VerticesCount(default_isolation_level_reader), 0);
    ASSERT_EQ(VerticesCount(override_isolation_level_reader), 0);

    constexpr auto iteration_count = 10;
    {
      SCOPED_TRACE(fmt::format(
          "Visibility while the creator transaction is active "
          "(default isolation level = {}, override isolation level = {})",
          IsolationLevelToString(default_isolation_level), IsolationLevelToString(override_isolation_level)));
      for (size_t i = 1; i <= iteration_count; ++i) {
        creator.CreateVertex();

        const auto check_vertices_count = [i](auto &accessor, const auto isolation_level) {
          const auto expected_count = isolation_level == storage::IsolationLevel::READ_UNCOMMITTED ? i : 0;
          EXPECT_EQ(VerticesCount(accessor), expected_count);
        };
        check_vertices_count(default_isolation_level_reader, default_isolation_level);
        check_vertices_count(override_isolation_level_reader, override_isolation_level);
      }
    }

    ASSERT_FALSE(creator.Commit().HasError());
    {
      SCOPED_TRACE(fmt::format(
          "Visibility after the creator transaction is committed "
          "(default isolation level = {}, override isolation level = {})",
          IsolationLevelToString(default_isolation_level), IsolationLevelToString(override_isolation_level)));
      const auto check_vertices_count = [iteration_count](auto &accessor, const auto isolation_level) {
        const auto expected_count =
            isolation_level == storage::IsolationLevel::SNAPSHOT_ISOLATION ? 0 : iteration_count;
        ASSERT_EQ(VerticesCount(accessor), expected_count);
      };

      check_vertices_count(default_isolation_level_reader, default_isolation_level);
      check_vertices_count(override_isolation_level_reader, override_isolation_level);
    }

    ASSERT_FALSE(default_isolation_level_reader.Commit().HasError());
    ASSERT_FALSE(override_isolation_level_reader.Commit().HasError());

    SCOPED_TRACE("Visibility after a new transaction is started");
    auto verifier = storage.Access();
    ASSERT_EQ(VerticesCount(verifier), iteration_count);
    ASSERT_FALSE(verifier.Commit().HasError());
  }
}

INSTANTIATE_TEST_CASE_P(ParameterizedStorageIsolationLevelTests, StorageIsolationLevelTest,
                        ::testing::ValuesIn(isolation_levels), StorageIsolationLevelTest::PrintToStringParamName());
