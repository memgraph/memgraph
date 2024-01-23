// Copyright 2024 Memgraph Ltd.
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

#include "disk_test_utils.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "utils/on_scope_exit.hpp"
using memgraph::replication_coordination_glue::ReplicationRole;

namespace {
int64_t VerticesCount(memgraph::storage::Storage::Accessor *accessor) {
  int64_t count{0};
  for ([[maybe_unused]] const auto &vertex : accessor->Vertices(memgraph::storage::View::NEW)) {
    ++count;
  }

  return count;
}

inline constexpr std::array isolation_levels{memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION,
                                             memgraph::storage::IsolationLevel::READ_COMMITTED,
                                             memgraph::storage::IsolationLevel::READ_UNCOMMITTED};

}  // namespace

class StorageIsolationLevelTest : public ::testing::TestWithParam<memgraph::storage::IsolationLevel> {
 public:
  struct PrintToStringParamName {
    std::string operator()(const testing::TestParamInfo<memgraph::storage::IsolationLevel> &info) {
      return std::string(IsolationLevelToString(static_cast<memgraph::storage::IsolationLevel>(info.param)));
    }
  };

  void TestVisibility(std::unique_ptr<memgraph::storage::Storage> &storage,
                      const memgraph::storage::IsolationLevel &default_isolation_level,
                      const memgraph::storage::IsolationLevel &override_isolation_level) {
    auto creator = storage->Access(ReplicationRole::MAIN);
    auto default_isolation_level_reader = storage->Access(ReplicationRole::MAIN);
    auto override_isolation_level_reader = storage->Access(ReplicationRole::MAIN, override_isolation_level);

    ASSERT_EQ(VerticesCount(default_isolation_level_reader.get()), 0);
    ASSERT_EQ(VerticesCount(override_isolation_level_reader.get()), 0);

    static constexpr auto iteration_count = 10;
    {
      SCOPED_TRACE(fmt::format(
          "Visibility while the creator transaction is active "
          "(default isolation level = {}, override isolation level = {})",
          IsolationLevelToString(default_isolation_level), IsolationLevelToString(override_isolation_level)));
      for (size_t i = 1; i <= iteration_count; ++i) {
        creator->CreateVertex();

        const auto check_vertices_count = [i](auto &accessor, const auto isolation_level) {
          const auto expected_count = isolation_level == memgraph::storage::IsolationLevel::READ_UNCOMMITTED ? i : 0;
          EXPECT_EQ(VerticesCount(accessor.get()), expected_count);
        };
        check_vertices_count(default_isolation_level_reader, default_isolation_level);
        check_vertices_count(override_isolation_level_reader, override_isolation_level);
      }
    }

    ASSERT_FALSE(creator->Commit().HasError());
    {
      SCOPED_TRACE(fmt::format(
          "Visibility after the creator transaction is committed "
          "(default isolation level = {}, override isolation level = {})",
          IsolationLevelToString(default_isolation_level), IsolationLevelToString(override_isolation_level)));
      const auto check_vertices_count = [](auto &accessor, const auto isolation_level) {
        const auto expected_count =
            isolation_level == memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION ? 0 : iteration_count;
        ASSERT_EQ(VerticesCount(accessor.get()), expected_count);
      };

      check_vertices_count(default_isolation_level_reader, default_isolation_level);
      check_vertices_count(override_isolation_level_reader, override_isolation_level);
    }

    ASSERT_FALSE(default_isolation_level_reader->Commit().HasError());
    ASSERT_FALSE(override_isolation_level_reader->Commit().HasError());

    SCOPED_TRACE("Visibility after a new transaction is started");
    auto verifier = storage->Access(ReplicationRole::MAIN);
    ASSERT_EQ(VerticesCount(verifier.get()), iteration_count);
    ASSERT_FALSE(verifier->Commit().HasError());
  }
};

TEST_P(StorageIsolationLevelTest, VisibilityInMemoryStorage) {
  const auto default_isolation_level = GetParam();

  for (const auto override_isolation_level : isolation_levels) {
    std::unique_ptr<memgraph::storage::Storage> storage(new memgraph::storage::InMemoryStorage(
        {memgraph::storage::Config{.transaction = {.isolation_level = default_isolation_level}}}));
    this->TestVisibility(storage, default_isolation_level, override_isolation_level);
  }
}

TEST_P(StorageIsolationLevelTest, VisibilityOnDiskStorage) {
  const auto default_isolation_level = GetParam();

  const std::string testSuite = "storage_v2_isolation_level";
  auto config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  config.transaction.isolation_level = default_isolation_level;

  for (const auto override_isolation_level : isolation_levels) {
    std::unique_ptr<memgraph::storage::Storage> storage(new memgraph::storage::DiskStorage(config));
    auto on_exit = memgraph::utils::OnScopeExit{[&]() { disk_test_utils::RemoveRocksDbDirs(testSuite); }};
    try {
      this->TestVisibility(storage, default_isolation_level, override_isolation_level);
    } catch (memgraph::utils::NotYetImplemented &) {
      if (default_isolation_level != memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION ||
          override_isolation_level != memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION) {
        continue;
      }
      throw;
    }
  }
}

INSTANTIATE_TEST_CASE_P(ParameterizedStorageIsolationLevelTests, StorageIsolationLevelTest,
                        ::testing::ValuesIn(isolation_levels), StorageIsolationLevelTest::PrintToStringParamName());
