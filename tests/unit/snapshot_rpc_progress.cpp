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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <optional>

#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/observer.hpp"

using memgraph::storage::Config;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryLabelIndex;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::LabelId;
using memgraph::storage::SnapshotObserverInfo;
using memgraph::storage::Vertex;
using memgraph::storage::durability::ParallelizedSchemaCreationInfo;
using memgraph::utils::Observer;
using memgraph::utils::SkipList;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

class SnapshotRpcProgressTest : public ::testing::Test {};

class MockedSnapshotObserver final : public Observer<void> {
 public:
  MOCK_METHOD(void, Update, (), (override));
};

TEST_F(SnapshotRpcProgressTest, TestLabelIndexSingleThreadedNoVertices) {
  InMemoryLabelIndex label_idx;

  auto label = LabelId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  std::optional<ParallelizedSchemaCreationInfo> par_schema_info = std::nullopt;
  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .vertices_snapshot_progress_size = 3};

  EXPECT_CALL(*mocked_observer, Update()).Times(0);
  label_idx.CreateIndex(label, vertices.access(), par_schema_info, snapshot_info);
}

TEST_F(SnapshotRpcProgressTest, TestLabelIndexSingleThreadedVertices) {
  InMemoryLabelIndex label_idx;

  auto label = LabelId::FromUint(1);
  auto vertices = SkipList<Vertex>();
  {
    auto acc = vertices.access();
    for (uint32_t i = 1; i <= 5; i++) {
      auto [_, inserted] = acc.insert(Vertex{Gid::FromUint(i), nullptr});
      ASSERT_TRUE(inserted);
    }
  }

  std::optional<ParallelizedSchemaCreationInfo> par_schema_info = std::nullopt;

  auto mocked_observer = std::make_shared<MockedSnapshotObserver>();
  SnapshotObserverInfo snapshot_info{.observer = mocked_observer, .vertices_snapshot_progress_size = 2};
  EXPECT_CALL(*mocked_observer, Update()).Times(2);
  label_idx.CreateIndex(label, vertices.access(), par_schema_info, snapshot_info);
}
