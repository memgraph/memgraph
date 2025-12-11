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

#include "kshortest_common.hpp"

#include <optional>
#include <string>
#include <unordered_map>

#include "disk_test_utils.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

#include <gtest/gtest.h>

using namespace memgraph::query;
using namespace memgraph::query::plan;

template <typename StorageType>
class VertexDb : public Database {
 public:
  const std::string testSuite = "kshortest_general";

  VertexDb() {
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    db_ = std::make_unique<StorageType>(config_);
  }

  ~VertexDb() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  std::unique_ptr<memgraph::storage::Storage::Accessor> Access() override { return db_->Access(); }

  std::unique_ptr<LogicalOperator> MakeKShortestOperator(Symbol source_sym, Symbol sink_sym, Symbol edge_sym,
                                                         EdgeAtom::Direction direction,
                                                         const std::vector<memgraph::storage::EdgeTypeId> &edge_types,
                                                         const std::shared_ptr<LogicalOperator> &input,
                                                         bool existing_node, memgraph::query::Expression *lower_bound,
                                                         memgraph::query::Expression *upper_bound,
                                                         memgraph::query::Expression *limit) override {
    return std::make_unique<ExpandVariable>(
        input, source_sym, sink_sym, edge_sym, EdgeAtom::Type::KSHORTEST, direction, edge_types, false, lower_bound,
        upper_bound, existing_node, memgraph::query::plan::ExpansionLambda{}, std::nullopt, std::nullopt, limit);
  }

  std::pair<std::vector<memgraph::query::VertexAccessor>, std::vector<memgraph::query::EdgeAccessor>> BuildGraph(
      memgraph::query::DbAccessor *dba, const std::vector<int> &vertex_locations,
      const std::vector<std::tuple<int, int, std::string>> &edges) override {
    std::vector<memgraph::query::VertexAccessor> vertex_addr;
    std::vector<memgraph::query::EdgeAccessor> edge_addr;

    for (size_t id = 0; id < vertex_locations.size(); ++id) {
      auto vertex = dba->InsertVertex();
      MG_ASSERT(
          vertex.SetProperty(dba->NameToProperty("id"), memgraph::storage::PropertyValue(static_cast<int64_t>(id)))
              .has_value());
      MG_ASSERT(vertex.AddLabel(dba->NameToLabel(std::to_string(id))).has_value());
      vertex_addr.push_back(vertex);
    }

    for (auto e : edges) {
      int u, v;
      std::string type;
      std::tie(u, v, type) = e;
      auto &from = vertex_addr[u];
      auto &to = vertex_addr[v];
      auto edge = dba->InsertEdge(&from, &to, dba->NameToEdgeType(type));
      MG_ASSERT(edge->SetProperty(dba->NameToProperty("from"), memgraph::storage::PropertyValue(u)).has_value());
      MG_ASSERT(edge->SetProperty(dba->NameToProperty("to"), memgraph::storage::PropertyValue(v)).has_value());
      edge_addr.push_back(*edge);
    }

    return std::make_pair(vertex_addr, edge_addr);
  }

 protected:
  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> db_;
};

class GeneralKShortestTestInMemory
    : public ::testing::TestWithParam<std::tuple<int, int, EdgeAtom::Direction, std::vector<std::string>>> {
 public:
  using StorageType = memgraph::storage::InMemoryStorage;
  static void SetUpTestCase() { db_ = std::make_unique<VertexDb<StorageType>>(); }
  static void TearDownTestCase() { db_ = nullptr; }

 protected:
  static std::unique_ptr<VertexDb<StorageType>> db_;
};

TEST_P(GeneralKShortestTestInMemory, All) {
  int lower_bound;
  int upper_bound;
  EdgeAtom::Direction direction;
  std::vector<std::string> edge_types;

  std::tie(lower_bound, upper_bound, direction, edge_types) = GetParam();

  this->db_->KShortestTest(db_.get(), lower_bound, upper_bound, direction, edge_types);
}

std::unique_ptr<VertexDb<GeneralKShortestTestInMemory::StorageType>> GeneralKShortestTestInMemory::db_{nullptr};

INSTANTIATE_TEST_SUITE_P(
    General, GeneralKShortestTestInMemory,
    testing::Combine(testing::Values(-1, 2, 3),  // Test different lower bounds
                     testing::Values(3, 5, -1),  // Test different upper bounds
                     testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN, EdgeAtom::Direction::BOTH),
                     testing::Values(std::vector<std::string>{}, std::vector<std::string>{"a"},
                                     std::vector<std::string>{"b"},
                                     std::vector<std::string>{"a", "b"})));  // Test different edge type filters

// Test kshortest with limit functionality
TEST_F(GeneralKShortestTestInMemory, KShortestWithLimit) {
  // Test with limit 1
  spdlog::info("KShortestTest: Testing with limit 1");
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::OUT, {}, 1);

  // Test with limit 2
  spdlog::info("KShortestTest: Testing with limit 2");
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::OUT, {}, 2);

  // Test with limit 5 (more than available paths)
  spdlog::info("KShortestTest: Testing with limit 5");
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::OUT, {}, 5);
}

class GeneralKShortestTestOnDisk
    : public ::testing::TestWithParam<std::tuple<int, int, EdgeAtom::Direction, std::vector<std::string>>> {
 public:
  using StorageType = memgraph::storage::DiskStorage;
  static void SetUpTestCase() { db_ = std::make_unique<VertexDb<StorageType>>(); }
  static void TearDownTestCase() { db_ = nullptr; }

 protected:
  static std::unique_ptr<VertexDb<StorageType>> db_;
};

TEST_P(GeneralKShortestTestOnDisk, All) {
  int lower_bound;
  int upper_bound;
  EdgeAtom::Direction direction;
  std::vector<std::string> edge_types;

  std::tie(lower_bound, upper_bound, direction, edge_types) = GetParam();

  this->db_->KShortestTest(db_.get(), lower_bound, upper_bound, direction, edge_types);
}

std::unique_ptr<VertexDb<GeneralKShortestTestOnDisk::StorageType>> GeneralKShortestTestOnDisk::db_{nullptr};

INSTANTIATE_TEST_SUITE_P(
    General, GeneralKShortestTestOnDisk,
    testing::Combine(testing::Values(-1, 2, 3),  // Test different lower bounds
                     testing::Values(3, 5, -1),  // Test different upper bounds
                     testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN, EdgeAtom::Direction::BOTH),
                     testing::Values(std::vector<std::string>{}, std::vector<std::string>{"a"},
                                     std::vector<std::string>{"b"},
                                     std::vector<std::string>{"a", "b"})));  // Test different edge type filters

// Test kshortest with limit functionality for disk storage
TEST_F(GeneralKShortestTestOnDisk, KShortestWithLimit) {
  // Test with limit 1
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::OUT, {}, 1);

  // Test with limit 2
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::OUT, {}, 2);

  // Test with limit 5 (more than available paths)
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::OUT, {}, 5);
}
