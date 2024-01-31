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

#include "bfs_common.hpp"

#include "disk_test_utils.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

template <typename StorageType>
class SingleNodeDb : public Database {
 public:
  const std::string testSuite = "bfs_single_node";

  SingleNodeDb() : config_(disk_test_utils::GenerateOnDiskConfig(testSuite)), db_(new StorageType(config_)) {}

  ~SingleNodeDb() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  std::unique_ptr<memgraph::storage::Storage::Accessor> Access() override {
    return db_->Access(memgraph::replication_coordination_glue::ReplicationRole::MAIN);
  }

  std::unique_ptr<LogicalOperator> MakeBfsOperator(Symbol source_sym, Symbol sink_sym, Symbol edge_sym,
                                                   EdgeAtom::Direction direction,
                                                   const std::vector<memgraph::storage::EdgeTypeId> &edge_types,
                                                   const std::shared_ptr<LogicalOperator> &input, bool existing_node,
                                                   Expression *lower_bound, Expression *upper_bound,
                                                   const ExpansionLambda &filter_lambda) override {
    return std::make_unique<ExpandVariable>(input, source_sym, sink_sym, edge_sym, EdgeAtom::Type::BREADTH_FIRST,
                                            direction, edge_types, false, lower_bound, upper_bound, existing_node,
                                            filter_lambda, std::nullopt, std::nullopt);
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
              .HasValue());
      vertex_addr.push_back(vertex);
    }

    for (auto e : edges) {
      int u, v;
      std::string type;
      std::tie(u, v, type) = e;
      auto &from = vertex_addr[u];
      auto &to = vertex_addr[v];
      auto edge = dba->InsertEdge(&from, &to, dba->NameToEdgeType(type));
      MG_ASSERT(edge->SetProperty(dba->NameToProperty("from"), memgraph::storage::PropertyValue(u)).HasValue());
      MG_ASSERT(edge->SetProperty(dba->NameToProperty("to"), memgraph::storage::PropertyValue(v)).HasValue());
      edge_addr.push_back(*edge);
    }

    return std::make_pair(vertex_addr, edge_addr);
  }

 protected:
  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> db_;
};

class SingleNodeBfsTestInMemory
    : public ::testing::TestWithParam<
          std::tuple<int, int, EdgeAtom::Direction, std::vector<std::string>, bool, FilterLambdaType>> {
 public:
  using StorageType = memgraph::storage::InMemoryStorage;
  static void SetUpTestCase() { db_ = std::make_unique<SingleNodeDb<StorageType>>(); }
  static void TearDownTestCase() { db_ = nullptr; }

 protected:
  static std::unique_ptr<SingleNodeDb<StorageType>> db_;
};

TEST_P(SingleNodeBfsTestInMemory, All) {
  int lower_bound;
  int upper_bound;
  EdgeAtom::Direction direction;
  std::vector<std::string> edge_types;
  bool known_sink;
  FilterLambdaType filter_lambda_type;
  std::tie(lower_bound, upper_bound, direction, edge_types, known_sink, filter_lambda_type) = GetParam();
  this->db_->BfsTest(db_.get(), lower_bound, upper_bound, direction, edge_types, known_sink, filter_lambda_type);
}

std::unique_ptr<SingleNodeDb<SingleNodeBfsTestInMemory::StorageType>> SingleNodeBfsTestInMemory::db_{nullptr};

INSTANTIATE_TEST_CASE_P(DirectionAndExpansionDepth, SingleNodeBfsTestInMemory,
                        testing::Combine(testing::Range(-1, kVertexCount), testing::Range(-1, kVertexCount),
                                         testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN,
                                                         EdgeAtom::Direction::BOTH),
                                         testing::Values(std::vector<std::string>{}), testing::Bool(),
                                         testing::Values(FilterLambdaType::NONE)));

INSTANTIATE_TEST_CASE_P(
    EdgeType, SingleNodeBfsTestInMemory,
    testing::Combine(testing::Values(-1), testing::Values(-1),
                     testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN, EdgeAtom::Direction::BOTH),
                     testing::Values(std::vector<std::string>{}, std::vector<std::string>{"a"},
                                     std::vector<std::string>{"b"}, std::vector<std::string>{"a", "b"}),
                     testing::Bool(), testing::Values(FilterLambdaType::NONE)));

INSTANTIATE_TEST_CASE_P(FilterLambda, SingleNodeBfsTestInMemory,
                        testing::Combine(testing::Values(-1), testing::Values(-1),
                                         testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN,
                                                         EdgeAtom::Direction::BOTH),
                                         testing::Values(std::vector<std::string>{}), testing::Bool(),
                                         testing::Values(FilterLambdaType::NONE, FilterLambdaType::USE_FRAME,
                                                         FilterLambdaType::USE_FRAME_NULL, FilterLambdaType::USE_CTX,
                                                         FilterLambdaType::ERROR)));

class SingleNodeBfsTestOnDisk
    : public ::testing::TestWithParam<
          std::tuple<int, int, EdgeAtom::Direction, std::vector<std::string>, bool, FilterLambdaType>> {
 public:
  using StorageType = memgraph::storage::DiskStorage;
  static void SetUpTestCase() { db_ = std::make_unique<SingleNodeDb<StorageType>>(); }
  static void TearDownTestCase() { db_ = nullptr; }

 protected:
  static std::unique_ptr<SingleNodeDb<StorageType>> db_;
};

TEST_P(SingleNodeBfsTestOnDisk, All) {
  int lower_bound;
  int upper_bound;
  EdgeAtom::Direction direction;
  std::vector<std::string> edge_types;
  bool known_sink;
  FilterLambdaType filter_lambda_type;
  std::tie(lower_bound, upper_bound, direction, edge_types, known_sink, filter_lambda_type) = GetParam();
  this->db_->BfsTest(db_.get(), lower_bound, upper_bound, direction, edge_types, known_sink, filter_lambda_type);
}

std::unique_ptr<SingleNodeDb<SingleNodeBfsTestOnDisk::StorageType>> SingleNodeBfsTestOnDisk::db_{nullptr};

INSTANTIATE_TEST_CASE_P(DirectionAndExpansionDepth, SingleNodeBfsTestOnDisk,
                        testing::Combine(testing::Range(-1, kVertexCount), testing::Range(-1, kVertexCount),
                                         testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN,
                                                         EdgeAtom::Direction::BOTH),
                                         testing::Values(std::vector<std::string>{}), testing::Bool(),
                                         testing::Values(FilterLambdaType::NONE)));

INSTANTIATE_TEST_CASE_P(
    EdgeType, SingleNodeBfsTestOnDisk,
    testing::Combine(testing::Values(-1), testing::Values(-1),
                     testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN, EdgeAtom::Direction::BOTH),
                     testing::Values(std::vector<std::string>{}, std::vector<std::string>{"a"},
                                     std::vector<std::string>{"b"}, std::vector<std::string>{"a", "b"}),
                     testing::Bool(), testing::Values(FilterLambdaType::NONE)));

INSTANTIATE_TEST_CASE_P(FilterLambda, SingleNodeBfsTestOnDisk,
                        testing::Combine(testing::Values(-1), testing::Values(-1),
                                         testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN,
                                                         EdgeAtom::Direction::BOTH),
                                         testing::Values(std::vector<std::string>{}), testing::Bool(),
                                         testing::Values(FilterLambdaType::NONE, FilterLambdaType::USE_FRAME,
                                                         FilterLambdaType::USE_FRAME_NULL, FilterLambdaType::USE_CTX,
                                                         FilterLambdaType::ERROR)));
