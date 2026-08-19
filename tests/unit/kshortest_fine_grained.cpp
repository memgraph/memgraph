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

#include "kshortest_common.hpp"

#include <optional>
#include <string>
#include <unordered_map>

#include "auth/models.hpp"
#include "disk_test_utils.hpp"
#include "license/license.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

#include <gtest/gtest.h>

using namespace memgraph::query;
using namespace memgraph::query::plan;

template <typename StorageType>
class VertexDb : public Database {
 public:
  const std::string testSuite = "kshortest_fine_grained";

  VertexDb() {
    config_ = disk_test_utils::GenerateOnDiskConfig(testSuite);
    db_ = std::make_unique<StorageType>(config_);
  }

  ~VertexDb() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  std::unique_ptr<memgraph::storage::Storage::Accessor> Access() override {
    return db_->Access(memgraph::storage::WRITE);
  }

  std::unique_ptr<LogicalOperator> MakeKShortestOperator(
      Symbol source_sym, Symbol sink_sym, Symbol edge_sym, EdgeAtom::Direction direction,
      const std::vector<memgraph::storage::EdgeTypeId> &edge_types, const std::shared_ptr<LogicalOperator> &input,
      bool existing_node, memgraph::query::Expression *lower_bound, memgraph::query::Expression *upper_bound,
      const memgraph::query::plan::ExpansionLambda &filter_lambda, memgraph::query::Expression *limit) override {
    return std::make_unique<ExpandVariable>(input,
                                            source_sym,
                                            sink_sym,
                                            edge_sym,
                                            EdgeAtom::Type::KSHORTEST,
                                            direction,
                                            edge_types,
                                            false,
                                            nullptr,
                                            upper_bound,
                                            existing_node,
                                            filter_lambda,
                                            std::nullopt,
                                            std::nullopt,
                                            limit);
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

#ifdef MG_ENTERPRISE
// One access-check case. `blocked_vertex` nullopt runs the access checks alone; 4 adds a filter
// lambda blocking every edge into vertex 4 on top of them.
struct FineGrainedCase {
  int upper_bound;
  EdgeAtom::Direction direction;
  std::vector<std::string> edge_types;
  int limit;
  FineGrainedTestType test_type;
  std::optional<int> blocked_vertex;
};

// Enumerated rather than combined so the combinations that cannot assert anything stay out: without
// a lambda and without a limit the harness would compare the reference run to itself, and with
// everything denied the result is empty whatever the lambda does.
inline std::vector<FineGrainedCase> FineGrainedCases() {
  static constexpr auto kTypes = std::array{FineGrainedTestType::ALL_GRANTED,
                                            FineGrainedTestType::ALL_DENIED,
                                            FineGrainedTestType::EDGE_TYPE_A_DENIED,
                                            FineGrainedTestType::EDGE_TYPE_B_DENIED,
                                            FineGrainedTestType::LABEL_0_DENIED,
                                            FineGrainedTestType::LABEL_3_DENIED};
  std::vector<FineGrainedCase> cases;
  for (auto direction : {EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN, EdgeAtom::Direction::BOTH}) {
    for (auto limit : {-1, 3}) {
      for (auto test_type : kTypes) {
        for (std::optional<int> blocked : {std::optional<int>{}, std::optional<int>{4}}) {
          if (!blocked && limit == -1) continue;
          if (blocked && test_type == FineGrainedTestType::ALL_DENIED) continue;
          cases.push_back({3, direction, {"a", "b"}, limit, test_type, blocked});
        }
      }
    }
  }
  return cases;
}

// On disk only the cases without a lambda are worth repeating; see the smoke test below.
inline std::vector<FineGrainedCase> FineGrainedCasesWithoutLambda() {
  auto cases = FineGrainedCases();
  std::erase_if(cases, [](const FineGrainedCase &c) { return c.blocked_vertex.has_value(); });
  return cases;
}

class FineGrainedKShortestTestInMemory : public ::testing::TestWithParam<FineGrainedCase> {
 public:
  using StorageType = memgraph::storage::InMemoryStorage;

  static void SetUpTestCase() {
    memgraph::license::global_license_checker.EnableTesting();
    db_ = std::make_unique<VertexDb<StorageType>>();
  }

  static void TearDownTestCase() { db_ = nullptr; }

 protected:
  static std::unique_ptr<VertexDb<StorageType>> db_;
};

TEST_P(FineGrainedKShortestTestInMemory, AccessChecks) {
  const auto &c = GetParam();
  this->db_->KShortestTestWithFineGrainedFiltering(
      db_.get(), c.upper_bound, c.direction, c.edge_types, c.limit, c.test_type, c.blocked_vertex);
}

std::unique_ptr<VertexDb<FineGrainedKShortestTestInMemory::StorageType>> FineGrainedKShortestTestInMemory::db_{nullptr};

INSTANTIATE_TEST_SUITE_P(FineGrained, FineGrainedKShortestTestInMemory, testing::ValuesIn(FineGrainedCases()));

TEST_F(FineGrainedKShortestTestInMemory, AccessCheckRunsBeforeFilterLambda) {
  db_->KShortestTestAccessCheckBeforeFilterLambda(db_.get());
}

TEST_F(FineGrainedKShortestTestInMemory, MemoDistinguishesSearchDirections) {
  db_->KShortestTestMemoDistinguishesSearchDirections(db_.get());
}

class FineGrainedKShortestTestOnDisk : public ::testing::TestWithParam<FineGrainedCase> {
 public:
  using StorageType = memgraph::storage::DiskStorage;

  static void SetUpTestCase() {
    memgraph::license::global_license_checker.EnableTesting();
    db_ = std::make_unique<VertexDb<StorageType>>();
  }

  static void TearDownTestCase() { db_ = nullptr; }

 protected:
  static std::unique_ptr<VertexDb<StorageType>> db_;
};

TEST_P(FineGrainedKShortestTestOnDisk, AccessChecks) {
  const auto &c = GetParam();
  this->db_->KShortestTestWithFineGrainedFiltering(
      db_.get(), c.upper_bound, c.direction, c.edge_types, c.limit, c.test_type, c.blocked_vertex);
}

// One on-disk case for the lambda on top of the access checks; the matrix runs in memory, since
// neither the cursor nor the auth checker has a storage-mode-specific path. All granted with no
// limit is the arm that compares path for path.
TEST_F(FineGrainedKShortestTestOnDisk, WithFilterLambda) {
  db_->KShortestTestWithFineGrainedFiltering(
      db_.get(), 3, EdgeAtom::Direction::BOTH, {"a", "b"}, -1, FineGrainedTestType::ALL_GRANTED, 4);
}

std::unique_ptr<VertexDb<FineGrainedKShortestTestOnDisk::StorageType>> FineGrainedKShortestTestOnDisk::db_{nullptr};

INSTANTIATE_TEST_SUITE_P(FineGrained, FineGrainedKShortestTestOnDisk,
                         testing::ValuesIn(FineGrainedCasesWithoutLambda()));
#endif
