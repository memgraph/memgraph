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

  std::unique_ptr<memgraph::storage::Storage::Accessor> Access() override {
    return db_->Access(memgraph::storage::WRITE);
  }

  std::unique_ptr<LogicalOperator> MakeKShortestOperator(
      Symbol source_sym, Symbol sink_sym, Symbol edge_sym, EdgeAtom::Direction direction,
      const std::vector<memgraph::storage::EdgeTypeId> &edge_types, const std::shared_ptr<LogicalOperator> &input,
      bool existing_node, memgraph::query::Expression *lower_bound, memgraph::query::Expression *upper_bound,
      const memgraph::query::plan::ExpansionLambda &filter_lambda, memgraph::query::Expression *limit,
      std::optional<memgraph::query::plan::ExpansionLambda> weight_lambda,
      std::optional<memgraph::query::Symbol> total_weight) override {
    return std::make_unique<ExpandVariable>(input,
                                            source_sym,
                                            sink_sym,
                                            edge_sym,
                                            EdgeAtom::Type::KSHORTEST,
                                            direction,
                                            edge_types,
                                            false,
                                            lower_bound,
                                            upper_bound,
                                            existing_node,
                                            filter_lambda,
                                            std::move(weight_lambda),
                                            std::move(total_weight),
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
      // The weighted arm reads this; `EdgeWeightFor` is the same formula, so the oracle can replay it.
      MG_ASSERT(edge->SetProperty(dba->NameToProperty("weight"), memgraph::storage::PropertyValue(EdgeWeightFor(u, v)))
                    .has_value());
      edge_addr.push_back(*edge);
    }

    return std::make_pair(vertex_addr, edge_addr);
  }

 protected:
  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> db_;
};

// A suite that builds one database for all its tests.
template <typename Storage, typename Base>
class SharedDbTest : public Base {
 public:
  using StorageType = Storage;

  static void SetUpTestSuite() { db_ = std::make_unique<VertexDb<Storage>>(); }

  static void TearDownTestSuite() { db_ = nullptr; }

 protected:
  inline static std::unique_ptr<VertexDb<Storage>> db_;
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

  // The top-up loop skips paths below the bound before the limit is served, so the two interact.
  spdlog::info("KShortestTest: Testing lower bound 2 with limit 1");
  db_->KShortestTest(db_.get(), 2, -1, EdgeAtom::Direction::OUT, {}, 1);
}

// A dropped candidate shows up as a missing path only once deviations start deep, which the
// 6-vertex fixture never does - it stops at index 3 where the ladder reaches 10. Runs the ladder
// unbounded and checks the exact count and per-index length against the brute-force enumerator.
// The bounded, truncated case below drives the same depth through the length bound and `| k`.
TEST_F(GeneralKShortestTestInMemory, LadderMatchesOracleAtDepth) {
  for (auto direction : {EdgeAtom::Direction::OUT, EdgeAtom::Direction::BOTH}) {
    db_->KShortestTest(
        db_.get(), -1, -1, direction, {}, -1, FilterLambdaType::NONE, kLadderVertexLocations, kLadderEdges);
  }
}

// The unbounded case leaves the length bound, the `| k` truncation and the expansion memo out of
// the deep regime entirely: with no bound the candidate-too-long return never fires, and with no
// filter lambda the memo is never consulted. This case drives all three at deviation index 10, and
// its `| k` truncates most of the groups it enumerates.
TEST_F(GeneralKShortestTestInMemory, LadderBoundedAndTruncatedAtDepth) {
  db_->KShortestTest(db_.get(),
                     -1,
                     12,
                     EdgeAtom::Direction::BOTH,
                     {},
                     5,
                     FilterLambdaType::USE_CTX,
                     kLadderVertexLocations,
                     kLadderEdges);
}

// Depth is not enough on its own: every deviation root on the ladder stays productive, so a driver
// that stops deviating once the index is deep still reaches every path there. This graph's roots
// interleave, and three of its pairs lose a path when such a root is skipped.
TEST_F(GeneralKShortestTestInMemory, InterleavedRootsMatchOracle) {
  db_->KShortestTest(db_.get(),
                     -1,
                     -1,
                     EdgeAtom::Direction::BOTH,
                     {},
                     -1,
                     FilterLambdaType::NONE,
                     kInterleavedVertexLocations,
                     kInterleavedEdges);
}

// --- weighted ---------------------------------------------------------------------------------

// The weighted matrix over the default fixture: every pair, every direction, every edge-type filter,
// with and without a length bound. Compared path for path against a brute-force enumeration, or
// class for class under a `|k`.
using WeightedMatrixParam = std::tuple<int, int, EdgeAtom::Direction, std::vector<std::string>, int>;

// Runs one matrix cell; without a type filter or a limit the fixture's weighted order must depart
// from the hop-count one.
template <typename TDb>
void RunWeightedMatrixCell(TDb *db, const WeightedMatrixParam &param) {
  auto const [lower_bound, upper_bound, direction, edge_types, limit] = param;
  const auto hop_inversions = db->KShortestWeightedTest(db, lower_bound, upper_bound, direction, edge_types, limit);
  if (edge_types.empty() && limit == -1) EXPECT_GT(hop_inversions, 0U);
}

const auto kWeightedDirections =
    testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN, EdgeAtom::Direction::BOTH);
const auto kWeightedMatrix = testing::Combine(
    testing::Values(-1, 2), testing::Values(3, -1), kWeightedDirections,
    testing::Values(std::vector<std::string>{}, std::vector<std::string>{"a"}, std::vector<std::string>{"b"}),
    testing::Values(-1));
// `|k` with and without a lower bound, which skips paths before the limit counts them.
const auto kWeightedLimitMatrix = testing::Combine(testing::Values(-1, 2), testing::Values(-1), kWeightedDirections,
                                                   testing::Values(std::vector<std::string>{}), testing::Values(1, 3));

class WeightedKShortestTestInMemory
    : public SharedDbTest<memgraph::storage::InMemoryStorage, ::testing::TestWithParam<WeightedMatrixParam>> {};

TEST_P(WeightedKShortestTestInMemory, All) { RunWeightedMatrixCell(db_.get(), GetParam()); }

INSTANTIATE_TEST_SUITE_P(Weighted, WeightedKShortestTestInMemory, kWeightedMatrix);
INSTANTIATE_TEST_SUITE_P(WeightedLimit, WeightedKShortestTestInMemory, kWeightedLimitMatrix);

class WeightedKShortestCasesInMemory : public SharedDbTest<memgraph::storage::InMemoryStorage, ::testing::Test> {};

TEST_F(WeightedKShortestCasesInMemory, ZeroWeights) { db_->KShortestWeightedTestZeroWeights(db_.get()); }

TEST_F(WeightedKShortestCasesInMemory, DurationWeights) { db_->KShortestWeightedTestDurationWeights(db_.get()); }

TEST_F(WeightedKShortestCasesInMemory, MixedNumericWeights) {
  db_->KShortestWeightedTestMixedNumericWeights(db_.get());
}

TEST_F(WeightedKShortestCasesInMemory, LowerBoundKeepsDeviations) {
  db_->KShortestWeightedTestLowerBoundKeepsDeviations(db_.get());
}

TEST_F(WeightedKShortestCasesInMemory, ZeroWeightCycle) { db_->KShortestWeightedTestZeroWeightCycle(db_.get()); }

TEST_F(WeightedKShortestCasesInMemory, NonBooleanFilterThrows) {
  db_->KShortestWeightedTestNonBooleanFilterThrows(db_.get());
}

TEST_F(WeightedKShortestCasesInMemory, UnreachableTargetDoesNotSearch) {
  db_->KShortestWeightedTestUnreachableTargetDoesNotSearch(db_.get());
}

TEST_F(WeightedKShortestCasesInMemory, WeightErrorTexts) { db_->KShortestWeightedTestWeightErrorTexts(db_.get()); }

TEST_F(WeightedKShortestCasesInMemory, IntOverflow) { db_->KShortestWeightedTestIntOverflow(db_.get()); }

TEST_F(WeightedKShortestCasesInMemory, FilterLambda) { db_->KShortestWeightedTestFilterLambda(db_.get()); }

TEST_F(WeightedKShortestCasesInMemory, UnusedArcWeightValidated) {
  db_->KShortestWeightedTestUnusedArcWeightValidated(db_.get());
}

// The ladder drives deviation indices the 6-vertex fixture never reaches; under weights the root
// prefix's cost has to be accumulated correctly all the way down it.
TEST_F(WeightedKShortestCasesInMemory, LadderMatchesOracleAtDepth) {
  db_->KShortestWeightedTest(db_.get(), -1, -1, EdgeAtom::Direction::OUT, {}, -1, kLadderVertexLocations, kLadderEdges);
}

TEST_F(WeightedKShortestCasesInMemory, InterleavedRootsMatchOracle) {
  db_->KShortestWeightedTest(
      db_.get(), -1, -1, EdgeAtom::Direction::BOTH, {}, -1, kInterleavedVertexLocations, kInterleavedEdges);
}

// Random graphs at fixed seeds, every pair, against the brute-force enumeration. This is what would
// catch a spur search that loses a path on a shape nobody thought to write down.
class WeightedKShortestRandomInMemory
    : public SharedDbTest<memgraph::storage::InMemoryStorage,
                          ::testing::TestWithParam<std::tuple<uint32_t, int, double>>> {};

TEST_P(WeightedKShortestRandomInMemory, MatchesOracle) {
  auto const [seed, vertex_count, density] = GetParam();
  auto const [locations, edges] = RandomKShortestGraph(seed, vertex_count, density);
  SCOPED_TRACE(fmt::format("seed = {}, vertices = {}, density = {}", seed, vertex_count, density));
  for (auto direction : {EdgeAtom::Direction::OUT, EdgeAtom::Direction::BOTH}) {
    // Bounded and unbounded: the bound is what puts depth into the search's state key.
    db_->KShortestWeightedTest(db_.get(), -1, -1, direction, {}, -1, locations, edges);
    db_->KShortestWeightedTest(db_.get(), -1, 3, direction, {}, -1, locations, edges);
  }
}

INSTANTIATE_TEST_SUITE_P(Random, WeightedKShortestRandomInMemory,
                         testing::Combine(testing::Range(uint32_t{1}, uint32_t{13}), testing::Values(5, 6),
                                          testing::Values(0.3, 0.5)));

TEST_F(GeneralKShortestTestInMemory, InvertedRangeDoesNotSearch) {
  db_->KShortestTestInvertedRangeDoesNotSearch(db_.get());
}

// Filter lambda: the k shortest paths of the subgraph the lambda leaves behind.
class FilteredKShortestTestInMemory
    : public ::testing::TestWithParam<std::tuple<int, EdgeAtom::Direction, FilterLambdaType>> {
 public:
  using StorageType = memgraph::storage::InMemoryStorage;

  static void SetUpTestCase() { db_ = std::make_unique<VertexDb<StorageType>>(); }

  static void TearDownTestCase() { db_ = nullptr; }

 protected:
  static std::unique_ptr<VertexDb<StorageType>> db_;
};

TEST_P(FilteredKShortestTestInMemory, All) {
  auto const [upper_bound, direction, filter_lambda_type] = GetParam();
  this->db_->KShortestTest(db_.get(), -1, upper_bound, direction, {}, -1, filter_lambda_type);
}

std::unique_ptr<VertexDb<FilteredKShortestTestInMemory::StorageType>> FilteredKShortestTestInMemory::db_{nullptr};

INSTANTIATE_TEST_SUITE_P(Filtered, FilteredKShortestTestInMemory,
                         testing::Combine(testing::Values(3, -1),
                                          testing::Values(EdgeAtom::Direction::OUT, EdgeAtom::Direction::IN,
                                                          EdgeAtom::Direction::BOTH),
                                          testing::Values(FilterLambdaType::USE_FRAME, FilterLambdaType::USE_CTX)));

// A null verdict prunes exactly as false does, one line apart in `EvaluateFilterLambda`, so the
// expectations match `USE_FRAME` and there is nothing for the matrix to vary.
TEST_F(FilteredKShortestTestInMemory, NullLambdaPrunesLikeFalse) {
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::BOTH, {}, -1, FilterLambdaType::USE_FRAME_NULL);
}

// The non-boolean verdict throws on the first edge that reaches vertex 5, which every direction and
// bound does, so the harness returns before it looks at either.
TEST_F(FilteredKShortestTestInMemory, NonBooleanLambdaThrows) {
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::BOTH, {}, -1, FilterLambdaType::ERROR);
}

// Filter lambda combined with a path limit, which the lambda must not disturb.
TEST_F(FilteredKShortestTestInMemory, FilteredWithLimit) {
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::OUT, {}, 1, FilterLambdaType::USE_CTX);
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::OUT, {}, 3, FilterLambdaType::USE_CTX);
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

// One filter lambda case on disk storage; the exhaustive matrix runs in memory.
TEST_F(GeneralKShortestTestOnDisk, KShortestWithFilterLambda) {
  db_->KShortestTest(db_.get(), -1, -1, EdgeAtom::Direction::BOTH, {}, -1, FilterLambdaType::USE_CTX);
}

class WeightedKShortestTestOnDisk
    : public SharedDbTest<memgraph::storage::DiskStorage, ::testing::TestWithParam<WeightedMatrixParam>> {};

TEST_P(WeightedKShortestTestOnDisk, All) { RunWeightedMatrixCell(db_.get(), GetParam()); }

INSTANTIATE_TEST_SUITE_P(Weighted, WeightedKShortestTestOnDisk, kWeightedMatrix);
INSTANTIATE_TEST_SUITE_P(WeightedLimit, WeightedKShortestTestOnDisk, kWeightedLimitMatrix);
