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

#include "storage/v2/config.hpp"
#ifdef MG_ENTERPRISE

#include <gtest/gtest.h>

// Has to be before the rest of includes because of TRUE redefinition.
#include "query_plan_common.hpp"
// Do NOT remove this comment because clang-format will reorder includes.
#include "flags/bolt.hpp"
#include "license/license.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/planner.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/priority_thread_pool.hpp"

using namespace memgraph::query::plan;
using memgraph::query::AstStorage;
using memgraph::query::SymbolTable;

namespace {

constexpr uint16_t kBranches = 4;

/// Plans @p query as the interpreter does, including the rewrite into parallel operators.
std::unique_ptr<LogicalOperator> PlanQuery(memgraph::query::CypherQuery *query, AstStorage &ast,
                                           SymbolTable &symbol_table, memgraph::query::DbAccessor *dba) {
  auto planning_context = MakePlanningContext(&ast, &symbol_table, query, dba);
  auto query_parts = CollectQueryParts(symbol_table, ast, query, false);
  memgraph::query::Parameters parameters;
  PostProcessor post_processor(parameters, {}, dba);
  auto plan = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(query_parts, &planning_context);
  return post_processor.Rewrite(std::move(plan), &planning_context);
}

/// The projection in @p plan. It is the root only where the sort sank below it.
Produce *FindProduce(LogicalOperator *op) {
  while (op != nullptr) {
    if (auto *produce = dynamic_cast<Produce *>(op)) return produce;
    if (!op->HasSingleInput()) return nullptr;
    op = op->input().get();
  }
  return nullptr;
}

/// Reads the columns the projection names off every row @p plan produces.
std::vector<std::vector<TypedValue>> CollectRows(LogicalOperator &plan, ExecutionContext *context) {
  auto *produce = FindProduce(&plan);
  EXPECT_NE(produce, nullptr) << "plan has no projection to read";
  if (produce == nullptr) return {};

  std::vector<memgraph::query::Symbol> symbols;
  symbols.reserve(produce->named_expressions_.size());
  for (auto *named_expression : produce->named_expressions_) {
    symbols.emplace_back(context->symbol_table.at(*named_expression));
  }

  Frame frame(context->symbol_table.max_position());
  auto cursor = plan.MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());
  std::vector<std::vector<TypedValue>> rows;
  while (cursor->Pull(frame, *context)) {
    std::vector<TypedValue> values;
    values.reserve(symbols.size());
    for (const auto &symbol : symbols) values.emplace_back(frame[symbol]);
    rows.emplace_back(std::move(values));
  }
  return rows;
}

template <typename StorageType>
class ParallelExecution : public ::testing::Test {
 public:
  void SetUp() override {
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    // The rewrite takes its branch count from here when the query does not name one.
    saved_num_workers_ = FLAGS_bolt_num_workers;
    FLAGS_bolt_num_workers = kBranches;
  }

  void TearDown() override {
    FLAGS_bolt_num_workers = saved_num_workers_;
    memgraph::license::global_license_checker.DisableTesting();
  }

  /// Adds what a parallel branch needs: somewhere to run, and a flag to publish an escaping
  /// exception through.
  ExecutionContext MakeParallelContext(const SymbolTable &symbol_table, memgraph::query::DbAccessor *dba) {
    auto context = MakeContext(storage, symbol_table, dba);
    context.worker_pool = &pool;
    context.stopping_context.exception_occurred = std::make_shared<std::atomic<uint8_t>>(0);
    return context;
  }

  std::unique_ptr<memgraph::storage::Storage> db{new StorageType()};
  AstStorage storage;
  memgraph::utils::PriorityThreadPool pool{kBranches, 1};

 private:
  int32_t saved_num_workers_{0};
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage>;
TYPED_TEST_SUITE(ParallelExecution, StorageTypes);

TYPED_TEST(ParallelExecution, OrderByLimitBelowTheProjectionTakesTheTopRowsInOrder) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label = dba.NameToLabel("P");
  auto prop_a = dba.NameToProperty("a");
  auto prop_b = dba.NameToProperty("b");

  // Distinct keys in scattered order: scan order cannot pass by luck, and which branch sorted a row
  // cannot change which rows the limit keeps.
  constexpr int64_t kRows = 97;
  constexpr int64_t kStride = 37;
  for (int64_t i = 0; i < kRows; ++i) {
    auto vertex = dba.InsertVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    const int64_t b = (i * kStride) % kRows;
    ASSERT_TRUE(vertex.SetProperty(prop_b, memgraph::storage::PropertyValue(b)).has_value());
    ASSERT_TRUE(vertex.SetProperty(prop_a, memgraph::storage::PropertyValue(b * 10)).has_value());
  }
  dba.AdvanceCommand();

  // Test USING PARALLEL EXECUTION MATCH (n:P) RETURN n.a AS a ORDER BY n.b LIMIT 10
  constexpr int64_t kLimit = 10;
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", "P"))),
                                            RETURN(PROPERTY_LOOKUP(dba, "n", prop_a),
                                                   AS("a"),
                                                   ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b)),
                                                   LIMIT(LITERAL(kLimit)))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto plan = PlanQuery(query, this->storage, symbol_table, &dba);

  // Without this shape the row assertions below prove nothing.
  auto *produce = dynamic_cast<Produce *>(plan.get());
  ASSERT_NE(produce, nullptr);
  auto *limit = dynamic_cast<Limit *>(produce->input_.get());
  ASSERT_NE(limit, nullptr);
  ASSERT_NE(dynamic_cast<OrderByParallel *>(limit->input_.get()), nullptr);

  auto context = this->MakeParallelContext(symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  ASSERT_EQ(results.size(), kLimit);
  for (int64_t i = 0; i < kLimit; ++i) {
    ASSERT_EQ(results[i].size(), 1);
    EXPECT_EQ(results[i][0].ValueInt(), i * 10) << "row " << i;
  }
}

TYPED_TEST(ParallelExecution, OrderBySkipLimitBelowTheProjectionTakesTheMiddleRowsInOrder) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label = dba.NameToLabel("P");
  auto prop_a = dba.NameToProperty("a");
  auto prop_b = dba.NameToProperty("b");

  constexpr int64_t kRows = 97;
  constexpr int64_t kStride = 37;
  for (int64_t i = 0; i < kRows; ++i) {
    auto vertex = dba.InsertVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    const int64_t b = (i * kStride) % kRows;
    ASSERT_TRUE(vertex.SetProperty(prop_b, memgraph::storage::PropertyValue(b)).has_value());
    ASSERT_TRUE(vertex.SetProperty(prop_a, memgraph::storage::PropertyValue(b * 10)).has_value());
  }
  dba.AdvanceCommand();

  // Test USING PARALLEL EXECUTION MATCH (n:P) RETURN n.a AS a ORDER BY n.b DESC SKIP 5 LIMIT 7
  // Skip and limit evaluate above the merge, so they must count merged rows, not branch-local ones.
  constexpr int64_t kSkip = 5;
  constexpr int64_t kLimit = 7;
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", "P"))),
                                            RETURN(PROPERTY_LOOKUP(dba, "n", prop_a),
                                                   AS("a"),
                                                   ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b), Ordering::DESC),
                                                   SKIP(LITERAL(kSkip)),
                                                   LIMIT(LITERAL(kLimit)))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto plan = PlanQuery(query, this->storage, symbol_table, &dba);

  auto *produce = dynamic_cast<Produce *>(plan.get());
  ASSERT_NE(produce, nullptr);
  auto *limit = dynamic_cast<Limit *>(produce->input_.get());
  ASSERT_NE(limit, nullptr);
  auto *skip = dynamic_cast<Skip *>(limit->input_.get());
  ASSERT_NE(skip, nullptr);
  ASSERT_NE(dynamic_cast<OrderByParallel *>(skip->input_.get()), nullptr);

  auto context = this->MakeParallelContext(symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  ASSERT_EQ(results.size(), kLimit);
  for (int64_t i = 0; i < kLimit; ++i) {
    ASSERT_EQ(results[i].size(), 1);
    EXPECT_EQ(results[i][0].ValueInt(), (kRows - 1 - kSkip - i) * 10) << "row " << i;
  }
}

TYPED_TEST(ParallelExecution, OrderByLimitBelowTheProjectionSortsWhenBranchesHaveNoRows) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label = dba.NameToLabel("P");
  auto prop_a = dba.NameToProperty("a");
  auto prop_b = dba.NameToProperty("b");

  // Fewer rows than branches, so the merge is built partly from empty branches, and the limit asks
  // for more rows than exist.
  constexpr int64_t kRows = 2;
  for (int64_t i = 0; i < kRows; ++i) {
    auto vertex = dba.InsertVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    const int64_t b = kRows - 1 - i;
    ASSERT_TRUE(vertex.SetProperty(prop_b, memgraph::storage::PropertyValue(b)).has_value());
    ASSERT_TRUE(vertex.SetProperty(prop_a, memgraph::storage::PropertyValue(b * 10)).has_value());
  }
  dba.AdvanceCommand();

  // Test USING PARALLEL EXECUTION MATCH (n:P) RETURN n.a AS a ORDER BY n.b LIMIT 10
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", "P"))),
                                            RETURN(PROPERTY_LOOKUP(dba, "n", prop_a),
                                                   AS("a"),
                                                   ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b)),
                                                   LIMIT(LITERAL(10)))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto plan = PlanQuery(query, this->storage, symbol_table, &dba);

  auto *produce = dynamic_cast<Produce *>(plan.get());
  ASSERT_NE(produce, nullptr);
  auto context = this->MakeParallelContext(symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  ASSERT_EQ(results.size(), kRows);
  for (int64_t i = 0; i < kRows; ++i) {
    ASSERT_EQ(results[i].size(), 1);
    EXPECT_EQ(results[i][0].ValueInt(), i * 10) << "row " << i;
  }
}

TYPED_TEST(ParallelExecution, OrderByLimitBelowTheProjectionRestoresEverythingTheProjectionReads) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label_p = dba.NameToLabel("P");
  auto label_q = dba.NameToLabel("Q");
  auto edge_type = dba.NameToEdgeType("E");
  auto prop_a = dba.NameToProperty("a");
  auto prop_b = dba.NameToProperty("b");
  auto prop_c = dba.NameToProperty("c");

  // The projection reads two symbols and the sort key one of them, so a merged row has to carry the
  // expanded vertex as well as the scanned one.
  constexpr int64_t kRows = 61;
  constexpr int64_t kStride = 23;
  for (int64_t i = 0; i < kRows; ++i) {
    auto from = dba.InsertVertex();
    auto to = dba.InsertVertex();
    ASSERT_TRUE(from.AddLabel(label_p).has_value());
    ASSERT_TRUE(to.AddLabel(label_q).has_value());
    const int64_t b = (i * kStride) % kRows;
    ASSERT_TRUE(from.SetProperty(prop_b, memgraph::storage::PropertyValue(b)).has_value());
    ASSERT_TRUE(from.SetProperty(prop_a, memgraph::storage::PropertyValue(b * 10)).has_value());
    ASSERT_TRUE(to.SetProperty(prop_c, memgraph::storage::PropertyValue(b * 100)).has_value());
    ASSERT_TRUE(dba.InsertEdge(&from, &to, edge_type).has_value());
  }
  dba.AdvanceCommand();

  // Test USING PARALLEL EXECUTION
  //   MATCH (n:P)-[e:E]->(m:Q) RETURN n.a AS a, m.c AS c ORDER BY n.b LIMIT 5
  constexpr int64_t kLimit = 5;
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n", "P"), EDGE("e", memgraph::query::EdgeAtom::Direction::OUT, {"E"}), NODE("m", "Q"))),
      RETURN(PROPERTY_LOOKUP(dba, "n", prop_a),
             AS("a"),
             PROPERTY_LOOKUP(dba, "m", prop_c),
             AS("c"),
             ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b)),
             LIMIT(LITERAL(kLimit)))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto plan = PlanQuery(query, this->storage, symbol_table, &dba);

  auto *produce = dynamic_cast<Produce *>(plan.get());
  ASSERT_NE(produce, nullptr);
  auto *limit = dynamic_cast<Limit *>(produce->input_.get());
  ASSERT_NE(limit, nullptr);
  ASSERT_NE(dynamic_cast<OrderByParallel *>(limit->input_.get()), nullptr);

  auto context = this->MakeParallelContext(symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  ASSERT_EQ(results.size(), kLimit);
  for (int64_t i = 0; i < kLimit; ++i) {
    ASSERT_EQ(results[i].size(), 2);
    EXPECT_EQ(results[i][0].ValueInt(), i * 10) << "row " << i;
    EXPECT_EQ(results[i][1].ValueInt(), i * 100) << "row " << i;
  }
}

TYPED_TEST(ParallelExecution, OrderByLimitBelowTheProjectionKeepsTheSmallestKeysWhenKeysRepeat) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label = dba.NameToLabel("P");
  auto prop_b = dba.NameToProperty("b");

  // With tied keys, which row of a group the limit keeps is up to the branches, so assert over the
  // keys instead.
  constexpr int64_t kRows = 64;
  constexpr int64_t kDistinctKeys = 8;
  for (int64_t i = 0; i < kRows; ++i) {
    auto vertex = dba.InsertVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    ASSERT_TRUE(vertex.SetProperty(prop_b, memgraph::storage::PropertyValue(i % kDistinctKeys)).has_value());
  }
  dba.AdvanceCommand();

  // Test USING PARALLEL EXECUTION MATCH (n:P) RETURN n.b AS b ORDER BY n.b LIMIT 20
  constexpr int64_t kLimit = 20;
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", "P"))),
                                            RETURN(PROPERTY_LOOKUP(dba, "n", prop_b),
                                                   AS("b"),
                                                   ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b)),
                                                   LIMIT(LITERAL(kLimit)))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto plan = PlanQuery(query, this->storage, symbol_table, &dba);

  auto *produce = dynamic_cast<Produce *>(plan.get());
  ASSERT_NE(produce, nullptr);
  auto context = this->MakeParallelContext(symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  // Every key has the same number of rows, so the limit takes whole groups of keys.
  ASSERT_EQ(results.size(), kLimit);
  constexpr int64_t kRowsPerKey = kRows / kDistinctKeys;
  for (int64_t i = 0; i < kLimit; ++i) {
    ASSERT_EQ(results[i].size(), 1);
    EXPECT_EQ(results[i][0].ValueInt(), i / kRowsPerKey) << "row " << i;
  }
}

TYPED_TEST(ParallelExecution, OrderByLimitOverAnAggregateGivesEachGroupItsOwnValue) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label = dba.NameToLabel("P");
  auto prop_g = dba.NameToProperty("g");
  auto prop_v = dba.NameToProperty("v");

  // Equal-sized groups with distinct sums, so a total reported against the wrong group shows up.
  constexpr int64_t kGroups = 6;
  constexpr int64_t kRowsPerGroup = 7;
  for (int64_t g = 0; g < kGroups; ++g) {
    for (int64_t i = 0; i < kRowsPerGroup; ++i) {
      auto vertex = dba.InsertVertex();
      ASSERT_TRUE(vertex.AddLabel(label).has_value());
      ASSERT_TRUE(vertex.SetProperty(prop_g, memgraph::storage::PropertyValue(g)).has_value());
      ASSERT_TRUE(vertex.SetProperty(prop_v, memgraph::storage::PropertyValue(g + 1)).has_value());
    }
  }
  dba.AdvanceCommand();

  // Test USING PARALLEL EXECUTION
  //   MATCH (n:P) RETURN n.g AS g, sum(n.v) AS s ORDER BY s DESC LIMIT 3
  constexpr int64_t kLimit = 3;
  auto *as_g = NEXPR("g", PROPERTY_LOOKUP(dba, "n", prop_g));
  auto *as_s = NEXPR("s", SUM(PROPERTY_LOOKUP(dba, "n", prop_v), false));
  auto *query =
      PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", "P"))),
                                  RETURN(as_g, as_s, ORDER_BY(IDENT("s"), Ordering::DESC), LIMIT(LITERAL(kLimit)))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto plan = PlanQuery(query, this->storage, symbol_table, &dba);

  auto context = this->MakeParallelContext(symbol_table, &dba);
  auto results = CollectRows(*plan, &context);

  ASSERT_EQ(results.size(), kLimit);
  for (int64_t i = 0; i < kLimit; ++i) {
    const int64_t group = kGroups - 1 - i;
    ASSERT_EQ(results[i].size(), 2);
    EXPECT_EQ(results[i][0].ValueInt(), group) << "row " << i;
    EXPECT_EQ(results[i][1].ValueInt(), (group + 1) * kRowsPerGroup) << "row " << i;
  }
}

TYPED_TEST(ParallelExecution, OrderByLimitOverAProjectedBranchEvaluatesTheBranchPerRow) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label = dba.NameToLabel("P");
  auto edge_type = dba.NameToEdgeType("E");
  auto prop_b = dba.NameToProperty("b");

  // Each vertex has as many out-edges as its sort key, so a branch evaluated once and reused, or
  // evaluated against a replayed row, gives a visibly wrong list length.
  constexpr int64_t kRows = 17;
  for (int64_t i = 0; i < kRows; ++i) {
    auto from = dba.InsertVertex();
    ASSERT_TRUE(from.AddLabel(label).has_value());
    ASSERT_TRUE(from.SetProperty(prop_b, memgraph::storage::PropertyValue(i)).has_value());
    for (int64_t e = 0; e < i; ++e) {
      auto to = dba.InsertVertex();
      ASSERT_TRUE(dba.InsertEdge(&from, &to, edge_type).has_value());
    }
  }
  dba.AdvanceCommand();

  // Test USING PARALLEL EXECUTION
  //   MATCH (n:P) RETURN n.b AS b, [(n)-[]->(m) | 1] AS ms ORDER BY n.b LIMIT 4
  constexpr int64_t kLimit = 4;
  auto *pattern_comp =
      PATTERN_COMPREHENSION(nullptr,
                            PATTERN(NODE("n"), EDGE("anon1", memgraph::query::EdgeAtom::Direction::OUT), NODE("m")),
                            nullptr,
                            LITERAL(1));
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", "P"))),
                                            RETURN(PROPERTY_LOOKUP(dba, "n", prop_b),
                                                   AS("b"),
                                                   pattern_comp,
                                                   AS("ms"),
                                                   ORDER_BY(PROPERTY_LOOKUP(dba, "n", prop_b)),
                                                   LIMIT(LITERAL(kLimit)))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto plan = PlanQuery(query, this->storage, symbol_table, &dba);

  auto context = this->MakeParallelContext(symbol_table, &dba);
  auto results = CollectRows(*plan, &context);

  ASSERT_EQ(results.size(), kLimit);
  for (int64_t i = 0; i < kLimit; ++i) {
    ASSERT_EQ(results[i].size(), 2);
    EXPECT_EQ(results[i][0].ValueInt(), i) << "row " << i;
    ASSERT_TRUE(results[i][1].IsList()) << "row " << i;
    EXPECT_EQ(results[i][1].ValueList().size(), i) << "row " << i;
  }
}

}  // namespace

#endif  // MG_ENTERPRISE
