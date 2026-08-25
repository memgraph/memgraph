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

/// DISTINCT returns each row of its projection once, whatever the planner does to the plan.
///
/// A plan that deduplicates on too few columns drops rows that differ, and one that stops
/// deduplicating rows that do repeat returns them twice. Neither surfaces as an error, only as a
/// count nobody checked, so every query here is answered twice: once by the plan, and once by
/// projecting without deduplicating and reducing the rows here, which is what DISTINCT means.

#include <algorithm>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/planner.hpp"
#include "storage/v2/inmemory/storage.hpp"

#include "query_plan_common.hpp"

using memgraph::storage::PropertyValue;

namespace {

using Row = std::vector<TypedValue>;

struct RowHash {
  size_t operator()(Row const &row) const {
    size_t hash = 0;
    for (auto const &value : row) hash ^= TypedValue::Hash{}(value) + 0x9e3779b9 + (hash << 6U) + (hash >> 2U);
    return hash;
  }
};

/// The equality the deduplicating operator itself uses, so this agrees with the plan about which
/// rows are the same row.
struct RowEqual {
  bool operator()(Row const &left, Row const &right) const {
    return left.size() == right.size() && std::equal(left.begin(), left.end(), right.begin(), TypedValue::BoolEqual{});
  }
};

std::vector<Row> Reduce(std::vector<Row> const &rows) {
  std::unordered_set<Row, RowHash, RowEqual> seen;
  std::vector<Row> reduced;
  for (auto const &row : rows) {
    if (seen.insert(row).second) reduced.push_back(row);
  }
  return reduced;
}

/// Rows differing only in order are the same answer, so the two sides are matched off against each
/// other rather than lined up.
testing::AssertionResult SameRows(std::vector<Row> const &actual, std::vector<Row> const &expected) {
  if (actual.size() != expected.size()) {
    return testing::AssertionFailure() << "returned " << actual.size() << " rows, expected " << expected.size();
  }
  std::vector<bool> matched(expected.size(), false);
  for (auto const &row : actual) {
    auto found = false;
    for (size_t i = 0; i != expected.size(); ++i) {
      if (!matched[i] && RowEqual{}(row, expected[i])) {
        matched[i] = true;
        found = true;
        break;
      }
    }
    if (!found) return testing::AssertionFailure() << "returned a row that was not expected";
  }
  return testing::AssertionSuccess();
}

class DistinctSemanticsTest : public testing::TestWithParam<std::string> {
 protected:
  void SetUp() override {
    db_ = std::make_unique<memgraph::storage::InMemoryStorage>(
        memgraph::storage::Config{.salient = {.items = {.properties_on_edges = true}}});
    storage_accessor_ = db_->Access(memgraph::storage::WRITE);
    dba_ = std::make_unique<DbAccessor>(storage_accessor_.get());
    BuildGraph();
  }

  /// Duplicates are what DISTINCT has to remove, so the graph supplies them several ways: two
  /// vertices share an id, two share a name, one pair is joined by two edges, and one vertex is
  /// reachable by two paths.
  void BuildGraph() {
    auto const item = dba_->NameToLabel("Item");
    auto const id = dba_->NameToProperty("id");
    auto const name = dba_->NameToProperty("name");
    auto const link = dba_->NameToEdgeType("LINK");

    std::vector<std::pair<int64_t, std::string>> const contents{{1, "a"}, {1, "b"}, {2, "c"}, {3, "c"}};
    std::vector<memgraph::query::VertexAccessor> vertices;
    for (auto const &[id_value, name_value] : contents) {
      auto vertex = dba_->InsertVertex();
      MG_ASSERT(vertex.AddLabel(item).has_value());
      MG_ASSERT(vertex.SetProperty(id, PropertyValue(id_value)).has_value());
      MG_ASSERT(vertex.SetProperty(name, PropertyValue(name_value)).has_value());
      vertices.push_back(vertex);
    }

    auto connect = [&](size_t from, size_t to) {
      MG_ASSERT(dba_->InsertEdge(&vertices[from], &vertices[to], link).has_value());
    };
    connect(0, 1);
    connect(0, 1);
    connect(0, 2);
    connect(1, 3);
    connect(2, 3);
    dba_->AdvanceCommand();
  }

  CypherQuery *Parse(std::string const &query_string) {
    memgraph::query::Parameters parameters;
    memgraph::query::frontend::ParsingContext parsing_context{.is_query_cached = false};
    memgraph::query::frontend::opencypher::Parser parser(query_string);
    memgraph::query::frontend::CypherMainVisitor visitor(parsing_context, &ast_, &parameters);
    visitor.visit(parser.tree());
    return memgraph::utils::Downcast<CypherQuery>(visitor.query());
  }

  std::vector<Row> Run(LogicalOperator const &plan, std::vector<Symbol> const &symbols) {
    auto context = MakeContext(ast_, symbol_table_, dba_.get());
    Frame frame(symbol_table_.max_position());
    auto cursor = plan.MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());
    std::vector<Row> rows;
    while (cursor->Pull(frame, context)) {
      Row row;
      row.reserve(symbols.size());
      for (auto const &symbol : symbols) row.emplace_back(frame[symbol]);
      rows.push_back(std::move(row));
    }
    return rows;
  }

  std::unique_ptr<memgraph::storage::Storage> db_;
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_accessor_;
  std::unique_ptr<DbAccessor> dba_;
  AstStorage ast_;
  SymbolTable symbol_table_;
};

TEST_P(DistinctSemanticsTest, ReturnsEachProjectedRowOnce) {
  auto const &query_string = GetParam();
  auto *query = Parse(query_string);
  ASSERT_NE(query, nullptr);
  symbol_table_ = MakeSymbolTable(query);

  memgraph::query::Parameters parameters;
  auto planning_context = MakePlanningContext(&ast_, &symbol_table_, query, dba_.get());
  auto plan = MakeLogicalPlan(&planning_context, parameters, /*use_cost_estimator=*/false).plan;
  auto const symbols = plan->OutputSymbols(symbol_table_);

  // Projecting without the deduplicating operator gives the rows DISTINCT is defined over. Where
  // the planner decided no operator was needed there is none to leave out, and reducing the rows
  // here is what holds it to that.
  auto const *distinct = memgraph::utils::Downcast<Distinct>(plan.get());
  auto const &projection = distinct != nullptr ? *distinct->input_ : *plan;
  ASSERT_EQ(symbols, projection.OutputSymbols(symbol_table_)) << "for " << query_string;

  auto const expected = Reduce(Run(projection, symbols));
  auto const actual = Run(*plan, symbols);
  EXPECT_TRUE(SameRows(actual, expected)) << "for " << query_string;
}

// Each query reaches the deduplicating operator with the operator under test directly beneath it.
// A query ordering, skipping or limiting its rows puts an operator above the deduplication instead,
// which this cannot take apart, so those belong with a plan walk rather than here.
INSTANTIATE_TEST_SUITE_P(
    Queries, DistinctSemanticsTest,
    testing::Values("MATCH (n:Item) RETURN DISTINCT n", "MATCH (n:Item) RETURN DISTINCT n.id",
                    "MATCH (n:Item) RETURN DISTINCT n, n.id", "MATCH (n:Item) RETURN DISTINCT n.id, n",
                    "MATCH (n:Item) RETURN DISTINCT n.id, n.name", "MATCH (n:Item) RETURN DISTINCT n, n.id, n.name",
                    "MATCH (n:Item) RETURN DISTINCT n.name", "MATCH (n:Item) RETURN DISTINCT 1",
                    "MATCH (n:Item) RETURN DISTINCT n.id + 1", "MATCH (n:Item) WHERE n.id > 1 RETURN DISTINCT n",
                    "MATCH (a:Item)-->(b:Item) RETURN DISTINCT b", "MATCH (a:Item)-[r]->(b:Item) RETURN DISTINCT r",
                    "MATCH (a:Item)-[r]->(b:Item) RETURN DISTINCT a, b",
                    "MATCH (a:Item)-[r]->(b:Item) RETURN DISTINCT r, a",
                    "MATCH (a:Item)-[*1..2]->(b:Item) RETURN DISTINCT b",
                    "MATCH (a:Item)-[*1..2]->(b:Item) RETURN DISTINCT a, b", "UNWIND [1, 1, 2] AS x RETURN DISTINCT x",
                    "MATCH (n:Item) UNWIND [1, 1] AS x RETURN DISTINCT n, x",
                    "MATCH (n:Item), (m:Item) RETURN DISTINCT n, m", "MATCH (n:Item), (m:Item) RETURN DISTINCT n"));

}  // namespace
