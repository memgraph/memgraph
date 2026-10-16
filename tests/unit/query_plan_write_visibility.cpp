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

// Plans for reads and writes in one query: where the planner puts command barriers, which view each read uses,
// and which rewrites must not move a read across a write.

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/rw_checker.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/parameters.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/read_write_set.hpp"
#include "query_plan_checker.hpp"
#include "utils/typeinfo.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using memgraph::query::frontend::CypherMainVisitor;
using memgraph::query::frontend::ParsingContext;

namespace {

/// Collects every operator of type TOp in a plan, branches included, in visit order (top first).
template <class TOp>
class OpCollector final : public HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool PreVisit(TOp &op) override {
    found.push_back(&op);
    return true;
  }

  bool Visit(Once & /*op*/) override { return true; }

  std::vector<TOp *> found;
};

template <class TOp>
std::vector<TOp *> Collect(LogicalOperator &plan) {
  OpCollector<TOp> collector;
  plan.Accept(collector);
  return collector.found;
}

class WriteVisibilityPlan : public ::testing::Test {
 protected:
  CypherQuery *Parse(std::string_view query_string) {
    ::frontend::opencypher::Parser parser{std::string{query_string}};
    ParsingContext context{.is_query_cached = false};
    CypherMainVisitor visitor{context, &storage_, &parameters_};
    visitor.visit(parser.tree());
    auto *query = memgraph::utils::Downcast<CypherQuery>(visitor.query());
    EXPECT_NE(query, nullptr) << query_string;
    symbol_table_ = MakeSymbolTable(query);
    return query;
  }

  ReadsAndWrites Access(std::string_view query_string) {
    auto *query = Parse(query_string);
    return CollectReadsAndWrites(*query, symbol_table_);
  }

  LogicalOperator &Plan(std::string_view query_string) {
    auto *query = Parse(query_string);
    auto planning_context = MakePlanningContext(&storage_, &symbol_table_, query, &dba_);
    PostProcessor post_processor(parameters_, {}, &dba_);
    auto query_parts = CollectQueryParts(symbol_table_, storage_, query, false);
    plan_ = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(query_parts, &planning_context);
    plan_ = post_processor.Rewrite(std::move(plan_), &planning_context);
    return *plan_;
  }

  void LabelPropertyIndex(std::string const &label, std::string const &property) {
    dba_.SetIndexCount(dba_.Label(label), 1);
    dba_.SetIndexCount(dba_.Label(label), memgraph::storage::PropertyPath{dba_.Property(property)}, 1);
  }

  AstStorage storage_;
  Parameters parameters_;
  SymbolTable symbol_table_;
  FakeDbAccessor dba_;
  std::unique_ptr<LogicalOperator> plan_;
};

// ---------------------------------------------------------------- rewriter fence

TEST_F(WriteVisibilityPlan, IndexLookupKeepsFilterAboveSet) {
  LabelPropertyIndex("Q", "x");
  auto &plan = Plan("MATCH (a:Q) SET a.x = 5 WITH * WHERE a.x = 5 RETURN count(*) AS c");
  EXPECT_TRUE(Collect<ScanAllByLabelProperties>(plan).empty());
  ASSERT_EQ(Collect<Filter>(plan).size(), 1);
}

TEST_F(WriteVisibilityPlan, IndexLookupStillSeeksFilterBelowSet) {
  LabelPropertyIndex("Q", "x");
  auto &plan = Plan("MATCH (a:Q) WHERE a.x = 5 SET a.x = 6 RETURN count(*) AS c");
  EXPECT_EQ(Collect<ScanAllByLabelProperties>(plan).size(), 1);
  EXPECT_TRUE(Collect<Filter>(plan).empty());
}

TEST_F(WriteVisibilityPlan, JoinKeepsFilterAboveSet) {
  auto &plan = Plan("MATCH (a:Q), (b:P) SET a.x = 5 WITH * WHERE a.x = b.y RETURN count(*) AS c");
  EXPECT_TRUE(Collect<HashJoin>(plan).empty());
}

TEST_F(WriteVisibilityPlan, JoinStillHashesFilterBelowSet) {
  auto &plan = Plan("MATCH (a:Q), (b:P) WHERE a.x = b.y SET a.x = 5 RETURN count(*) AS c");
  EXPECT_EQ(Collect<HashJoin>(plan).size(), 1);
}

TEST_F(WriteVisibilityPlan, EdgeIndexKeepsFilterAboveSet) {
  auto edge_type = dba_.EdgeType("E");
  dba_.SetIndexCount(edge_type, 1);
  dba_.SetIndexCount(edge_type, dba_.Property("w"), 1);
  auto &plan = Plan("MATCH ()-[r:E]->() SET r.w = 5 WITH * WHERE r.w = 5 RETURN count(*) AS c");
  EXPECT_TRUE(Collect<ScanAllByEdgeTypeProperty>(plan).empty());
  EXPECT_EQ(Collect<Filter>(plan).size(), 1);
}

TEST_F(WriteVisibilityPlan, EdgeIndexStillSeeksFilterBelowSet) {
  auto edge_type = dba_.EdgeType("E");
  dba_.SetIndexCount(edge_type, 1);
  dba_.SetIndexCount(edge_type, dba_.Property("w"), 1);
  auto &plan = Plan("MATCH ()-[r:E]->() WHERE r.w = 5 SET r.w = 6 RETURN count(*) AS c");
  EXPECT_EQ(Collect<ScanAllByEdgeTypeProperty>(plan).size(), 1);
}

// ---------------------------------------------------------------- RETURN drain

TEST_F(WriteVisibilityPlan, RootReturnDrainAdvances) {
  auto accumulates = Collect<Accumulate>(Plan("MATCH (n) SET n.x = 1 RETURN n.x AS x ORDER BY x"));
  ASSERT_EQ(accumulates.size(), 1);
  EXPECT_TRUE(accumulates[0]->advance_command_);
}

TEST_F(WriteVisibilityPlan, BodyReturnDrainDoesNotAdvance) {
  auto accumulates = Collect<Accumulate>(Plan("MATCH (n) CALL (n) { SET n.x = 1 RETURN n.x AS x } RETURN x"));
  // Top first: the root RETURN, the drain after the writing CALL, then the body's RETURN.
  ASSERT_EQ(accumulates.size(), 3);
  EXPECT_TRUE(accumulates[0]->advance_command_);
  EXPECT_TRUE(accumulates[1]->advance_command_);
  EXPECT_FALSE(accumulates[2]->advance_command_);
}

TEST_F(WriteVisibilityPlan, ReadOnlyReturnHasNoDrain) {
  EXPECT_TRUE(Collect<Accumulate>(Plan("MATCH (n) RETURN n.x AS x ORDER BY x")).empty());
}

// ---------------------------------------------------------------- root reads use View::OLD

using memgraph::storage::View;

template <class TOp>
View &ViewOf(TOp &op) {
  if constexpr (std::is_same_v<TOp, Conditional>) {
    return op.predicate_view_;
  } else {
    return op.view_;
  }
}

template <class TOp>
std::vector<View> Views(LogicalOperator &plan) {
  std::vector<View> views;
  for (auto *op : Collect<TOp>(plan)) views.push_back(ViewOf(*op));
  return views;
}

// ---------------------------------------------------------------- view fields survive Clone

template <class TOp>
void ExpectCloneKeepsView(LogicalOperator &plan, AstStorage &storage) {
  auto ops = Collect<TOp>(plan);
  ASSERT_FALSE(ops.empty());
  // Not the planned view, so a Clone that drops the field reads back the default.
  auto const flipped = ViewOf(*ops.front()) == View::NEW ? View::OLD : View::NEW;
  for (auto *op : ops) ViewOf(*op) = flipped;
  auto clone = plan.Clone(&storage);
  for (auto *op : Collect<TOp>(*clone)) EXPECT_EQ(ViewOf(*op), flipped);
}

TEST_F(WriteVisibilityPlan, CloneKeepsProduceAndAggregateView) {
  auto &plan = Plan("MATCH (n) RETURN n.x AS x, count(*) AS c");
  ExpectCloneKeepsView<Produce>(plan, storage_);
  ExpectCloneKeepsView<Aggregate>(plan, storage_);
}

TEST_F(WriteVisibilityPlan, CloneKeepsReadViews) {
  auto &plan = Plan("MATCH (a)-[*1..2]->(b) WHERE a.x = 1 UNWIND [a.y] AS u WITH a, u ORDER BY a.z RETURN a, u");
  ExpectCloneKeepsView<Filter>(plan, storage_);
  ExpectCloneKeepsView<memgraph::query::plan::Unwind>(plan, storage_);
  ExpectCloneKeepsView<OrderBy>(plan, storage_);
  ExpectCloneKeepsView<ExpandVariable>(plan, storage_);
  ExpectCloneKeepsView<HashJoin>(Plan("MATCH (a), (b) WHERE a.x = b.y RETURN a"), storage_);
  ExpectCloneKeepsView<ExpandVariable>(Plan("MATCH p = (a)-[*BFS]->(b) RETURN p"), storage_);
  ExpectCloneKeepsView<ExpandVariable>(Plan("MATCH p = (a)-[*WSHORTEST (r, n | 1)]->(b) RETURN p"), storage_);
}

TEST_F(WriteVisibilityPlan, CloneKeepsCreateView) {
  auto &plan = Plan("CREATE (a {v: 1})-[:R {w: 2}]->(b)");
  ExpectCloneKeepsView<CreateNode>(plan, storage_);
  ExpectCloneKeepsView<CreateExpand>(plan, storage_);
}

TEST_F(WriteVisibilityPlan, RootProjectionAndCreateReadOld) {
  auto &plan = Plan("MATCH (a) WITH a, a.x AS v CREATE (b {v: v})-[:R {w: a.x}]->(c) RETURN count(*) AS c");
  EXPECT_EQ(Views<Produce>(plan), (std::vector{View::OLD, View::OLD}));
  EXPECT_EQ(Views<Aggregate>(plan), (std::vector{View::OLD}));
  EXPECT_EQ(Views<CreateNode>(plan), (std::vector{View::OLD}));
  EXPECT_EQ(Views<CreateExpand>(plan), (std::vector{View::OLD}));
}

TEST_F(WriteVisibilityPlan, BatchingKeepsMastersViews) {
  auto &plan = Plan("USING PERIODIC COMMIT 10 UNWIND range(1, 3) AS i CREATE (b {v: i}) RETURN count(*) AS c");
  EXPECT_EQ(Views<Produce>(plan), (std::vector{View::NEW}));
  EXPECT_EQ(Views<Aggregate>(plan), (std::vector{View::NEW}));
  EXPECT_EQ(Views<CreateNode>(plan), (std::vector{View::NEW}));
}

TEST_F(WriteVisibilityPlan, MergeAndForeachCreateReadNew) {
  EXPECT_EQ(Views<CreateNode>(Plan("UNWIND [1, 2] AS i MERGE (n {v: i})")), (std::vector{View::NEW}));
  EXPECT_EQ(Views<CreateNode>(Plan("FOREACH (i IN [1, 2] | CREATE (n {v: i}))")), (std::vector{View::NEW}));
}

TEST_F(WriteVisibilityPlan, WritingCallBodyReadsNew) {
  auto &plan = Plan("UNWIND [1, 2] AS i CALL (i) { CREATE (n {v: i}) RETURN n.v AS v } RETURN v");
  // Top first: the root RETURN, then the body's RETURN and CREATE.
  EXPECT_EQ(Views<Produce>(plan), (std::vector{View::OLD, View::NEW}));
  EXPECT_EQ(Views<CreateNode>(plan), (std::vector{View::NEW}));
}

// ---------------------------------------------------------------- write-to-read barriers at the root

/// The operator names on the input chain from the root down to Once, `Accumulate+` for an advancing one. A branching
/// operator contributes its main input only.
std::vector<std::string> Chain(LogicalOperator &plan) {
  std::vector<std::string> names;
  for (auto *op = &plan; op != nullptr;) {
    std::string name{op->GetTypeInfo().name};
    if (auto *acc = dynamic_cast<Accumulate *>(op); acc && acc->advance_command_) name += "+";
    names.push_back(std::move(name));
    if (dynamic_cast<Once *>(op) || dynamic_cast<Cartesian *>(op)) break;
    op = op->input().get();
  }
  return names;
}

using Names = std::vector<std::string>;

TEST_F(WriteVisibilityPlan, BarrierBeforeSetThatReadsAnEarlierSet) {
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 SET a.y = a.x")),
            (Names{"EmptyResult", "SetProperty", "Accumulate+", "SetProperty", "ScanAll", "Once"}));
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 SET a.y = a.z")),
            (Names{"EmptyResult", "SetProperty", "SetProperty", "ScanAll", "Once"}));
}

TEST_F(WriteVisibilityPlan, TwoBarriersInOnePart) {
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 SET a.y = a.x SET a.z = a.y")),
            (Names{"EmptyResult",
                   "SetProperty",
                   "Accumulate+",
                   "SetProperty",
                   "Accumulate+",
                   "SetProperty",
                   "ScanAll",
                   "Once"}));
}

TEST_F(WriteVisibilityPlan, BarrierBeforeCreateMapThatReadsAnEarlierSet) {
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 CREATE ({v: a.x})")),
            (Names{"EmptyResult", "CreateNode", "Accumulate+", "SetProperty", "ScanAll", "Once"}));
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 CREATE ({v: a.z})")),
            (Names{"EmptyResult", "CreateNode", "SetProperty", "ScanAll", "Once"}));
}

TEST_F(WriteVisibilityPlan, BarrierBeforeForeachListThatReadsAnEarlierSet) {
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 FOREACH (i IN [a.x] | CREATE ())")),
            (Names{"EmptyResult", "Foreach", "Accumulate+", "SetProperty", "ScanAll", "Once"}));
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 FOREACH (i IN [a.z] | CREATE ())")),
            (Names{"EmptyResult", "Foreach", "SetProperty", "ScanAll", "Once"}));
}

TEST_F(WriteVisibilityPlan, BarrierBeforeCallBodyThatReadsAnEarlierSet) {
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 CALL (a) { RETURN a.x AS v } RETURN v")),
            (Names{"Produce", "Apply", "Accumulate+", "SetProperty", "ScanAll", "Once"}));
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 CALL (a) { RETURN a.z AS v } RETURN v")),
            (Names{"Produce", "Accumulate+", "Apply", "SetProperty", "ScanAll", "Once"}));
}

TEST_F(WriteVisibilityPlan, BarrierBeforeExpansionFromACreatedNode) {
  // OLD cannot see `a` at all until the command advances, whatever the edge type.
  EXPECT_EQ(Chain(Plan("CREATE (a)-[:R]->(b) SET a.c = size([(a)-[:S*]->(x) | x])")),
            (Names{"EmptyResult", "SetProperty", "RollUpApply", "Accumulate+", "CreateExpand", "CreateNode", "Once"}));
}

// ---------------------------------------------------------------- read-to-write drains after View::NEW readers

TEST_F(WriteVisibilityPlan, DrainBetweenSetThatReadsAndALaterSetOfTheKey) {
  EXPECT_EQ(Chain(Plan("MATCH (a), (b) SET a.y = b.x SET b.x = 0")),
            (Names{"EmptyResult", "SetProperty", "Accumulate+", "SetProperty", "Cartesian"}));
  EXPECT_EQ(Chain(Plan("MATCH (a), (b) SET a.y = b.x SET b.z = 0")),
            (Names{"EmptyResult", "SetProperty", "SetProperty", "Cartesian"}));
}

// ---------------------------------------------------------------- CALL bodies at the root

TEST_F(WriteVisibilityPlan, ReadOnlyConditionalPredicateReadsOld) {
  auto &plan = Plan("MATCH (b) CALL (b) { WHEN b.x = 1 THEN RETURN 1 AS r } SET b.c = r");
  EXPECT_EQ(Views<Conditional>(plan), (std::vector{View::OLD}));
  ExpectCloneKeepsView<Conditional>(plan, storage_);
}

TEST_F(WriteVisibilityPlan, WritingConditionalPredicateReadsNew) {
  auto &plan = Plan("MATCH (b) CALL (b) { WHEN b.x = 1 THEN SET b.c = 1 RETURN 1 AS r } RETURN r");
  EXPECT_EQ(Views<Conditional>(plan), (std::vector{View::NEW}));
}

// ---------------------------------------------------------------- read and write sets

TEST_F(WriteVisibilityPlan, SetWritesItsKey) {
  auto access = Access("MATCH (n) SET n.x = n.y + 1");
  EXPECT_EQ(access.writes.properties, (std::set<std::string>{"x"}));
  EXPECT_FALSE(access.writes.any_property);
  EXPECT_EQ(access.reads.properties, (std::set<std::string>{"y"}));
}

TEST_F(WriteVisibilityPlan, MapSetWritesAnyKey) {
  EXPECT_TRUE(Access("MATCH (n) SET n += {a: 1}").writes.any_property);
  EXPECT_TRUE(Access("MATCH (n) SET n = $m").writes.any_property);
}

TEST_F(WriteVisibilityPlan, PropertiesFunctionReadsEverything) {
  EXPECT_TRUE(Access("MATCH (n) RETURN properties(n).x AS x").reads.everything);
}

TEST_F(WriteVisibilityPlan, MapLookupReadsNoGraph) {
  auto access = Access("LOAD CSV FROM '/x.csv' WITH HEADER AS row CREATE (:X {name: row.name})");
  EXPECT_TRUE(access.reads.properties.empty());
  EXPECT_FALSE(access.reads.any_property);
  EXPECT_TRUE(Access("WITH {name: 1} AS m RETURN {name: 2}.name AS n").reads.Empty());
}

TEST_F(WriteVisibilityPlan, FoldBodyReads) {
  auto access = Access("RETURN COUNT { MATCH (b:L)-[:R]->() WHERE b.x = 1 } AS c");
  EXPECT_TRUE(access.reads.labels.contains("L"));
  EXPECT_TRUE(access.reads.edge_types.contains("R"));
  EXPECT_TRUE(access.reads.properties.contains("x"));
  EXPECT_TRUE(access.writes.Empty());
}

TEST_F(WriteVisibilityPlan, CreateWritesLabelsAndKeys) {
  auto access = Access("CREATE (:A:B {k: 1})-[:T {w: 2}]->()");
  EXPECT_EQ(access.writes.labels, (std::set<std::string>{"A", "B"}));
  EXPECT_EQ(access.writes.properties, (std::set<std::string>{"k", "w"}));
  EXPECT_EQ(access.writes.edge_types, (std::set<std::string>{"T"}));
  EXPECT_TRUE(access.writes.nodes);
}

TEST_F(WriteVisibilityPlan, ConflictIsPerKey) {
  auto set_x = Access("MATCH (n) SET n.x = 1").writes;
  EXPECT_TRUE(Conflicts(Access("MATCH (n) WHERE n.x = 1 RETURN n").reads, set_x));
  EXPECT_FALSE(Conflicts(Access("MATCH (n) WHERE n.y = 1 RETURN n").reads, set_x));
  auto create_a = Access("CREATE (:A)").writes;
  EXPECT_TRUE(Conflicts(Access("MATCH (n:A) RETURN n").reads, create_a));
  EXPECT_TRUE(Conflicts(Access("MATCH (n) RETURN n").reads, create_a));
  EXPECT_FALSE(Conflicts(Access("MATCH (n:B) RETURN n").reads, create_a));
}

// Generated from write_visibility_cases.yaml: every case query and ablation.
constexpr std::string_view kCaseQueries[] = {
    R"cypher(MATCH (a:WvP {name: 'A'}) SET a.flag = a.k RETURN EXISTS { MATCH (b:WvP) WHERE b.flag = 1 } AS e)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) WITH a, EXISTS { MATCH (b:WvP) WHERE b.flag = 1 } AS e SET a.flag = a.k RETURN e)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvS {id: i}) SET a.x = a.k RETURN collect(COUNT { MATCH (b:WvS) WHERE b.x = 1 }) AS r)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvS {id: i}) WITH a, COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS c SET a.x = a.k RETURN collect(c) AS r)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) SET a.flag = a.k RETURN COLLECT { MATCH (b:WvP) WHERE b.flag = 1 RETURN b.name } AS r)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) WITH a, COLLECT { MATCH (b:WvP) WHERE b.flag = 1 RETURN b.name } AS r SET a.flag = a.k RETURN r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) SET a:WvT RETURN EXISTS { MATCH (a) WHERE a:WvT } AS r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) WITH a, EXISTS { MATCH (a) WHERE a:WvT } AS r SET a:WvT RETURN r)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) SET a.flag = a.k RETURN size([(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name]) AS r)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) WITH a, size([(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name]) AS r SET a.flag = a.k RETURN r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN [n IN [a] WHERE n.x = 1 | n.id] AS r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) WITH a, [n IN [a] WHERE n.x = 1 | n.id] AS r SET a.x = a.k RETURN r)cypher",
    R"cypher(MATCH (a:WvS) SET a.x = a.k WITH collect(a) AS ns RETURN size([n IN ns WHERE n.x = 1]) AS r)cypher",
    R"cypher(MATCH (a:WvS) WITH collect(a) AS ns, size([n IN collect(a) WHERE n.x = 1]) AS r UNWIND ns AS a SET a.x = a.k RETURN DISTINCT r)cypher",
    R"cypher(MATCH (a:WvS) SET a.x = a.k WITH count(*) AS n RETURN n, COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r)cypher",
    R"cypher(MATCH (a:WvS) WITH count(*) AS n, COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r MATCH (a:WvS) SET a.x = a.k RETURN DISTINCT n, r)cypher",
    R"cypher(CREATE (:WvX) CALL () { MATCH (x:WvX) RETURN count(x) AS c } RETURN c)cypher",
    R"cypher(CALL () { MATCH (x:WvX) RETURN count(x) AS c } CREATE (:WvX) RETURN c)cypher",
    R"cypher(CREATE (:WvX) CALL { MATCH (x:WvX) RETURN count(x) AS c } RETURN c)cypher",
    R"cypher(CALL { MATCH (x:WvX) RETURN count(x) AS c } CREATE (:WvX) RETURN c)cypher",
    R"cypher(CREATE (:WvX) WITH * CALL () { MATCH (x:WvX) RETURN count(x) AS c } RETURN c)cypher",
    R"cypher(CALL () { CALL (*) { CREATE (:WvI {id: 1}) RETURN 1 AS x } MATCH (n:WvI) RETURN count(n) AS rows } RETURN rows)cypher",
    R"cypher(CALL () { MATCH (n:WvI) WITH count(n) AS rows CALL (*) { CREATE (:WvI {id: 1}) RETURN 1 AS x } RETURN rows } RETURN rows)cypher",
    R"cypher(CALL (*) { CREATE (:WvI {id: 1}) RETURN 1 AS x } MATCH (n:WvI) RETURN count(n) AS c)cypher",
    R"cypher(MATCH (n:WvI) WITH count(n) AS c CALL (*) { CREATE (:WvI {id: 1}) RETURN 1 AS x } RETURN c)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (b:WvB) CALL (b, i) { WITH b, i WHERE b.x = 0 SET b.x = i RETURN i AS r } RETURN collect(r) AS rs, collect(b.x) AS xs)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (b:WvB) WITH b, i, b.x AS x0 CALL (b, i, x0) { WITH b, i, x0 WHERE x0 = 0 SET b.x = i RETURN i AS r } RETURN collect(r) AS rs, collect(b.x) AS xs)cypher",
    R"cypher(UNWIND [1, 2] AS i CALL (i) { MATCH (b:WvB) WHERE b.x = 0 SET b.x = i RETURN i AS r } WITH collect(r) AS rs MATCH (b:WvB) RETURN rs, b.x AS x)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (b0:WvB) WITH i, b0.x AS x0 CALL (i, x0) { MATCH (b:WvB) WHERE x0 = 0 SET b.x = i RETURN i AS r } WITH collect(r) AS rs MATCH (b:WvB) RETURN rs, b.x AS x)cypher",
    R"cypher(UNWIND [1, 2, 3] AS i CALL (i) { MATCH (n:WvB) WITH count(n) AS c CREATE (:WvB) RETURN c } RETURN collect(c) AS cs)cypher",
    R"cypher(OPTIONAL MATCH (n:WvB) WITH count(n) AS c0 UNWIND [1, 2, 3] AS i CALL (i) { CREATE (:WvB) RETURN 1 AS one } RETURN collect(c0) AS cs)cypher",
    R"cypher(UNWIND [1, 2, 3] AS i CALL (i) { MATCH (n:WvB), (m:WvM) WITH count(*) AS c CREATE (:WvB) RETURN c } RETURN collect(c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvC) SET b.z = i CALL (b, i) { WITH b, i WHERE b.z = i AND b.x = i RETURN 1 AS r } SET b.x = 1, b.c = b.c + 1 WITH count(*) AS n MATCH (y:WvC) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvC) CALL (b, i) { WITH b, i WHERE b.z = i AND b.x = i RETURN 1 AS r } SET b.z = i, b.x = 1, b.c = b.c + 1 WITH count(*) AS n MATCH (y:WvC) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvC) SET b.z = i CALL (b, i) { WHEN b.z = i AND b.x = i THEN RETURN 1 AS r } SET b.x = 1, b.c = b.c + 1 WITH count(*) AS n MATCH (y:WvC) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvC) CALL (b, i) { WHEN b.z = i AND b.x = i THEN RETURN 1 AS r } SET b.z = i, b.x = 1, b.c = b.c + 1 WITH count(*) AS n MATCH (y:WvC) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvC) SET b.z = i WITH * CALL (b, i) { WITH b, i WHERE b.z = i AND b.x = i RETURN 1 AS r } SET b.x = 1, b.c = b.c + 1 WITH count(*) AS n MATCH (y:WvC) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvT) CALL (b, i) { WHEN b.x = i THEN RETURN 1 AS r } SET b.x = 1, b.c = b.c + 1 WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvT) SET b.x = 1, b.c = b.c + 1 WITH b, i CALL (b, i) { WHEN b.x = i THEN RETURN 1 AS r } WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvT) WHERE b.x = i SET b.x = 1, b.c = b.c + 1 WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvT) SET b.x = 1, b.c = b.c + 1 WITH b, i WHERE b.x = i WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvT) CALL (b, i) { WITH b, i WHERE b.x = i RETURN 1 AS r } SET b.x = 1, b.c = b.c + 1 WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvT) SET b.x = 1, b.c = b.c + 1 WITH b, i CALL (b, i) { WITH b, i WHERE b.x = i RETURN 1 AS r } WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(MATCH (n:WvA) WHERE n.p > 0 AND n.p < 15 CALL (n) { SET n.p = n.p + 10 WITH n MATCH (m:WvA) RETURN count(m) AS k } WITH count(*) AS c MATCH (x:WvA) WITH c, x ORDER BY x.p RETURN c, collect(x.p) AS ps)cypher",
    R"cypher(MATCH (n:WvA) WHERE n.p > 0 AND n.p < 15 CALL (n) { SET n.p = n.p + 10 WITH n MATCH (m:WvA) RETURN count(m) AS k } WITH count(*) AS c0 MATCH (n:WvA) WHERE n.p > 0 AND n.p < 15 CALL (n) { SET n.p = n.p + 10 WITH n MATCH (m:WvA) RETURN count(m) AS k } WITH count(*) AS c MATCH (x:WvA) WITH c0 + c AS c, x ORDER BY x.p RETURN c, collect(x.p) AS ps)cypher",
    R"cypher(UNWIND [1, 2] AS i MERGE (e:WvG) MERGE (:WvG {name: 'Eve'}) WITH count(*) AS n MATCH (g:WvG) RETURN n, count(g) AS g)cypher",
    R"cypher(MERGE (:WvG {name: 'Eve'}) WITH 1 AS d UNWIND [1, 2] AS i MERGE (e:WvG) MERGE (:WvG {name: 'Eve'}) WITH count(*) AS n MATCH (g:WvG) RETURN n, count(g) AS g)cypher",
    R"cypher(UNWIND [1, 2] AS i CREATE (:WvK) MERGE (m:WvK) WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k)cypher",
    R"cypher(UNWIND [1, 2] AS i MERGE (m:WvK) CREATE (:WvK) WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k)cypher",
    R"cypher(CREATE (:WvK) MERGE (m:WvK) WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k)cypher",
    R"cypher(MERGE (m:WvK) CREATE (:WvK) WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k)cypher",
    R"cypher(UNWIND [1, 1, 2] AS i MERGE (e:WvG {k: i}) WITH count(*) AS n MATCH (g:WvG) RETURN n, count(g) AS g)cypher",
    R"cypher(UNWIND [1, 1, 2] AS i CREATE (e:WvG {k: i}) WITH count(*) AS n MATCH (g:WvG) RETURN n, count(g) AS g)cypher",
    R"cypher(UNWIND [1, 2, 3] AS i MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) MERGE (a)-[q:R]->(b) WITH count(*) AS n MATCH (:WvF)-[r:R]->() RETURN n, count(r) AS r)cypher",
    R"cypher(UNWIND [1, 2, 3] AS i MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) CREATE (a)-[q:R]->(b) WITH count(*) AS n MATCH (:WvF)-[r:R]->() RETURN n, count(r) AS r)cypher",
    R"cypher(MATCH (a:WvJ) SET a.x = a.x * a.m RETURN a.id AS id ORDER BY a.x LIMIT 1)cypher",
    R"cypher(MATCH (a:WvJ) WITH a ORDER BY a.x LIMIT 1 SET a.x = a.x * a.m RETURN a.id AS id)cypher",
    R"cypher(MATCH (a:WvJ) SET a.x = a.x * a.m RETURN a.id AS id, a.x ORDER BY a.x LIMIT 1)cypher",
    R"cypher(MATCH (a:WvJ) WITH a ORDER BY a.x LIMIT 1 SET a.x = a.x * a.m RETURN a.id AS id, a.x)cypher",
    R"cypher(MATCH (a:WvJ) SET a.x = a.x * a.m RETURN a.id AS id, a.x AS x ORDER BY x LIMIT 1)cypher",
    R"cypher(MATCH (a:WvJ) WITH a ORDER BY a.x LIMIT 1 SET a.x = a.x * a.m RETURN a.id AS id, a.x AS x)cypher",
    R"cypher(MATCH (a:WvJ) SET a.x = a.x * a.m WITH a ORDER BY a.x LIMIT 1 RETURN a.id AS id)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvJ) SET a.x = i RETURN collect(a.x) AS r)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvJ) SET a.x = i RETURN collect(i) AS r)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvJ) SET a.x = i RETURN a.x AS x, count(*) AS c ORDER BY x)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvJ) SET a.x = i RETURN i AS x, count(*) AS c ORDER BY x)cypher",
    R"cypher(CREATE (:WvK) RETURN 1 AS x UNION ALL MATCH (n:WvK) RETURN count(n) AS x)cypher",
    R"cypher(MATCH (n:WvK) RETURN count(n) AS x UNION ALL CREATE (:WvK) RETURN 1 AS x)cypher",
    R"cypher(CREATE (:WvK) RETURN 5 AS x UNION MATCH (n:WvK) RETURN count(n) AS x)cypher",
    R"cypher(MATCH (n:WvK) RETURN count(n) AS x UNION CREATE (:WvK) RETURN 5 AS x)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN 0 AS x UNION ALL MATCH (b:WvS) WHERE b.x = 1 RETURN count(b) AS x)cypher",
    R"cypher(MATCH (b:WvS) WHERE b.x = 1 RETURN count(b) AS x UNION ALL MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN 0 AS x)cypher",
    R"cypher(MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) CREATE (a)-[:R]->(b) RETURN 0 AS x UNION ALL MATCH (:WvF)-[q:R]->() RETURN count(q) AS x)cypher",
    R"cypher(MATCH (:WvF)-[q:R]->() RETURN count(q) AS x UNION ALL MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) CREATE (a)-[:R]->(b) RETURN 0 AS x)cypher",
    R"cypher(MATCH (a:WvK {id: 1}) DELETE a RETURN 0 AS x UNION ALL MATCH (n:WvK) RETURN count(n) AS x)cypher",
    R"cypher(MATCH (n:WvK) RETURN count(n) AS x UNION ALL MATCH (a:WvK {id: 1}) DELETE a RETURN 0 AS x)cypher",
    R"cypher(CREATE (:WvK) RETURN 0 AS x UNION ALL CREATE (:WvK) RETURN 0 AS x UNION ALL MATCH (n:WvK) RETURN count(n) AS x)cypher",
    R"cypher(MATCH (n:WvK) RETURN count(n) AS x UNION ALL CREATE (:WvK) RETURN 0 AS x UNION ALL CREATE (:WvK) RETURN 0 AS x)cypher",
    R"cypher(MATCH (a:WvW {id: 1}), (b:WvW {id: 2}) SET a.x = b.x, b.x = a.x RETURN a.x AS ax, b.x AS bx)cypher",
    R"cypher(MATCH (a:WvW {id: 1}), (b:WvW {id: 2}) WITH a, b, a.x AS ax0, b.x AS bx0 SET a.x = bx0, b.x = ax0 RETURN a.x AS ax, b.x AS bx)cypher",
    R"cypher(MATCH (a:WvU) SET a += {x: a.y, y: a.x} RETURN a.x AS x, a.y AS y)cypher",
    R"cypher(MATCH (a:WvU) SET a.x = a.y SET a.y = a.x RETURN a.x AS x, a.y AS y)cypher",
    R"cypher(MATCH (a:WvS) SET a:WvT WITH a MATCH (b:WvT) RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvS) WITH a MATCH (b:WvT) SET a:WvT RETURN count(*) AS c)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvS {id: i}) SET a.x = a.k WITH i MATCH (b:WvS) WHERE b.x = 1 RETURN count(*) AS c)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvS {id: i}) WITH a, i MATCH (b:WvS) WHERE b.x = 1 SET a.x = a.k RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) REMOVE a:WvS WITH a MATCH (b:WvS) RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) WITH a MATCH (b:WvS) REMOVE a:WvS RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) REMOVE a.x WITH a MATCH (b:WvS) WHERE b.x IS NOT NULL RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) WITH a MATCH (b:WvS) WHERE b.x IS NOT NULL REMOVE a.x RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvK {id: 1}) DELETE a WITH 1 AS d MATCH (b:WvK) RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvK {id: 1}) WITH a MATCH (b:WvK) DELETE a RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvK {id: 1}) DELETE a WITH 1 AS d OPTIONAL MATCH (b:WvK {id: 1}) RETURN count(b) AS c)cypher",
    R"cypher(MATCH (a:WvK {id: 1}) WITH a OPTIONAL MATCH (b:WvK {id: 1}) DELETE a RETURN count(b) AS c)cypher",
    R"cypher(MATCH (a:WvE {id: 1}) DETACH DELETE a WITH 1 AS d MATCH (x:WvE)-[r]-() RETURN count(r) AS c)cypher",
    R"cypher(MATCH (a:WvE {id: 1}) WITH a MATCH (x:WvE)-[r]-() DETACH DELETE a RETURN count(r) AS c)cypher",
    R"cypher(MATCH (:WvE {id: 1})-[r:R]->() DELETE r WITH 1 AS d MATCH (x:WvE {id: 2})-[q]-() RETURN count(q) AS c)cypher",
    R"cypher(MATCH (:WvE {id: 1})-[r:R]->() WITH r MATCH (x:WvE {id: 2})-[q]-() DELETE r RETURN count(q) AS c)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) CREATE (a)-[:R]->(b) WITH a MATCH (a)-[q:R]->() RETURN count(*) AS c)cypher",
    R"cypher(UNWIND [1, 2] AS i MATCH (a:WvF {id: 1}), (b:WvF {id: 2}) WITH a, b OPTIONAL MATCH (a)-[q:R]->() CREATE (a)-[:R]->(b) RETURN count(q) AS c)cypher",
    R"cypher(MATCH (a:WvS) FOREACH (k IN [a.k] | SET a.x = k) WITH a MATCH (b:WvS) WHERE b.x = 1 RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvS) WITH a MATCH (b:WvS) WHERE b.x = 1 FOREACH (k IN [a.k] | SET a.x = k) RETURN count(*) AS c)cypher",
    R"cypher(UNWIND [1, 2] AS i FOREACH (k IN [1] | CREATE (:WvK)) WITH i MATCH (n:WvK) RETURN count(*) AS c)cypher",
    R"cypher(UNWIND [1, 2] AS i OPTIONAL MATCH (n:WvK) FOREACH (k IN [1] | CREATE (:WvK)) RETURN count(n) AS c)cypher",
    R"cypher(MATCH (a:WvH) FOREACH (k IN [a.s, a.s, a.s] | SET a.x = a.x + k) RETURN a.x AS x)cypher",
    R"cypher(MATCH (a:WvH) FOREACH (k IN [a.s] | SET a.x = a.x + k) RETURN a.x AS x)cypher",
    R"cypher(MATCH (a:WvL) SET a.p = a.k WITH a MATCH (b:WvL) WHERE b.p = 5 RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvL) WITH a MATCH (b:WvL) WHERE b.p = 5 SET a.p = a.k RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvL {id: 1}) SET a.p = a.k WITH a MATCH (b:WvL) WHERE b.p = 1 RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvL {id: 1}) WITH a MATCH (b:WvL) WHERE b.p = 1 SET a.p = a.k RETURN count(*) AS c)cypher",
    R"cypher(MATCH (a:WvK) CREATE (:WvK) WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k)cypher",
    R"cypher(MATCH (a:WvK) MATCH (b:WvK) CREATE (:WvK) WITH count(*) AS n MATCH (k:WvK) RETURN n, count(k) AS k)cypher",
    R"cypher(CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) RETURN [(a)-[*]->(x) | x.id] AS r)cypher",
    R"cypher(CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) RETURN [(a)-[*2..]->(x) | x.id] AS r)cypher",
    R"cypher(CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) RETURN COUNT { (a)-[*]->(x) } AS r)cypher",
    R"cypher(CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) RETURN COUNT { (a)-[*2..]->(x) } AS r)cypher",
    R"cypher(CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) CALL (a) { MATCH (a)-[*]->(x) RETURN x.id AS xid } RETURN xid)cypher",
    R"cypher(CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) CALL (a) { MATCH (a)-[*2..]->(x) RETURN x.id AS xid } RETURN xid)cypher",
    R"cypher(CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) WITH a MATCH (a)-[*]->(x) RETURN x.id AS xid)cypher",
    R"cypher(CREATE (a:WvV {id: 1})-[:R]->(:WvV {id: 2}) WITH a MATCH (a)-[*2..]->(x) RETURN x.id AS xid)cypher",
    R"cypher(MATCH (a:WvQ) SET a.x = a.k WITH * WHERE a.x = 5 WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs)cypher",
    R"cypher(MATCH (a:WvQ) WITH * WHERE a.x = 5 SET a.x = a.k WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs)cypher",
    R"cypher(MATCH (a:WvQ) SET a.x = a.k WITH a WHERE a.x = 5 WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs)cypher",
    R"cypher(MATCH (a:WvQ) WITH a WHERE a.x = 5 SET a.x = a.k WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs)cypher",
    R"cypher(MATCH (a:WvQ) SET a.x = a.k CALL mg.procedures() YIELD name WHERE a.x = 5 AND name = 'mg.procedures' WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs)cypher",
    R"cypher(MATCH (a:WvQ) CALL db.labels() YIELD label WHERE a.x = 5 AND label = 'WvQ' SET a.x = a.k WITH count(*) AS c MATCH (q:WvQ) WITH c, q ORDER BY q.x RETURN c, collect(q.x) AS xs)cypher",
    R"cypher(MATCH ()-[r:WvE]->() SET r.w = r.k WITH * WHERE r.w = 5 WITH count(*) AS c MATCH ()-[q:WvE]->() RETURN c, collect(q.w) AS ws)cypher",
    R"cypher(MATCH ()-[r:WvE]->() WITH * WHERE r.w = 5 SET r.w = r.k WITH count(*) AS c MATCH ()-[q:WvE]->() RETURN c, collect(q.w) AS ws)cypher",
    R"cypher(MATCH (a:WvR), (b:WvR) WHERE a.id <> b.id WITH a, b, b.x AS v SET a.x = a.t WITH a, v ORDER BY a.id RETURN collect(v) AS vs, collect(a.x) AS xs)cypher",
    R"cypher(MATCH (a:WvR), (b:WvR) WHERE a.id <> b.id SET a.x = a.t WITH a, b, b.x AS v WITH a, v ORDER BY a.id RETURN collect(v) AS vs, collect(a.x) AS xs)cypher",
    R"cypher(MATCH (a:WvR), (b:WvR) WHERE a.id <> b.id SET a.y = b.x SET a.x = a.t WITH a ORDER BY a.id RETURN collect(a.y) AS ys, collect(a.x) AS xs)cypher",
    R"cypher(MATCH (a:WvR), (b:WvR) WHERE a.id <> b.id SET a.x = a.t SET a.y = b.x WITH a ORDER BY a.id RETURN collect(a.y) AS ys, collect(a.x) AS xs)cypher",
    R"cypher(MATCH (a:WvR), (b:WvR) WHERE a.id <> b.id CREATE (:WvR2 {v: b.x, id: a.id}) SET a.x = a.t WITH count(*) AS n MATCH (c:WvR2) WITH c ORDER BY c.id RETURN collect(c.v) AS vs)cypher",
    R"cypher(MATCH (a:WvR), (b:WvR) WHERE a.id <> b.id SET a.x = a.t CREATE (:WvR2 {v: b.x, id: a.id}) WITH count(*) AS n MATCH (c:WvR2) WITH c ORDER BY c.id RETURN collect(c.v) AS vs)cypher",
    R"cypher(MATCH (a:WvY), (b:WvZ) SET a.y = b.x SET b.x = b.z WITH a ORDER BY a.id RETURN collect(a.y) AS ys)cypher",
    R"cypher(MATCH (a:WvY), (b:WvZ) SET b.x = b.z SET a.y = b.x WITH a ORDER BY a.id RETURN collect(a.y) AS ys)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) SET a.flag = a.k SET a.n = size([(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name]) RETURN a.n AS n)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) SET a.n = size([(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name]) SET a.flag = a.k RETURN a.n AS n)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) SET a.flag = a.k CREATE (m:WvM {c: size([(b:WvP)-[:R]->() WHERE b.flag = 1 | 1])}) WITH count(*) AS n MATCH (m:WvM) RETURN n, collect(m.c) AS cs)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) CREATE (m:WvM {c: size([(b:WvP)-[:R]->() WHERE b.flag = 1 | 1])}) SET a.flag = a.k WITH count(*) AS n MATCH (m:WvM) RETURN n, collect(m.c) AS cs)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) SET a.flag = a.k FOREACH (v IN [(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name] | CREATE (:WvM {v: v})) WITH count(*) AS n OPTIONAL MATCH (m:WvM) RETURN n, collect(m.v) AS vs)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) FOREACH (v IN [(b:WvP)-[:R]->() WHERE b.flag = 1 | b.name] | CREATE (:WvM {v: v})) SET a.flag = a.k WITH count(*) AS n OPTIONAL MATCH (m:WvM) RETURN n, collect(m.v) AS vs)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) SET a.flag = a.k CALL (a) { RETURN size([(b:WvP)-[:R]->() WHERE b.flag = 1 | 1]) AS c } RETURN c)cypher",
    R"cypher(MATCH (a:WvP {name: 'A'}) CALL (a) { RETURN size([(b:WvP)-[:R]->() WHERE b.flag = 1 | 1]) AS c } SET a.flag = a.k RETURN c)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k MERGE (m:WvM {id: a.id, v: b.x}) WITH count(*) AS n MATCH (m:WvM) WITH n, m ORDER BY m.id RETURN n, collect(m.v) AS vs)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id MERGE (m:WvM {id: a.id, v: b.x}) SET a.x = a.k WITH count(*) AS n MATCH (m:WvM) WITH n, m ORDER BY m.id RETURN n, collect(m.v) AS vs)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k CREATE (m:WvM {id: a.id, v: b.x}) WITH count(*) AS n MATCH (m:WvM) WITH n, m ORDER BY m.id RETURN n, collect(m.v) AS vs)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id CREATE (m:WvM {id: a.id, v: b.x}) SET a.x = a.k WITH count(*) AS n MATCH (m:WvM) WITH n, m ORDER BY m.id RETURN n, collect(m.v) AS vs)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k FOREACH (v IN [b.x] | CREATE (:WvM {id: a.id, v: v})) WITH count(*) AS n MATCH (m:WvM) WITH n, m ORDER BY m.id RETURN n, collect(m.v) AS vs)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id FOREACH (v IN [b.x] | CREATE (:WvM {id: a.id, v: v})) SET a.x = a.k WITH count(*) AS n MATCH (m:WvM) WITH n, m ORDER BY m.id RETURN n, collect(m.v) AS vs)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k SET a.v = b.x WITH a ORDER BY a.id RETURN collect(a.v) AS vs)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.v = b.x SET a.x = a.k WITH a ORDER BY a.id RETURN collect(a.v) AS vs)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id SET a.x = a.k CALL (a, b) { RETURN b.x AS v } WITH a, v ORDER BY a.id RETURN collect(v) AS vs)cypher",
    R"cypher(MATCH (a:WvS), (b:WvS) WHERE a.id <> b.id CALL (a, b) { RETURN b.x AS v } SET a.x = a.k WITH a, v ORDER BY a.id RETURN collect(v) AS vs)cypher",
    R"cypher(CALL () { CREATE (:WvK) RETURN 1 AS x UNION ALL MATCH (n:WvK) RETURN count(n) AS x } RETURN collect(x) AS xs)cypher",
    R"cypher(CALL () { MATCH (n:WvK) RETURN count(n) AS x UNION ALL CREATE (:WvK) RETURN 1 AS x } RETURN collect(x) AS xs)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) SET a.x = a.k CALL () { MATCH (b:WvS) WHERE b.x = 1 RETURN count(b) AS c1 } SET a.y = c1 CALL () { MATCH (b:WvS) WHERE b.y = 1 RETURN count(b) AS c2 } RETURN c1, c2)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) CALL () { MATCH (b:WvS) WHERE b.x = 1 RETURN count(b) AS c1 } CALL () { MATCH (b:WvS) WHERE b.y = 1 RETURN count(b) AS c2 } SET a.x = a.k, a.y = c1 RETURN c1, c2)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN COUNT { MATCH (b:WvS) WHERE b[$k] = 1 } AS r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) WITH a, COUNT { MATCH (b:WvS) WHERE b[$k] = 1 } AS r SET a.x = a.k RETURN r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) SET a += $m RETURN COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) WITH a, COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r SET a += $m RETURN r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) SET a = $m RETURN COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) WITH a, COUNT { MATCH (b:WvS) WHERE b.x = 1 } AS r SET a = $m RETURN r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) SET a:$l RETURN EXISTS { MATCH (b:WvT) } AS r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) WITH a, EXISTS { MATCH (b:WvT) } AS r SET a:$($l) RETURN r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) SET a.x = a.k RETURN COUNT { MATCH (b:WvS) WHERE properties(b).x = 1 } AS r)cypher",
    R"cypher(MATCH (a:WvS {id: 1}) WITH a, COUNT { MATCH (b:WvS) WHERE properties(b).x = 1 } AS r SET a.x = a.k RETURN r)cypher",
    R"cypher(MATCH (s:WvV {id: 1}) CREATE (s)-[:R]->(:WvV {id: 3}) CALL (s) { MATCH (s)-[*BFS]->(x) RETURN count(x) AS c } RETURN c)cypher",
    R"cypher(MATCH (s:WvV {id: 1}) CALL (s) { MATCH p = ANY SHORTEST (s)-[*]->(x) RETURN count(x) AS c } CREATE (s)-[:R]->(:WvV {id: 3}) RETURN c)cypher",
    R"cypher(MATCH (s:WvV {id: 1}) CREATE (s)-[:R]->(:WvV {id: 3}) RETURN size([(s)-[*BFS]->(x) | x]) AS c)cypher",
    R"cypher(MATCH (s:WvV {id: 1}) WITH s, COUNT { MATCH p = ANY SHORTEST (s)-[*]->(x) } AS c CREATE (s)-[:R]->(:WvV {id: 3}) RETURN c)cypher",
    R"cypher(MATCH (s:WvV {id: 1}) CREATE (s)-[:R]->(:WvV {id: 3}) WITH s MATCH (s)-[*BFS]->(x) RETURN count(x) AS c)cypher",
    R"cypher(MATCH (s:WvV {id: 1}) CREATE (s)-[:R {w: 1}]->(:WvV {id: 3}) CALL (s) { MATCH (s)-[*WSHORTEST (r, n | r.w) total]->(x) RETURN count(x) AS c } RETURN c)cypher",
    R"cypher(MATCH (s:WvV {id: 1}) CALL (s) { MATCH p = ANY SHORTEST (s)-[*]->(x) RETURN count(x) AS c } CREATE (s)-[:R {w: 1}]->(:WvV {id: 3}) RETURN c)cypher",
    R"cypher(MATCH (s:WvV {id: 1}) CREATE (s)-[:R {w: 1}]->(:WvV {id: 3}) WITH s MATCH (s)-[*WSHORTEST (r, n | r.w) total]->(x) RETURN count(x) AS c)cypher",
    R"cypher(MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CREATE (s)-[:R]->(t) CALL (s, t) { MATCH p = (s)-[*KSHORTEST]->(t) RETURN collect(size(p)) AS ls } RETURN ls)cypher",
    R"cypher(MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CALL (s, t) { MATCH p = SHORTEST 5 (s)-[*]->(t) RETURN collect(length(p)) AS ls } CREATE (s)-[:R]->(t) RETURN ls)cypher",
    R"cypher(MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CREATE (s)-[:R]->(t) WITH s, t MATCH p = (s)-[*KSHORTEST]->(t) RETURN collect(size(p)) AS ls)cypher",
    R"cypher(MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CREATE (s)-[:R {w: 1}]->(t) CALL (s, t) { MATCH p = (s)-[*ALLSHORTEST (r, n | r.w) total]->(t) RETURN collect(size(p)) AS ls } RETURN ls)cypher",
    R"cypher(MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CALL (s, t) { MATCH p = ALL SHORTEST (s)-[*]->(t) RETURN collect(length(p)) AS ls } CREATE (s)-[:R {w: 1}]->(t) RETURN ls)cypher",
    R"cypher(MATCH (s:WvV {id: 1}), (t:WvV {id: 3}) CREATE (s)-[:R {w: 1}]->(t) WITH s, t MATCH p = (s)-[*ALLSHORTEST (r, n | r.w) total]->(t) RETURN collect(size(p)) AS ls)cypher",
    R"cypher(UNWIND [{a: 1, b: 2}, {a: 2, b: 3}, {a: 1, b: 3}, {a: 3, b: 1}] AS r MERGE (a:WvL {id: r.a}) MERGE (b:WvL {id: r.b}) MERGE (a)-[:R]->(b) WITH count(*) AS n MATCH (x:WvL) WITH n, count(x) AS nodes MATCH (:WvL)-[e:R]->() RETURN n, nodes, count(e) AS edges)cypher",
    R"cypher(UNWIND [{a: 1, b: 2}, {a: 2, b: 3}, {a: 1, b: 3}, {a: 3, b: 1}] AS r CREATE (a:WvL {id: r.a}) MERGE (b:WvL {id: r.b}) MERGE (a)-[:R]->(b) WITH count(*) AS n MATCH (x:WvL) WITH n, count(x) AS nodes MATCH (:WvL)-[e:R]->() RETURN n, nodes, count(e) AS edges)cypher",
    R"cypher(UNWIND [{a: 1, b: 2}, {a: 2, b: 3}, {a: 1, b: 3}, {a: 3, b: 1}] AS r MERGE (a:WvL1 {id: r.a}) MERGE (b:WvL2 {id: r.b}) MERGE (a)-[:R]->(b) WITH count(*) AS n MATCH (x) WHERE x:WvL1 OR x:WvL2 WITH n, count(x) AS nodes MATCH ()-[e:R]->() RETURN n, nodes, count(e) AS edges)cypher",
    R"cypher(UNWIND [{a: 1, b: 2}, {a: 2, b: 3}, {a: 1, b: 3}, {a: 3, b: 1}] AS r CREATE (a:WvL1 {id: r.a}) MERGE (b:WvL2 {id: r.b}) MERGE (a)-[:R]->(b) WITH count(*) AS n MATCH (x) WHERE x:WvL1 OR x:WvL2 WITH n, count(x) AS nodes MATCH ()-[e:R]->() RETURN n, nodes, count(e) AS edges)cypher",
    R"cypher(UNWIND [1, 2] AS i CALL (i) { MATCH (n:WvU) SET n.l = [i, i] WITH n UNWIND n.l AS v RETURN collect(v) AS vs } RETURN collect(vs) AS r)cypher",
    R"cypher(UNWIND [1, 2] AS i CALL (i) { MATCH (n:WvU) WITH n, i, n.l AS old SET n.l = [i, i] WITH old UNWIND coalesce(old, [0]) AS v RETURN collect(v) AS vs } RETURN collect(vs) AS r)cypher",
    R"cypher(UNWIND [1, 2] AS i CALL (i) { MATCH (a:WvV {id: 1}) CREATE (a)-[:R]->(:WvV {id: 2}) WITH * MATCH (a)-[*]->(x) RETURN count(x) AS c } RETURN collect(c) AS cs)cypher",
    R"cypher(UNWIND [1, 2] AS i CALL (i) { MATCH (a:WvV {id: 1}) OPTIONAL MATCH (a)-[*]->(x) WITH a, count(x) AS c CREATE (a)-[:R]->(:WvV {id: 2}) RETURN c } RETURN collect(c) AS cs)cypher",
    R"cypher(UNWIND [1, 2] AS i CALL (i) { MATCH (a:WvV {id: 1}) CREATE (a)-[:R]->(:WvV {id: 2}) WITH a MATCH (a)-[*]->(x) RETURN count(x) AS c } RETURN collect(c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvT) CALL (b, i) { CALL (b, i) { WITH b, i WHERE b.x = i RETURN 1 AS r } RETURN r } SET b.x = 1, b.c = b.c + 1 WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(UNWIND [0, 1] AS i MATCH (b:WvT) SET b.x = 1, b.c = b.c + 1 WITH b, i CALL (b, i) { CALL (b, i) { WITH b, i WHERE b.x = i RETURN 1 AS r } RETURN r } WITH count(*) AS n MATCH (y:WvT) WITH n, y ORDER BY y.c RETURN n, collect(y.c) AS cs)cypher",
    R"cypher(MATCH (n:WvA4) CALL (n) { SET n.x = 1 WITH n MATCH (n)-[*]->(m) RETURN count(m) AS c } RETURN c)cypher",
    R"cypher(MATCH (n:WvA4) CALL (n) { SET n.x = 1 WITH n MATCH (n)-[*2..]->(m) RETURN count(m) AS c } RETURN c)cypher",
    R"cypher(MATCH (n:WvA4) CALL (n) { CREATE (n)-[:R]->(k:WvM4) WITH n MATCH (n)-[*]->(m) RETURN count(m) AS c } RETURN c)cypher",
    R"cypher(MATCH (n:WvA4) CALL (n) { OPTIONAL MATCH (n)-[*]->(m) WITH n, count(m) AS c CREATE (n)-[:R]->(k:WvM4) RETURN c } RETURN c)cypher",
    R"cypher(MATCH (n:WvA4) CALL (n) { CREATE (n)-[:R]->(a:WvQ4)-[:R]->(:WvQ4) WITH a RETURN [(a)-[*]->(x) | 1] AS r } RETURN r)cypher",
    R"cypher(MATCH (n:WvA4) CALL (n) { CREATE (n)-[:R]->(a:WvQ4)-[:R]->(:WvQ4) WITH a RETURN [(a)-[*2..]->(x) | 1] AS r } RETURN r)cypher",
    R"cypher(CREATE (a:WvL {v: 1}), (b:WvL {w: a.v}) RETURN b.w AS w)cypher",
};

// The new write facts agree with the coarse ones the planner and the interpreter use today.
TEST_F(WriteVisibilityPlan, WritesAgreeWithRwCheckerAndQueryParts) {
  int checked = 0;
  for (auto query_string : kCaseQueries) {
    // A procedure needs the module registry, and a label parameter a value at parse time; this test has neither.
    if (query_string.find("CALL mg.") != std::string_view::npos ||
        query_string.find("CALL db.") != std::string_view::npos || query_string.find(":$") != std::string_view::npos) {
      continue;
    }
    SCOPED_TRACE(query_string);
    CypherQuery *query = nullptr;
    try {
      query = Parse(query_string);
    } catch (const memgraph::utils::BasicException &) {
      // Some ablations are shapes only the oracle accepts.
      continue;
    }
    ASSERT_NE(query, nullptr);
    RWChecker rw_checker;
    query->Accept(rw_checker);
    auto const writes = !CollectReadsAndWrites(*query, symbol_table_).writes.Empty();
    EXPECT_EQ(writes, rw_checker.IsWrite());
    EXPECT_EQ(writes, CollectQueryParts(symbol_table_, storage_, query, false).writes);
    ++checked;
  }
  EXPECT_GT(checked, 150);
}

}  // namespace
