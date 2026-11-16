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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/rw_checker.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/parameters.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/pretty_print.hpp"
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

using memgraph::storage::View;

template <class TOp>
std::vector<View> Views(LogicalOperator &plan) {
  std::vector<View> views;
  for (auto *op : Collect<TOp>(plan)) views.push_back(op->view_);
  return views;
}

// ---------------------------------------------------------------- view fields survive Clone

template <class TOp>
void ExpectCloneKeepsView(LogicalOperator &plan, AstStorage &storage) {
  auto ops = Collect<TOp>(plan);
  ASSERT_FALSE(ops.empty());
  // Not the planned view, so a Clone that drops the field reads back the default.
  auto const flipped = ops.front()->view_ == View::NEW ? View::OLD : View::NEW;
  for (auto *op : ops) op->view_ = flipped;
  auto clone = plan.Clone(&storage);
  for (auto *op : Collect<TOp>(*clone)) EXPECT_EQ(op->view_, flipped);
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
}

TEST_F(WriteVisibilityPlan, CloneKeepsCreateView) {
  auto &plan = Plan("CREATE (a {v: 1})-[:R {w: 2}]->(b)");
  ExpectCloneKeepsView<CreateNode>(plan, storage_);
  ExpectCloneKeepsView<CreateExpand>(plan, storage_);
}

// ---------------------------------------------------------------- root reads use View::OLD

TEST_F(WriteVisibilityPlan, RootProjectionAndCreateReadOld) {
  auto &plan = Plan("MATCH (a) WITH a, a.x AS v CREATE (b {v: v})-[:R {w: a.x}]->(c) RETURN count(*) AS c");
  EXPECT_EQ(Views<Produce>(plan), (std::vector{View::OLD, View::OLD}));
  EXPECT_EQ(Views<Aggregate>(plan), (std::vector{View::OLD}));
  EXPECT_EQ(Views<CreateNode>(plan), (std::vector{View::OLD}));
  EXPECT_EQ(Views<CreateExpand>(plan), (std::vector{View::OLD}));
}

TEST_F(WriteVisibilityPlan, BatchingReadsNew) {
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
    if (auto *acc = dynamic_cast<Accumulate *>(op); acc && acc->access_check_only_) name += "(access)";
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
  // An unlabelled `x` reads which nodes exist, which the CREATE writes.
  EXPECT_EQ(Chain(Plan("CREATE (a)-[:R]->(b) SET a.c = size([(a)-[:S*]->(x) | x])")),
            (Names{"EmptyResult", "SetProperty", "RollUpApply", "Accumulate+", "CreateExpand", "CreateNode", "Once"}));
  // Only the symbol conflicts: a CREATE map reads View::OLD, which cannot see `a` until the command advances. A SET
  // reads View::NEW, which sees it, so the same expansion there needs no barrier.
  EXPECT_EQ(Chain(Plan("CREATE (a)-[:R]->(b) CREATE (c {n: size([(a)-[:S*]->(x:Q) | x])})")),
            (Names{"EmptyResult", "CreateNode", "RollUpApply", "Accumulate+", "CreateExpand", "CreateNode", "Once"}));
  EXPECT_EQ(Chain(Plan("CREATE (a)-[:R]->(b) SET a.c = size([(a)-[:S*]->(x:Q) | x])")),
            (Names{"EmptyResult", "SetProperty", "RollUpApply", "CreateExpand", "CreateNode", "Once"}));
}

#ifdef MG_ENTERPRISE
// ---------------------------------------------------------------- barriers only the access check needs

// With fine-grained access control every node read checks the node's labels, so a label write before it needs a
// barrier; without a checker that barrier passes rows through.
TEST_F(WriteVisibilityPlan, ANodeReadAfterALabelWriteGetsAnAccessCheckBarrier) {
  auto &plan = Plan("MATCH (a:P) WITH a ORDER BY a.k SET a:Secret CALL () { MATCH (n) RETURN count(n) AS c } RETURN c");
  auto applies = Collect<Apply>(plan);
  ASSERT_EQ(applies.size(), 1);
  auto *barrier = dynamic_cast<Accumulate *>(applies[0]->input_.get());
  ASSERT_NE(barrier, nullptr);
  EXPECT_TRUE(barrier->access_check_only_);
}

TEST_F(WriteVisibilityPlan, ChainedLabelSetsGetAnAccessCheckBarrier) {
  EXPECT_EQ(Chain(Plan("MATCH (a:P) SET a:X SET a:Y")),
            (Names{"EmptyResult", "SetLabels", "Accumulate+(access)", "SetLabels", "Filter", "ScanAll", "Once"}));
}

TEST_F(WriteVisibilityPlan, ALabelReadKeepsAFullBarrier) {
  auto &plan = Plan("MATCH (a:P) SET a:X CALL () { MATCH (n:X) RETURN count(n) AS c } RETURN c");
  auto applies = Collect<Apply>(plan);
  ASSERT_EQ(applies.size(), 1);
  auto *barrier = dynamic_cast<Accumulate *>(applies[0]->input_.get());
  ASSERT_NE(barrier, nullptr);
  EXPECT_FALSE(barrier->access_check_only_);
}

// Without a checker the access check barrier advances nothing, so a later read of the label still needs its own.
TEST_F(WriteVisibilityPlan, AnAccessCheckBarrierKeepsTheWritesPending) {
  auto &plan = Plan(
      "MATCH (a:P) SET a:X CALL () { MATCH (n) RETURN count(n) AS c } CALL () { MATCH (m:X) RETURN count(m) AS d } "
      "RETURN c, d");
  auto applies = Collect<Apply>(plan);  // top first
  ASSERT_EQ(applies.size(), 2);
  auto *upper = dynamic_cast<Accumulate *>(applies[0]->input_.get());
  auto *lower = dynamic_cast<Accumulate *>(applies[1]->input_.get());
  ASSERT_NE(upper, nullptr);
  ASSERT_NE(lower, nullptr);
  EXPECT_FALSE(upper->access_check_only_);
  EXPECT_TRUE(lower->access_check_only_);
}
#endif

// ---------------------------------------------------------------- a map row is not a graph entity

// A row unwound from a literal list of literal maps, or a WITH alias of a map literal whose values hold no entity,
// is a map: r.name reads no graph property, so a later SET of n.name needs no barrier before r.name is read again.
TEST_F(WriteVisibilityPlan, AMapRowReadsNoGraphProperty) {
  EXPECT_EQ(Chain(Plan("UNWIND [{id: 1, name: 'a'}, {id: 2, name: 'b'}] AS r MATCH (n:P) WHERE n.id = r.id "
                       "SET n.name = r.name SET n.alias = r.name")),
            (Names{"EmptyResult", "SetProperty", "SetProperty", "Filter", "ScanAll", "Unwind", "Once"}));
  EXPECT_TRUE(Collect<Accumulate>(Plan("MATCH (n:P) WITH n, {name: toString(n.id)} AS r SET n.name = r.name "
                                       "SET n.alias = r.name"))
                  .empty());
}

// A map that may hold an entity keeps the barrier: m.a.x reads the node's x.
TEST_F(WriteVisibilityPlan, AMapHoldingANodeReadsItsProperties) {
  EXPECT_FALSE(Collect<Accumulate>(Plan("MATCH (n) WITH n, {a: n} AS m SET n.x = 1 SET n.y = m.a.x")).empty());
  EXPECT_FALSE(Collect<Accumulate>(Plan("MATCH (n) UNWIND [{a: n}] AS m SET n.x = 1 SET n.y = m.a.x")).empty());
  EXPECT_FALSE(Collect<Accumulate>(Plan("MATCH (n) WITH n, {a: head([n])} AS m SET n.x = 1 SET n.y = m.a.x")).empty());
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

// ---------------------------------------------------------------- writing CALL bodies

TEST_F(WriteVisibilityPlan, WritingBodyWithDoesNotAdvance) {
  auto accumulates = Collect<Accumulate>(
      Plan("MATCH (n) CALL (n) { SET n.p = n.p + 1 WITH n MATCH (m) RETURN count(m) AS k } RETURN k"));
  // Top first: the root RETURN, the drain after the writing CALL, then the body's WITH.
  ASSERT_EQ(accumulates.size(), 3);
  EXPECT_TRUE(accumulates[0]->advance_command_);
  EXPECT_TRUE(accumulates[1]->advance_command_);
  EXPECT_FALSE(accumulates[2]->advance_command_);
}

TEST_F(WriteVisibilityPlan, WritingBodyReadsNew) {
  auto &plan = Plan(
      "UNWIND [1, 2] AS i CALL (i) { MATCH (b)-[*1..2]->(c) WHERE b.x = 0 SET b.x = i RETURN i AS r } "
      "RETURN r");
  auto *apply = Collect<Apply>(plan).front();
  EXPECT_EQ(Views<ScanAll>(*apply->subquery_), (std::vector{View::NEW}));
  EXPECT_EQ(Views<ExpandVariable>(*apply->subquery_), (std::vector{View::NEW}));
  EXPECT_EQ(Views<Filter>(*apply->subquery_), (std::vector{View::NEW}));
}

TEST_F(WriteVisibilityPlan, ReadOnlyBodyReadsOld) {
  auto &plan = Plan("MATCH (a) CALL (a) { MATCH (b) WHERE b.x = a.x RETURN count(b) AS c } SET a.c = c");
  auto *apply = Collect<Apply>(plan).front();
  EXPECT_EQ(Views<ScanAll>(*apply->subquery_), (std::vector{View::OLD}));
  EXPECT_EQ(Views<Filter>(*apply->subquery_), (std::vector{View::OLD}));
}

TEST_F(WriteVisibilityPlan, WritingBodyDrainsBetweenAReadAndAWriteOfItsKey) {
  auto &plan = Plan("UNWIND [1, 2] AS i CALL (i) { MATCH (b) WHERE b.x = 0 SET b.x = i } RETURN count(*) AS c");
  auto *apply = Collect<Apply>(plan).front();
  auto chain = Chain(*apply->subquery_);
  ASSERT_GE(chain.size(), 3);
  EXPECT_EQ(chain[1], "SetProperty");
  EXPECT_EQ(chain[2], "Accumulate");
  auto &twin = Plan("UNWIND [1, 2] AS i CALL (i) { MATCH (b) WHERE b.y = 0 SET b.x = i } RETURN count(*) AS c");
  EXPECT_EQ(Collect<Accumulate>(*Collect<Apply>(twin).front()->subquery_).size(), 0);
}

// ---------------------------------------------------------------- MERGE

TEST_F(WriteVisibilityPlan, BarrierBetweenTwoMergesOnOneLabel) {
  EXPECT_EQ(Chain(Plan("UNWIND [1, 2] AS i MERGE (a:L {id: i}) MERGE (b:L {id: i + 1})")),
            (Names{"EmptyResult", "Merge", "Accumulate+", "Merge", "Unwind", "Once"}));
}

TEST_F(WriteVisibilityPlan, BarrierKeepsTheSymbolsAMergeBinds) {
  auto &plan = Plan("UNWIND [1, 2] AS i MERGE (a:L {id: i}) MERGE (b:L {id: i + 1}) RETURN a, b");
  auto accumulates = Collect<Accumulate>(plan);
  ASSERT_FALSE(accumulates.empty());
  auto const &kept = accumulates.back()->symbols_;
  EXPECT_TRUE(std::ranges::any_of(kept, [](const Symbol &sym) { return sym.name() == "a"; }));
}

TEST_F(WriteVisibilityPlan, TwoLabelImportShapeHasNoBarrier) {
  EXPECT_EQ(Chain(Plan("UNWIND [1, 2] AS i MERGE (a:A {id: i}) MERGE (b:B {id: i}) MERGE (a)-[:R]->(b)")),
            (Names{"EmptyResult", "Merge", "Merge", "Merge", "Unwind", "Once"}));
}

TEST_F(WriteVisibilityPlan, DrainBetweenMergeAndALaterWriteOfWhatItMatched) {
  EXPECT_EQ(Chain(Plan("UNWIND [1, 2] AS i MERGE (a:L {id: i}) CREATE (:L)")),
            (Names{"EmptyResult", "CreateNode", "Accumulate+", "Merge", "Unwind", "Once"}));
  EXPECT_EQ(Chain(Plan("UNWIND [1, 2] AS i MERGE (a:L {id: i}) CREATE (:M)")),
            (Names{"EmptyResult", "CreateNode", "Merge", "Unwind", "Once"}));
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
  EXPECT_TRUE(Access("RETURN {name: 2}.name AS n").reads.Empty());
}

TEST_F(WriteVisibilityPlan, FoldBodyReads) {
  auto access = Access("RETURN COUNT { MATCH (b:L)-[:R]->() WHERE b.x = 1 } AS c");
  EXPECT_TRUE(access.reads.labels.contains("L"));
  EXPECT_TRUE(access.reads.edge_types.contains("R"));
  EXPECT_TRUE(access.reads.properties.contains("x"));
  EXPECT_TRUE(access.writes.Empty());
}

TEST_F(WriteVisibilityPlan, CreateWritesLabelsAndTypesNotKeys) {
  // A created entity is reachable only through its labels, its type or its own symbol.
  auto access = Access("CREATE (:A:B {k: 1})-[:T {w: 2}]->()");
  EXPECT_EQ(access.writes.labels, (std::set<std::string>{"A", "B"}));
  EXPECT_TRUE(access.writes.properties.empty());
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

// The queries of write_visibility.feature and their ablations; keep in step with it.
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

// ---------------------------------------------------------------- reads the collector must not miss

// The target of a SET or REMOVE item is an expression too: `(CASE WHEN a.f THEN b ELSE a END).x` reads a.f.
TEST_F(WriteVisibilityPlan, SetAndRemoveTargetsAreRead) {
  for (auto const *query : {
           "MATCH (a), (b) SET (CASE WHEN a.f THEN b ELSE a END).x = 1",
           "MATCH (a), (b) REMOVE (CASE WHEN a.f THEN b ELSE a END).x",
           "MATCH (a), (b) FOREACH (i IN [1] | SET (CASE WHEN a.f THEN b ELSE a END).x = i)",
           "MATCH (a), (b) MERGE (b)-[:R]->(a) ON CREATE SET (CASE WHEN a.f THEN b ELSE a END).x = 1",
       }) {
    auto access = Access(query);
    EXPECT_TRUE(access.reads.properties.contains("f")) << query;
    EXPECT_TRUE(access.writes.properties.contains("x")) << query;
  }
}

TEST_F(WriteVisibilityPlan, ValuesAndPropertySizeReadEverything) {
  EXPECT_TRUE(Access("MATCH (n) RETURN values(n) AS v").reads.everything);
  EXPECT_TRUE(Access("MATCH (n) RETURN propertySize(n, 'x') AS s").reads.everything);
  EXPECT_FALSE(Access("MATCH (n) RETURN toUpper(n.x) AS s").reads.everything);
}

TEST_F(WriteVisibilityPlan, ComputedEntityValueReadsAnyKey) {
  EXPECT_TRUE(Access("MATCH (a), (b) SET a = coalesce(b, a)").reads.any_property);
  EXPECT_TRUE(Access("MATCH (a), (b) SET a += head([b])").reads.any_property);
  EXPECT_FALSE(Access("MATCH (a) SET a = {x: 1}").reads.any_property);
  EXPECT_FALSE(Access("MATCH (a) SET a += $m").reads.any_property);
}

TEST_F(WriteVisibilityPlan, LabelNegationReadsNodeExistence) {
  EXPECT_TRUE(Access("MATCH (n:!L) RETURN n").reads.nodes);
  EXPECT_TRUE(Access("MATCH (n:L|!M) RETURN n").reads.nodes);
  EXPECT_FALSE(Access("MATCH (n:L|M) RETURN n").reads.nodes);
}

TEST_F(WriteVisibilityPlan, BarrierBeforeValuesThatReadsAnEarlierSet) {
  EXPECT_EQ(Chain(Plan("MATCH (n) SET n.x = 10 CREATE ({v: values(n)})")),
            (Names{"EmptyResult", "CreateNode", "Accumulate+", "SetProperty", "ScanAll", "Once"}));
}

TEST_F(WriteVisibilityPlan, DrainBetweenAComputedEntitySetAndALaterWrite) {
  EXPECT_EQ(Chain(Plan("MATCH (a), (b) SET a = coalesce(b, a) SET b.x = 0")),
            (Names{"EmptyResult", "SetProperty", "Accumulate+", "SetProperties", "Cartesian"}));
}

// A read of an entity this command created goes through the evaluator's View::NEW fallback, so it is a View::NEW
// read: a later write of what it read gets a drain, and a read of a key nothing later writes gets none.
TEST_F(WriteVisibilityPlan, DrainBetweenAReadOfAMergedNodeAndALaterWriteOfTheKey) {
  EXPECT_EQ(Chain(Plan("UNWIND [1, 2] AS i MERGE (a:A {id: 1}) CREATE (c:C {v: a.x}) SET a.x = i")),
            (Names{"EmptyResult", "SetProperty", "Accumulate+", "CreateNode", "Merge", "Unwind", "Once"}));
  EXPECT_EQ(Chain(Plan("UNWIND [1, 2] AS i MERGE (a:A {id: 1}) CREATE (c:C {v: a.x}) SET a.y = i")),
            (Names{"EmptyResult", "SetProperty", "CreateNode", "Merge", "Unwind", "Once"}));
  EXPECT_EQ(Chain(Plan("CREATE (a {x: 1}) CREATE (b {v: a.x})")),
            (Names{"EmptyResult", "CreateNode", "CreateNode", "Once"}));
}

bool DrainsBeforeSet(Names const &chain) {
  auto const set = std::ranges::find(chain, "SetProperty");
  return set != chain.end() && std::next(set) != chain.end() && *std::next(set) == "Accumulate+";
}

// A path or a CALL result can hold an entity the command created, so a read through it is a View::NEW read too. Every
// node is labelled: an unlabelled one makes the MERGE read which nodes exist, and the CREATE then gets a barrier.
TEST_F(WriteVisibilityPlan, DrainBetweenAReadThroughAnAliasOfAMergedNodeAndALaterWrite) {
  EXPECT_TRUE(DrainsBeforeSet(
      Chain(Plan("UNWIND [1, 2] AS i MERGE p = (a:A)-[:R]->(b:B) CREATE (c {v: nodes(p)[0].x}) SET a.x = i"))));
  EXPECT_TRUE(DrainsBeforeSet(Chain(
      Plan("UNWIND [1, 2] AS i MERGE p = (a:A)-[r:R]->(b:B) CREATE (c {v: relationships(p)[0].w}) SET r.w = i"))));
  EXPECT_TRUE(DrainsBeforeSet(
      Chain(Plan("UNWIND [1, 2] AS i MERGE (a:A) CALL (a) { RETURN a AS b } CREATE (c {v: b.x}) SET a.x = i"))));
  // A key nothing later writes needs no drain.
  EXPECT_FALSE(DrainsBeforeSet(
      Chain(Plan("UNWIND [1, 2] AS i MERGE p = (a:A)-[:R]->(b:B) CREATE (c {v: nodes(p)[0].x}) SET a.y = i"))));
}

// A CALL result is fresh only when it may hold an entity and derives from a fresh one: a scalar, or a node the body
// only passes through, needs no barrier or drain. One the body binds itself may derive from the fresh node.
TEST_F(WriteVisibilityPlan, CallResultIsFreshOnlyWhenItMayHoldAFreshEntity) {
  EXPECT_TRUE(
      Collect<Accumulate>(Plan("MATCH (o:O) UNWIND [1, 2] AS i MERGE (n:N {id: i}) CALL (n) { RETURN n.id AS j } "
                               "CREATE (:M {v: o.x, j: j}) SET o.x = i"))
          .empty());
  EXPECT_TRUE(
      Collect<Accumulate>(Plan("MATCH (o:O) UNWIND [1, 2] AS i MERGE (a:F) CALL (a, o) { RETURN o AS o2, a AS a2 } "
                               "CREATE (:C {v: size([(o2)-[:R]->(:Q) | 1])})"))
          .empty());
  EXPECT_TRUE(DrainsBeforeSet(Chain(
      Plan("UNWIND [1, 2] AS i MERGE (a:A) CALL (a) { WITH a AS x RETURN x AS b } CREATE (c {v: b.x}) SET a.x = i"))));
  EXPECT_TRUE(DrainsBeforeSet(
      Chain(Plan("UNWIND [1, 2] AS i MERGE (a:A) CALL (a) { RETURN [a] AS b } CREATE (c {v: b[0].x}) SET a.x = i"))));
}

TEST_F(WriteVisibilityPlan, DrainBetweenADeleteThatReadsAndALaterWriteOfTheKey) {
  EXPECT_EQ(Chain(Plan("MATCH (a), (b) DETACH DELETE (CASE WHEN b.x = 1 THEN a ELSE null END) SET b.x = 1")),
            (Names{"EmptyResult", "SetProperty", "Accumulate+", "Delete", "Cartesian"}));
  EXPECT_EQ(Chain(Plan("MATCH (a), (b) DETACH DELETE (CASE WHEN b.x = 1 THEN a ELSE null END) SET b.y = 1")),
            (Names{"EmptyResult", "SetProperty", "Delete", "Cartesian"}));
}

// ---------------------------------------------------------------- every operator of a clause reads its view

TEST_F(WriteVisibilityPlan, RootPatternComprehensionReadsOld) {
  auto &plan = Plan("MATCH (a), (c) WITH a, c, [(a)-[:R]->(m) | m.x] AS v SET c.x = 0 RETURN v");
  auto views = Views<Produce>(plan);
  ASSERT_FALSE(views.empty());
  for (auto view : views) EXPECT_EQ(view, View::OLD);
}

TEST_F(WriteVisibilityPlan, MergeMatchFilterReadsNew) {
  auto &plan = Plan("UNWIND [1, 2] AS i MERGE (n:L {k: 1}) ON MATCH SET n.k = 5");
  auto views = Views<Filter>(plan);
  ASSERT_FALSE(views.empty());
  for (auto view : views) EXPECT_EQ(view, View::NEW);
}

// An EXISTS or a pattern comprehension in a Filter's WHERE reads the Filter's view.
TEST_F(WriteVisibilityPlan, SubqueriesInTheFilterOfANewMatchingReadNew) {
  for (auto const *query : {
           "MATCH (a) SET a.c = size([(a)-[*0]-(x) WHERE EXISTS { MATCH (x) WHERE x.c IS NOT NULL } | x])",
           "MATCH (a) SET a.c = size([(a)-[*0]-(x) WHERE [(x)-[*0]-(y) WHERE y.c IS NOT NULL | y] <> [] | x])",
       }) {
    auto &plan = Plan(query);
    auto filters = Views<Filter>(plan);
    ASSERT_GE(filters.size(), 2) << query;
    for (auto view : filters) EXPECT_EQ(view, View::NEW) << query;
    for (auto view : Views<Produce>(plan)) EXPECT_EQ(view, View::NEW) << query;
  }
}

TEST_F(WriteVisibilityPlan, RewrittenHashJoinInAWritingBodyReadsNew) {
  auto &plan = Plan(
      "CALL () { MATCH (a:A) SET a.k = 2 WITH * MATCH (x:A), (b:B) WHERE x.k = b.k RETURN count(*) AS c } "
      "RETURN c");
  EXPECT_EQ(Views<HashJoin>(plan), (std::vector{View::NEW}));
}

TEST_F(WriteVisibilityPlan, RewrittenMembershipUnwindInAWritingBodyReadsNew) {
  LabelPropertyIndex("L", "k");
  auto &plan = Plan(
      "CALL () { MATCH (a:A) SET a.l = [2] WITH * MATCH (x:L) WHERE x.k IN a.l RETURN count(*) AS c } "
      "RETURN c");
  ASSERT_EQ(Collect<ScanAllByLabelProperties>(plan).size(), 1);
  auto views = Views<memgraph::query::plan::Unwind>(plan);
  ASSERT_FALSE(views.empty());
  for (auto view : views) EXPECT_EQ(view, View::NEW);
}

TEST_F(WriteVisibilityPlan, PropertyCacheReadsTheProjectionsView) {
  auto &plan = Plan("MATCH (a), (b) WITH a, b, b.x AS bx, b.y AS by SET b.x = 0 RETURN bx");
  EXPECT_EQ(Views<CacheProperties>(plan), (std::vector{View::OLD}));
  ExpectCloneKeepsView<CacheProperties>(plan, storage_);
}

TEST_F(WriteVisibilityPlan, FunctionTableMarksTheGraphReadingFunctions) {
  for (auto name : {"labels", "keys", "properties", "values", "propertySize", "degree", "inDegree", "outDegree"}) {
    EXPECT_TRUE(FunctionReadsGraph(name)) << name;
  }
  EXPECT_FALSE(FunctionReadsGraph("toUpper"));
  EXPECT_FALSE(FunctionReadsGraph("id"));
  // Not built in, so a module function, which may read anything.
  EXPECT_TRUE(FunctionReadsGraph("module.function"));
}

// ---------------------------------------------------------------- the items of one SET or REMOVE

TEST_F(WriteVisibilityPlan, ParserMarksTheItemsAfterTheFirstOfOneSetOrRemove) {
  auto *query = Parse("MATCH (a) SET a.x = 1, a.y = 2 REMOVE a.z, a:L SET a.w = 3");
  auto const &clauses = query->single_query_->clauses_;
  ASSERT_EQ(clauses.size(), 6);
  std::vector<bool> continues;
  for (auto *clause : clauses) continues.push_back(clause->continues_previous_);
  EXPECT_EQ(continues, (std::vector<bool>{false, false, true, false, true, false}));
  EXPECT_TRUE(clauses[2]->Clone(&storage_)->continues_previous_);
  EXPECT_TRUE(clauses[4]->Clone(&storage_)->continues_previous_);
}

TEST_F(WriteVisibilityPlan, NoBarrierBetweenTheItemsOfOneSet) {
  // The second item reads what the first writes, so a Produce evaluates the right-hand sides first.
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1, a.y = a.x")),
            (Names{"EmptyResult", "SetProperty", "SetProperty", "Produce", "ScanAll", "Once"}));
  // Separate clauses keep their barrier.
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 SET a.y = a.x")),
            (Names{"EmptyResult", "SetProperty", "Accumulate+", "SetProperty", "ScanAll", "Once"}));
}

TEST_F(WriteVisibilityPlan, OneSetDecidesItsBarrierFromAllItsItems) {
  // The second item reads what an earlier clause wrote: the barrier goes before the whole SET.
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1 SET a.z = 0, a.y = a.x")),
            (Names{"EmptyResult", "SetProperty", "SetProperty", "Accumulate+", "SetProperty", "ScanAll", "Once"}));
}

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

// One SET evaluates every right-hand side before its first write when a later item reads what an earlier item
// writes: a Produce of the right-hand sides goes before the group. Independent items keep their plan.
TEST_F(WriteVisibilityPlan, OneSetEvaluatesItsRightHandSidesFirst) {
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1, a.y = a.x + 1")),
            (Names{"EmptyResult", "SetProperty", "SetProperty", "Produce", "ScanAll", "Once"}));
  // The property cache under the Produce reads the two keys once.
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = a.y, a.y = a.x")),
            (Names{"EmptyResult", "SetProperty", "SetProperty", "Produce", "CacheProperties", "ScanAll", "Once"}));
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.x = 1, a.y = a.z + 1")),
            (Names{"EmptyResult", "SetProperty", "SetProperty", "ScanAll", "Once"}));
  // The snapshot is per variable: items on two variables read each other in order.
  EXPECT_TRUE(Collect<Produce>(Plan("MATCH (a), (b) SET a.x = b.x, b.x = a.x")).empty());
  EXPECT_FALSE(Collect<Produce>(Plan("MATCH (a) FOREACH (i IN [1] | SET a.x = i, a.y = a.x)")).empty());
  EXPECT_FALSE(Collect<Produce>(Plan("MERGE (a:L {k: 1}) ON CREATE SET a.x = 1, a.y = a.x")).empty());
  EXPECT_FALSE(Collect<Produce>(Plan("MERGE (a:L {k: 1}) ON MATCH SET a.x = 1, a.y = a.x")).empty());
}

// ---------------------------------------------------------------- each write fences a filter on its own

// Under batching no barrier is planned, so a CALL ... YIELD WHERE sits directly on the write before it, and the write's
// own fence decides whether a rewriter moves the filter into a scan below it.
TEST_F(WriteVisibilityPlan, EachWriteKeepsAYieldWhereAboveIt) {
  LabelPropertyIndex("Q", "x");
  for (auto const *write : {"SET a.x = 5",
                            "SET a += {x: 5}",
                            "SET a = {x: 5}",
                            "SET a:L",
                            "REMOVE a.x",
                            "REMOVE a:L",
                            "CREATE (:Z)",
                            "CREATE (a)-[:R]->(:Z)",
                            "MERGE (:Z)",
                            "DETACH DELETE a",
                            "FOREACH (i IN [1] | SET a.x = i)"}) {
    auto &plan = Plan(fmt::format(
        "USING PERIODIC COMMIT 10 MATCH (a:Q) {} CALL mg.procedures() YIELD name WHERE a.x = 5 RETURN count(*) AS c",
        write));
    EXPECT_TRUE(Collect<ScanAllByLabelProperties>(plan).empty()) << write;
  }
  // A read procedure is no fence.
  auto &plan =
      Plan("USING PERIODIC COMMIT 10 MATCH (a:Q) CALL mg.procedures() YIELD name WHERE a.x = 5 RETURN count(*) AS c");
  EXPECT_EQ(Collect<ScanAllByLabelProperties>(plan).size(), 1);
}

TEST_F(WriteVisibilityPlan, EachWriteKeepsAYieldWhereOutOfAHashJoin) {
  for (auto const *write : {"SET a.x = 5",
                            "SET a += {x: 5}",
                            "SET a:L",
                            "REMOVE a.x",
                            "CREATE (:Z)",
                            "MERGE (:Z)",
                            "DETACH DELETE a",
                            "FOREACH (i IN [1] | SET a.x = i)"}) {
    auto &plan =
        Plan(fmt::format("USING PERIODIC COMMIT 10 MATCH (a:Q), (b:P) {} CALL mg.procedures() YIELD name WHERE a.x = "
                         "b.y RETURN count(*) AS c",
                         write));
    EXPECT_TRUE(Collect<HashJoin>(plan).empty()) << write;
  }
  auto &plan = Plan(
      "USING PERIODIC COMMIT 10 MATCH (a:Q), (b:P) CALL mg.procedures() YIELD name WHERE a.x = b.y RETURN count(*) AS "
      "c");
  EXPECT_EQ(Collect<HashJoin>(plan).size(), 1);
}

TEST_F(WriteVisibilityPlan, EachWriteKeepsAYieldWhereOutOfAnEdgeIndexSeek) {
  auto const edge_type = dba_.EdgeType("E");
  dba_.SetIndexCount(edge_type, 1);
  dba_.SetIndexCount(edge_type, dba_.Property("w"), 1);
  for (auto const *write : {"SET r.w = 5",
                            "SET r += {w: 5}",
                            "REMOVE r.w",
                            "SET n:L",
                            "CREATE (:Z)",
                            "MERGE (:Z)",
                            "DELETE r",
                            "FOREACH (i IN [1] | SET r.w = i)"}) {
    auto &plan =
        Plan(fmt::format("USING PERIODIC COMMIT 10 MATCH (n)-[r:E]->() {} CALL mg.procedures() YIELD name WHERE r.w = "
                         "5 RETURN count(*) AS c",
                         write));
    EXPECT_TRUE(Collect<ScanAllByEdgeTypeProperty>(plan).empty()) << write;
  }
  auto &plan = Plan(
      "USING PERIODIC COMMIT 10 MATCH (n)-[r:E]->() CALL mg.procedures() YIELD name WHERE r.w = 5 RETURN count(*) AS "
      "c");
  EXPECT_EQ(Collect<ScanAllByEdgeTypeProperty>(plan).size(), 1);
}

// The point index answers at View::OLD only, so a writing body, which reads View::NEW, scans and filters instead.
TEST_F(WriteVisibilityPlan, AWritingCallBodySeeksNoPointIndex) {
  dba_.SetPointIndex(dba_.Label("Pt"), dba_.Property("loc"));
  auto const *filter = "WHERE point.distance(n.loc, point({x: 0, y: 0})) < 10 RETURN count(n) AS c";
  EXPECT_EQ(Collect<ScanAllByPointDistance>(Plan(fmt::format("MATCH (n:Pt) {}", filter))).size(), 1);
  EXPECT_TRUE(Collect<ScanAllByPointDistance>(
                  Plan(fmt::format("UNWIND [1, 2] AS i CALL (i) {{ CREATE (:Pt {{loc: point({{x: i, y: 0}})}}) WITH 1 "
                                   "AS one MATCH (n:Pt) {} }} RETURN collect(c) AS cs",
                                   filter)))
                  .empty());
}

// ---------------------------------------------------------------- every reader of a writing CALL body reads View::NEW

// Batching is per level: the root of a query with an IN TRANSACTIONS call keeps its advancing barriers.
TEST_F(WriteVisibilityPlan, ARootWithAnInTransactionsCallKeepsItsBarriers) {
  auto &plan = Plan(
      "UNWIND [1, 2] AS i CREATE (:L) MERGE (m:L) WITH count(*) AS n CALL () { CREATE (:Z) } IN TRANSACTIONS OF 10 "
      "ROWS MATCH (k:L) RETURN n, count(k) AS c");
  auto merges = Collect<memgraph::query::plan::Merge>(plan);
  ASSERT_EQ(merges.size(), 1);
  auto *barrier = dynamic_cast<Accumulate *>(merges[0]->input_.get());
  ASSERT_NE(barrier, nullptr);
  EXPECT_TRUE(barrier->advance_command_);
}

// Pinned, known (write_visibility_followups_plan.md §8.4, Q3 interim): under USING PERIODIC COMMIT no barrier is
// planned and the views stay master's: projections read NEW, ORDER BY reads OLD. So ORDER BY n.x LIMIT 1 after a
// SET with a batch larger than the rows returns 9, where the clause rule gives 7.
TEST_F(WriteVisibilityPlan, PeriodicCommitKeepsMastersViews) {
  auto &plan = Plan("USING PERIODIC COMMIT 100 MATCH (n:P) SET n.x = 10 - n.x RETURN n.x AS x ORDER BY n.x LIMIT 1");
  EXPECT_EQ(Views<Produce>(plan), (std::vector{View::NEW}));
  EXPECT_EQ(Views<OrderBy>(plan), (std::vector{View::OLD}));
}

// Pinned, known: under USING PERIODIC COMMIT a read after a write in the same batch sees later clauses' writes of
// earlier rows. Neither shape gets a barrier (i14_pc_drain, review B3).
TEST_F(WriteVisibilityPlan, PeriodicCommitPlansNoBarriers) {
  EXPECT_TRUE(
      Collect<Accumulate>(Plan("USING PERIODIC COMMIT 100 MATCH (a:A), (b:A) SET a.y = b.x SET b.x = 0")).empty());
  // B3: the WITH after the SET keeps master's drain, and nothing fences the EXISTS from the later CREATE.
  EXPECT_EQ(Chain(Plan("USING PERIODIC COMMIT 10 UNWIND [1, 2] AS i MATCH (a:BA) SET a.k = i WITH i, a, "
                       "EXISTS { MATCH (a)-[*]->() } AS e CREATE (a)-[:R]->(:BB) RETURN i, e ORDER BY i")),
            (Names{"OrderBy",
                   "Produce",
                   "PeriodicCommit",
                   "CreateExpand",
                   "Produce",
                   "RollUpApply",
                   "Accumulate+",
                   "SetProperty",
                   "Filter",
                   "ScanAll",
                   "Unwind",
                   "Once"}));
}

// The root RETURN after an IN TRANSACTIONS call drains every batch before it advances, so no scan is open: kept.
TEST_F(WriteVisibilityPlan, RootReturnAfterAnInTransactionsCallAdvances) {
  auto &plan = Plan("MATCH (n:P) CALL (n) { SET n.x = 1 } IN TRANSACTIONS OF 10 ROWS RETURN n.x AS x ORDER BY x");
  auto accumulates = Collect<Accumulate>(plan);
  ASSERT_FALSE(accumulates.empty());
  EXPECT_TRUE(accumulates.front()->advance_command_);
}

// A writing IN TRANSACTIONS body is a writing tree: its cursor runs the body to the end for each input row before it
// commits, so a barrier inside it drains without advancing, and its readers read NEW.
TEST_F(WriteVisibilityPlan, AWritingInTransactionsBodyIsAWritingTree) {
  auto &plan = Plan(
      "CALL () { UNWIND [1, 2] AS j CREATE (:L) MERGE (m:L) RETURN count(*) AS n } IN TRANSACTIONS OF 1 ROWS "
      "RETURN n");
  auto periodic = Collect<PeriodicSubquery>(plan);
  ASSERT_EQ(periodic.size(), 1);
  auto &body = *periodic[0]->subquery_;
  auto accumulates = Collect<Accumulate>(body);
  ASSERT_EQ(accumulates.size(), 1);
  EXPECT_FALSE(accumulates[0]->advance_command_);
  for (auto view : Views<Produce>(body)) EXPECT_EQ(view, View::NEW);
  for (auto view : Views<memgraph::query::plan::Unwind>(body)) EXPECT_EQ(view, View::NEW);
}

/// Counts, per operator type, the operators under every Apply of @p plan, and names each that does not read View::NEW.
template <class... TOps>
void CheckBodyViews(LogicalOperator &plan, std::map<std::string, int> &counts, std::vector<std::string> &old_readers) {
  for (auto *apply : Collect<Apply>(plan)) {
    (
        [&] {
          for (auto *op : Collect<TOps>(*apply->subquery_)) {
            std::string const name{TOps::kType.name};
            ++counts[name];
            if (op->view_ != View::NEW) old_readers.push_back(name);
          }
        }(),
        ...);
  }
}

// After the rewriters too: a rewriter that builds an operator must copy the view of what it replaces.
TEST_F(WriteVisibilityPlan, EveryReaderOfAWritingCallBodyReadsNew) {
  LabelPropertyIndex("Q", "x");
  dba_.SetIndexCount(dba_.Label("L"), 1);
  dba_.SetIndexCount(dba_.EdgeType("E"), 1);
  std::map<std::string, int> counts;
  std::vector<std::string> old_readers;
  for (auto const *body : {
           "MATCH (n:Q {x: 1}) RETURN n.x AS v",
           "MATCH (n:Q) WHERE n.x IN [1, 2] RETURN n.x AS v",
           "MATCH (n:L) RETURN n.x AS v",
           "MATCH (n) WHERE id(n) = 1 RETURN n.x AS v",
           "MATCH ()-[r:E]->() RETURN r.w AS v",
           "MATCH (a:P), (b:K) WHERE a.x = b.y RETURN a.x AS v",
           "MATCH (n:P)-[:R]->(m) RETURN m.x AS v",
           "MATCH (n:P)-[*1..2]->(m) RETURN count(m) AS v",
           "MATCH (n:P) WITH n MATCH (n)-[*1..2]->(m:L) RETURN count(m) AS v",
           "MATCH (n:P) WITH n ORDER BY n.x RETURN n.x AS v",
           "MATCH (n:P) RETURN n.x + n.y + n.z AS v",
           "CALL mg.procedures() YIELD name RETURN name AS v",
           "UNWIND [1, 2] AS u RETURN u AS v",
       }) {
    auto &plan = Plan(fmt::format("UNWIND [1, 2] AS i CALL (i) {{ CREATE (:N {{i: i}}) WITH i {} }} RETURN v", body));
    CheckBodyViews<ScanAll,
                   ScanAllByLabel,
                   ScanAllByLabelProperties,
                   ScanAllById,
                   ScanAllByEdgeType,
                   Expand,
                   ExpandVariable,
                   Filter,
                   Produce,
                   Aggregate,
                   OrderBy,
                   memgraph::query::plan::Unwind,
                   HashJoin,
                   CacheProperties,
                   CreateNode,
                   memgraph::query::plan::CallProcedure>(plan, counts, old_readers);
  }
  EXPECT_TRUE(old_readers.empty()) << fmt::format("{}", fmt::join(old_readers, ", "));
  for (auto const *name : {"ScanAll",
                           "ScanAllByLabel",
                           "ScanAllByLabelProperties",
                           "ScanAllById",
                           "ScanAllByEdgeType",
                           "Expand",
                           "ExpandVariable",
                           "Filter",
                           "Produce",
                           "Aggregate",
                           "OrderBy",
                           "Unwind",
                           "HashJoin",
                           "CacheProperties",
                           "CreateNode",
                           "CallProcedure"}) {
    EXPECT_GT(counts[name], 0) << name;
  }
}

// An EXISTS branch planned after a write, here inside a SET's fold, reads View::NEW in every operator, not only in its
// MATCH; the root scan and the root RETURN still read View::OLD.
TEST_F(WriteVisibilityPlan, AnExistsBranchAfterAWriteReadsNewThroughout) {
  struct Case {
    char const *body;
    char const *reader;
  };

  for (auto const [body, reader] : {
           Case{"MATCH (b) WITH b.x AS bx WHERE bx = 1 RETURN 1", "Filter"},
           Case{"MATCH (b) WITH b WHERE b.x = 1 RETURN 1", "Filter"},
           Case{"MATCH (b) WITH b ORDER BY b.x LIMIT 1 RETURN b.x AS bx", "OrderBy"},
           Case{"MATCH (b) UNWIND [b.x] AS u WITH u WHERE u = 1 RETURN 1", "Unwind"},
       }) {
    auto &plan = Plan(fmt::format(
        "MATCH (a:A) SET a.x = 1, a.c = size([(a)-->(b) WHERE EXISTS {{ {} }} | b]) RETURN a.c AS c", body));
    auto *root = dynamic_cast<Produce *>(&plan);
    ASSERT_NE(root, nullptr) << body;
    EXPECT_EQ(root->view_, View::OLD) << body;
    auto const root_scans = Collect<ScanAll>(plan);
    ASSERT_EQ(root_scans.size(), 1U) << body;
    EXPECT_EQ(root_scans[0]->view_, View::OLD) << body;
    std::map<std::string, int> counts;
    std::vector<std::string> old_readers;
    auto check = [&]<class TOp>(TOp *) {
      for (auto *fold : Collect<RollUpApply>(plan)) {
        for (auto *op : Collect<TOp>(*fold->list_collection_branch_)) {
          std::string const name{TOp::kType.name};
          ++counts[name];
          if (op->view_ != View::NEW) old_readers.push_back(name);
        }
      }
    };
    check(static_cast<Produce *>(nullptr));
    check(static_cast<Filter *>(nullptr));
    check(static_cast<OrderBy *>(nullptr));
    check(static_cast<memgraph::query::plan::Unwind *>(nullptr));
    EXPECT_GT(counts["Produce"], 0) << body;
    EXPECT_GT(counts[reader], 0) << body;
    EXPECT_TRUE(old_readers.empty()) << body << ": " << fmt::format("{}", fmt::join(old_readers, ", "));
  }
}

// ---------------------------------------------------------------- read and write sets of single arms

TEST_F(WriteVisibilityPlan, ExpressionReadsOfLabelsAndKeys) {
  EXPECT_TRUE(Access("MATCH (n) RETURN n{.*} AS m").reads.any_property);
  EXPECT_TRUE(Access("MATCH (n) RETURN n['x'] AS v").reads.properties.contains("x"));
  EXPECT_TRUE(Access("MATCH (n) RETURN n[$k] AS v").reads.any_property);
  EXPECT_TRUE(Access("RETURN [1, 2][0] AS v").reads.Empty());
  EXPECT_TRUE(Access("MATCH (n) RETURN n:A AS v").reads.labels.contains("A"));
  EXPECT_TRUE(Access("MATCH (n) SET n += $m").writes.any_property);
}

TEST_F(WriteVisibilityPlan, DeleteWritesEverything) {
  EXPECT_TRUE(Access("MATCH (n) DETACH DELETE n").writes.everything);
}

TEST_F(WriteVisibilityPlan, ConflictOnEdgeTypeAlone) {
  auto const create_t = Access("CREATE (x)-[y:T]->(z)").writes;
  EXPECT_TRUE(Conflicts(Access("MATCH (p:A)-[e:T]->(q:A) RETURN 1 AS one").reads, create_t));
  EXPECT_FALSE(Conflicts(Access("MATCH (p:A)-[e:U]->(q:A) RETURN 1 AS one").reads, create_t));
}

#ifdef MG_ENTERPRISE
// The access check of a label write reads the node's labels: it waits for a label write, and only for that.
TEST_F(WriteVisibilityPlan, LabelWriteAccessCheckConflictsOnlyWithALabelWrite) {
  auto const reads = Access("MATCH (n:A) SET n:B").reads;
  EXPECT_TRUE(reads.access_check_labels);
  EXPECT_TRUE(Conflicts(reads, Access("MATCH (m:C) SET m:D").writes));
  EXPECT_FALSE(Conflicts(Access("MATCH (n:A) SET n:B").reads, Access("MATCH (m:C) SET m.x = 1").writes));
}
#endif

TEST_F(WriteVisibilityPlan, NoDrainInsideOneSet) {
  EXPECT_EQ(Chain(Plan("MATCH (a) SET a.y = a.x, a.x = 1")),
            (Names{"EmptyResult", "SetProperty", "SetProperty", "ScanAll", "Once"}));
}

// A result no RETURN names, as in a UNION, may hold the fresh node: the scalar that drains nothing alone drains here.
TEST_F(WriteVisibilityPlan, AnUnnamedCallResultIsFresh) {
  EXPECT_TRUE(
      Collect<Accumulate>(Plan("MATCH (o:O) UNWIND [1, 2] AS i MERGE (n:N {id: i}) CALL (n) { RETURN n.id AS j } "
                               "CREATE (:M {v: o.x, j: j}) SET o.x = i"))
          .empty());
  EXPECT_FALSE(
      Collect<Accumulate>(Plan("MATCH (o:O) UNWIND [1, 2] AS i MERGE (n:N {id: i}) CALL (n) { RETURN n.id AS j "
                               "UNION RETURN n.id AS j } CREATE (:M {v: o.x, j: j}) SET o.x = i"))
          .empty());
}

// ---------------------------------------------------------------- EXPLAIN prints the views on request

std::vector<std::string> PrintedLines(LogicalOperator &plan, bool print_views) {
  std::stringstream out;
  PrettyPrint(static_cast<DbAccessor const *>(nullptr), &plan, &out, nullptr, print_views);
  std::vector<std::string> lines;
  for (std::string line; std::getline(out, line);) lines.push_back(line);
  return lines;
}

TEST_F(WriteVisibilityPlan, ExplainPrintsTheViewOfEachReaderOnRequest) {
  auto &plan = Plan("CALL () { MATCH (n) SET n.x = 1 RETURN count(n) AS c } MATCH (m) WHERE m.y = c RETURN c");
  auto const lines = PrintedLines(plan, true);
  auto const line_of = [&](std::string_view op) {
    auto it = std::ranges::find_if(lines, [&](auto const &line) { return line.contains(op); });
    return it == lines.end() ? std::string{} : *it;
  };
  EXPECT_TRUE(line_of("ScanAll (n)").ends_with(" [view: NEW]")) << fmt::format("{}", fmt::join(lines, "\n"));
  EXPECT_TRUE(line_of("ScanAll (m)").ends_with(" [view: OLD]")) << fmt::format("{}", fmt::join(lines, "\n"));
  EXPECT_TRUE(line_of("Filter").ends_with(" [view: OLD]")) << fmt::format("{}", fmt::join(lines, "\n"));
  EXPECT_TRUE(line_of("SetProperty").ends_with("SetProperty"));
  for (auto const &line : PrintedLines(plan, false)) EXPECT_FALSE(line.contains("[view:")) << line;
}

// ---------------------------------------------------------------- view oracle

// The view rule (write_visibility_plan.md §4.1), computed from an operator's ancestors rather than from the
// planner's scope state: OLD at the root and in a read-only CALL body; NEW in a writing CALL body tree, a MERGE,
// a FOREACH body, and a fold branch whose consumer reads NEW. A query under batching keeps master's views: no check.
class ViewOracle {
 public:
  struct Mismatch {
    std::string op;
    View expected;
    View actual;
  };

  std::vector<Mismatch> Check(LogicalOperator &root) {
    mismatches_.clear();
    set_values_.clear();
    CollectSetValues(root);
    if (!ContainsBatching(root)) Walk(root, false);
    return mismatches_;
  }

 private:
  template <class TOp>
  static bool Is(LogicalOperator const &op) {
    return memgraph::utils::IsSubtype(op, TOp::kType);
  }

  static bool IsWrite(LogicalOperator const &op) {
    if (auto const *proc = memgraph::utils::Downcast<plan::CallProcedure const>(&op)) {
      return proc->graph_access_ == GraphAccess::Write;
    }
    return Is<plan::CreateNode>(op) || Is<plan::CreateExpand>(op) || Is<plan::Delete>(op) ||
           Is<plan::SetProperty>(op) || Is<plan::SetProperties>(op) || Is<plan::SetLabels>(op) ||
           Is<plan::RemoveProperty>(op) || Is<plan::RemoveLabels>(op) || Is<plan::SetNestedProperty>(op) ||
           Is<plan::RemoveNestedProperty>(op) || Is<plan::Merge>(op) || Is<plan::Foreach>(op);
  }

  /// A clause that must see its own earlier rows, so its expressions read NEW. A CREATE map and a procedure's
  /// arguments read the scope's view.
  static bool ReadsNewClause(LogicalOperator const &op) {
    return Is<plan::Delete>(op) || Is<plan::SetProperty>(op) || Is<plan::SetProperties>(op) ||
           Is<plan::SetLabels>(op) || Is<plan::RemoveProperty>(op) || Is<plan::RemoveLabels>(op) ||
           Is<plan::SetNestedProperty>(op) || Is<plan::RemoveNestedProperty>(op) || Is<plan::Merge>(op) ||
           Is<plan::Foreach>(op);
  }

  /// Every child of an operator, branches included, paired with whether the branch is a nested body (a CALL body or
  /// a conditional branch), a writing clause's branch (MERGE, FOREACH) or a fold branch.
  enum class Edge : uint8_t { kInput, kBody, kWritingClause, kFold };

  static std::vector<std::pair<LogicalOperator *, Edge>> Children(LogicalOperator &op) {
    std::vector<std::pair<LogicalOperator *, Edge>> out;
    auto add = [&](auto const &child, Edge edge) {
      if (child) out.emplace_back(child.get(), edge);
    };
    if (auto *apply = memgraph::utils::Downcast<plan::Apply>(&op)) {
      add(apply->input_, Edge::kInput);
      add(apply->subquery_, Edge::kBody);
    } else if (auto *merge = memgraph::utils::Downcast<plan::Merge>(&op)) {
      add(merge->input_, Edge::kInput);
      add(merge->merge_match_, Edge::kWritingClause);
      add(merge->merge_create_, Edge::kWritingClause);
    } else if (auto *foreach = memgraph::utils::Downcast<plan::Foreach>(&op)) {
      add(foreach->input_, Edge::kInput);
      add(foreach->update_clauses_, Edge::kWritingClause);
    } else if (auto *filter = memgraph::utils::Downcast<plan::Filter>(&op)) {
      add(filter->input_, Edge::kInput);
      for (auto const &branch : filter->pattern_filters_) add(branch, Edge::kFold);
    } else if (auto *rollup = memgraph::utils::Downcast<plan::RollUpApply>(&op)) {
      add(rollup->input_, Edge::kInput);
      add(rollup->list_collection_branch_, Edge::kFold);
    } else if (auto *conditional = memgraph::utils::Downcast<plan::Conditional>(&op)) {
      add(conditional->input(), Edge::kInput);
      for (auto const &branch : conditional->branches_) {
        for (auto const &fold : branch.pattern_filters) add(fold, Edge::kFold);
        add(branch.plan, Edge::kBody);
      }
    } else if (auto *optional = memgraph::utils::Downcast<plan::Optional>(&op)) {
      add(optional->input_, Edge::kInput);
      add(optional->optional_, Edge::kInput);
    } else if (auto *u = memgraph::utils::Downcast<plan::Union>(&op)) {
      add(u->left_op_, Edge::kInput);
      add(u->right_op_, Edge::kInput);
    } else if (auto *cartesian = memgraph::utils::Downcast<plan::Cartesian>(&op)) {
      add(cartesian->left_op_, Edge::kInput);
      add(cartesian->right_op_, Edge::kInput);
    } else if (auto *join = memgraph::utils::Downcast<plan::HashJoin>(&op)) {
      add(join->left_op_, Edge::kInput);
      add(join->right_op_, Edge::kInput);
    } else if (auto *join = memgraph::utils::Downcast<plan::IndexedJoin>(&op)) {
      add(join->main_branch_, Edge::kInput);
      add(join->sub_branch_, Edge::kInput);
    } else if (auto *periodic = memgraph::utils::Downcast<plan::PeriodicSubquery>(&op)) {
      add(periodic->input_, Edge::kInput);
      add(periodic->subquery_, Edge::kBody);
    } else if (op.HasSingleInput()) {
      add(op.input(), Edge::kInput);
    } else if (!Is<plan::Once>(op)) {
      ADD_FAILURE() << "the oracle does not know the children of " << op.GetTypeInfo().name;
    }
    return out;
  }

  static bool ContainsWrite(LogicalOperator &op) {
    if (IsWrite(op)) return true;
    return std::ranges::any_of(Children(op), [](auto const &child) { return ContainsWrite(*child.first); });
  }

  /// USING PERIODIC COMMIT keeps master's views throughout; an IN TRANSACTIONS call is batching for its body only.
  static bool ContainsBatching(LogicalOperator &op) {
    if (Is<plan::PeriodicCommit>(op)) return true;
    return std::ranges::any_of(Children(op), [](auto const &child) { return ContainsBatching(*child.first); });
  }

  /// `reads_new`: the operator is in a scope whose every read is a NEW reader. `consumer_reads_new`: the operator
  /// that uses this one's rows is a clause that reads NEW; it decides a comprehension branch, whose list RollUpApply
  /// passes up to the operator above it (a SET right-hand side, a FOREACH list).
  void Walk(LogicalOperator &op, bool reads_new, bool consumer_reads_new = false, bool under_set_values = false) {
    auto const expected = reads_new || IsSetGroupProduce(op) || (Is<plan::CacheProperties>(op) && under_set_values)
                              ? View::NEW
                              : View::OLD;
    if (auto const actual = op.ReadView(); actual && *actual != expected) {
      mismatches_.push_back({std::string{op.GetTypeInfo().name}, expected, *actual});
    }
    auto const is_rollup = Is<plan::RollUpApply>(op);
    for (auto [child, edge] : Children(op)) {
      switch (edge) {
        case Edge::kInput:
          // The property cache under a SET's Produce caches what that Produce reads.
          Walk(*child, reads_new, is_rollup ? consumer_reads_new : ReadsNewClause(op), IsSetGroupProduce(op));
          break;
        case Edge::kBody:
          // A read-only IN TRANSACTIONS body keeps master's views.
          if (Is<plan::PeriodicSubquery>(op) && !ContainsWrite(*child)) break;
          Walk(*child, reads_new || ContainsWrite(*child));
          break;
        case Edge::kWritingClause:
          Walk(*child, true);
          break;
        case Edge::kFold:
          // A Filter or Conditional evaluates its own folds; a RollUpApply's list goes to the operator above it.
          Walk(*child, reads_new || (is_rollup && consumer_reads_new));
          break;
      }
    }
  }

  /// The symbols SET items read their right-hand side from, when one SET evaluated them up front.
  void CollectSetValues(LogicalOperator &op) {
    Expression *rhs = nullptr;
    if (auto *set = memgraph::utils::Downcast<plan::SetProperty>(&op)) rhs = set->rhs_;
    if (auto *set = memgraph::utils::Downcast<plan::SetProperties>(&op)) rhs = set->rhs_;
    if (auto *set = memgraph::utils::Downcast<plan::SetNestedProperty>(&op)) rhs = set->rhs_;
    if (auto *ident = memgraph::utils::Downcast<Identifier>(rhs)) set_values_.insert(ident->symbol_pos_);
    for (auto [child, edge] : Children(op)) CollectSetValues(*child);
  }

  /// The Produce of one SET's right-hand sides is part of that SET, which reads NEW: the planner names each value
  /// with an anonymous symbol that a SET item reads.
  bool IsSetGroupProduce(LogicalOperator &op) const {
    auto *produce = memgraph::utils::Downcast<plan::Produce>(&op);
    return produce && !produce->named_expressions_.empty() &&
           std::ranges::all_of(produce->named_expressions_, [this](auto const *named) {
             return named->name_.starts_with("anon") && set_values_.contains(named->symbol_pos_);
           });
  }

  std::vector<Mismatch> mismatches_;
  std::unordered_set<int32_t> set_values_;
};

/// One planning environment per query, so the indexes of one scenario do not leak into the next.
struct PlannedQuery {
  AstStorage storage;
  Parameters parameters;
  SymbolTable symbol_table;
  FakeDbAccessor dba;
  std::unique_ptr<LogicalOperator> plan;

  /// Null on a query the unit planner cannot plan (a procedure, a parameter, a refusal).
  LogicalOperator *Plan(std::string const &query_string) try {
    // A parameter's value is looked up while planning; the fixture has none to give.
    if (query_string.contains('$')) return nullptr;
    ::frontend::opencypher::Parser parser{query_string};
    ParsingContext context{.is_query_cached = false};
    CypherMainVisitor visitor{context, &storage, &parameters};
    visitor.visit(parser.tree());
    auto *query = memgraph::utils::Downcast<CypherQuery>(visitor.query());
    if (query == nullptr) return nullptr;
    symbol_table = MakeSymbolTable(query);
    auto planning_context = MakePlanningContext(&storage, &symbol_table, query, &dba);
    PostProcessor post_processor(parameters, {}, &dba);
    auto query_parts = CollectQueryParts(symbol_table, storage, query, false);
    plan = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(query_parts, &planning_context);
    plan = post_processor.Rewrite(std::move(plan), &planning_context);
    return plan.get();
  } catch (std::exception const &) {
    return nullptr;
  }
};

struct CorpusQuery {
  std::string scenario;
  std::string query;
  std::vector<std::pair<std::string, std::optional<std::string>>> indexes;  // label, property
};

std::vector<CorpusQuery> FeatureCorpus(std::filesystem::path const &path) {
  std::ifstream in(path);
  std::vector<CorpusQuery> corpus;
  std::string scenario;
  std::vector<std::pair<std::string, std::optional<std::string>>> indexes;
  static std::regex const index_re{R"(with new index :(\w+)(?:\((\w+)\))?)"};
  for (std::string line; std::getline(in, line);) {
    std::smatch match;
    if (auto pos = line.find("Scenario"); pos != std::string::npos && line.find(':', pos) != std::string::npos) {
      scenario = line.substr(line.find(':', pos) + 2);
      indexes.clear();
    } else if (std::regex_search(line, match, index_re)) {
      indexes.emplace_back(match[1], match[2].matched ? std::optional<std::string>{match[2]} : std::nullopt);
    } else if (line.find("When executing query") != std::string::npos) {
      std::getline(in, line);  // the opening """
      std::string query;
      while (std::getline(in, line) && line.find(R"(""")") == std::string::npos) query += line + "\n";
      corpus.push_back({scenario, query, indexes});
    }
  }
  return corpus;
}

std::filesystem::path FeaturePath() {
  return std::filesystem::path{MG_SOURCE_DIR} / "tests/gql_behave/tests/memgraph_V1/features/write_visibility.feature";
}

/// The queries of write_visibility.feature, planned with and without their scenario's indexes, plus the shapes
/// the feature does not cover. Every view of every plan must follow the rule.
TEST(WriteVisibilityOracle, EveryViewFollowsTheRule) {
  auto corpus = FeatureCorpus(FeaturePath());
  ASSERT_GE(corpus.size(), 150) << FeaturePath();
  for (auto const *extra : {
           "MATCH (a:Pt) WHERE point.distance(a.loc, point({x: 0, y: 0})) < 10 SET a.k = 1 RETURN count(*) AS c",
           "UNWIND [1, 2] AS i CALL (i) { MATCH (n:Pt) WHERE point.distance(n.loc, point({x: 0, y: 0})) < 10 "
           "SET n.k = i RETURN count(n) AS c } RETURN collect(c) AS cs",
           "MATCH ()-[r:E]->() WHERE r.w = 1 SET r.w = 2 WITH * MATCH ()-[s:E]->() WHERE s.w = 2 RETURN count(*) AS c",
           "CALL () { MATCH (a:A) SET a.l = [2] WITH * MATCH (x:L) WHERE x.k IN a.l RETURN count(*) AS c } RETURN c",
           "CALL () { MATCH (a:A) SET a.k = 2 WITH * MATCH (x:A), (b:B) WHERE x.k = b.k RETURN count(*) AS c } "
           "RETURN c",
           "MATCH (n:L) SET n.k = n.k + 1 WITH n ORDER BY n.k RETURN n.k AS k",
           "CALL () { MATCH (n:L) SET n.k = 1 WITH n ORDER BY n.k RETURN collect(n.k) AS ks } RETURN ks",
           "MATCH (a:A) SET a.c = size([(a)-[*0]-(x) WHERE EXISTS { MATCH (x) WHERE x.c IS NOT NULL } | x])",
           "MATCH (a:A), (b:B) CALL (a) { MATCH (a)-[*BFS]->(x) RETURN count(x) AS n } SET b.n = n",
       }) {
    corpus.push_back({"extra", extra, {{"Pt", "loc"}, {"E", "w"}, {"L", "k"}, {"A", std::nullopt}}});
  }

  size_t planned = 0;
  std::vector<std::string> failures;
  for (auto const &item : corpus) {
    for (bool const with_indexes : {false, true}) {
      PlannedQuery planned_query;
      if (with_indexes) {
        for (auto const &[label, property] : item.indexes) {
          auto &dba = planned_query.dba;
          if (label == "E") {
            dba.SetIndexCount(dba.EdgeType(label), 1);
            if (property) dba.SetIndexCount(dba.EdgeType(label), dba.Property(*property), 1);
            continue;
          }
          dba.SetIndexCount(dba.Label(label), 1);
          if (property == "loc") {
            dba.SetPointIndex(dba.Label(label), dba.Property(*property));
          } else if (property) {
            dba.SetIndexCount(dba.Label(label), memgraph::storage::PropertyPath{dba.Property(*property)}, 1);
          }
        }
      }
      auto *plan = planned_query.Plan(item.query);
      if (plan == nullptr) continue;
      ++planned;
      for (auto const &m : ViewOracle{}.Check(*plan)) {
        failures.push_back(fmt::format("{} [{}] {}: expected {}, planned {}\n  {}",
                                       item.scenario,
                                       with_indexes ? "indexes" : "plain",
                                       m.op,
                                       m.expected == View::NEW ? "NEW" : "OLD",
                                       m.actual == View::NEW ? "NEW" : "OLD",
                                       item.query));
      }
    }
  }
  // Most of the corpus plans in the unit planner; a parser or fixture change that drops it must not pass silently.
  EXPECT_GE(planned, 2 * 150);
  EXPECT_TRUE(failures.empty()) << failures.size() << " views off the rule:\n"
                                << fmt::format("{}", fmt::join(failures, "\n"));
}

}  // namespace
