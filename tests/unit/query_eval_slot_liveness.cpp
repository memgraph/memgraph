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

#include <gtest/gtest.h>

#include <string>
#include <unordered_set>
#include <vector>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/stripped.hpp"

namespace {

using namespace memgraph::query;
using memgraph::query::frontend::CypherMainVisitor;
using memgraph::query::frontend::ParsingContext;

// Records every node the standard walk reaches. Evaluation gives each node one
// slot, so a node reached twice in one query would hold one value for two
// places in the tree, and an operator that evaluates both its operands before
// reading either would overwrite the first with the second.
class NodeCollector : public HierarchicalTreeVisitor {
 public:
  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;

  // void *, because a few node types reach Tree by more than one path.
  std::vector<void *> seen;

#define MG_RECORD_COMPOSITE(TYPE)      \
  bool PreVisit(TYPE &node) override { \
    seen.push_back(&node);             \
    return true;                       \
  }
#define MG_RECORD_LEAF(TYPE)        \
  bool Visit(TYPE &node) override { \
    seen.push_back(&node);          \
    return true;                    \
  }

  MG_RECORD_COMPOSITE(SingleQuery)
  MG_RECORD_COMPOSITE(CypherUnion)
  MG_RECORD_COMPOSITE(NamedExpression)
  MG_RECORD_COMPOSITE(OrOperator)
  MG_RECORD_COMPOSITE(XorOperator)
  MG_RECORD_COMPOSITE(AndOperator)
  MG_RECORD_COMPOSITE(NotOperator)
  MG_RECORD_COMPOSITE(AdditionOperator)
  MG_RECORD_COMPOSITE(SubtractionOperator)
  MG_RECORD_COMPOSITE(MultiplicationOperator)
  MG_RECORD_COMPOSITE(DivisionOperator)
  MG_RECORD_COMPOSITE(ModOperator)
  MG_RECORD_COMPOSITE(ExponentiationOperator)
  MG_RECORD_COMPOSITE(NotEqualOperator)
  MG_RECORD_COMPOSITE(EqualOperator)
  MG_RECORD_COMPOSITE(LessOperator)
  MG_RECORD_COMPOSITE(GreaterOperator)
  MG_RECORD_COMPOSITE(LessEqualOperator)
  MG_RECORD_COMPOSITE(GreaterEqualOperator)
  MG_RECORD_COMPOSITE(RangeOperator)
  MG_RECORD_COMPOSITE(InListOperator)
  MG_RECORD_COMPOSITE(SubscriptOperator)
  MG_RECORD_COMPOSITE(ListSlicingOperator)
  MG_RECORD_COMPOSITE(IfOperator)
  MG_RECORD_COMPOSITE(UnaryPlusOperator)
  MG_RECORD_COMPOSITE(UnaryMinusOperator)
  MG_RECORD_COMPOSITE(IsNullOperator)
  MG_RECORD_COMPOSITE(ListLiteral)
  MG_RECORD_COMPOSITE(MapLiteral)
  MG_RECORD_COMPOSITE(MapProjectionLiteral)
  MG_RECORD_COMPOSITE(PropertyLookup)
  MG_RECORD_COMPOSITE(AllPropertiesLookup)
  MG_RECORD_COMPOSITE(LabelsTest)
  MG_RECORD_COMPOSITE(Aggregation)
  MG_RECORD_COMPOSITE(Function)
  MG_RECORD_COMPOSITE(Reduce)
  MG_RECORD_COMPOSITE(Coalesce)
  MG_RECORD_COMPOSITE(Extract)
  MG_RECORD_COMPOSITE(All)
  MG_RECORD_COMPOSITE(Single)
  MG_RECORD_COMPOSITE(Any)
  MG_RECORD_COMPOSITE(None)
  MG_RECORD_COMPOSITE(ListComprehension)
  MG_RECORD_COMPOSITE(CallProcedure)
  MG_RECORD_COMPOSITE(Create)
  MG_RECORD_COMPOSITE(Match)
  MG_RECORD_COMPOSITE(Return)
  MG_RECORD_COMPOSITE(With)
  MG_RECORD_COMPOSITE(Pattern)
  MG_RECORD_COMPOSITE(NodeAtom)
  MG_RECORD_COMPOSITE(EdgeAtom)
  MG_RECORD_COMPOSITE(Delete)
  MG_RECORD_COMPOSITE(Where)
  MG_RECORD_COMPOSITE(SetProperty)
  MG_RECORD_COMPOSITE(SetProperties)
  MG_RECORD_COMPOSITE(SetLabels)
  MG_RECORD_COMPOSITE(RemoveProperty)
  MG_RECORD_COMPOSITE(RemoveLabels)
  MG_RECORD_COMPOSITE(Merge)
  MG_RECORD_COMPOSITE(Unwind)
  MG_RECORD_COMPOSITE(RegexMatch)
  MG_RECORD_COMPOSITE(LoadCsv)
  MG_RECORD_COMPOSITE(Foreach)
  MG_RECORD_COMPOSITE(SubqueryExpression)
  MG_RECORD_COMPOSITE(CallSubquery)
  MG_RECORD_COMPOSITE(CypherQuery)
  MG_RECORD_COMPOSITE(PatternComprehension)
  MG_RECORD_COMPOSITE(LoadParquet)
  MG_RECORD_COMPOSITE(EdgeTypesTest)
  MG_RECORD_COMPOSITE(LoadJsonl)

  MG_RECORD_LEAF(Identifier)
  MG_RECORD_LEAF(PrimitiveLiteral)
  MG_RECORD_LEAF(ParameterLookup)
  MG_RECORD_LEAF(EnumValueAccess)

#undef MG_RECORD_COMPOSITE
#undef MG_RECORD_LEAF
};

// The queries to walk. Each is here because it is a way one expression could
// come to stand in two places: a repeated subexpression, a value used twice, a
// predicate reused inside a comprehension, a pattern filtered twice.
std::vector<std::string> const kCorpus{
    "MATCH (n) WHERE n.a > 1 AND n.a > 1 RETURN n",
    "MATCH (n) RETURN n.a + n.a",
    "MATCH (n) WHERE n.a > 1 RETURN n.a, n.a",
    "MATCH (n) RETURN n.a AS x ORDER BY n.a",
    "MATCH (n) WITH n.a AS a WHERE a > 1 RETURN a + a",
    "MATCH (n) RETURN CASE WHEN n.a > 1 THEN n.a ELSE n.a END",
    "MATCH (n) RETURN [x IN n.list WHERE x > n.a | x + n.a]",
    "MATCH (n) RETURN reduce(acc = n.a, x IN n.list | acc + n.a)",
    "MATCH (n) WHERE n.a IN [n.a, n.b] RETURN n",
    "MATCH (n)-[e]->(m) WHERE n.a = m.a AND n.a = m.b RETURN n, m",
    "MATCH (n) RETURN all(x IN n.list WHERE x > n.a) AND any(y IN n.list WHERE y > n.a)",
    "MATCH (n) WHERE n.a > 1 RETURN count(n.a), sum(n.a)",
    "UNWIND [1, 2] AS i MATCH (n) WHERE n.a = i RETURN i + i",
    "MATCH (n) RETURN coalesce(n.a, n.a, n.b)",
    "MATCH (n) WHERE exists((n)-[]->()) AND n.a > 1 RETURN n",
};

Query *ParseOne(std::string const &query, AstStorage *storage) {
  ::frontend::opencypher::Parser parser(query);
  Parameters parameters;
  ParsingContext context;
  CypherMainVisitor visitor(context, storage, &parameters);
  visitor.visit(parser.tree());
  return visitor.query();
}

}  // namespace

// L2 in the plan: one slot per node is only sound if no node stands in two
// places. If this fails, the failing query names the shape that breaks it and
// the scheme needs a slot per use rather than per node.
//
// This covers the tree the parser builds. The rewrites that run between
// parsing and execution are free to point two parents at one node, and an
// expression reaching execution that way would break the same invariant, so
// the planner's output needs the same check before the evaluator relies on it.
TEST(EvalSlotLiveness, NoNodeIsReachedTwiceInOneQuery) {
  for (auto const &query : kCorpus) {
    AstStorage storage;
    auto *parsed = ParseOne(query, &storage);
    ASSERT_NE(parsed, nullptr) << query;
    auto *root = dynamic_cast<CypherQuery *>(parsed);
    ASSERT_NE(root, nullptr) << "not a cypher query: " << query;

    NodeCollector collector;
    root->Accept(collector);

    std::unordered_set<void *> distinct;
    for (auto *node : collector.seen) {
      EXPECT_TRUE(distinct.insert(node).second)
          << "a node stands in two places, so one slot would serve both, in: " << query;
    }
    EXPECT_FALSE(collector.seen.empty()) << query;
  }
}
