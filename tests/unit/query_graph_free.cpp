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

#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/graph_free.hpp"
#include "query/parameters.hpp"
#include "utils/typeinfo.hpp"

using namespace memgraph::query;
using memgraph::query::frontend::CypherMainVisitor;
using memgraph::query::frontend::ParsingContext;

namespace {

class GraphFreeTest : public ::testing::Test {
 protected:
  // Parses as an uncached query so literals stay literals. Stripping turns them into parameters, and both
  // forms have to answer the same way, which StrippedLiteralsAreStillGraphFree covers.
  bool IsGraphFreeQuery(std::string_view query_string, bool is_query_cached = false) {
    ::frontend::opencypher::Parser parser{std::string{query_string}};
    Parameters parameters;
    ParsingContext context{.is_query_cached = is_query_cached};
    CypherMainVisitor visitor{context, &storage_, &parameters};
    visitor.visit(parser.tree());
    auto *query = memgraph::utils::Downcast<CypherQuery>(visitor.query());
    EXPECT_NE(query, nullptr) << query_string;
    return IsGraphFree(*query);
  }

  void ExpectGraphFree(std::string_view query_string) {
    SCOPED_TRACE(query_string);
    EXPECT_TRUE(IsGraphFreeQuery(query_string));
  }

  void ExpectNeedsGraph(std::string_view query_string) {
    SCOPED_TRACE(query_string);
    EXPECT_FALSE(IsGraphFreeQuery(query_string));
  }

  AstStorage storage_;
};

TEST_F(GraphFreeTest, ConstantProjections) {
  ExpectGraphFree("RETURN 1");
  ExpectGraphFree("RETURN 1 AS APP_INTERNAL_EXEC_VAR");
  ExpectGraphFree("RETURN 1, 'x' AS s, true AS b, null AS n");
  ExpectGraphFree("RETURN 1 + 2 * 3 AS x");
  ExpectGraphFree("RETURN NOT true AND (1 < 2 OR 3 >= 4) AS x");
  ExpectGraphFree("RETURN CASE WHEN 1 < 2 THEN 'a' ELSE 'b' END AS x");
  ExpectGraphFree("RETURN coalesce(null, 2) AS x");
  ExpectGraphFree("RETURN [1, 2, 3] AS l, {a: 1, b: 'two'} AS m");
  ExpectGraphFree("RETURN [1, 2, 3][1] AS x");
  ExpectGraphFree("RETURN [1, 2, 3][0..2] AS x");
  ExpectGraphFree("RETURN 2 IN [1, 2, 3] AS x");
  ExpectGraphFree("RETURN 'abc' =~ 'a.*' AS x");
  ExpectGraphFree("RETURN $param AS x");
}

// Shapes the two hardcoded recognizers this analysis replaces could not accept.
TEST_F(GraphFreeTest, ComposedGraphFreeQueries) {
  ExpectGraphFree("UNWIND [1, 2, 3] AS x RETURN x");
  ExpectGraphFree("WITH 1 AS x RETURN x");
  ExpectGraphFree("WITH 1 AS x WHERE x > 0 RETURN x");
  ExpectGraphFree("UNWIND [3, 1, 2] AS x RETURN DISTINCT x ORDER BY x SKIP 1 LIMIT 1");
  ExpectGraphFree("UNWIND [1, 2, 3] AS x RETURN count(x) AS c, sum(x) AS s");
  ExpectGraphFree("RETURN 1 UNION RETURN 2");
  ExpectGraphFree("RETURN 1 UNION ALL RETURN 1");
  ExpectGraphFree("CALL mg.procedures() YIELD name RETURN count(name) AS c");
  ExpectGraphFree("CALL mg.functions() YIELD name, signature WHERE name <> signature RETURN name");
}

TEST_F(GraphFreeTest, GraphFreeProcedureCalls) {
  ExpectGraphFree("CALL mg.procedures() YIELD *");
  ExpectGraphFree("CALL mg.procedures() YIELD name");
  ExpectGraphFree("CALL mg.functions() YIELD name");
  ExpectGraphFree("CALL mg.transformations() YIELD name");
}

// Which builtins declare that they reach no graph. Every mg.* callback ignores its graph argument, so
// this set is a choice about which are worth running without a transaction, not a statement about which
// could be. Pinned here so that adding to it is deliberate.
TEST_F(GraphFreeTest, BuiltinsThatDeclareGraphFreedom) {
  ExpectGraphFree("CALL mg.procedures() YIELD name");
  ExpectGraphFree("CALL mg.functions() YIELD name");
  ExpectGraphFree("CALL mg.transformations() YIELD name");
  ExpectGraphFree("CALL mg.get_module_files() YIELD path");

  // Undeclared, so assumed to reach the graph even though it does not.
  ExpectNeedsGraph("CALL mg.get_module_file('example.py') YIELD content");
  ExpectNeedsGraph("CALL mg.load('example')");
  ExpectNeedsGraph("CALL mg.load_all()");
}

TEST_F(GraphFreeTest, PatternsNeedTheGraph) {
  ExpectNeedsGraph("MATCH (n) RETURN n");
  ExpectNeedsGraph("MATCH (n) RETURN 1");
  ExpectNeedsGraph("CREATE (n) RETURN 1");
  ExpectNeedsGraph("MERGE (n:Label) RETURN 1");
  ExpectNeedsGraph("MATCH (n) DETACH DELETE n");
  ExpectNeedsGraph("MATCH (n) SET n.p = 1");
  ExpectNeedsGraph("FOREACH (i IN [1] | CREATE (n))");
  ExpectNeedsGraph("UNWIND [1] AS x MATCH (n) RETURN x");
  ExpectNeedsGraph("RETURN 1 UNION MATCH (n) RETURN 1");
  ExpectNeedsGraph("MATCH (n) WHERE exists((n)-[]-()) RETURN n");
  ExpectNeedsGraph("MATCH (n) RETURN [(n)-[]->(m) | m] AS l");
}

TEST_F(GraphFreeTest, ExpressionsThatReachStorage) {
  // Every function receives the accessor and may use it, so a function of any kind is rejected. This is
  // what keeps the string predicates, which parse as functions, out: `x STARTS WITH 'a'` and friends.
  ExpectNeedsGraph("RETURN toString(1) AS x");
  ExpectNeedsGraph("RETURN 1 AS x ORDER BY toString(1)");
  ExpectNeedsGraph("RETURN 1 AS x LIMIT toInteger('1')");
  ExpectNeedsGraph("WITH 'abc' AS s WHERE s STARTS WITH 'a' RETURN s");
  // Enums are storage state.
  ExpectNeedsGraph("RETURN Color::RED AS x");
  // Property lookup names graph state, whatever it is applied to.
  ExpectNeedsGraph("WITH {a: 1} AS m RETURN m.a");
  // Expressions that bind an identifier of their own, so that the clause walker stays the only place
  // that decides what an identifier can be bound to.
  ExpectNeedsGraph("RETURN [x IN [1, 2] | x] AS l");
  ExpectNeedsGraph("RETURN all(x IN [1, 2] WHERE x > 0) AS b");
  ExpectNeedsGraph("RETURN reduce(acc = 0, x IN [1, 2] | acc + x) AS r");
}

TEST_F(GraphFreeTest, ProjectingEverythingNeedsTheGraph) { ExpectNeedsGraph("WITH 1 AS x RETURN *"); }

TEST_F(GraphFreeTest, ModifiersNeedATransaction) {
  ExpectNeedsGraph("RETURN 1 QUERY MEMORY LIMIT 1MB");
  ExpectNeedsGraph("USING HOPS LIMIT 1 RETURN 1");
  ExpectNeedsGraph("USING PERIODIC COMMIT 1 UNWIND [1] AS x RETURN x");
}

// Memgraph strips literals into parameters before planning a cacheable query, so the analysis sees
// ParameterLookup where the text had a literal. Both readings must agree.
TEST_F(GraphFreeTest, StrippedLiteralsAreStillGraphFree) {
  EXPECT_TRUE(IsGraphFreeQuery("RETURN 1", /*is_query_cached=*/true));
  EXPECT_TRUE(IsGraphFreeQuery("UNWIND [1, 2] AS x RETURN x", /*is_query_cached=*/true));
  EXPECT_FALSE(IsGraphFreeQuery("MATCH (n) RETURN 1", /*is_query_cached=*/true));
}

}  // namespace
