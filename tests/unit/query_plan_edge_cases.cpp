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

// tests in this suite deal with edge cases in logical operator behavior
// that's not easily testable with single-phase testing. instead, for
// easy testing and latter readability they are tested end-to-end.

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "query_execution_fixture.hpp"

DECLARE_bool(query_cost_planner);
DECLARE_bool(query_prefetch_properties);

template <typename StorageType>
class QueryExecution : public QueryExecutionFixture<StorageType> {
 protected:
  std::string SuiteName() const override { return "query_plan_edge_cases"; }
};

TYPED_TEST_SUITE(QueryExecution, StorageTypes);

TYPED_TEST(QueryExecution, MissingOptionalIntoExpand) {
  // validating bug where expanding from Null (due to a preceeding optional
  // match) exhausts the expansion cursor, even if it's input is still not
  // exhausted
  this->Execute(
      "CREATE (a:Person {id: 1}), (b:Person "
      "{id:2})-[:Has]->(:Dog)-[:Likes]->(:Food )");
  ASSERT_EQ(this->Execute("MATCH (n) RETURN n").size(), 4);

  auto Exec = [this](bool desc, const std::string &edge_pattern) {
    // this test depends on left-to-right query planning
    FLAGS_query_cost_planner = false;
    return this
        ->Execute(std::string("MATCH (p:Person) WITH p ORDER BY p.id ") + (desc ? "DESC " : "") +
                  "OPTIONAL MATCH (p)-->(d:Dog) WITH p, d "
                  "MATCH (d)" +
                  edge_pattern +
                  "(f:Food) "
                  "RETURN p, d, f")
        .size();
  };

  std::string expand = "-->";
  std::string variable = "-[*1]->";
  std::string bfs = "-[*bfs..1]->";

  EXPECT_EQ(Exec(false, expand), 1);
  EXPECT_EQ(Exec(true, expand), 1);
  EXPECT_EQ(Exec(false, variable), 1);
  EXPECT_EQ(Exec(true, bfs), 1);
  EXPECT_EQ(Exec(true, bfs), 1);
}

TYPED_TEST(QueryExecution, EdgeUniquenessInOptional) {
  // Validating that an edge uniqueness check can't fail when the edge is Null
  // due to optonal match. Since edge-uniqueness only happens in one OPTIONAL
  // MATCH, we only need to check that scenario.
  this->Execute("CREATE (), ()-[:Type]->()");
  ASSERT_EQ(this->Execute("MATCH (n) RETURN n").size(), 3);
  EXPECT_EQ(this->Execute("MATCH (n) OPTIONAL MATCH (n)-[r1]->(), (n)-[r2]->() "
                          "RETURN n, r1, r2")
                .size(),
            3);
}

// Two patterns in one MATCH may not both use the same relationship, and a variable-length
// relationship is still relationships for that purpose. The check runs over the symbols the
// second pattern brings in, so it has to recognise a symbol holding a list of relationships as
// well as one holding a single relationship. Written both ways round because only the second
// pattern's symbols are checked, so one order exercises the list and the other does not.
TYPED_TEST(QueryExecution, EdgeUniquenessAcrossPatternsOverAVariableLengthRelationship) {
  this->Execute("CREATE (:A)-[:T]->(:B)");

  // The graph holds one relationship, and it cannot serve both patterns, so there is nothing
  // to count and nothing to find.
  auto counted = this->Execute("MATCH (n:A) RETURN COUNT { MATCH (a)-[r*1..1]->(b), (c)-[e]->(d) RETURN 1 } AS k");
  EXPECT_EQ(counted[0][0].ValueInt(), 0);

  auto found = this->Execute("MATCH (n:A) RETURN EXISTS { MATCH (a)-[r*1..1]->(b), (c)-[e]->(d) RETURN 1 } AS k");
  EXPECT_FALSE(found[0][0].ValueBool());

  auto reversed = this->Execute("MATCH (n:A) RETURN COUNT { MATCH (c)-[e]->(d), (a)-[r*1..1]->(b) RETURN 1 } AS k");
  EXPECT_EQ(reversed[0][0].ValueInt(), 0);

  // Two relationships do give the patterns something to match, so the check is not refusing
  // everything.
  this->Execute("MATCH (n) DETACH DELETE n");
  this->Execute("CREATE (:A)-[:T]->(:B), (:A)-[:T]->(:B)");
  auto pairs = this->Execute("MATCH (n:A) RETURN COUNT { MATCH (a)-[r*1..1]->(b), (c)-[e]->(d) RETURN 1 } AS k");
  EXPECT_EQ(pairs[0][0].ValueInt(), 2);
}

TYPED_TEST(QueryExecution, NamedPathOverBoundNodeInSubqueryBody) {
  // A named path over nothing but one already-bound node is built on the value the caller bound.
  this->Execute("CREATE (:Node)");

  auto results = this->Execute("MATCH (n) RETURN COUNT { MATCH p = (n) } AS c");

  ASSERT_EQ(results.size(), 1U);
  EXPECT_EQ(results[0][0].ValueInt(), 1);
}

TYPED_TEST(QueryExecution, BoundNodeInSubqueryBodyIsNull) {
  // A pattern never matches a null node, so a body that is one already-bound node has to read the bound
  // value rather than fold to a constant.
  this->Execute("CREATE (:Person {name: 'lonely'})");

  auto results = this->Execute(
      "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(f) "
      "RETURN EXISTS { MATCH (f) } AS e, COUNT { MATCH (f) } AS c");

  ASSERT_EQ(results.size(), 1U);
  EXPECT_FALSE(results[0][0].ValueBool());
  EXPECT_EQ(results[0][1].ValueInt(), 0);
}

TYPED_TEST(QueryExecution, MatchOnBoundNodeIsNull) {
  // The same holds outside a subquery: re-stating a bound node as a whole pattern is a match, not a no-op.
  auto results = this->Execute("WITH null AS f MATCH (f) RETURN count(*) AS c");

  ASSERT_EQ(results.size(), 1U);
  EXPECT_EQ(results[0][0].ValueInt(), 0);
}

TYPED_TEST(QueryExecution, MatchOnBoundNodeIsNotANode) {
  // A null cannot match a pattern, but a value of any other type cannot even be asked: the pattern says the
  // variable holds a node, so anything else is a type error rather than a row that quietly matches.
  this->Execute("CREATE (:Person)-[:KNOWS]->(:Person)");

  EXPECT_THROW(this->Execute("WITH 1 AS f MATCH (f) RETURN count(*) AS c"), memgraph::query::QueryRuntimeException);

  // The type is not always known before the query runs: here only the rows without a friend hold the integer.
  EXPECT_THROW(this->Execute("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) "
                             "WITH coalesce(f, 1) AS g MATCH (g) RETURN count(*) AS c"),
               memgraph::query::QueryRuntimeException);

  // A node passes, so what the guard rejects is the type and not the reuse of a variable.
  auto results = this->Execute("MATCH (p:Person) WITH p AS g MATCH (g) RETURN count(*) AS c");
  ASSERT_EQ(results.size(), 1U);
  EXPECT_EQ(results[0][0].ValueInt(), 2);
}

TYPED_TEST(QueryExecution, MatchAfterOptionalMatchAndUnwind) {
  // MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b) UNWIND [1, 2] AS x MATCH (b)-[:S]->(c) RETURN a.i, x, c.i: the MATCH
  // expands from the optional's b, so a = 4, whose b is null, has no row.
  this->Execute(
      "CREATE (a:A {i: 1})-[:R]->(b {i: 2})-[:S]->({i: 3}), (b)-[:S]->({i: 5}), (:A {i: 4}), ({i: 6})-[:S]->({i: 7})");

  auto results = this->Execute(
      "MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b) UNWIND [1, 2] AS x MATCH (b)-[:S]->(c) "
      "RETURN a.i, x, c.i ORDER BY a.i, x, c.i");
  const std::vector<std::array<int64_t, 3>> expected{{1, 1, 3}, {1, 1, 5}, {1, 2, 3}, {1, 2, 5}};
  ASSERT_EQ(results.size(), expected.size());
  for (size_t row = 0; row < expected.size(); ++row) {
    for (size_t col = 0; col < 3; ++col) EXPECT_EQ(results[row][col].ValueInt(), expected[row][col]) << row;
  }
}

TYPED_TEST(QueryExecution, SubqueryBodyCorrelatesAcrossSeparatePatterns) {
  // Two patterns in a body reach each other's variables through what the body's first operator says it modifies.
  // The caller's variables have to be among them, or a filter naming one of them together with a variable of the
  // other pattern belongs to neither, and planning gives up.
  this->Execute("CREATE (:Person {name: 'a', age: 5}), (:Person {name: 'b', age: 1})");
  this->Execute("CREATE (:X {name: 'a'})-[:R]->(:Y {name: 'a'})");

  auto results = this->Execute(
      "MATCH (a:Person) "
      "RETURN COUNT { MATCH (a), (x)-[:R]->(y) WHERE y.name = a.name AND a.age > 3 } AS c "
      "ORDER BY c DESC");

  ASSERT_EQ(results.size(), 2U);
  EXPECT_EQ(results[0][0].ValueInt(), 1);
  EXPECT_EQ(results[1][0].ValueInt(), 0);
}

TYPED_TEST(QueryExecution, EqualityWithinOnePatternSurvivesAnUnrelatedPrecedingMatch) {
  // An equality between two variables of one pattern constrains that pattern alone. Pairing it with a second
  // pattern that matches once leaves the count unchanged.
  this->Execute("CREATE (:Meta), ({v: 1})-[:R]->({v: 1})");

  auto alone = this->Execute("MATCH (a)-->(b) WHERE a.v = b.v RETURN count(*) AS c");
  ASSERT_EQ(alone.size(), 1U);
  ASSERT_EQ(alone[0][0].ValueInt(), 1);

  auto with_preceding_match = this->Execute("MATCH (x:Meta) MATCH (a)-->(b) WHERE a.v = b.v RETURN count(*) AS c");
  ASSERT_EQ(with_preceding_match.size(), 1U);
  EXPECT_EQ(with_preceding_match[0][0].ValueInt(), 1);

  // Writing the two patterns as one clause is the same question.
  auto as_one_clause = this->Execute("MATCH (x:Meta), (a)-->(b) WHERE a.v = b.v RETURN count(*) AS c");
  ASSERT_EQ(as_one_clause.size(), 1U);
  EXPECT_EQ(as_one_clause[0][0].ValueInt(), 1);
}

TYPED_TEST(QueryExecution, ADeepVariableExpansionWalksBothWaysToEveryLength) {
  // The stack of one expansion per depth is grown several times over by a walk this deep, and how
  // far the walk has got into each depth has to survive that. Starting in the middle and going
  // undirected makes each step away follow a relationship against its direction on one side and
  // along it on the other, so the direction an edge was found in decides which end the walk
  // arrives at. From an end of the path, uniqueness would reject the one backward relationship
  // before its direction mattered.
  constexpr auto kReach = 8;

  this->Execute("CREATE (:P {id: 0})");
  for (auto i = 1; i <= 2 * kReach; ++i) {
    this->Execute("MATCH (prev:P {id: " + std::to_string(i - 1) +
                  "}) CREATE (prev)-[:R]->(:P {id: " + std::to_string(i) + "})");
  }

  auto const results = this->Execute("MATCH (:P {id: 8})-[*1..8]-(other) RETURN other.id AS id ORDER BY id");

  // Where the walk arrives, rather than how far it went. The lengths alone would be the same
  // whichever end of a relationship the walk took, so they do not say that it took the right one.
  auto expected = std::vector<int64_t>{};
  for (auto id = int64_t{0}; id <= 2 * kReach; ++id) {
    if (id != kReach) expected.push_back(id);
  }

  auto reached = std::vector<int64_t>{};
  reached.reserve(results.size());
  for (auto const &row : results) reached.push_back(row[0].ValueInt());

  EXPECT_EQ(reached, expected);
}

/// The queries the pass is allowed to change the plan of, plus the ones it must leave alone. Each fixture
/// gets a fresh plan cache, so the flag has to be compared across two tests rather than toggled inside one.
#define CHECK_PREFETCH_PROPERTIES_RESULTS()                                                                        \
  do {                                                                                                             \
    using BoltValue = memgraph::communication::bolt::Value;                                                        \
    this->Execute("CREATE (:P {id: 1, a: 1, b: 'x'}), (:P {id: 2, a: 2})");                                        \
    {                                                                                                              \
      /* Two properties on one symbol, the second missing on one of the nodes. */                                  \
      auto results = this->Execute("MATCH (n:P) RETURN n.a AS a, n.b AS b ORDER BY a");                            \
      ASSERT_EQ(results.size(), 2);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
      EXPECT_EQ(results[0][1].ValueString(), "x");                                                                 \
      EXPECT_EQ(results[1][0].ValueInt(), 2);                                                                      \
      EXPECT_EQ(results[1][1].type(), BoltValue::Type::Null);                                                      \
    }                                                                                                              \
    {                                                                                                              \
      /* A single lookup must not earn a pass, but must still answer the same. */                                  \
      auto results = this->Execute("MATCH (n:P) RETURN n.a AS a ORDER BY a");                                      \
      ASSERT_EQ(results.size(), 2);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
      EXPECT_EQ(results[1][0].ValueInt(), 2);                                                                      \
    }                                                                                                              \
    {                                                                                                              \
      /* A repeated property keeps the rows the two comparisons kept. */                                           \
      auto results = this->Execute("MATCH (n:P) WHERE n.a > 0 AND n.a < 2 RETURN n.id AS id");                     \
      ASSERT_EQ(results.size(), 1);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
    }                                                                                                              \
    {                                                                                                              \
      /* A property absent from a record compares as Null there, so that row is still left out. */                 \
      auto results = this->Execute("MATCH (n:P) WHERE n.b >= 'x' AND n.b <= 'x' RETURN n.id AS id");               \
      ASSERT_EQ(results.size(), 1);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
    }                                                                                                              \
    {                                                                                                              \
      /* Both lookups of one property answer it, and a property absent from a record still reads Null. */          \
      auto results = this->Execute("MATCH (n:P) RETURN n.a AS x, n.a AS y, n.b AS z ORDER BY x");                  \
      ASSERT_EQ(results.size(), 2);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
      EXPECT_EQ(results[0][1].ValueInt(), 1);                                                                      \
      EXPECT_EQ(results[0][2].ValueString(), "x");                                                                 \
      EXPECT_EQ(results[1][0].ValueInt(), 2);                                                                      \
      EXPECT_EQ(results[1][1].ValueInt(), 2);                                                                      \
      EXPECT_EQ(results[1][2].type(), BoltValue::Type::Null);                                                      \
    }                                                                                                              \
    {                                                                                                              \
      /* One lookup each is not worth a pass of its own, whether or not they share a symbol. */                    \
      auto results = this->Execute("MATCH (n:P), (m:P) RETURN n.id AS x, m.a AS y ORDER BY x, y");                 \
      ASSERT_EQ(results.size(), 4);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
      EXPECT_EQ(results[0][1].ValueInt(), 1);                                                                      \
      EXPECT_EQ(results[3][0].ValueInt(), 2);                                                                      \
      EXPECT_EQ(results[3][1].ValueInt(), 2);                                                                      \
    }                                                                                                              \
    {                                                                                                              \
      /* The prefetched symbol is Null for every row here. */                                                      \
      auto results =                                                                                               \
          this->Execute("MATCH (n:P) OPTIONAL MATCH (n)-[:R]->(o) RETURN o.a AS a, o.b AS b ORDER BY n.id");       \
      ASSERT_EQ(results.size(), 2);                                                                                \
      for (const auto &row : results) {                                                                            \
        EXPECT_EQ(row[0].type(), BoltValue::Type::Null);                                                           \
        EXPECT_EQ(row[1].type(), BoltValue::Type::Null);                                                           \
      }                                                                                                            \
    }                                                                                                              \
    {                                                                                                              \
      /* What a predicate read and the row returns is one value, however it reaches the row. */                    \
      auto results = this->Execute("MATCH (n) WHERE n.a > 0 AND n.a < 2 RETURN n.a AS a, n.a AS b");               \
      ASSERT_EQ(results.size(), 1);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
      EXPECT_EQ(results[0][1].ValueInt(), 1);                                                                      \
    }                                                                                                              \
    {                                                                                                              \
      /* A second predicate reads what the first one's pass left, and the row returns it after both. */            \
      auto results = this->Execute("MATCH (n) WHERE n.a > 0 AND n.a < 2 WITH n WHERE n.a > 0 RETURN n.a AS a");    \
      ASSERT_EQ(results.size(), 1);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
    }                                                                                                              \
    {                                                                                                              \
      /* One property comes from a slot a predicate filled and the other from a pass of this */                    \
      /* row's own, so each has to answer its own property. */                                                     \
      auto results = this->Execute("MATCH (n) WHERE n.a > 0 AND n.a < 2 RETURN n.a AS x, n.b + n.b AS y");         \
      ASSERT_EQ(results.size(), 1);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
      EXPECT_EQ(results[0][1].ValueString(), "xx");                                                                \
    }                                                                                                              \
    {                                                                                                              \
      /* A write stands between the predicate and the row, so the row answers what the write left. */              \
      auto results = this->Execute("MATCH (n) WHERE n.a > 1 AND n.a < 3 SET n.a = 99 RETURN n.a AS a, n.a AS b");  \
      ASSERT_EQ(results.size(), 1);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 99);                                                                     \
      EXPECT_EQ(results[0][1].ValueInt(), 99);                                                                     \
    }                                                                                                              \
    {                                                                                                              \
      /* An expansion stands between the predicate and the row. It binds its own edge and node, so */              \
      /* the row answers the property of the node the expansion started from. */                                   \
      this->Execute("MATCH (p:P {id: 1}), (q:P {id: 2}) CREATE (p)-[:R]->(q)");                                    \
      auto results = this->Execute("MATCH (n:P)-[:R]->(m:P) WHERE n.a > 0 AND n.a < 2 RETURN n.a AS x, n.a AS y"); \
      ASSERT_EQ(results.size(), 1);                                                                                \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                      \
      EXPECT_EQ(results[0][1].ValueInt(), 1);                                                                      \
    }                                                                                                              \
  } while (false)

TYPED_TEST(QueryExecution, PrefetchPropertiesResultsWithFlagOff) {
  PrefetchPropertiesFlagGuard guard{false};
  CHECK_PREFETCH_PROPERTIES_RESULTS();
}

TYPED_TEST(QueryExecution, PrefetchPropertiesResultsWithFlagOn) {
  PrefetchPropertiesFlagGuard guard{true};
  CHECK_PREFETCH_PROPERTIES_RESULTS();
}

/// What an aggregation answers, with the pass reading its properties and without. Each fixture gets
/// a fresh plan cache, so the two have to be separate tests rather than one toggling the flag.
#define CHECK_AGGREGATION_PREFETCH_RESULTS()                                                                 \
  do {                                                                                                       \
    using BoltValue = memgraph::communication::bolt::Value;                                                  \
    this->Execute("CREATE (:P {id: 1, a: 10, b: 'x'}), (:P {id: 2, a: 20, b: 'y'}), (:P {id: 3, b: 'x'})");  \
    {                                                                                                        \
      /* One property that four aggregations read. The record carrying no `a` counts for none of them. */    \
      auto results =                                                                                         \
          this->Execute("MATCH (n:P) RETURN sum(n.a) AS s, avg(n.a) AS av, min(n.a) AS lo, max(n.a) AS hi"); \
      ASSERT_EQ(results.size(), 1);                                                                          \
      EXPECT_EQ(results[0][0].ValueInt(), 30);                                                               \
      EXPECT_DOUBLE_EQ(results[0][1].ValueDouble(), 15.0);                                                   \
      EXPECT_EQ(results[0][2].ValueInt(), 10);                                                               \
      EXPECT_EQ(results[0][3].ValueInt(), 20);                                                               \
    }                                                                                                        \
    {                                                                                                        \
      /* A grouping key the aggregation reads again. Counting a property counts the records holding it. */   \
      auto results = this->Execute("MATCH (n:P) RETURN n.b AS k, count(n.b) AS c ORDER BY k");               \
      ASSERT_EQ(results.size(), 2);                                                                          \
      EXPECT_EQ(results[0][0].ValueString(), "x");                                                           \
      EXPECT_EQ(results[0][1].ValueInt(), 2);                                                                \
      EXPECT_EQ(results[1][0].ValueString(), "y");                                                           \
      EXPECT_EQ(results[1][1].ValueInt(), 1);                                                                \
    }                                                                                                        \
    {                                                                                                        \
      /* Two properties read once each: no read a pass would save, so it must answer as it always did. */    \
      auto results = this->Execute("MATCH (n:P) RETURN sum(n.a) AS s, count(n.b) AS c");                     \
      ASSERT_EQ(results.size(), 1);                                                                          \
      EXPECT_EQ(results[0][0].ValueInt(), 30);                                                               \
      EXPECT_EQ(results[0][1].ValueInt(), 3);                                                                \
    }                                                                                                        \
    {                                                                                                        \
      /* A row reading what this query has just written to the record it is on. The write leaves a delta,    \
         which is the one way the record is read other than straight out of the store. */                    \
      auto results = this->Execute("MATCH (n:P) SET n.w = n.id * 100 RETURN sum(n.w) AS s, max(n.w) AS m");  \
      ASSERT_EQ(results.size(), 1);                                                                          \
      EXPECT_EQ(results[0][0].ValueInt(), 600);                                                              \
      EXPECT_EQ(results[0][1].ValueInt(), 300);                                                              \
    }                                                                                                        \
    {                                                                                                        \
      /* Every record missing the property, so every aggregation sees Null and sums nothing. */              \
      auto results = this->Execute("MATCH (n:P) RETURN sum(n.absent) AS s, max(n.absent) AS m");             \
      ASSERT_EQ(results.size(), 1);                                                                          \
      EXPECT_EQ(results[0][0].ValueInt(), 0);                                                                \
      EXPECT_EQ(results[0][1].type(), BoltValue::Type::Null);                                                \
    }                                                                                                        \
    {                                                                                                        \
      /* A relationship's property, which is read from a record just as a node's is. */                      \
      this->Execute("MATCH (p:P {id: 1}), (q:P {id: 2}) CREATE (p)-[:R {w: 7}]->(q)");                       \
      auto results = this->Execute("MATCH ()-[r:R]->() RETURN sum(r.w) AS s, max(r.w) AS m");                \
      ASSERT_EQ(results.size(), 1);                                                                          \
      EXPECT_EQ(results[0][0].ValueInt(), 7);                                                                \
      EXPECT_EQ(results[0][1].ValueInt(), 7);                                                                \
    }                                                                                                        \
  } while (false)

TYPED_TEST(QueryExecution, AggregationPrefetchResultsWithFlagOff) {
  PrefetchPropertiesFlagGuard guard{false};
  CHECK_AGGREGATION_PREFETCH_RESULTS();
}

TYPED_TEST(QueryExecution, AggregationPrefetchResultsWithFlagOn) {
  PrefetchPropertiesFlagGuard guard{true};
  CHECK_AGGREGATION_PREFETCH_RESULTS();
}

// A lookup the pass cannot take, because it sits where it may never be evaluated, still answers
// what it answered before: changing how it reads must not change what it reads. Asked with the
// rewrite off as well, so the expectations are the ones the old reading gives rather than
// whatever the new one happens to produce.
#define CHECK_LEFTOVER_LOOKUP_RESULTS()                                                                        \
  do {                                                                                                         \
    using BoltValue = memgraph::communication::bolt::Value;                                                    \
    this->Execute("MATCH (n) DETACH DELETE n");                                                                \
    this->Execute("CREATE ({a: 1, b: 2, c: 1, d: 5})");                                                        \
                                                                                                               \
    /* The disjunction is answered by its first operand, so the second is never read. */                       \
    auto disjunction = this->Execute("MATCH (n) RETURN n.a AS a, n.b AS b, n.c = 1 OR n.missing = 2 AS flag"); \
    ASSERT_EQ(disjunction.size(), 1);                                                                          \
    EXPECT_EQ(disjunction[0][0].ValueInt(), 1);                                                                \
    EXPECT_EQ(disjunction[0][1].ValueInt(), 2);                                                                \
    EXPECT_TRUE(disjunction[0][2].ValueBool());                                                                \
                                                                                                               \
    /* A property the record does not carry reads as Null, so the disjunction is Null. */                      \
    auto absent = this->Execute("MATCH (n) RETURN n.a AS a, n.b AS b, n.nope = 1 OR n.alsonope = 2 AS flag");  \
    ASSERT_EQ(absent.size(), 1);                                                                               \
    EXPECT_EQ(absent[0][2].type(), BoltValue::Type::Null);                                                     \
                                                                                                               \
    /* A map literal is built from its entries, so the lookups inside it are left where they are. */           \
    auto mapped = this->Execute("MATCH (n) RETURN n.c AS c, n.d AS d, {a: n.a, b: n.b} AS m");                 \
    ASSERT_EQ(mapped.size(), 1);                                                                               \
    EXPECT_EQ(mapped[0][0].ValueInt(), 1);                                                                     \
    EXPECT_EQ(mapped[0][1].ValueInt(), 5);                                                                     \
    EXPECT_EQ(mapped[0][2].ValueMap().at("a").ValueInt(), 1);                                                  \
    EXPECT_EQ(mapped[0][2].ValueMap().at("b").ValueInt(), 2);                                                  \
                                                                                                               \
    auto listed = this->Execute("MATCH (n) RETURN n.a AS a, n.b AS b, [n.c, n.d] AS rest");                    \
    ASSERT_EQ(listed.size(), 1);                                                                               \
    EXPECT_EQ(listed[0][2].ValueList().at(0).ValueInt(), 1);                                                   \
    EXPECT_EQ(listed[0][2].ValueList().at(1).ValueInt(), 5);                                                   \
  } while (false)

/// Reading a record the same query has deleted refuses, whatever reads it: there is nothing to
/// read either way, so when the read happens must not decide whether the query answers. `reads` is
/// the clause that reads it.
#define CHECK_A_DELETED_RECORD_REFUSES(reads)                                                                \
  do {                                                                                                       \
    this->Execute("MATCH (n) DETACH DELETE n");                                                              \
    this->Execute("CREATE ({a: 1, b: 2})");                                                                  \
    EXPECT_THROW(this->Execute("MATCH (n) DETACH DELETE n " reads), memgraph::query::QueryRuntimeException); \
  } while (false)

TYPED_TEST(QueryExecution, ADeletedRecordRefusesWithFlagOff) {
  PrefetchPropertiesFlagGuard guard{false};
  CHECK_A_DELETED_RECORD_REFUSES("RETURN n.a AS a, n.b AS b");
  CHECK_A_DELETED_RECORD_REFUSES("RETURN sum(n.a) AS s, max(n.a) AS m");
  CHECK_A_DELETED_RECORD_REFUSES("WITH n WHERE n.a > 0 AND n.a < 10 RETURN n.b AS b");
  CHECK_A_DELETED_RECORD_REFUSES("WITH n WHERE n.a > 0 AND n.a < 10 RETURN n.a AS a");
}

TYPED_TEST(QueryExecution, ADeletedRecordRefusesWithFlagOn) {
  PrefetchPropertiesFlagGuard guard{true};
  CHECK_A_DELETED_RECORD_REFUSES("RETURN n.a AS a, n.b AS b");
  CHECK_A_DELETED_RECORD_REFUSES("RETURN sum(n.a) AS s, max(n.a) AS m");
  CHECK_A_DELETED_RECORD_REFUSES("WITH n WHERE n.a > 0 AND n.a < 10 RETURN n.b AS b");
  CHECK_A_DELETED_RECORD_REFUSES("WITH n WHERE n.a > 0 AND n.a < 10 RETURN n.a AS a");
}

/// A predicate tests a record as it stood when the row was produced, which is before a write above
/// the filter has run for any later row. Two paths into one record therefore both reach the write,
/// whether the predicate read the record itself or a slot filled for it.
#define CHECK_A_WRITE_ABOVE_A_FILTER_LEAVES_ITS_READS_ALONE()                                                   \
  do {                                                                                                          \
    this->Execute("MATCH (n) DETACH DELETE n");                                                                 \
    this->Execute("CREATE (a1 {id: 1}), (a2 {id: 2}), (b {p: 3}) CREATE (a1)-[:E]->(b) CREATE (a2)-[:E]->(b)"); \
    /* The filter on a starts the scan there, so the predicate on b stands above the expansion. */              \
    auto rows = this->Execute(                                                                                  \
        "MATCH (a)-[:E]->(b) WHERE a.id >= 1 AND b.p > 1 AND b.p < 10 SET b.p = 99 RETURN count(*) AS c");      \
    ASSERT_EQ(rows.size(), 1);                                                                                  \
    EXPECT_EQ(rows[0][0].ValueInt(), 2);                                                                        \
  } while (false)

TYPED_TEST(QueryExecution, AWriteAboveAFilterLeavesItsReadsAloneWithFlagOff) {
  PrefetchPropertiesFlagGuard guard{false};
  CHECK_A_WRITE_ABOVE_A_FILTER_LEAVES_ITS_READS_ALONE();
}

TYPED_TEST(QueryExecution, AWriteAboveAFilterLeavesItsReadsAloneWithFlagOn) {
  PrefetchPropertiesFlagGuard guard{true};
  CHECK_A_WRITE_ABOVE_A_FILTER_LEAVES_ITS_READS_ALONE();
}

/// A slot stands in for a lookup only where that lookup would have read the same value, and a
/// filter tests a row at an earlier view than a projection returns one at. Those answer differently
/// for a record the query has already written, so a projection takes the record rather than a slot
/// a predicate below it filled.
#define CHECK_A_PROJECTION_DOES_NOT_TAKE_A_FILTERS_SLOT_ACROSS_A_WRITE()                                        \
  do {                                                                                                          \
    this->Execute("MATCH (n) DETACH DELETE n");                                                                 \
    this->Execute("CREATE (a1 {id: 1}), (a2 {id: 2}), (b {p: 3}) CREATE (a1)-[:E]->(b) CREATE (a2)-[:E]->(b)"); \
    /* The second path into b reaches it after the first path's write, which the projection returns  */         \
    /* and the predicate below it never sees.                                                        */         \
    auto rows = this->Execute(                                                                                  \
        "MATCH (a)-[:E]->(b) WHERE a.id >= 1 AND b.p > 1 AND b.p < 10 "                                         \
        "WITH b, b.p AS v SET b.p = 99 RETURN v ORDER BY v");                                                   \
    ASSERT_EQ(rows.size(), 2);                                                                                  \
    EXPECT_EQ(rows[0][0].ValueInt(), 3);                                                                        \
    EXPECT_EQ(rows[1][0].ValueInt(), 99);                                                                       \
  } while (false)

/// Each stage of a read-modify-write chain reads what the stage before it wrote, so the value
/// doubles per stage. The numbers here are known rather than agreed between the two plans, which is
/// what comparing the plans against each other cannot say on its own.
#define CHECK_A_CHAIN_OF_READ_MODIFY_WRITES_DOUBLES()                                                            \
  do {                                                                                                           \
    this->Execute("MATCH (n) DETACH DELETE n");                                                                  \
    this->Execute("CREATE ({p: 1})");                                                                            \
    auto doubled = this->Execute("MATCH (n) SET n.p = n.p + n.p WITH n SET n.p = n.p + n.p RETURN n.p AS p");    \
    ASSERT_EQ(doubled.size(), 1);                                                                                \
    EXPECT_EQ(doubled[0][0].ValueInt(), 4);                                                                      \
    /* Starts from four, and the predicate ahead of the writes is what hangs a pass in the chain. */             \
    auto again = this->Execute(                                                                                  \
        "MATCH (n) WHERE n.p > 0 AND n.p < 100 SET n.p = n.p + n.p WITH n SET n.p = n.p + n.p RETURN n.p AS q"); \
    ASSERT_EQ(again.size(), 1);                                                                                  \
    EXPECT_EQ(again[0][0].ValueInt(), 16);                                                                       \
  } while (false)

TYPED_TEST(QueryExecution, AChainOfReadModifyWritesDoublesWithFlagOff) {
  PrefetchPropertiesFlagGuard guard{false};
  CHECK_A_CHAIN_OF_READ_MODIFY_WRITES_DOUBLES();
}

TYPED_TEST(QueryExecution, AChainOfReadModifyWritesDoublesWithFlagOn) {
  PrefetchPropertiesFlagGuard guard{true};
  CHECK_A_CHAIN_OF_READ_MODIFY_WRITES_DOUBLES();
}

TYPED_TEST(QueryExecution, AProjectionDoesNotTakeAFiltersSlotAcrossAWriteWithFlagOff) {
  PrefetchPropertiesFlagGuard guard{false};
  CHECK_A_PROJECTION_DOES_NOT_TAKE_A_FILTERS_SLOT_ACROSS_A_WRITE();
}

TYPED_TEST(QueryExecution, AProjectionDoesNotTakeAFiltersSlotAcrossAWriteWithFlagOn) {
  PrefetchPropertiesFlagGuard guard{true};
  CHECK_A_PROJECTION_DOES_NOT_TAKE_A_FILTERS_SLOT_ACROSS_A_WRITE();
}

TYPED_TEST(QueryExecution, LeftoverLookupResultsWithFlagOff) {
  PrefetchPropertiesFlagGuard guard{false};
  CHECK_LEFTOVER_LOOKUP_RESULTS();
}

TYPED_TEST(QueryExecution, LeftoverLookupResultsWithFlagOn) {
  PrefetchPropertiesFlagGuard guard{true};
  CHECK_LEFTOVER_LOOKUP_RESULTS();
}

TYPED_TEST(QueryExecution, PrefetchPropertiesReadsARelationshipsProperties) {
  // A relationship keeps its properties in the same store a node does, so a projection of two of
  // them is the same shape the rewrite is for. The second is missing on one relationship, so the
  // slots have to answer Null there rather than drop the column.
  PrefetchPropertiesFlagGuard guard{true};
  using BoltValue = memgraph::communication::bolt::Value;

  this->Execute("CREATE (:P)-[:R {a: 1, b: 'x'}]->(:P), (:P)-[:R {a: 2}]->(:P)");

  auto results = this->Execute("MATCH ()-[r:R]->() RETURN r.a AS a, r.b AS b ORDER BY a");
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0][0].ValueInt(), 1);
  EXPECT_EQ(results[0][1].ValueString(), "x");
  EXPECT_EQ(results[1][0].ValueInt(), 2);
  EXPECT_EQ(results[1][1].type(), BoltValue::Type::Null);
}

TYPED_TEST(QueryExecution, PrefetchPropertiesReachesTheRealPlanForARelationship) {
  // Guards the test above from passing because the rewrite never fired. The two runs must not share
  // a plan cache entry, so they differ in the returned column names.
  auto explained = [this](const std::string &query) {
    auto results = this->Execute(query);
    std::string joined;
    for (const auto &row : results) joined += row[0].ValueString();
    return joined;
  };

  {
    PrefetchPropertiesFlagGuard guard{false};
    EXPECT_EQ(explained("EXPLAIN MATCH ()-[r:R]->() RETURN r.a AS a, r.b AS b").find("PrefetchProperties"),
              std::string::npos);
  }
  {
    PrefetchPropertiesFlagGuard guard{true};
    EXPECT_NE(explained("EXPLAIN MATCH ()-[r:R]->() RETURN r.a AS c, r.b AS d").find("PrefetchProperties"),
              std::string::npos);
  }
}

TYPED_TEST(QueryExecution, PrefetchPropertiesUnderASunkOrderByWithALimit) {
  // An ORDER BY with a LIMIT sinks below the projection so that only the rows it keeps are
  // projected, and it has to carry forward everything above it still reads. The pass reads the
  // record, so what it needs carried is the symbol itself rather than the properties the
  // projection used to read. An omission here does not fail the query: the rows come back Null.
  PrefetchPropertiesFlagGuard guard{true};

  this->Execute("CREATE (:P {id: 1, a: 10, b: 'x'}), (:P {id: 2, a: 20, b: 'y'}), (:P {id: 3, a: 30, b: 'z'})");

  auto results = this->Execute("MATCH (n:P) RETURN n.a AS a, n.b AS b ORDER BY n.id LIMIT 2");
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0][0].ValueInt(), 10);
  EXPECT_EQ(results[0][1].ValueString(), "x");
  EXPECT_EQ(results[1][0].ValueInt(), 20);
  EXPECT_EQ(results[1][1].ValueString(), "y");
}

TYPED_TEST(QueryExecution, PrefetchPropertiesGroupsTwoSymbols) {
  // Two symbols are two records and so two reads whichever way the plan is built. Gathering them
  // into one operator keeps that to one pull per row, and a symbol with only one lookup of its own
  // is left alone, since one pass for one property is what a lookup already does.
  PrefetchPropertiesFlagGuard guard{true};
  using BoltValue = memgraph::communication::bolt::Value;

  this->Execute("CREATE (:P {id: 1, a: 1, b: 'x'})");

  auto results = this->Execute("MATCH (n:P), (m:P) RETURN n.a AS na, n.b AS nb, m.a AS ma, m.b AS mb");
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0][0].ValueInt(), 1);
  EXPECT_EQ(results[0][1].ValueString(), "x");
  EXPECT_EQ(results[0][2].ValueInt(), 1);
  EXPECT_EQ(results[0][3].ValueString(), "x");

  // The second symbol carries one lookup, so only the first is worth a pass of its own.
  auto mixed = this->Execute("MATCH (n:P), (m:P) RETURN n.a AS p, n.b AS q, m.id AS r");
  ASSERT_EQ(mixed.size(), 1);
  EXPECT_EQ(mixed[0][0].ValueInt(), 1);
  EXPECT_EQ(mixed[0][1].ValueString(), "x");
  EXPECT_EQ(mixed[0][2].ValueInt(), 1);
}

// A lookup reachable only through an AND or an OR earns no pass of its own. Where another property
// of the same record is read on every row it rides that read, and where none is, the record is one
// no pass may touch.
TYPED_TEST(QueryExecution, PrefetchPropertiesReadsBehindAShortCircuitRideAReadEveryRowMakes) {
  this->Execute("CREATE (:Q {a: 1, b: 2}), (:G {t: true})");

  auto explained = [this](const std::string &query) {
    auto results = this->Execute(query);
    std::string joined;
    for (const auto &row : results) joined += row[0].ValueString();
    return joined;
  };

  PrefetchPropertiesFlagGuard guard{true};
  // The first operand is read from another node, so it is not folded away before planning. Here
  // n.b is read on every row, so the record is read regardless and n.a joins that one read.
  EXPECT_NE(explained("EXPLAIN MATCH (n:Q), (g:G) RETURN g.t OR n.a AS x, n.b AS y").find("PrefetchProperties"),
            std::string::npos);
  EXPECT_NE(explained("EXPLAIN MATCH (n:Q), (g:G) RETURN g.t AND n.a AS p, n.b AS q").find("PrefetchProperties"),
            std::string::npos);

  // Nothing of n is read on every row here, so no pass may touch the record: on a deleted one that
  // would turn an answer into a refusal.
  EXPECT_EQ(explained("EXPLAIN MATCH (n:Q), (g:G) RETURN g.t OR n.a AS x").find("PrefetchProperties"),
            std::string::npos);
  EXPECT_EQ(explained("EXPLAIN MATCH (n:Q), (g:G) RETURN g.t AND n.a AS p").find("PrefetchProperties"),
            std::string::npos);

  // A projection reading the same properties outside a short circuit still gets its pass.
  EXPECT_NE(explained("EXPLAIN MATCH (n:Q) RETURN n.a AS r, n.b AS s").find("PrefetchProperties"), std::string::npos);
}

// A lookup inside the body of a reduce or an extract keeps its own lookup.
TYPED_TEST(QueryExecution, PrefetchPropertiesLeavesLookupsInsideAListBody) {
  this->Execute("CREATE (:Q {a: 1, b: 2})");

  auto explained = [this](const std::string &query) {
    std::string joined;
    for (const auto &row : this->Execute(query)) joined += row[0].ValueString();
    return joined;
  };

  PrefetchPropertiesFlagGuard guard{true};
  EXPECT_EQ(explained("EXPLAIN MATCH (n:Q) RETURN reduce(s = 0, x IN [] | s + n.a) AS u, "
                      "reduce(s = 0, y IN [] | s + n.b) AS v")
                .find("PrefetchProperties"),
            std::string::npos);
  EXPECT_EQ(explained("EXPLAIN MATCH (n:Q) RETURN extract(x IN [] | n.a) AS p, extract(y IN [] | n.b) AS q")
                .find("PrefetchProperties"),
            std::string::npos);
}

// A relationship matched over a variable length binds the list of relationships it matched, inside
// the body of an EXISTS, a COUNT or a COLLECT as anywhere else. Reading a property of a list is an
// error, and reading two of them in one pass is the same error: answering Null instead would turn a
// query the server refuses into one that quietly returns a row per element with nothing in it.
// Asking one question two ways: in an ordinary MATCH, and with that MATCH inside a subquery body.
// A symbol carries a declared type, and the pass that reads several properties of one record in a
// single pass is chosen from it, so a type that disagreed with the value it binds would let the
// position of a MATCH decide whether a query is refused. The two spellings answer alike, or one of
// them is wrong.
TYPED_TEST(QueryExecution, ASubqueryBodyAnswersAsAnOrdinaryMatchDoes) {
  PrefetchPropertiesFlagGuard guard{true};

  this->Execute(
      "CREATE (a:P {name: 'a', w: 1})-[:R {amount: 1, fee: 1, w: 1}]->(b:P {name: 'b', w: 2}), "
      "(a)-[:R {amount: 2, fee: 2, w: 2}]->(c:P {name: 'c', w: 3}), "
      "(b)-[:R {amount: 3, fee: 3, w: 3}]->(c);");

  // A refusal and its wording, or an answer. The two spellings return different row shapes, so the
  // rows themselves cannot be compared; how the query is answered can.
  auto verdict = [this](std::string const &query) {
    try {
      this->Execute(query);
      return std::string{"ANSWERED"};
    } catch (const std::exception &e) {
      return std::string{e.what()};
    }
  };

  auto agree = [&verdict](std::string const &outside, std::string const &inside) {
    EXPECT_EQ(verdict(outside), verdict(inside)) << "outside: " << outside << "\ninside:  " << inside;
  };

  // Each variable-length spelling binds the list of relationships it matched, so each refuses a
  // property read. A fixed-length one binds a single relationship, so each answers.
  agree("MATCH (p:P {name:'a'})-[r:R*1..2]->(q) RETURN r.amount + r.fee AS v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p)-[r:R*1..2]->(q) RETURN r.amount + r.fee AS v } AS z");
  agree("MATCH (p:P {name:'a'})-[r:R *BFS 1..2]->(q) RETURN r.amount + r.fee AS v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p)-[r:R *BFS 1..2]->(q) RETURN r.amount + r.fee AS v } AS z");
  agree("MATCH (p:P {name:'a'})-[r:R *WSHORTEST 2 (e, n | e.w) tw]->(q) RETURN r.amount + r.fee AS v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p)-[r:R *WSHORTEST 2 (e, n | e.w) tw]->(q) "
        "RETURN r.amount + r.fee AS v } AS z");
  agree("MATCH (p:P {name:'a'})-[r:R]->(q) RETURN r.amount + r.fee AS v ORDER BY v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p)-[r:R]->(q) RETURN r.amount + r.fee AS v } AS z");
  agree("MATCH (p:P {name:'a'})-[:R]->(q) RETURN q.name + q.w AS v ORDER BY v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p)-[:R]->(q) RETURN q.name + q.w AS v } AS z");

  // A body nested one level deeper, and a body that is a CALL rather than a fold.
  agree("MATCH (p:P {name:'a'})-[r:R*1..2]->(q) RETURN r.amount + r.fee AS v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p) RETURN COLLECT { MATCH (p)-[r:R*1..2]->(q) "
        "RETURN r.amount + r.fee AS v } AS inner } AS z");
  agree("MATCH (p:P {name:'a'})-[r:R*1..2]->(q) RETURN r.amount + r.fee AS v",
        "MATCH (p:P {name:'a'}) CALL { WITH p MATCH (p)-[r:R*1..2]->(q) RETURN r.amount + r.fee AS v } RETURN v");

  // A named path binds a path rather than a record, and a relationship matched over a variable
  // length still counts its elements, inside a body as outside one.
  agree("MATCH pp = (p:P {name:'a'})-[:R]->(q) RETURN pp.amount AS v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH pp = (p)-[:R]->(q) RETURN pp.amount AS v } AS z");
  agree("MATCH (p:P {name:'a'})-[r:R*1..2]->(q) RETURN size(r) AS v ORDER BY v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p)-[r:R*1..2]->(q) RETURN size(r) AS v } AS z");

  // Two branches out of one node, where each pair of relationships has to be distinct. An unnamed
  // relationship is bound the same way a named one is, so it is counted the same.
  agree("MATCH (p:P {name:'a'}), (p)-[r:R]->(x), (p)-[s:R]->(y) RETURN count(*) AS v",
        "MATCH (p:P {name:'a'}) RETURN COUNT { MATCH (p)-[r:R]->(x), (p)-[s:R]->(y) RETURN x } AS v");
  agree("MATCH (p:P {name:'a'}), (p)-[:R]->(x), (p)-[:R]->(y) RETURN count(*) AS v",
        "MATCH (p:P {name:'a'}) RETURN COUNT { MATCH (p)-[:R]->(x), (p)-[:R]->(y) RETURN x } AS v");

  // Bodies that run per element, which read a property one at a time wherever they sit.
  agree("MATCH (p:P {name:'a'})-[r:R*1..2]->(q) UNWIND r AS e RETURN e.amount + e.fee AS v ORDER BY v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p)-[r:R*1..2]->(q) UNWIND r AS e "
        "RETURN e.amount + e.fee AS v } AS z");
  agree("MATCH (p:P {name:'a'})-[r:R*1..2]->(q) RETURN reduce(a = 0, e IN r | a + e.amount + e.fee) AS v ORDER BY v",
        "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p)-[r:R*1..2]->(q) "
        "RETURN reduce(a = 0, e IN r | a + e.amount + e.fee) AS v } AS z");
  agree(
      "MATCH (p:P {name:'a'}) RETURN [(p)-[r:R*1..2]->(q) | r.amount + r.fee] AS v",
      "MATCH (p:P {name:'a'}) RETURN COLLECT { MATCH (p) RETURN [(p)-[r:R*1..2]->(q) | r.amount + r.fee] AS v } AS z");
}

TYPED_TEST(QueryExecution, ReadingTwoPropertiesOfAListOfRelationshipsIsRefused) {
  PrefetchPropertiesFlagGuard guard{true};

  this->Execute("CREATE (:Person)-[:PAID {amount: 10, fee: 1}]->(:Person)");

  EXPECT_THROW(this->Execute("MATCH (p:Person) RETURN "
                             "COLLECT { MATCH (p)-[r:PAID*1..2]->(q) RETURN r.amount + r.fee } AS c"),
               memgraph::query::QueryRuntimeException);
}

TYPED_TEST(QueryExecution, PrefetchPropertiesReadsAGroupWhoseOrderIsNotTheRecordsOrder) {
  // One pass over a record finds each property from where the last one stopped, so it cannot go
  // back for one it has already passed. A projection names its properties in whatever order the
  // query was written in, which has nothing to do with the order the record holds them. Asking both
  // ways means one of these two is against the record's order whichever way the names interned.
  PrefetchPropertiesFlagGuard guard{true};

  this->Execute("CREATE (:Z {zebra: 1, apple: 'x'})");

  auto forward = this->Execute("MATCH (n:Z) RETURN n.zebra AS f1, n.apple AS f2");
  ASSERT_EQ(forward.size(), 1);
  EXPECT_EQ(forward[0][0].ValueInt(), 1);
  EXPECT_EQ(forward[0][1].ValueString(), "x");

  // Different aliases, so this does not reuse the plan cached for the query above.
  auto reverse = this->Execute("MATCH (n:Z) RETURN n.apple AS r1, n.zebra AS r2");
  ASSERT_EQ(reverse.size(), 1);
  EXPECT_EQ(reverse[0][0].ValueString(), "x");
  EXPECT_EQ(reverse[0][1].ValueInt(), 1);
}

TYPED_TEST(QueryExecution, PrefetchPropertiesGroupsTwoSymbolsInTheRealPlan) {
  // Guards the test above from passing because the rewrite never fired.
  auto explained = [this](const std::string &query) {
    auto results = this->Execute(query);
    std::string joined;
    for (const auto &row : results) joined += row[0].ValueString();
    return joined;
  };

  {
    PrefetchPropertiesFlagGuard guard{false};
    EXPECT_EQ(explained("EXPLAIN MATCH (n:P), (m:P) RETURN n.a AS a, n.b AS b, m.a AS c, m.b AS d")
                  .find("PrefetchProperties"),
              std::string::npos);
  }
  {
    PrefetchPropertiesFlagGuard guard{true};
    EXPECT_NE(explained("EXPLAIN MATCH (n:P), (m:P) RETURN n.a AS e, n.b AS f, m.a AS g, m.b AS h")
                  .find("PrefetchProperties"),
              std::string::npos);
  }
}

TYPED_TEST(QueryExecution, PrefetchPropertiesReachesTheRealPlan) {
  // Guards the results tests above from passing because the rewrite never fired.
  // The two runs must not share a plan cache entry, so they differ in the returned column names.
  auto explained = [this](const std::string &query) {
    auto results = this->Execute(query);
    std::string joined;
    for (const auto &row : results) joined += row[0].ValueString();
    return joined;
  };

  {
    PrefetchPropertiesFlagGuard guard{false};
    EXPECT_EQ(explained("EXPLAIN MATCH (n:P) RETURN n.a AS a, n.b AS b").find("PrefetchProperties"), std::string::npos);
  }
  {
    PrefetchPropertiesFlagGuard guard{true};
    EXPECT_NE(explained("EXPLAIN MATCH (n:P) RETURN n.a AS c, n.b AS d").find("PrefetchProperties"), std::string::npos);
  }
}
