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

// Index scans on a branch: main@fork's index merged with the branch's own changes, the mirrored index definitions, and
// the full-scan fallback for an index main did not have at the fork.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "flags/run_time_configurable.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/settings.hpp"
#include "versioning_fixture.hpp"

using memgraph::tests::versioning_fixture::ExplainText;
using memgraph::tests::versioning_fixture::IntRows;
using memgraph::tests::versioning_fixture::PlanHasOperator;
using memgraph::tests::versioning_fixture::Rows;

namespace {

using Strings = std::vector<std::string>;

class VersioningIndexTest : public VersioningGatedTest {
 protected:
  void UseMain(InterpreterFaker &session) { session.interpreter.current_db_.LeaveBranch(); }

  void UseMain() { UseMain(faker); }

  void Use(InterpreterFaker &session, const std::string &name) {
    UseMain(session);
    Attach(session, name);
  }
};

Strings StringRows(InterpreterFaker &session, const std::string &query) {
  auto stream = session.Interpret(query);
  Strings out;
  for (const auto &row : stream.GetResults()) out.push_back(row[0].ValueString());
  return out;
}

// The value assertions only pin the branch merge if the planner really scans the index; ScanAll + Filter would satisfy
// them without touching it.
void ExpectPlanUses(InterpreterFaker &session, const std::string &query, std::string_view op) {
  const auto plan = ExplainText(session, query);
  EXPECT_TRUE(PlanHasOperator(plan, op)) << "plan of `" << query << "` does not use " << op << ":\n" << plan;
}

// The row order asserted for `query` is the index's own only if the planner dropped the explicit sort.
void ExpectNoOrderBy(InterpreterFaker &session, const std::string &query) {
  const auto plan = ExplainText(session, query);
  EXPECT_FALSE(PlanHasOperator(plan, "OrderBy")) << "plan of `" << query << "` still sorts:\n" << plan;
}

void RunStatements(InterpreterFaker &session, std::string_view script) {
  for (auto stmt : std::views::split(script, ';')) session.Interpret(std::string(std::string_view(stmt)));
}

}  // namespace

// The planner's per-label and label+property counts on a branch are main's at the fork plus the branch's own, so the
// branch plans as main does.
class VersioningIndexPlannerCountsTest : public VersioningIndexTest {
 protected:
  static constexpr auto kNeighbours = "MATCH (s:User {id: 7})-[*1..2]->(n:User) RETURN DISTINCT n.id";
  static constexpr auto kPlanQueries = std::array{
      "MATCH (n:User) RETURN count(n)",
      "MATCH (n:User {id: 7}) RETURN n",
      "MATCH (n:User) WHERE n.id > 390 RETURN n",
      kNeighbours,
  };

  void SetUp() override {
    VersioningIndexTest::SetUp();
    faker.Interpret("CREATE INDEX ON :User");
    faker.Interpret("CREATE INDEX ON :User(id)");
    faker.Interpret("UNWIND range(1, 400) AS i CREATE (:User {id: i})");
    faker.Interpret(
        "UNWIND range(1, 399) AS i MATCH (a:User {id: i}), (b:User {id: i + 1}) CREATE (a)-[:FOLLOWS]->(b)");
    faker.Interpret("UNWIND range(1, 50) AS i CREATE (:Other {id: i})");
  }

  std::vector<std::string> PlansOfTheQueries() {
    std::vector<std::string> plans;
    for (auto const *query : kPlanQueries) plans.push_back(ExplainText(faker, query));
    return plans;
  }

  void ExpectPlansAs(std::vector<std::string> const &expected) {
    size_t i = 0;
    for (auto const *query : kPlanQueries) EXPECT_EQ(ExplainText(faker, query), expected[i++]) << query;
  }
};

TEST_F(VersioningIndexPlannerCountsTest, BranchPlansAsMainBeforeAndAfterItsWrites) {
  auto const main_plans = PlansOfTheQueries();
  ASSERT_TRUE(PlanHasOperator(main_plans.back(), "ScanAllByLabelProperties")) << main_plans.back();
  Fork();
  ExpectPlansAs(main_plans);
  faker.Interpret("MATCH (n:User) WHERE n.id % 100 = 0 SET n.touched = true");
  faker.Interpret("UNWIND range(1, 4) AS i CREATE (:User {id: 1000 + i})");
  faker.Interpret("MATCH (n:User {id: 5}) DETACH DELETE n");
  ExpectPlansAs(main_plans);
}

// With main's plan this is a bounded expansion from one vertex; without the fork term in the per-label count the
// planner scans every :L and expands from each.
TEST_F(VersioningIndexTest, NeighboursQueryOnABranchPlansAndWorksAsMain) {
  constexpr auto kQuery = "MATCH (s:L {id: 1})-[*1..2]->(n:L) RETURN count(*)";
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("UNWIND range(1, 1000) AS i CREATE (:L {id: i})");
  faker.Interpret("MATCH (a:L {id: 1}), (b:L {id: 2}) CREATE (a)-[:E]->(b)");
  auto const hits = [&] {
    int64_t total = 0;
    for (const auto &row : faker.Interpret(std::string("PROFILE ") + kQuery).GetResults()) total += row[1].ValueInt();
    return total;
  };
  auto const main_plan = ExplainText(faker, kQuery);
  auto const main_hits = hits();

  Fork();
  faker.Interpret("CREATE (:L {id: 5000})");
  EXPECT_EQ(ExplainText(faker, kQuery), main_plan);
  EXPECT_LE(hits(), 2 * main_hits + 16);
}

// A branch that deleted fork edges expands as main does after the same deletes (shortest paths, hop counts under a
// limit), and no vertex needs the merged expansion for it.
TEST_F(VersioningIndexTest, ExpansionOnABranchThatDeletedEdgesIsMainsAfterTheSameDeletes) {
  faker.Interpret("CREATE INDEX ON :N(id)");
  faker.Interpret("UNWIND range(0, 299) AS i CREATE (:N {id: i})");
  faker.Interpret("UNWIND range(0, 298) AS i MATCH (a:N {id: i}), (b:N {id: i + 1}) CREATE (a)-[:E]->(b)");
  faker.Interpret("UNWIND range(0, 292) AS i MATCH (a:N {id: i}), (b:N {id: i + 7}) CREATE (a)-[:F]->(b)");
  faker.Interpret("UNWIND range(1, 30) AS i MATCH (h:N {id: 0}), (t:N {id: 100 + i}) CREATE (h)-[:H]->(t)");
  Fork();
  constexpr auto kDelete = "MATCH ()-[e]->() WHERE id(e) % 7 = 0 DELETE e";
  faker.Interpret(kDelete);

  auto const run = [&] {
    std::vector<Rows> results;
    for (auto const &[from, to] :
         std::vector<std::pair<int, int>>{{0, 299}, {3, 250}, {120, 290}, {17, 18}, {200, 5}}) {
      for (auto const *pattern : {"[*bfs..40]", "[:E *bfs..300]", "[:F|E *bfs..40]"}) {
        results.push_back(IntRows(faker,
                                  fmt::format("MATCH (n:N {{id: {}}}), (m:N {{id: {}}}) WITH n, m "
                                              "MATCH p=(n)-{}->(m) RETURN length(p)",
                                              from,
                                              to,
                                              pattern)));
      }
    }
    for (auto const limit : {5, 10, 31, 1000}) {
      results.push_back(IntRows(faker,
                                fmt::format("USING HOPS LIMIT {} MATCH (:N {{id: 0}})-[r]->() WITH count(r) AS c "
                                            "RETURN c, getHopsCounter()",
                                            limit)));
      results.push_back(IntRows(faker,
                                fmt::format("USING HOPS LIMIT {} MATCH (:N {{id: 110}})<-[r]-() WITH count(r) AS c "
                                            "RETURN c, getHopsCounter()",
                                            limit)));
    }
    return results;
  };
  auto const branch_results = run();
  ASSERT_FALSE(branch_results.front().empty()) << "main's shortest path survives the deletes";

  {
    Use(faker, "b");
    auto *ctx = faker.interpreter.current_db_.branch_context();
    ASSERT_NE(ctx, nullptr);
    auto diff = ctx->diff_engine().Access(memgraph::storage::READ);
    ASSERT_TRUE(ctx->BeginTransaction(diff.get()).has_value());
    size_t vertices = 0;
    size_t merged = 0;
    for (auto vertex : ctx->Vertices(memgraph::storage::View::NEW)) {
      for (auto const direction : {memgraph::storage::EdgeDirection::OUT, memgraph::storage::EdgeDirection::IN}) {
        ++vertices;
        if (!ctx->DirectExpansion(vertex, direction, memgraph::storage::View::NEW)) ++merged;
      }
    }
    ctx->EndTransaction();
    EXPECT_GT(vertices, 0U);
    EXPECT_EQ(merged, 0U);
  }

  UseMain();
  faker.Interpret(kDelete);
  EXPECT_EQ(run(), branch_results);
}

// The edge-type counts the planner reads on a branch are main's at the fork plus the branch's own.
TEST_F(VersioningIndexPlannerCountsTest, EdgeTypeScansOnABranchPlanAsMain) {
  faker.Interpret("CREATE EDGE INDEX ON :FOLLOWS");
  faker.Interpret("CREATE INDEX ON :Other");
  auto const queries = std::array{
      "MATCH ()-[r:FOLLOWS]->() RETURN count(r)",
      "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN count(r)",
      "MATCH (a:Other)-[r:FOLLOWS]->(b:User) RETURN count(r)",
  };
  std::vector<std::string> main_plans;
  for (auto const *query : queries) main_plans.push_back(ExplainText(faker, query));
  ASSERT_TRUE(PlanHasOperator(main_plans.front(), "ScanAllByEdgeType")) << main_plans.front();

  Fork();
  size_t i = 0;
  for (auto const *query : queries) EXPECT_EQ(ExplainText(faker, query), main_plans[i++]) << query;
  faker.Interpret("UNWIND range(1, 4) AS i CREATE (:User {id: 2000 + i})-[:FOLLOWS]->(:Other)");
  i = 0;
  for (auto const *query : queries) EXPECT_EQ(ExplainText(faker, query), main_plans[i++]) << query;
}

TEST_F(VersioningIndexTest, BranchLabelIndexScanMatchesLabelMembership) {
  faker.Interpret("CREATE INDEX ON :User");
  faker.Interpret(
      "CREATE (:User {i:1}), (:User {i:2}), (:User {i:3}), (:User {i:4}), (:User {i:5, t:1}), (:Other {i:6})");
  Fork();

  faker.Interpret("MATCH (n:User) WHERE n.i IN [1, 2] SET n:Probe");
  faker.Interpret("MATCH (n:User {i:3}) REMOVE n:User");
  faker.Interpret("MATCH (n:User {i:4}) DETACH DELETE n");
  faker.Interpret("MATCH (n:Other {i:6}) SET n:User");
  faker.Interpret("CREATE (:User {i:7})");
  faker.Interpret("MATCH (n:User {i:5}) SET n.t = 2");  // touched but keeps :User: yielded once, at its branch value

  ExpectPlanUses(faker, "MATCH (n:User) RETURN n.i", "ScanAllByLabel");
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN count(n)"), (Rows{{5}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {5}, {6}, {7}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) WHERE n.t IS NOT NULL RETURN n.t"), (Rows{{2}}));

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:User {i:5}) REMOVE n:User");
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN count(n)"), (Rows{{4}}));
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {5}, {6}, {7}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {3}, {4}, {5}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) WHERE n.t IS NOT NULL RETURN n.t"), (Rows{{1}}));
}

// Unindexed label filters over many label writes: HasLabel is exact in any probe order, including the session's own
// uncommitted writes, removals and deletes.
TEST_F(VersioningIndexTest, BranchHasLabelExactMembershipAcrossManyChanges) {
  faker.Interpret("UNWIND range(0, 299) AS i CREATE (:Probe {i: i})");
  Fork();

  faker.Interpret("MATCH (n:Probe) WHERE n.i % 2 = 0 SET n:L");
  faker.Interpret("MATCH (n:Probe) WHERE n.i % 4 = 0 REMOVE n:L");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE n:L RETURN count(n)"), (Rows{{75}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n:L AND n.i > 290 RETURN n.i ORDER BY n.i"), (Rows{{294}, {298}}));

  Rows ascending;
  std::string ids;
  Rows shuffled;
  for (int64_t i = 0; i < 300; ++i) {
    ascending.push_back({i, i % 4 == 2 ? 1 : 0});
    const int64_t id = (i * 37) % 300;
    ids += (i == 0 ? "" : ",") + std::to_string(id);
    shuffled.push_back({id, id % 4 == 2 ? 1 : 0});
  }
  const std::string has_l = "CASE WHEN n:L THEN 1 ELSE 0 END";
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) RETURN n.i, " + has_l + " ORDER BY n.i"), ascending);
  auto descending = IntRows(faker, "MATCH (n:Probe) RETURN n.i, " + has_l + " ORDER BY n.i DESC");
  std::ranges::reverse(descending);
  EXPECT_EQ(descending, ascending);
  EXPECT_EQ(IntRows(faker, "UNWIND [" + ids + "] AS id MATCH (n:Probe {i: id}) RETURN id, " + has_l), shuffled);

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:Probe {i: 1}) SET n:L");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE n:L RETURN count(n)"), (Rows{{76}}));
  faker.Interpret("MATCH (n:Probe {i: 2}) REMOVE n:L");
  faker.Interpret("MATCH (n:Probe {i: 3}) DETACH DELETE n");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE n:L RETURN count(n)"), (Rows{{75}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) RETURN count(n)"), (Rows{{299}}));
  faker.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE n:L RETURN count(n)"), (Rows{{75}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE n:L RETURN count(n)"), (Rows{{0}}));
}

// Reading and writing one label per row: the session's own label writes, property and label writes in one statement,
// and removals inside an explicit transaction.
TEST_F(VersioningIndexTest, BranchReadAndWriteSameLabelPerRow) {
  faker.Interpret("UNWIND range(1, 300) AS i CREATE (:Probe {i: i})");
  faker.Interpret("MATCH (n:Probe) WHERE n.i % 2 = 0 SET n:L");
  Fork();

  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE n:L SET n.x = 1, n:M RETURN count(n)"), (Rows{{150}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE n:M AND NOT n:L RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE n.x = 1 RETURN count(n)"), (Rows{{150}}));

  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE NOT n:Seen SET n:Seen RETURN count(n)"), (Rows{{300}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE NOT n:Seen SET n:Seen RETURN count(n)"), (Rows{{0}}));

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:Probe) WHERE n.i <= 100 REMOVE n:Seen");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Seen) RETURN count(n)"), (Rows{{200}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE NOT n:Seen RETURN count(n)"), (Rows{{100}}));
  faker.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker, "MATCH (n:Probe) WHERE NOT n:Seen AND n.i > 100 RETURN count(n)"), (Rows{{0}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:Seen) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:M) RETURN count(n)"), (Rows{{0}}));
}

// A NEW-view scan filtered by label observes a delete made earlier in the same query, as on main.
TEST_F(VersioningIndexTest, BranchLabelScanSeesSameQueryDeleteLikeMain) {
  faker.Interpret("CREATE (:A {i:1}), (:A {i:2}), (:A {i:3}), (:A {i:4})");
  Fork();

  const std::string label_scan = "MATCH (z:A {i: 4}) MERGE (n:A) FOREACH (x IN [1] | DELETE z) RETURN count(n)";
  const auto on_branch = IntRows(faker, label_scan);
  UseMain();
  EXPECT_EQ(on_branch, IntRows(faker, label_scan));
}

// Nothing touched and no deletes, yet the label-property scan still merges in branch-native vertices.
TEST_F(VersioningIndexTest, BranchIndexHitMergesUntouchedForkAndNativeVertices) {
  faker.Interpret("CREATE INDEX ON :P(v)");
  faker.Interpret("CREATE (:P {v: 1}), (:P {v: 2}), (:P {v: 3})");
  Fork();
  faker.Interpret("CREATE (:P {v: 5})");

  const std::string query = "MATCH (n:P) WHERE n.v > 0 RETURN n.v ORDER BY n.v";
  ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  EXPECT_EQ(IntRows(faker, query), (Rows{{1}, {2}, {3}, {5}}));
  UseMain();
  EXPECT_EQ(IntRows(faker, query), (Rows{{1}, {2}, {3}}));
}

TEST_F(VersioningIndexTest, BranchTwoLabelPropertyIndexesInOneOrExpression) {
  faker.Interpret("CREATE INDEX ON :Label1(id)");
  faker.Interpret("CREATE INDEX ON :Label2(id)");
  faker.Interpret("CREATE (:Label1 {id: 1}), (:Label2 {id: 2}), (:Label1:Label2 {id: 1})");
  Fork();

  auto const query = "MATCH (n:Label1|Label2) WHERE n.id < 2 RETURN count(n)";
  ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  EXPECT_EQ(IntRows(faker, query), (Rows{{2}}));
}

namespace {
struct MirroredIndexCase {
  const char *name;
  const char *ddl;
  const char *seed;
  const char *query;
  const char *op;
  // The scan already yields ascending order, so the planner drops the sort.
  bool orderby_elided;
};

constexpr MirroredIndexCase kMirroredIndexCases[] = {
    {"EdgeType",
     "CREATE EDGE INDEX ON :KNOWS",
     "CREATE (:N)-[:KNOWS]->(:N)",
     "MATCH ()-[r:KNOWS]->() RETURN r",
     "ScanAllByEdgeType",
     false},
    {"EdgeTypeProperty",
     "CREATE EDGE INDEX ON :KNOWS(w)",
     "CREATE (:N)-[:KNOWS {w: 1}]->(:N)",
     "MATCH ()-[r:KNOWS]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w",
     "ScanAllByEdgeTypeProperty",
     true},
    {"GlobalEdgeProperty",
     "CREATE GLOBAL EDGE INDEX ON :(w)",
     "CREATE (:N)-[:KNOWS {w: 1}]->(:N)",
     "MATCH ()-[r]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w",
     "ScanAllByEdgeProperty",
     true},
    {"GlobalVertexProperty",
     "CREATE GLOBAL INDEX ON :(w)",
     "CREATE (:V {w: 1}), (:V {w: 2})",
     "MATCH (n) WHERE n.w > 0 RETURN n.w ORDER BY n.w",
     "ScanAllByVertexProperty",
     true},
    {"LabelOnly", "CREATE INDEX ON :L", "CREATE (:L), (:L)", "MATCH (n:L) RETURN n", "ScanAllByLabel", false},
    {"CompositeLabelProperty",
     "CREATE INDEX ON :P(a, b)",
     "CREATE (:P {a:1, b:1}), (:P {a:2, b:2})",
     "MATCH (n:P) WHERE n.a > 0 RETURN n ORDER BY n.a, n.b",
     "ScanAllByLabelProperties",
     false},
    {"NestedLabelProperty",
     "CREATE INDEX ON :P(a.b)",
     "CREATE (:P {a:{b:1}}), (:P {a:{b:1}})",
     "MATCH (n:P) WHERE n.a.b > 0 RETURN n ORDER BY n.a.b",
     "ScanAllByLabelProperties",
     false},
};

class VersioningMirroredIndexPlanTest : public VersioningIndexTest,
                                        public ::testing::WithParamInterface<MirroredIndexCase> {};
}  // namespace

// Every index main held at the fork is mirrored onto the branch, so the planner picks the index operator.
TEST_P(VersioningMirroredIndexPlanTest, BranchMirroredIndexIsSelectedInPlan) {
  const auto &c = GetParam();
  faker.Interpret(c.ddl);
  faker.Interpret(c.seed);
  Fork();

  const auto plan = ExplainText(faker, c.query);
  EXPECT_TRUE(PlanHasOperator(plan, c.op)) << plan;
  if (c.orderby_elided) EXPECT_THAT(plan, ::testing::Not(::testing::HasSubstr("OrderBy"))) << plan;
}

INSTANTIATE_TEST_SUITE_P(Indexes, VersioningMirroredIndexPlanTest, ::testing::ValuesIn(kMirroredIndexCases),
                         [](const ::testing::TestParamInfo<MirroredIndexCase> &info) {
                           return std::string{info.param.name};
                         });

TEST_F(VersioningIndexTest, BranchNestedLabelPropertyIndexReflectsBranchMutations) {
  faker.Interpret("CREATE INDEX ON :P(a.b)");
  // Creation order differs from b order.
  faker.Interpret("CREATE (:P {n:'v1', a:{b:50}}), (:P {n:'v2', a:{b:20}}), (:P {n:'v3', a:{b:40}})");
  Fork();

  faker.Interpret("MATCH (n:P {n:'v2'}) SET n.a = {b: 99}");
  faker.Interpret("MATCH (n:P {n:'v3'}) DETACH DELETE n");
  faker.Interpret("CREATE (:P {n:'v4', a:{b:25}})");

  const std::string query = "MATCH (n:P) WHERE n.a.b > 0 RETURN n.a.b ORDER BY n.a.b";
  ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  EXPECT_EQ(IntRows(faker, query), (Rows{{25}, {50}, {99}}));
}

TEST_F(VersioningIndexTest, BranchLabelPropertyPointLookupOverlay) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret(
      "CREATE (:L {p: 1, tag: 'untouched'}), (:L {p: 1, tag: 'untouched2'}), (:L {p: 2, tag: 'changed'}), "
      "(:L {p: 3, tag: 'unlabeled'}), (:M {p: 4, tag: 'gained'}), (:L {p: 5, tag: 'other'}), "
      "(:L {p: 7, tag: 'deleted'})");
  Fork();

  faker.Interpret("MATCH (n:L {tag: 'changed'}) SET n.p = 20");
  faker.Interpret("MATCH (n:L {tag: 'unlabeled'}) REMOVE n:L");
  faker.Interpret("MATCH (n:M {tag: 'gained'}) SET n:L");
  faker.Interpret("MATCH (n:L {tag: 'other'}) SET n.tag = 'other2'");
  faker.Interpret("MATCH (n:L {tag: 'deleted'}) DETACH DELETE n");
  faker.Interpret("CREATE (:L {p: 6, tag: 'native'})");

  ExpectPlanUses(faker, "MATCH (n:L {p: 1}) RETURN n.tag", "ScanAllByLabelProperties");
  auto const lookup = [&](int64_t p) {
    auto tags = StringRows(faker, "MATCH (n:L {p: " + std::to_string(p) + "}) RETURN n.tag");
    std::ranges::sort(tags);
    return tags;
  };
  EXPECT_EQ(lookup(1), (Strings{"untouched", "untouched2"}));
  EXPECT_EQ(lookup(2), Strings{}) << "the fork entry of a changed key must not match";
  EXPECT_EQ(lookup(20), Strings{"changed"});
  EXPECT_EQ(lookup(3), Strings{}) << "a removed label drops the fork hit";
  EXPECT_EQ(lookup(4), Strings{"gained"}) << "a gained label matches on the fork value";
  EXPECT_EQ(lookup(5), Strings{"other2"}) << "an unrelated change keeps the vertex";
  EXPECT_EQ(lookup(6), Strings{"native"});
  EXPECT_EQ(lookup(7), Strings{});
}

TEST_F(VersioningIndexTest, BranchLabelPropertyRangeOverlayIsOrderedAcrossForkAndBranch) {
  faker.Interpret("CREATE INDEX ON :P(v)");
  faker.Interpret(R"(CREATE INDEX ON :P(v) WITH CONFIG {"order": "DESC"})");
  faker.Interpret(
      "CREATE (:P {v: 10, name: 'A'}), (:P {v: 20, name: 'B'}), (:P {v: 30, name: 'C'}), (:P {v: 40, name: 'D'}), "
      "(:P {v: 50, name: 'E'}), (:P {v: 15, name: 'G'}), (:Q {v: 25, name: 'F'})");
  Fork();

  faker.Interpret("MATCH (n:P {name: 'B'}) SET n.v = 45");      // moves within the range
  faker.Interpret("MATCH (n:P {name: 'D'}) SET n.note = 'x'");  // key unchanged
  faker.Interpret("MATCH (n:P {name: 'E'}) REMOVE n:P");
  faker.Interpret("MATCH (n:Q {name: 'F'}) SET n:P");  // gains :P at its fork key
  faker.Interpret("MATCH (n:P {name: 'G'}) DETACH DELETE n");
  faker.Interpret("CREATE (:P {v: 35, name: 'N1'}), (:P {v: 5, name: 'N2'})");

  const std::string range = "MATCH (n:P) WHERE n.v >= 10 AND n.v <= 60 RETURN n.name ORDER BY n.v";
  ExpectPlanUses(faker, range, "ScanAllByLabelProperties");
  EXPECT_EQ(StringRows(faker, range), (Strings{"A", "F", "C", "N1", "D", "B"}));
  const auto desc_plan = ExplainText(faker, range + " DESC");
  EXPECT_THAT(desc_plan, ::testing::HasSubstr("(DESC)")) << desc_plan;
  EXPECT_THAT(desc_plan, ::testing::Not(::testing::HasSubstr("OrderBy"))) << desc_plan;
  EXPECT_EQ(StringRows(faker, range + " DESC"), (Strings{"B", "D", "N1", "C", "F", "A"}));
}

// A write to an unindexed property keeps the index order and the row; a later key change moves the row.
TEST_F(VersioningIndexTest, BranchLabelPropertySeekUnaffectedByUnrelatedPropertyChange) {
  faker.Interpret("CREATE INDEX ON :P(v)");
  faker.Interpret("CREATE (:P {v: 10, note: 'a'}), (:P {v: 20, note: 'b'}), (:P {v: 30, note: 'c'})");
  Fork();

  const std::string query = "MATCH (n:P) WHERE n.v >= 10 RETURN n.note ORDER BY n.v";
  ExpectPlanUses(faker, query + " DESC LIMIT 2", "ScanAllByLabelProperties");
  ExpectNoOrderBy(faker, query);
  EXPECT_EQ(StringRows(faker, query), (Strings{"a", "b", "c"}));

  faker.Interpret("MATCH (n:P {v: 20}) SET n.note = 'B2'");
  EXPECT_EQ(StringRows(faker, query), (Strings{"a", "B2", "c"}));
  EXPECT_EQ(StringRows(faker, query + " DESC LIMIT 2"), (Strings{"c", "B2"}));

  faker.Interpret("MATCH (n:P {v: 20}) SET n.v = 40");
  EXPECT_EQ(StringRows(faker, query), (Strings{"a", "c", "B2"}));
}

TEST_F(VersioningIndexTest, BranchCompositeLabelPropertyPrefixOverlay) {
  faker.Interpret("CREATE INDEX ON :L(a, b)");
  faker.Interpret(
      "CREATE (:L {a: 1, b: 1, t: 'x1'}), (:L {a: 1, b: 2, t: 'x2'}), (:L {a: 2, b: 5, t: 'x3'}), "
      "(:L {a: 1, b: 9, t: 'x4'}), (:L {a: 1, b: 4, t: 'x5'})");
  Fork();

  faker.Interpret("MATCH (n:L {t: 'x2'}) SET n.b = 0");  // still a = 1, earlier in (a, b) order
  faker.Interpret("MATCH (n:L {t: 'x3'}) SET n.a = 1");  // newly a = 1
  faker.Interpret("MATCH (n:L {t: 'x4'}) SET n.a = 7");  // no longer a = 1
  faker.Interpret("MATCH (n:L {t: 'x5'}) DETACH DELETE n");
  faker.Interpret("CREATE (:L {a: 1, b: 3, t: 'n1'})");

  const std::string prefix = "MATCH (n:L) WHERE n.a = 1 RETURN n.t ORDER BY n.a, n.b";
  const std::string full = "MATCH (n:L) WHERE n.a = 1 AND n.b = 5 RETURN n.t";
  const std::string range = "MATCH (n:L) WHERE n.a > 0 RETURN n.t ORDER BY n.a, n.b";
  for (const auto &query : {prefix, full, range}) ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  EXPECT_EQ(StringRows(faker, prefix), (Strings{"x2", "x1", "n1", "x3"}));
  EXPECT_EQ(StringRows(faker, full), (Strings{"x3"}));
  EXPECT_EQ(StringRows(faker, "MATCH (n:L) WHERE n.a = 7 RETURN n.t"), (Strings{"x4"}));
  EXPECT_EQ(StringRows(faker, range), (Strings{"x2", "x1", "n1", "x3", "x4"}));
}

TEST_F(VersioningIndexTest, BranchGlobalPropertyOverlay) {
  faker.Interpret("CREATE GLOBAL INDEX ON :(w)");
  faker.Interpret("CREATE (:A {w: 1, t: 'a'}), (:B {w: 2, t: 'b'}), (:C {w: 3, t: 'c'}), (:D {w: 4, t: 'd'})");
  Fork();

  faker.Interpret("MATCH (n {t: 'b'}) SET n.w = 10");
  faker.Interpret("MATCH (n {t: 'c'}) REMOVE n.w");
  faker.Interpret("MATCH (n {t: 'd'}) DETACH DELETE n");
  faker.Interpret("CREATE (:E {w: 2, t: 'e'})");

  const std::string range = "MATCH (n) WHERE n.w > 0 RETURN n.t ORDER BY n.w";
  const std::string presence = "MATCH (n) WHERE n.w IS NOT NULL RETURN n.t ORDER BY n.w";
  ExpectPlanUses(faker, range, "ScanAllByVertexProperty");
  ExpectNoOrderBy(faker, range);
  ExpectPlanUses(faker, presence, "ScanAllByVertexProperty");
  ExpectPlanUses(faker, "MATCH (n) WHERE n.w = 2 RETURN n.t", "ScanAllByVertexProperty");
  EXPECT_EQ(StringRows(faker, range), (Strings{"a", "e", "b"}));
  EXPECT_EQ(StringRows(faker, presence), (Strings{"a", "e", "b"}));
  EXPECT_EQ(StringRows(faker, "MATCH (n) WHERE n.w = 2 RETURN n.t"), (Strings{"e"}));
  EXPECT_EQ(StringRows(faker, "MATCH (n) WHERE n.w IN [3, 10] RETURN n.t"), (Strings{"b"}));
}

namespace {
using TagP = std::vector<std::pair<std::string, int64_t>>;

Strings Tags(const TagP &rows, std::optional<int64_t> p = std::nullopt) {
  Strings out;
  for (const auto &[tag, value] : rows) {
    if (!p || value == *p) out.push_back(tag);
  }
  std::ranges::sort(out);
  return out;
}

constexpr auto kSeedVertices =
    "CREATE (:L {p:1, t:'a'}), (:M {p:2, t:'b'}), (:L {p:3, t:'c'}), (:L {p:5, t:'e'}), (:L {p:6, t:'f'})";
constexpr auto kMainDrift =
    "MATCH (n {t:'b'}) SET n:L;"
    "MATCH (n {t:'c'}) REMOVE n:L;"
    "MATCH (n {t:'e'}) SET n.p = 50;"
    "CREATE (:L {p:9, t:'n'});"
    "MATCH (n {t:'f'}) DETACH DELETE n";
constexpr auto kBranchWrites =
    "CREATE (:L {p:7, t:'bn'});"
    "MATCH (n {t:'a'}) SET n.p = 10;"
    "MATCH (n {t:'c'}) REMOVE n:L";

struct LateVertexIndexCase {
  const char *name;
  // The :L indexes exist at the fork and are re-created on main; otherwise they are first created after it.
  bool rebuilt;
  bool branch_writes_before;  // before main drifts and the indexes are built
  bool branch_labels_b;       // with branch_writes_before: also label the fork vertex 'b' as :L on the branch
  bool main_drift;
  bool branch_writes_after;  // after the indexes are built
  TagP expected;
  std::vector<int64_t> probe_p;
};

const std::vector<LateVertexIndexCase> kLateVertexIndexCases = {
    {"LateMainDrift",
     false,
     false,
     false,
     true,
     false,
     {{"a", 1}, {"c", 3}, {"e", 5}, {"f", 6}},
     {1, 3, 5, 6, 9, 50, 2}},
    {"LateBranchWritesBefore",
     false,
     true,
     true,
     false,
     false,
     {{"a", 10}, {"b", 2}, {"bn", 7}, {"e", 5}, {"f", 6}},
     {1, 2, 3, 5, 6, 7, 10}},
    {"LateMainDriftAndBranchWritesBefore",
     false,
     true,
     false,
     true,
     false,
     {{"a", 10}, {"bn", 7}, {"e", 5}, {"f", 6}},
     {1, 3, 5, 6, 7, 9, 10, 50, 2}},
    {"LateBranchWritesAfter",
     false,
     false,
     false,
     true,
     true,
     {{"a", 10}, {"bn", 7}, {"e", 5}, {"f", 6}},
     {1, 3, 5, 6, 7, 9, 10, 50, 2}},
    {"RebuiltMainDrift",
     true,
     false,
     false,
     true,
     false,
     {{"a", 1}, {"c", 3}, {"e", 5}, {"f", 6}},
     {1, 3, 5, 6, 9, 50, 2}},
    {"RebuiltBranchWritesAndMainDrift",
     true,
     true,
     false,
     true,
     false,
     {{"a", 10}, {"bn", 7}, {"e", 5}, {"f", 6}},
     {1, 3, 5, 6, 7, 9, 10, 50, 2}},
};

class VersioningLateVertexIndexTest : public VersioningIndexTest,
                                      public ::testing::WithParamInterface<LateVertexIndexCase> {};
}  // namespace

// An index main creates after the fork, or re-creates, is not visible at the fork: the branch falls back to a scan and
// reads fork state plus its own writes, never main's later state.
TEST_P(VersioningLateVertexIndexTest, BranchReadsForkStateAndOwnWrites) {
  const auto &c = GetParam();
  if (c.rebuilt) {
    faker.Interpret("CREATE INDEX ON :L");
    faker.Interpret("CREATE INDEX ON :L(p)");
  }
  faker.Interpret(kSeedVertices);
  Fork();
  if (c.branch_writes_before) {
    RunStatements(faker, kBranchWrites);
    if (c.branch_labels_b) faker.Interpret("MATCH (n {t:'b'}) SET n:L");
  }
  UseMain();
  if (c.main_drift) RunStatements(faker, kMainDrift);
  if (c.rebuilt) {
    faker.Interpret("DROP INDEX ON :L");
    faker.Interpret("DROP INDEX ON :L(p)");
  }
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE INDEX ON :L(p)");
  Use(faker, "b");
  if (c.branch_writes_after) RunStatements(faker, kBranchWrites);

  const std::string label_idx = "MATCH (n:L) RETURN n.t AS t ORDER BY t";
  const std::string label_scan = "MATCH (n) WHERE 'L' IN labels(n) RETURN n.t AS t ORDER BY t";
  const std::string prop_idx = "MATCH (n:L) WHERE n.p > 0 RETURN n.t AS t ORDER BY t";
  EXPECT_EQ(PlanHasOperator(ExplainText(faker, label_idx), "ScanAllByLabel"), c.rebuilt);
  EXPECT_EQ(PlanHasOperator(ExplainText(faker, prop_idx), "ScanAllByLabelProperties"), c.rebuilt);
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, label_scan), "ScanAllByLabel"));

  const auto want = Tags(c.expected);
  EXPECT_EQ(StringRows(faker, label_scan), want) << "the scan disagrees with the hand-derived state";
  EXPECT_EQ(StringRows(faker, label_idx), want);
  EXPECT_EQ(StringRows(faker, prop_idx), want);
  for (auto p : c.probe_p) {
    EXPECT_EQ(StringRows(faker, "MATCH (n:L {p: " + std::to_string(p) + "}) RETURN n.t AS t ORDER BY t"),
              Tags(c.expected, p))
        << "p = " << p;
  }
}

INSTANTIATE_TEST_SUITE_P(Cases, VersioningLateVertexIndexTest, ::testing::ValuesIn(kLateVertexIndexCases),
                         [](const ::testing::TestParamInfo<LateVertexIndexCase> &info) {
                           return std::string{info.param.name};
                         });

TEST_F(VersioningIndexTest, BranchRebuiltGlobalVertexPropertyIndexReadsForkState) {
  faker.Interpret("CREATE GLOBAL INDEX ON :(w)");
  faker.Interpret("CREATE (:A {w:1, t:'a'}), (:B {w:2, t:'b'}), (:C {w:3, t:'c'})");
  Fork();
  UseMain();
  RunStatements(faker,
                "MATCH (n {t:'b'}) SET n.w = 20;"
                "MATCH (n {t:'c'}) REMOVE n.w;"
                "CREATE (:D {w:4, t:'d'});"
                "MATCH (n {t:'a'}) DETACH DELETE n");
  faker.Interpret("DROP GLOBAL INDEX ON :(w)");
  faker.Interpret("CREATE GLOBAL INDEX ON :(w)");
  Use(faker, "b");

  const std::string range = "MATCH (n) WHERE n.w > 0 RETURN n.t ORDER BY n.w";
  ExpectPlanUses(faker, range, "ScanAllByVertexProperty");
  EXPECT_EQ(StringRows(faker, range), (Strings{"a", "b", "c"}));
  const std::vector<std::pair<int64_t, Strings>> probes = {{1, {"a"}}, {2, {"b"}}, {3, {"c"}}, {4, {}}, {20, {}}};
  for (const auto &[w, want] : probes) {
    EXPECT_EQ(StringRows(faker, "MATCH (n) WHERE n.w = " + std::to_string(w) + " RETURN n.t"), want) << "w = " << w;
  }
}

// Main is the oracle for each range kind (list bound, IS NOT NULL, upper-only bound) on every key part.
TEST_F(VersioningIndexTest, BranchCompositeIndexAppliesEveryRangeToEveryKeyPartLikeMain) {
  faker.Interpret("CREATE INDEX ON :L(q, p)");
  faker.Interpret("UNWIND [[1], [1, 2], [1, 3], [1, null], [null, 1], [2], 5, 'a'] AS v CREATE (:L {p: v, q: 1})");
  faker.Interpret("CREATE (:L {q: 1}), (:L)");

  const std::string project =
      " WITH CASE valueType(n.p) WHEN 'LIST' THEN reduce(s = '', x IN n.p | s + coalesce(toString(x), 'n')) "
      "WHEN 'NULL' THEN 'null' ELSE 'x' + toString(n.p) END AS s ORDER BY s RETURN s";
  const std::string list_bound = "MATCH (n:L) WHERE n.q = 1 AND n.p > [1, 2]" + project;
  const std::vector<std::string> queries{
      list_bound,
      "MATCH (n:L) WHERE n.q = 1 AND n.p IS NOT NULL" + project,
      "MATCH (n:L) WHERE n.q < 5" + project,
  };

  std::vector<Strings> oracle;
  for (auto const &query : queries) {
    ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
    oracle.push_back(StringRows(faker, query));
  }
  EXPECT_EQ(oracle[0], (Strings{"13", "2"})) << "main is the oracle";

  Fork();
  for (size_t i = 0; i < queries.size(); ++i) {
    EXPECT_EQ(StringRows(faker, queries[i]), oracle[i]) << "index ready on both sides: " << queries[i];
  }

  UseMain();
  faker.Interpret("DROP INDEX ON :L(q, p)");
  faker.Interpret("CREATE INDEX ON :L(q, p)");
  Use(faker, "b");
  for (size_t i = 0; i < queries.size(); ++i) {
    ExpectPlanUses(faker, queries[i], "ScanAllByLabelProperties");
    EXPECT_EQ(StringRows(faker, queries[i]), oracle[i]) << "full-scan fallback: " << queries[i];
  }
}

// Main indexes a missing nested part as null, so the branch must too.
TEST_F(VersioningIndexTest, BranchCompositeIndexWithANestedPartKeepsAVertexMissingThatPartLikeMain) {
  faker.Interpret("CREATE INDEX ON :L(a, b.c)");
  faker.Interpret(
      "CREATE (:L {id: 1, a: 1, b: {c: 2}}), (:L {id: 2, a: 1}), (:L {id: 3, a: 1, b: 5}), "
      "(:L {id: 4, a: 2, b: {c: 1}})");

  const std::string query = "MATCH (n:L) WHERE n.a = 1 RETURN n.id ORDER BY n.id";
  ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  const auto oracle = IntRows(faker, query);
  EXPECT_EQ(oracle, (Rows{{1}, {2}, {3}})) << "main is the oracle";

  Fork();
  faker.Interpret("MATCH (n:L) WHERE n.id IN [2, 3] SET n.a = 1");
  EXPECT_EQ(IntRows(faker, query), oracle) << "fork rows rechecked after a write to a key part";

  UseMain();
  faker.Interpret("DROP INDEX ON :L(a, b.c)");
  faker.Interpret("CREATE INDEX ON :L(a, b.c)");
  Use(faker, "b");
  ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  EXPECT_EQ(IntRows(faker, query), oracle) << "full-scan fallback";
}

// MERGE matches with View::NEW: a fork vertex whose key an earlier statement moved is matched once, at its new key.
TEST_F(VersioningIndexTest, NewViewScanYieldsAForkVertexWhoseKeyMovedOnceLikeMain) {
  faker.Interpret("CREATE INDEX ON :L(v, w)");
  faker.Interpret("CREATE (:L {id: 1, v: 3, w: 1}), (:L {id: 2, v: 3, w: 5}), (:L {id: 3, v: 4, w: 1})");

  Fork();
  const std::string move = "MATCH (n:L {id: 1}) SET n.w = 2";
  const std::string query = "MERGE (n:L {v: 3}) RETURN n.id ORDER BY n.id";
  faker.Interpret(move);
  ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  const auto branch_rows = IntRows(faker, query);
  EXPECT_EQ(branch_rows, (Rows{{1}, {2}})) << "each fork vertex once";

  UseMain();
  faker.Interpret(move);
  ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  EXPECT_EQ(IntRows(faker, query), branch_rows) << "main is the oracle";
}

// The diff engine mirrors no vector index, yet a branch-created vertex or edge still omits its vector-indexed property
// when the flag is on, like a fork object.
TEST_F(VersioningIndexTest, BranchObjectsOmitVectorIndexedPropertiesWhenFlagOn) {
  const auto settings_dir = data_directory / "settings";
  memgraph::utils::Settings settings(settings_dir);
  memgraph::flags::run_time::Initialize(settings);
  const auto set_omit = [&](bool enabled) {
    settings.SetValue("storage.omit_vector_index_properties_on_return", enabled ? "true" : "false");
  };
  memgraph::utils::OnScopeExit reset_flag{[&] { set_omit(false); }};

  const std::string config = R"( WITH CONFIG {"dimension": 2, "capacity": 10})";
  faker.Interpret("CREATE VECTOR INDEX vi ON :V(emb)" + config);
  faker.Interpret("CREATE VECTOR EDGE INDEX ve ON :E(emb)" + config);
  faker.Interpret("CREATE (:V {id: 1, emb: [1.0, 2.0]})-[:E {id: 1, emb: [1.0, 2.0]}]->(:V {id: 2, emb: [3.0, 4.0]})");
  Fork();
  faker.Interpret("MATCH (a:V {id: 1}) CREATE (a)-[:E {id: 2, emb: [5.0, 6.0]}]->(:V {id: 3, emb: [5.0, 6.0]})");

  set_omit(true);
  auto stream = faker.Interpret("MATCH (:V)-[e:E]->(b:V) RETURN e, b ORDER BY e.id");
  const auto &rows = stream.GetResults();
  ASSERT_EQ(rows.size(), 2U);
  for (const auto &row : rows) {
    const auto &edge = row[0].ValueEdge().properties;
    const auto &vertex = row[1].ValueVertex().properties;
    EXPECT_TRUE(edge.contains("id"));
    EXPECT_FALSE(edge.contains("emb")) << "edge " << edge.at("id").ValueInt();
    EXPECT_TRUE(vertex.contains("id"));
    EXPECT_FALSE(vertex.contains("emb")) << "vertex " << vertex.at("id").ValueInt();
  }
}

// Inside one transaction the label scan, the label-property scan and a full-scan filter agree and stay at the
// snapshot while another session commits on the branch and main commits more; they still agree after COMMIT.
TEST_F(VersioningIndexTest, IndexScansInOneTransactionStayStableWhileOthersCommit) {
  faker.Interpret("CREATE INDEX ON :User");
  faker.Interpret("CREATE INDEX ON :User(age)");
  faker.Interpret("UNWIND range(1, 20) AS i CREATE (:User {id: i, age: i % 5})");
  Fork();
  auto writers = SessionsOnBranch(1);
  auto &writer = *writers[0];
  InterpreterFaker main_session{&interpreter_context, db};
  interpreter_context.auth_checker = &faker.auth_checker;
  UseMain(main_session);

  auto const labelled = "MATCH (n:User) RETURN n.id ORDER BY n.id";
  auto const by_age = "MATCH (n:User) WHERE n.age = 2 RETURN n.id ORDER BY n.id";
  auto const full_labelled = "MATCH (n) WHERE 'User' IN labels(n) RETURN n.id ORDER BY n.id";
  auto const full_by_age = "MATCH (n) WHERE 'User' IN labels(n) AND n.age = 2 RETURN n.id ORDER BY n.id";
  ExpectPlanUses(faker, labelled, "ScanAllByLabel");
  ExpectPlanUses(faker, by_age, "ScanAllByLabelProperties");
  ExpectPlanUses(faker, full_by_age, "ScanAll");
  auto const expect_scans_agree = [&] {
    EXPECT_EQ(IntRows(faker, labelled), IntRows(faker, full_labelled));
    EXPECT_EQ(IntRows(faker, by_age), IntRows(faker, full_by_age));
  };

  faker.Interpret("BEGIN");
  auto const labelled_before = IntRows(faker, labelled);
  auto const by_age_before = IntRows(faker, by_age);
  ASSERT_EQ(labelled_before.size(), 20U);
  ASSERT_EQ(by_age_before, (Rows{{2}, {7}, {12}, {17}}));
  expect_scans_agree();

  writer.Interpret("MATCH (n:User) WHERE n.id <= 10 SET n.age = 2");
  writer.Interpret("MATCH (n:User) WHERE n.id > 15 REMOVE n:User");
  writer.Interpret("MATCH (n:User {id: 3}) DETACH DELETE n");
  writer.Interpret("MATCH (n:User {id: 12}) REMOVE n.age");
  writer.Interpret("CREATE (:User {id: 100, age: 2})");
  main_session.Interpret("MATCH (n:User) WHERE n.id > 10 SET n.age = 2");
  main_session.Interpret("CREATE (:User {id: 200, age: 2})");

  EXPECT_EQ(IntRows(faker, labelled), labelled_before);
  EXPECT_EQ(IntRows(faker, by_age), by_age_before);
  expect_scans_agree();
  faker.Interpret("COMMIT");

  expect_scans_agree();
  EXPECT_EQ(IntRows(faker, by_age), (Rows{{1}, {2}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {100}}));
  EXPECT_EQ(IntRows(faker, labelled).size(), 15U) << "main's later vertex and age writes never reach the branch";
}

// A rolled-back branch write leaves its touched-gate bits set (a superset); the scans must still yield the vertex
// exactly once, on the composite, global-property and label arms alike.
TEST_F(VersioningIndexTest, IndexScansYieldAVertexOnceAfterItsBranchWriteWasRolledBack) {
  faker.Interpret("CREATE INDEX ON :User(age, id)");
  faker.Interpret("CREATE INDEX ON :User");
  faker.Interpret("CREATE GLOBAL INDEX ON :(age)");
  faker.Interpret("UNWIND range(1, 3) AS i CREATE (:User {id: i, age: i})");
  Fork();

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:User {id: 2}) SET n.age = 20, n:Extra");
  faker.Interpret("ROLLBACK");

  auto const by_age = "MATCH (n:User) WHERE n.age >= 1 RETURN n.id ORDER BY n.id";
  ExpectPlanUses(faker, by_age, "ScanAllByLabelProperties");
  EXPECT_EQ(IntRows(faker, by_age), (Rows{{1}, {2}, {3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) WHERE n.age = 2 RETURN n.id"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) WHERE n.age = 2 AND n.id = 2 RETURN n.id"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n.age = 2 RETURN n.id"), (Rows{{2}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:Extra) RETURN count(n)"), (Rows{{0}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.id ORDER BY n.id"), (Rows{{1}, {2}, {3}}));
}

// One branch writer moves a label and a flag from vertex to vertex in a transaction while main adds both to every
// vertex; each reader transaction must see one holder, the same from the label, property and full scans.
TEST_F(VersioningIndexTest, ConcurrentIndexScansSeeExactlyOneHolderOfAMovingLabelAndProperty) {
  constexpr int kVertices = 10;
  constexpr int kRounds = 150;
  faker.Interpret("CREATE INDEX ON :A");
  faker.Interpret("CREATE INDEX ON :T(flag)");
  faker.Interpret("UNWIND range(0, 9) AS i CREATE (:T {id: i, flag: 0})");
  faker.Interpret("MATCH (n:T {id: 0}) SET n:A, n.flag = 1");
  Fork();
  auto sessions = SessionsOnBranch(3);
  InterpreterFaker main_session{&interpreter_context, db};
  interpreter_context.auth_checker = &faker.auth_checker;
  UseMain(main_session);
  std::atomic<int> writers_left{2};

  {
    std::vector<std::jthread> threads;
    threads.emplace_back([&session = *sessions[0], &writers_left] {
      for (int round = 0; round < kRounds; ++round) {
        session.Interpret("BEGIN");
        session.Interpret("MATCH (n:T {id: " + std::to_string(round % kVertices) + "}) REMOVE n:A SET n.flag = 0");
        session.Interpret("MATCH (n:T {id: " + std::to_string((round + 1) % kVertices) + "}) SET n:A, n.flag = 1");
        session.Interpret("COMMIT");
      }
      --writers_left;
    });
    threads.emplace_back([&session = main_session, &writers_left] {
      for (int round = 0; round < kRounds; ++round) session.Interpret("MATCH (n:T) SET n:A, n.flag = 1");
      --writers_left;
    });
    for (int r = 1; r < 3; ++r) {
      threads.emplace_back([&session = *sessions[r], &writers_left] {
        do {
          session.Interpret("BEGIN");
          auto const by_label = IntRows(session, "MATCH (n:A) RETURN n.id");
          auto const by_flag = IntRows(session, "MATCH (n:T) WHERE n.flag = 1 RETURN n.id");
          auto const full = IntRows(session, "MATCH (n) WHERE 'A' IN labels(n) RETURN n.id");
          auto const again = IntRows(session, "MATCH (n:A) RETURN n.id");
          session.Interpret("COMMIT");
          ASSERT_EQ(by_label.size(), 1U) << "the label moved atomically, main's copies never leak";
          ASSERT_EQ(by_flag, by_label);
          ASSERT_EQ(full, by_label);
          ASSERT_EQ(again, by_label) << "the snapshot moved inside one transaction";
        } while (writers_left.load() > 0);
      });
    }
  }

  EXPECT_EQ(IntRows(faker, "MATCH (n:A) RETURN n.id"), (Rows{{kRounds % kVertices}}));
  EXPECT_EQ(IntRows(main_session, "MATCH (n:A) RETURN count(n)"), (Rows{{kVertices}}));
}

namespace {

struct ModelVertex {
  int64_t id;
  std::set<std::string> labels;
  std::optional<int64_t> p;
};

using Model = std::map<int64_t, ModelVertex>;

Rows IdRows(Model const &model, auto &&keep) {
  Rows rows;
  for (auto const &[id, vertex] : model) {
    if (keep(vertex)) rows.push_back({id});
  }
  return rows;
}

void RandomBranchWrite(InterpreterFaker &session, std::mt19937 &rng, Model &model, int64_t &next_native_id) {
  auto const pick = [&](int64_t bound) { return static_cast<int64_t>(rng() % static_cast<uint64_t>(bound)); };
  auto const target = [&]() -> std::optional<int64_t> {
    if (model.empty()) return std::nullopt;
    auto it = model.begin();
    std::advance(it, pick(static_cast<int64_t>(model.size())));
    return it->first;
  };
  bool const rolled_back = pick(6) == 0;
  auto const run = [&](std::string const &statement) {
    if (!rolled_back) {
      session.Interpret(statement);
      return;
    }
    session.Interpret("BEGIN");
    session.Interpret(statement);
    session.Interpret("ROLLBACK");
  };
  auto const match = [](int64_t id) { return "MATCH (n {id: " + std::to_string(id) + "}) "; };

  auto const id = target();
  auto const kind = pick(7);
  if (kind == 6 || !id) {
    auto const native = next_native_id++;
    auto const p = pick(8);
    run("CREATE (:L {id: " + std::to_string(native) + ", p: " + std::to_string(p) + "})");
    if (!rolled_back) model[native] = ModelVertex{native, {"L"}, p};
    return;
  }
  auto &vertex = model.at(*id);
  switch (kind) {
    case 0: {
      auto const label = pick(2) == 0 ? "L" : "K";
      run(match(*id) + "SET n:" + label);
      if (!rolled_back) vertex.labels.insert(label);
      break;
    }
    case 1: {
      auto const label = pick(2) == 0 ? "L" : "K";
      run(match(*id) + "REMOVE n:" + label);
      if (!rolled_back) vertex.labels.erase(label);
      break;
    }
    case 2:
    case 3: {
      auto const p = pick(8);
      run(match(*id) + "SET n.p = " + std::to_string(p));
      if (!rolled_back) vertex.p = p;
      break;
    }
    case 4:
      run(match(*id) + "REMOVE n.p");
      if (!rolled_back) vertex.p.reset();
      break;
    default:
      run(match(*id) + "DETACH DELETE n");
      if (!rolled_back) model.erase(*id);
      break;
  }
}

}  // namespace

// Under random writes, some rolled back, every scan (the label-property one in index order) must equal the hand-kept
// model after each write, and again inside one explicit transaction.
TEST_F(VersioningIndexTest, BranchScansEqualAModelOfForkAndNativeWrites) {
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE INDEX ON :K");
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE GLOBAL INDEX ON :(p)");
  Model model;
  for (int64_t id = 0; id < 14; ++id) {
    std::string const props = "id: " + std::to_string(id) + (id % 3 == 0 ? "" : ", p: " + std::to_string(id % 5));
    faker.Interpret(std::string(id % 2 == 0 ? "CREATE (:L {" : "CREATE (:L:K {") + props + "})");
    model[id] = ModelVertex{id,
                            id % 2 == 0 ? std::set<std::string>{"L"} : std::set<std::string>{"L", "K"},
                            id % 3 == 0 ? std::nullopt : std::optional<int64_t>{id % 5}};
  }
  Fork();
  ExpectPlanUses(faker, "MATCH (n:K) RETURN n.id", "ScanAllByLabel");
  ExpectPlanUses(faker, "MATCH (n:L) WHERE n.p >= 0 RETURN n.id", "ScanAllByLabelProperties");
  ExpectPlanUses(faker, "MATCH (n) WHERE n.p >= 0 RETURN n.id", "ScanAllByVertexProperty");

  auto const expect_model = [&](std::string const &where) {
    auto const has_label = [](ModelVertex const &v) { return v.labels.contains("L"); };
    auto const has_p = [](ModelVertex const &v) { return v.p.has_value(); };
    auto const everything = [](ModelVertex const &) { return true; };
    EXPECT_EQ(IntRows(faker, "MATCH (n:L) RETURN n.id ORDER BY n.id"), IdRows(model, has_label)) << where;
    EXPECT_EQ(IntRows(faker, "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.id"),
              IdRows(model, [&](auto const &v) { return has_label(v) && has_p(v); }))
        << where;
    EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n.p >= 0 RETURN n.id ORDER BY n.id"), IdRows(model, has_p)) << where;
    EXPECT_EQ(IntRows(faker, "MATCH (n) RETURN n.id ORDER BY n.id"), IdRows(model, everything)) << where;
    EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE 'L' IN labels(n) RETURN n.id ORDER BY n.id"), IdRows(model, has_label))
        << where;
    EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN n.id ORDER BY n.id"),
              IdRows(model, [](auto const &v) { return v.labels.contains("K"); }))
        << where;
    EXPECT_EQ(IntRows(faker, "MATCH (n:L:K) RETURN n.id ORDER BY n.id"),
              IdRows(model, [&](auto const &v) { return has_label(v) && v.labels.contains("K"); }))
        << where;
    Rows by_p;
    for (auto const &[id, v] : model) {
      if (has_label(v) && has_p(v)) by_p.push_back({*v.p});
    }
    std::ranges::sort(by_p);
    EXPECT_EQ(IntRows(faker, "MATCH (n:L) WHERE n.p >= 0 RETURN n.p ORDER BY n.p"), by_p) << where;
  };

  expect_model("at the fork");
  int64_t next_native_id = 100;
  for (uint32_t seed = 1; seed <= 5; ++seed) {
    std::mt19937 rng(seed);
    for (int step = 0; step < 30; ++step) {
      RandomBranchWrite(faker, rng, model, next_native_id);
      expect_model("seed " + std::to_string(seed) + " step " + std::to_string(step));
    }
    faker.Interpret("BEGIN");
    expect_model("seed " + std::to_string(seed) + " inside one transaction");
    faker.Interpret("COMMIT");
  }
}

// A stub write that no change filter recorded is not read, and a diff-engine index hit on a stub is never yielded as
// a vertex.
TEST_F(VersioningIndexTest, BranchScansSkipFilteredStubsAndNeverYieldAStubHit) {
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE GLOBAL INDEX ON :(p)");
  faker.Interpret("CREATE INDEX ON :M");
  faker.Interpret("UNWIND range(0, 3) AS i CREATE (:L {id: i, p: i})");
  Fork();

  faker.Interpret("BEGIN");
  faker.Interpret("RETURN 1");
  auto *ctx = faker.interpreter.current_db_.branch_context();
  ASSERT_NE(ctx, nullptr);
  auto stub = ctx->EnsureDiffVertex(memgraph::storage::Gid::FromUint(1));
  ASSERT_TRUE(stub);
  auto *storage = db->storage();
  ASSERT_TRUE(stub->AddLabel(storage->NameToLabel("M")).has_value());
  ASSERT_TRUE(
      stub->SetProperty(storage->NameToProperty("p"), memgraph::storage::PropertyValue(int64_t{99})).has_value());

  EXPECT_EQ(IntRows(faker, "MATCH (n:M) RETURN n.id"), (Rows{}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n.p = 99 RETURN n.id"), (Rows{}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L) WHERE n.p = 99 RETURN n.id"), (Rows{}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.id"), (Rows{{0}, {1}, {2}, {3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n.id = 1 RETURN n.p"), (Rows{{1}}));
  faker.Interpret("ROLLBACK");
}

// Scans skip the merge while the touched gates are clear, so the first write of each kind to a fork vertex must set its
// gate: on a fresh branch each write is the only one, and the scans must show it.
TEST_F(VersioningIndexTest, IndexScansNeverHideTheFirstStubWriteOfAKind) {
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE GLOBAL INDEX ON :(p)");
  faker.Interpret("UNWIND range(0, 3) AS i CREATE (:L {id: i, p: i})");
  faker.Interpret("CREATE (:K {id: 4, p: 4})");

  struct Case {
    std::string write;
    Rows label_rows;
    Rows label_property_rows;
    Rows property_rows;
  };

  std::vector<Case> const cases{
      {"MATCH (n {id: 1}) REMOVE n:L", {{0}, {2}, {3}}, {{0}, {2}, {3}}, {{0}, {1}, {2}, {3}, {4}}},
      {"MATCH (n {id: 4}) SET n:L", {{0}, {1}, {2}, {3}, {4}}, {{0}, {1}, {2}, {3}, {4}}, {{0}, {1}, {2}, {3}, {4}}},
      {"MATCH (n {id: 1}) SET n.p = -5", {{0}, {1}, {2}, {3}}, {{0}, {2}, {3}}, {{0}, {2}, {3}, {4}}},
      {"MATCH (n {id: 2}) REMOVE n.p", {{0}, {1}, {2}, {3}}, {{0}, {1}, {3}}, {{0}, {1}, {3}, {4}}},
      {"MATCH (n {id: 3}) DETACH DELETE n", {{0}, {1}, {2}}, {{0}, {1}, {2}}, {{0}, {1}, {2}, {4}}},
  };
  for (size_t i = 0; i < cases.size(); ++i) {
    auto const &c = cases[i];
    Fork("b" + std::to_string(i));
    faker.Interpret(c.write);
    EXPECT_EQ(IntRows(faker, "MATCH (n:L) RETURN n.id ORDER BY n.id"), c.label_rows) << c.write;
    EXPECT_EQ(IntRows(faker, "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.id"), c.label_property_rows) << c.write;
    EXPECT_EQ(IntRows(faker, "MATCH (n) WHERE n.p >= 0 RETURN n.id ORDER BY n.id"), c.property_rows) << c.write;
  }
}

namespace {

// Opens branch scans through the DbAccessor of the session's open transaction, to see how many rows the stream pulled.
class StreamProbe {
 public:
  StreamProbe(InterpreterFaker &session, std::string_view label, std::string_view property)
      : ctx_(session.interpreter.current_db_.branch_context()) {
    EXPECT_NE(ctx_, nullptr);
    EXPECT_NE(ctx_->current_diff_txn(), nullptr);
    label_ = ctx_->main().NameToLabel(label);
    edge_type_ = ctx_->main().NameToEdgeType(label);
    property_ = ctx_->main().NameToProperty(property);
    id_ = ctx_->main().NameToProperty("id");
  }

  memgraph::query::DbAccessor Dba() const { return memgraph::query::DbAccessor{ctx_->current_diff_txn(), ctx_}; }

  memgraph::storage::LabelId label() const { return label_; }

  // The edge type named like the label, for the edge scans.
  memgraph::storage::EdgeTypeId edge_type() const { return edge_type_; }

  memgraph::storage::PropertyId property() const { return property_; }

  // The `id` of each row `iterable` yields; `between` runs once after `steps` rows were taken.
  template <typename Iterable>
  std::vector<int64_t> Ids(Iterable &iterable, size_t limit = std::numeric_limits<size_t>::max(), size_t steps = 0,
                           std::function<void()> const &between = {}) const {
    std::vector<int64_t> out;
    for (auto it = iterable.begin(); it != iterable.end() && out.size() < limit; ++it) {
      auto id = (*it).GetProperty(memgraph::storage::View::OLD, id_);
      out.push_back(id.has_value() ? id->ValueInt() : -1);
      if (between && out.size() == steps) between();
    }
    return out;
  }

 private:
  memgraph::versioning::BranchContext *ctx_;
  memgraph::storage::LabelId label_;
  memgraph::storage::EdgeTypeId edge_type_;
  memgraph::storage::PropertyId property_;
  memgraph::storage::PropertyId id_;
};

}  // namespace

// Every row the scan yields comes from a stub: main's index holds no row in the range at all.
TEST_F(VersioningIndexTest, StreamedRangeScanYieldsStubsWithNoNativeVertexOnTheBranch) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret(R"(CREATE INDEX ON :L(p) WITH CONFIG {"order": "DESC"})");
  faker.Interpret("UNWIND range(0, 9) AS i CREATE (:L {id: i, p: i})");
  Fork();
  faker.Interpret("MATCH (n:L) WHERE n.id IN [7, 2, 5] SET n.p = n.id + 100");

  auto const above = "MATCH (n:L) WHERE n.p >= 100 RETURN n.id, n.p ORDER BY n.p";
  ExpectPlanUses(faker, above, "ScanAllByLabelProperties");
  auto const all = "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.p";
  for (auto const *query : {above, all}) {
    ExpectNoOrderBy(faker, query);
    ExpectNoOrderBy(faker, std::string(query) + " DESC");
  }
  EXPECT_EQ(IntRows(faker, above), (Rows{{2, 102}, {5, 105}, {7, 107}}));
  EXPECT_EQ(IntRows(faker, std::string(above) + " DESC"), (Rows{{7, 107}, {5, 105}, {2, 102}}));
  EXPECT_EQ(IntRows(faker, all), (Rows{{0}, {1}, {3}, {4}, {6}, {8}, {9}, {2}, {5}, {7}}));
  EXPECT_EQ(IntRows(faker, std::string(all) + " DESC"), (Rows{{7}, {5}, {2}, {9}, {8}, {6}, {4}, {3}, {1}, {0}}));

  // Re-created indexes are not ready at the fork: the same rows through the full-scan fallback, in both directions.
  UseMain();
  faker.Interpret("DROP INDEX ON :L(p)");
  faker.Interpret(R"(DROP INDEX ON :L(p) WITH CONFIG {"order": "DESC"})");
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret(R"(CREATE INDEX ON :L(p) WITH CONFIG {"order": "DESC"})");
  Use(faker, "b");
  EXPECT_EQ(IntRows(faker, above), (Rows{{2, 102}, {5, 105}, {7, 107}}));
  EXPECT_EQ(IntRows(faker, std::string(above) + " DESC"), (Rows{{7, 107}, {5, 105}, {2, 102}}));
  EXPECT_EQ(IntRows(faker, all), (Rows{{0}, {1}, {3}, {4}, {6}, {8}, {9}, {2}, {5}, {7}}));
  EXPECT_EQ(IntRows(faker, std::string(all) + " DESC"), (Rows{{7}, {5}, {2}, {9}, {8}, {6}, {4}, {3}, {1}, {0}}));
}

// SET v:L on a fork vertex that already has the indexed property: the key (L, p) exists on the branch only through the
// copy of p onto the stub, and sorts among main's rows.
TEST_F(VersioningIndexTest, StreamedScanFindsAForkVertexThatGainedTheLabelAtItsForkValue) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE (:L {id: 1, p: 10}), (:L {id: 2, p: 30}), (:M {id: 3, p: 20}), (:M {id: 4, p: 40})");
  Fork();
  faker.Interpret("MATCH (n:M) WHERE n.id IN [3, 4] SET n:L");

  auto const query = "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.p";
  ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  ExpectNoOrderBy(faker, query);
  EXPECT_EQ(IntRows(faker, query), (Rows{{1}, {3}, {2}, {4}}));
  EXPECT_EQ(IntRows(faker, std::string(query) + " DESC"), (Rows{{4}, {2}, {3}, {1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L {p: 20}) RETURN n.id"), (Rows{{3}}));
  auto labelled = IntRows(faker, "MATCH (n:L) RETURN n.id");
  std::ranges::sort(labelled);
  EXPECT_EQ(labelled, (Rows{{1}, {2}, {3}, {4}}));
}

// REMOVE of one part of a composite key whose other parts are set leaves the vertex under its partial key.
TEST_F(VersioningIndexTest, StreamedCompositeScanKeepsAVertexThatLostOnePartOfItsKey) {
  faker.Interpret("CREATE INDEX ON :L(a, b)");
  faker.Interpret(
      "CREATE (:L {id: 1, a: 1, b: 1}), (:L {id: 2, a: 1, b: 2}), (:L {id: 3, a: 1, b: 3}), (:L {id: 4, a: 2, b: 1})");
  Fork();
  faker.Interpret("MATCH (n:L {id: 2}) REMOVE n.b");
  faker.Interpret("MATCH (n:L {id: 3}) REMOVE n.a");
  faker.Interpret("MATCH (n:L {id: 4}) REMOVE n.a, n.b");

  auto const by_a = "MATCH (n:L) WHERE n.a = 1 RETURN n.id";
  ExpectPlanUses(faker, by_a, "ScanAllByLabelProperties");
  auto ids = IntRows(faker, by_a);
  std::ranges::sort(ids);
  EXPECT_EQ(ids, (Rows{{1}, {2}}));
  auto present = IntRows(faker, "MATCH (n:L) WHERE n.b IS NOT NULL RETURN n.id");
  std::ranges::sort(present);
  EXPECT_EQ(present, (Rows{{1}, {3}}));
  auto labelled =
      IntRows(faker, "MATCH (n) WHERE 'L' IN labels(n) AND (n.a IS NOT NULL OR n.b IS NOT NULL) RETURN n.id");
  std::ranges::sort(labelled);
  EXPECT_EQ(labelled, (Rows{{1}, {2}, {3}}));
}

// Setting the indexed property copies the label onto the stub; removing the label then leaves the copy and the removal
// marker, so no scan yields the vertex until the label is added back.
TEST_F(VersioningIndexTest, StreamedScansHideAVertexWhoseCopiedLabelWasRemoved) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE (:L {id: 1, p: 1}), (:L {id: 2, p: 2}), (:L {id: 3, p: 3})");
  Fork();
  faker.Interpret("MATCH (n:L {id: 2}) SET n.p = 20");
  faker.Interpret("MATCH (n:L {id: 2}) REMOVE n:L");

  auto const by_p = "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.p";
  auto const by_label = "MATCH (n:L) RETURN n.id ORDER BY n.id";
  ExpectNoOrderBy(faker, by_p);
  EXPECT_EQ(IntRows(faker, by_p), (Rows{{1}, {3}}));
  EXPECT_EQ(IntRows(faker, by_label), (Rows{{1}, {3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L {p: 20}) RETURN n.id"), (Rows{}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L {p: 2}) RETURN n.id"), (Rows{}));

  faker.Interpret("MATCH (n {id: 2}) SET n:L");
  EXPECT_EQ(IntRows(faker, by_p), (Rows{{1}, {3}, {2}}));
  EXPECT_EQ(IntRows(faker, by_label), (Rows{{1}, {2}, {3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L {p: 20}) RETURN n.id"), (Rows{{2}}));
}

// A key set back to the fork's value is main's row again, at main's position, yielded once.
TEST_F(VersioningIndexTest, StreamedScansYieldAKeySetBackToTheForkValueOnce) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE GLOBAL INDEX ON :(p)");
  faker.Interpret("UNWIND range(1, 5) AS i CREATE (:L {id: i, p: i * 10})");
  Fork();
  faker.Interpret("MATCH (n:L {id: 3}) SET n.p = 99");
  faker.Interpret("MATCH (n:L {id: 3}) SET n.p = 30");

  auto const by_label = "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.p";
  auto const global = "MATCH (n) WHERE n.p >= 0 RETURN n.id ORDER BY n.p";
  ExpectNoOrderBy(faker, by_label);
  ExpectNoOrderBy(faker, global);
  EXPECT_EQ(IntRows(faker, by_label), (Rows{{1}, {2}, {3}, {4}, {5}}));
  EXPECT_EQ(IntRows(faker, global), (Rows{{1}, {2}, {3}, {4}, {5}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L {p: 30}) RETURN n.id"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L {p: 99}) RETURN n.id"), (Rows{}));
}

// A key-changing write still open in another session leaves main's row where it was.
TEST_F(VersioningIndexTest, StreamedScansKeepMainsRowForAnUncommittedKeyChange) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("UNWIND range(1, 4) AS i CREATE (:L {id: i, p: i})");
  Fork();
  auto writers = SessionsOnBranch(1);
  auto &writer = *writers[0];

  auto const query = "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.p";
  ExpectPlanUses(faker, query, "ScanAllByLabelProperties");
  ExpectNoOrderBy(faker, query);

  writer.Interpret("BEGIN");
  writer.Interpret("MATCH (n:L {id: 3}) SET n.p = 60");
  writer.Interpret("MATCH (n:L {id: 4}) REMOVE n:L");
  EXPECT_EQ(IntRows(faker, query), (Rows{{1}, {2}, {3}, {4}})) << "another session's open writes are invisible";
  writer.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker, query), (Rows{{1}, {2}, {3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L {p: 60}) RETURN n.id"), (Rows{{3}}));
}

// A deleted fork vertex's stub keeps its entries in the diff engine's index; the scans must not yield it.
TEST_F(VersioningIndexTest, StreamedScansDropTheStubOfADeletedVertex) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE GLOBAL INDEX ON :(p)");
  faker.Interpret("UNWIND range(1, 4) AS i CREATE (:L {id: i, p: i})");
  Fork();
  faker.Interpret("MATCH (n:L {id: 2}) SET n.p = 20, n:K");
  faker.Interpret("MATCH (n:L {id: 2}) DETACH DELETE n");

  auto const by_label = "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.p";
  auto const global = "MATCH (n) WHERE n.p >= 0 RETURN n.id ORDER BY n.p";
  ExpectNoOrderBy(faker, by_label);
  ExpectNoOrderBy(faker, global);
  EXPECT_EQ(IntRows(faker, by_label), (Rows{{1}, {3}, {4}}));
  EXPECT_EQ(IntRows(faker, global), (Rows{{1}, {3}, {4}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L) RETURN count(n)"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:K) RETURN count(n)"), (Rows{{0}}));
}

// LIMIT stops the merge early: a scan pulls only the rows it yields plus one look-ahead per stream, however large
// main's index is.
TEST_F(VersioningIndexTest, StreamedScansPullOnlyWhatALimitConsumes) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret(R"(CREATE INDEX ON :L(p) WITH CONFIG {"order": "DESC"})");
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE GLOBAL INDEX ON :(p)");
  faker.Interpret("UNWIND range(1, 300) AS i CREATE (:L {id: i, p: i})");
  Fork();
  faker.Interpret("MATCH (n:L {id: 200}) SET n.p = 500");
  faker.Interpret("MATCH (n:L {id: 250}) SET n.p = 0");
  // A diff-index hit for the unkeyed label scan; it has no p, so no other scan sees it.
  faker.Interpret("CREATE (:L {id: 1000})");

  faker.Interpret("BEGIN");
  faker.Interpret("RETURN 1");
  StreamProbe probe(faker, "L", "p");
  auto dba = probe.Dba();
  auto const view = memgraph::storage::View::OLD;
  std::array const paths{memgraph::storage::PropertyPath{probe.property()}};
  std::array const ranges{memgraph::storage::PropertyValueRange::IsNotNull()};

  auto keyed = dba.Vertices(view, probe.label(), paths, ranges);
  EXPECT_EQ(probe.Ids(keyed, 3), (std::vector<int64_t>{250, 1, 2}));
  EXPECT_LE(keyed.BranchSourceRows(), 12U) << "an ordered merge pulls a few rows, not the 300";

  auto descending = dba.Vertices(view, probe.label(), paths, ranges, memgraph::storage::IndexOrder::DESC);
  EXPECT_EQ(probe.Ids(descending, 1), (std::vector<int64_t>{200}));
  EXPECT_LE(descending.BranchSourceRows(), 8U);

  auto global = dba.Vertices(view, probe.property());
  EXPECT_EQ(probe.Ids(global, 1), (std::vector<int64_t>{250}));
  EXPECT_LE(global.BranchSourceRows(), 8U);

  auto labelled = dba.Vertices(view, probe.label());
  EXPECT_EQ(probe.Ids(labelled, 1).size(), 1U);
  // An unkeyed scan pulls the diff only once main's rows are exhausted; begin() and the step past the first row take
  // two fork rows.
  EXPECT_LE(labelled.BranchSourceRows(), 2U) << "an unordered scan yields main's rows first without touching the diff";
  faker.Interpret("ROLLBACK");

  EXPECT_EQ(IntRows(faker, "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.p LIMIT 2"), (Rows{{250}, {1}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L) WHERE n.p >= 0 RETURN n.id ORDER BY n.p DESC LIMIT 2"), (Rows{{200}, {300}}));
}

// A scan under way sees the rows of its start whatever other sessions commit while it steps: each row once, none lost
// to a key that moves ahead of or behind the cursor.
TEST_F(VersioningIndexTest, StreamedScanInFlightIsStableWhileAnotherSessionCommitsKeyChanges) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE GLOBAL INDEX ON :(p)");
  faker.Interpret("UNWIND range(1, 12) AS i CREATE (:L {id: i, p: i})");
  Fork();
  auto writers = SessionsOnBranch(1);
  auto &writer = *writers[0];
  writer.Interpret("MATCH (n:L {id: 4}) SET n.p = 40");  // before the scan opens: a diff-engine row

  faker.Interpret("BEGIN");
  faker.Interpret("RETURN 1");
  StreamProbe probe(faker, "L", "p");
  auto dba = probe.Dba();
  std::array const paths{memgraph::storage::PropertyPath{probe.property()}};
  std::array const ranges{memgraph::storage::PropertyValueRange::IsNotNull()};
  auto const view = memgraph::storage::View::OLD;

  auto reference = dba.Vertices(view, probe.label(), paths, ranges);
  auto const expected = probe.Ids(reference);
  ASSERT_EQ(expected, (std::vector<int64_t>{1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 4}));

  auto keyed = dba.Vertices(view, probe.label(), paths, ranges);
  auto const seen = probe.Ids(keyed, std::numeric_limits<size_t>::max(), 2, [&] {
    writer.Interpret("MATCH (n:L {id: 11}) SET n.p = -1");  // ahead of the cursor, to the front
    writer.Interpret("MATCH (n:L {id: 1}) SET n.p = 500");  // behind the cursor, to the back
    writer.Interpret("MATCH (n:L {id: 5}) REMOVE n:L");
    writer.Interpret("MATCH (n:L {id: 6}) DETACH DELETE n");
    writer.Interpret("MATCH (n:L {id: 4}) SET n.p = 4");
  });
  EXPECT_EQ(seen, expected);

  auto global = dba.Vertices(view, probe.property());
  auto const seen_global = probe.Ids(
      global, std::numeric_limits<size_t>::max(), 3, [&] { writer.Interpret("MATCH (n:L {id: 8}) SET n.p = 800"); });
  EXPECT_EQ(seen_global.size(), 12U);
  EXPECT_EQ(std::set<int64_t>(seen_global.begin(), seen_global.end()).size(), 12U);
  faker.Interpret("ROLLBACK");
}

// After each random write, some rolled back, the label, label-property (plain and composite, both orders, with ranges)
// and global-property scans must equal the same questions asked of a full scan.
TEST_F(VersioningIndexTest, StreamedScansEqualFullScansAfterRandomKeyChanges) {
  faker.Interpret("CREATE INDEX ON :L(a)");
  faker.Interpret(R"(CREATE INDEX ON :L(a) WITH CONFIG {"order": "DESC"})");
  faker.Interpret("CREATE INDEX ON :L(a, b)");
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE GLOBAL INDEX ON :(a)");
  faker.Interpret("UNWIND range(0, 15) AS i CREATE (:L {id: i, a: i % 6, b: i % 4})");
  faker.Interpret("UNWIND range(16, 21) AS i CREATE (:M {id: i, a: i % 6})");
  Fork();

  struct Check {
    std::string indexed;
    std::string reference;
    std::string_view op;
  };

  auto const sorted_ids = [](Rows rows) {
    std::ranges::sort(rows);
    return rows;
  };
  std::vector<Check> const checks{
      {"MATCH (n:L) WHERE n.a >= 2 AND n.a <= 4 RETURN n.a ORDER BY n.a",
       "MATCH (n) WHERE 'L' IN labels(n) AND n.a >= 2 AND n.a <= 4 RETURN n.a ORDER BY n.a",
       "ScanAllByLabelProperties"},
      {"MATCH (n:L) WHERE n.a >= 0 RETURN n.a ORDER BY n.a DESC",
       "MATCH (n) WHERE 'L' IN labels(n) AND n.a >= 0 RETURN n.a ORDER BY n.a DESC",
       "ScanAllByLabelProperties"},
      {"MATCH (n:L) WHERE n.a = 3 RETURN n.id ORDER BY n.id",
       "MATCH (n) WHERE 'L' IN labels(n) AND n.a = 3 RETURN n.id ORDER BY n.id",
       "ScanAllByLabelProperties"},
      {"MATCH (n:L) WHERE n.a >= 0 AND n.b >= 0 RETURN n.a, n.b ORDER BY n.a, n.b",
       "MATCH (n) WHERE 'L' IN labels(n) AND n.a >= 0 AND n.b >= 0 RETURN n.a, n.b ORDER BY n.a, n.b",
       "ScanAllByLabelProperties"},
      {"MATCH (n:L) WHERE n.a = 1 RETURN n.id, coalesce(n.b, -1) ORDER BY n.id",
       "MATCH (n) WHERE 'L' IN labels(n) AND n.a = 1 RETURN n.id, coalesce(n.b, -1) ORDER BY n.id",
       "ScanAllByLabelProperties"},
      {"MATCH (n) WHERE n.a >= 1 RETURN n.a ORDER BY n.a",
       "MATCH (n) WHERE n.a IS NOT NULL AND n.a >= 1 RETURN n.a ORDER BY n.a",
       "ScanAllByVertexProperty"},
      {"MATCH (n:L) RETURN n.id ORDER BY n.id",
       "MATCH (n) WHERE 'L' IN labels(n) RETURN n.id ORDER BY n.id",
       "ScanAllByLabel"},
  };
  for (auto const &check : checks) ExpectPlanUses(faker, check.indexed, check.op);
  EXPECT_FALSE(PlanHasOperator(ExplainText(faker, checks[0].indexed), "OrderBy")) << "the index order is main's";

  auto const expect_equal = [&](std::string const &where) {
    for (auto const &check : checks) {
      EXPECT_EQ(IntRows(faker, check.indexed), IntRows(faker, check.reference)) << where << ": " << check.indexed;
    }
    EXPECT_EQ(sorted_ids(IntRows(faker, "MATCH (n:L) WHERE n.a >= 0 RETURN n.id")),
              sorted_ids(IntRows(faker, "MATCH (n) WHERE 'L' IN labels(n) AND n.a >= 0 RETURN n.id")))
        << where;
  };

  expect_equal("at the fork");
  for (uint32_t seed = 1; seed <= 10; ++seed) {
    std::mt19937 rng(seed);
    for (int step = 0; step < 14; ++step) {
      auto const pick = [&](int bound) { return static_cast<int>(rng() % static_cast<uint32_t>(bound)); };
      auto const id = std::to_string(pick(26));
      auto const match = "MATCH (n {id: " + id + "}) ";
      std::string statement;
      switch (pick(10)) {
        case 0:
          statement = match + "SET n.a = " + std::to_string(pick(7));
          break;
        case 1:
          statement = match + "SET n.b = " + std::to_string(pick(5));
          break;
        case 2:
          statement = match + "REMOVE n.a";
          break;
        case 3:
          statement = match + "REMOVE n.b";
          break;
        case 4:
          statement = match + "SET n:L";
          break;
        case 5:
          statement = match + "REMOVE n:L";
          break;
        case 6:
          statement = match + "SET n += {a: " + std::to_string(pick(7)) + ", b: " + std::to_string(pick(5)) + "}";
          break;
        case 7:
          statement = match + "DETACH DELETE n";
          break;
        case 8:
          statement = "CREATE (:L {id: " + std::to_string(100 + seed * 20 + step) + ", a: " + std::to_string(pick(7)) +
                      ", b: " + std::to_string(pick(5)) + "})";
          break;
        default:
          statement = match + "SET n.a = n.a";
          break;
      }
      if (pick(5) == 0) {
        faker.Interpret("BEGIN");
        faker.Interpret(statement);
        faker.Interpret("ROLLBACK");
      } else {
        faker.Interpret(statement);
      }
      expect_equal("seed " + std::to_string(seed) + " after `" + statement + "`");
    }
    faker.Interpret("BEGIN");
    expect_equal("seed " + std::to_string(seed) + " inside one transaction");
    faker.Interpret("COMMIT");
  }
}

TEST_F(VersioningIndexTest, BranchEdgeTypeIndexScanReflectsBranchMutations) {
  faker.Interpret("CREATE EDGE INDEX ON :KNOWS");
  faker.Interpret(
      "CREATE (a:N {n:'a'}), (b:N {n:'b'}), (c:N {n:'c'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), "
      "(a)-[:LIKES]->(c)");
  Fork();

  faker.Interpret("MATCH (:N {n:'a'})-[r:KNOWS]->(:N {n:'b'}) DELETE r");
  faker.Interpret("MATCH (c:N {n:'c'}), (a:N {n:'a'}) CREATE (c)-[:KNOWS]->(a)");
  faker.Interpret("MATCH (b:N {n:'b'}) DETACH DELETE b");

  const std::string query = "MATCH (x)-[r:KNOWS]->(y) RETURN x.n + '->' + y.n AS e ORDER BY e";
  ExpectPlanUses(faker, query, "ScanAllByEdgeType");
  EXPECT_EQ(StringRows(faker, query), (Strings{"a->c", "c->a"}));
  UseMain();
  EXPECT_EQ(StringRows(faker, query), (Strings{"a->b", "a->c", "b->c"}));
}

// The planner drops ORDER BY for an edge-property index scan, so the merge alone must yield ascending values.
TEST_F(VersioningIndexTest, BranchEdgePropertyIndexScansAreOrderedOverBranchMutations) {
  faker.Interpret("CREATE EDGE INDEX ON :KNOWS(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  // Creation order differs from w order.
  faker.Interpret(
      "CREATE (a:N {n:'a'}), (b:N {n:'b'}), (c:N {n:'c'}), (e:N {n:'e'}), (a)-[:KNOWS {w: 50}]->(b), "
      "(b)-[:KNOWS {w: 20}]->(c), (a)-[:KNOWS {w: 40}]->(c), (c)-[:KNOWS {w: 10}]->(e), (a)-[:LIKES {w: 30}]->(e)");
  Fork();

  faker.Interpret("MATCH (:N {n:'b'})-[r:KNOWS]->(:N {n:'c'}) SET r.w = 99");
  faker.Interpret("MATCH (:N {n:'a'})-[r:KNOWS]->(:N {n:'c'}) DELETE r");
  faker.Interpret("MATCH (e:N {n:'e'}), (a:N {n:'a'}) CREATE (e)-[:KNOWS {w: 25}]->(a), (e)-[:FOLLOWS {w: 5}]->(a)");
  faker.Interpret("MATCH (:N {n:'a'})-[r:LIKES]->() SET r.w = 60");

  const std::string typed = "MATCH ()-[r:KNOWS]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w";
  const std::string global = "MATCH ()-[r]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w";
  ExpectPlanUses(faker, typed, "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, global, "ScanAllByEdgeProperty");
  EXPECT_EQ(IntRows(faker, typed), (Rows{{10}, {25}, {50}, {99}}));
  EXPECT_EQ(IntRows(faker, global), (Rows{{5}, {10}, {25}, {50}, {60}, {99}}));
  UseMain();
  EXPECT_EQ(IntRows(faker, global), (Rows{{10}, {20}, {30}, {40}, {50}}));
}

// A fork edge is found at its branch value only; an unrelated property write keeps the fork value findable.
TEST_F(VersioningIndexTest, BranchEdgePropertyIndexScansReadForkEdgesAtTheirBranchValue) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret(
      "CREATE (:A {id:1})-[:R {w:1}]->(:B {id:2}), (:A {id:3})-[:R {w:5}]->(:B {id:4}), "
      "(:A {id:5})-[:R {w:7}]->(:B {id:6}), (:A {id:7})-[:Q {w:3}]->(:B {id:8})");
  Fork();

  faker.Interpret("MATCH (:A {id:1})-[r:R]->() SET r.w = 9");
  faker.Interpret("MATCH (:A {id:3})-[r:R]->() SET r.x = 1");
  faker.Interpret("MATCH (:A {id:5})-[r:R]->() REMOVE r.w");
  faker.Interpret("MATCH (:A {id:7})-[r:Q]->() SET r.w = 2");  // another type: the global index only

  auto const typed = [&](const std::string &where) {
    return IntRows(faker, "MATCH ()-[r:R]->() WHERE " + where + " RETURN r.w ORDER BY r.w");
  };
  ExpectPlanUses(faker, "MATCH ()-[r:R]->() WHERE r.w = 1 RETURN r.w", "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, "MATCH ()-[r]->() WHERE r.w IS NOT NULL RETURN r.w", "ScanAllByEdgeProperty");
  EXPECT_EQ(typed("r.w = 1"), Rows{});
  EXPECT_EQ(typed("r.w = 9"), (Rows{{9}}));
  EXPECT_EQ(typed("r.w = 5"), (Rows{{5}}));
  EXPECT_EQ(typed("r.w = 7"), Rows{});
  EXPECT_EQ(typed("r.w > 0"), (Rows{{5}, {9}}));
  EXPECT_EQ(typed("r.w >= 5 AND r.w < 9"), (Rows{{5}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w IS NOT NULL RETURN r.w ORDER BY r.w"), (Rows{{2}, {5}, {9}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w = 3 RETURN count(r)"), (Rows{{0}}));
}

// A fork edge that gains the property is found only through the branch's record of it, through rollback and delete.
TEST_F(VersioningIndexTest, BranchEdgePropertyIndexScanFollowsGainedPropertyThroughRollbackAndDelete) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret(
      "CREATE (:A {id:1})-[:R {w:4}]->(:B {id:2}), (:A {id:3})-[:R {z:1}]->(:B {id:4}), "
      "(:A {id:5})-[:Q {z:1}]->(:B {id:6})");
  Fork();

  auto const all_typed = [&] { return IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w"); };
  auto const all_global = [&] {
    return IntRows(faker, "MATCH ()-[r]->() WHERE r.w IS NOT NULL RETURN r.w ORDER BY r.w");
  };
  ExpectPlanUses(faker, "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.w ORDER BY r.w", "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, "MATCH ()-[r]->() WHERE r.w IS NOT NULL RETURN r.w ORDER BY r.w", "ScanAllByEdgeProperty");
  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (:A {id:3})-[r:R]->() SET r.w = 3");
  faker.Interpret("MATCH (:A {id:5})-[r:Q]->() SET r.w = 6");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 3 RETURN count(r)"), (Rows{{1}}));
  EXPECT_EQ(all_typed(), (Rows{{3}, {4}}));
  EXPECT_EQ(all_global(), (Rows{{3}, {4}, {6}}));
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(all_typed(), (Rows{{4}}));
  EXPECT_EQ(all_global(), (Rows{{4}}));

  faker.Interpret("MATCH (:A {id:3})-[r:R]->() SET r.w = 3");
  faker.Interpret("MATCH (:A {id:5})-[r:Q]->() SET r.w = 6");
  EXPECT_EQ(all_global(), (Rows{{3}, {4}, {6}}));
  faker.Interpret("MATCH (:A {id:3})-[r:R]->() SET r.w = 8");
  EXPECT_EQ(all_typed(), (Rows{{4}, {8}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w >= 4 AND r.w < 8 RETURN r.w"), (Rows{{4}}));
  faker.Interpret("MATCH (:A {id:3})-[r:R]->() DELETE r");
  EXPECT_EQ(all_typed(), (Rows{{4}}));
  EXPECT_EQ(all_global(), (Rows{{4}, {6}}));

  UseMain();
  EXPECT_EQ(all_global(), (Rows{{4}}));
}

// An edge index main drops after the fork is not visible at it: the planner still picks the mirrored index and the
// branch answers, in order, from a scan.
TEST_F(VersioningIndexTest, BranchOrderedEdgePropertyScanSurvivesMainDroppingIndex) {
  struct Case {
    const char *ddl;
    const char *drop;
    const char *match;
    const char *op;
  };

  static constexpr Case kCases[] = {
      {"CREATE EDGE INDEX ON :T(p)", "DROP EDGE INDEX ON :T(p)", "()-[r:T]->()", "ScanAllByEdgeTypeProperty"},
      {"CREATE GLOBAL EDGE INDEX ON :(p)", "DROP GLOBAL EDGE INDEX ON :(p)", "()-[r]->()", "ScanAllByEdgeProperty"},
  };
  int branch = 0;
  for (const auto &c : kCases) {
    SCOPED_TRACE(c.ddl);
    UseMain();
    faker.Interpret("MATCH (n) DETACH DELETE n");
    faker.Interpret(c.ddl);
    // Creation order differs from p order.
    faker.Interpret("CREATE (:V)-[:T {p: 3}]->(:V), (:V)-[:T {p: 1}]->(:V), (:V)-[:T {p: 2}]->(:V)");
    const auto name = "d" + std::to_string(branch++);
    Fork(name);
    UseMain();
    faker.Interpret(c.drop);
    Use(faker, name);

    const std::string presence = std::string{"MATCH "} + c.match + " WHERE r.p IS NOT NULL RETURN r.p ORDER BY r.p";
    const std::string range = std::string{"MATCH "} + c.match + " WHERE r.p > 1 RETURN r.p ORDER BY r.p";
    ExpectPlanUses(faker, range, c.op);
    EXPECT_EQ(IntRows(faker, presence), (Rows{{1}, {2}, {3}}));
    EXPECT_EQ(IntRows(faker, range), (Rows{{2}, {3}}));
  }
}

namespace {
constexpr auto kSeedEdges =
    "CREATE (a:N {n:'a'}), (b:N {n:'b'}), (c:N {n:'c'}),"
    " (a)-[:R {w:1, t:'e1'}]->(b), (b)-[:R {w:2, t:'e2'}]->(c), (a)-[:Q {w:3, t:'e3'}]->(c)";
constexpr auto kEdgeMainDrift =
    "MATCH ()-[e {t:'e1'}]->() DELETE e;"
    "MATCH ()-[e {t:'e2'}]->() SET e.w = 20;"
    "MATCH (a:N {n:'a'}), (c:N {n:'c'}) CREATE (a)-[:R {w:4, t:'e4'}]->(c);"
    "MATCH ()-[e {t:'e3'}]->() DELETE e";
constexpr auto kEdgeBranchWrites =
    "MATCH (c:N {n:'c'}), (a:N {n:'a'}) CREATE (c)-[:R {w:7, t:'bn'}]->(a);"
    "MATCH ()-[e {t:'e1'}]->() SET e.w = 10";

struct LateEdgeIndexCase {
  const char *name;
  bool rebuilt;
  bool branch_writes_before;
  TagP expected_r;
  std::vector<int64_t> probe_w;
};

const std::vector<LateEdgeIndexCase> kLateEdgeIndexCases = {
    {"LateMainDrift", false, false, {{"e1", 1}, {"e2", 2}}, {1, 2, 4, 20}},
    {"LateMainDriftAndBranchWritesBefore", false, true, {{"bn", 7}, {"e1", 10}, {"e2", 2}}, {1, 2, 4, 7, 10, 20}},
    {"RebuiltMainDrift", true, false, {{"e1", 1}, {"e2", 2}}, {1, 2, 4, 20}},
};

class VersioningLateEdgeIndexTest : public VersioningIndexTest,
                                    public ::testing::WithParamInterface<LateEdgeIndexCase> {};
}  // namespace

TEST_P(VersioningLateEdgeIndexTest, BranchReadsForkStateAndOwnWrites) {
  const auto &c = GetParam();
  if (c.rebuilt) {
    faker.Interpret("CREATE EDGE INDEX ON :R");
    faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  }
  faker.Interpret(kSeedEdges);
  Fork();
  if (c.branch_writes_before) RunStatements(faker, kEdgeBranchWrites);
  UseMain();
  RunStatements(faker, kEdgeMainDrift);
  if (c.rebuilt) {
    faker.Interpret("DROP EDGE INDEX ON :R");
    faker.Interpret("DROP EDGE INDEX ON :R(w)");
  }
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  Use(faker, "b");

  const std::string type_idx = "MATCH ()-[e:R]->() RETURN e.t AS t ORDER BY t";
  const std::string type_scan = "MATCH ()-[e]->() WHERE type(e) = 'R' RETURN e.t AS t ORDER BY t";
  const std::string prop_idx = "MATCH ()-[e:R]->() WHERE e.w > 0 RETURN e.t AS t ORDER BY t";
  EXPECT_EQ(PlanHasOperator(ExplainText(faker, type_idx), "ScanAllByEdgeType"), c.rebuilt);
  EXPECT_EQ(PlanHasOperator(ExplainText(faker, prop_idx), "ScanAllByEdgeTypeProperty"), c.rebuilt);

  const auto want = Tags(c.expected_r);
  EXPECT_EQ(StringRows(faker, type_scan), want) << "the scan disagrees with the hand-derived state";
  EXPECT_EQ(StringRows(faker, type_idx), want);
  EXPECT_EQ(StringRows(faker, prop_idx), want);
  for (auto w : c.probe_w) {
    EXPECT_EQ(StringRows(faker, "MATCH ()-[e:R]->() WHERE e.w = " + std::to_string(w) + " RETURN e.t AS t ORDER BY t"),
              Tags(c.expected_r, w))
        << "w = " << w;
  }
}

INSTANTIATE_TEST_SUITE_P(Cases, VersioningLateEdgeIndexTest, ::testing::ValuesIn(kLateEdgeIndexCases),
                         [](const ::testing::TestParamInfo<LateEdgeIndexCase> &info) {
                           return std::string{info.param.name};
                         });

TEST_F(VersioningIndexTest, BranchRebuiltGlobalEdgePropertyIndexReadsForkState) {
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret(kSeedEdges);
  Fork();
  UseMain();
  RunStatements(faker, kEdgeMainDrift);
  faker.Interpret("DROP GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  Use(faker, "b");

  const std::string range = "MATCH ()-[e]->() WHERE e.w > 0 RETURN e.t ORDER BY e.w";
  ExpectPlanUses(faker, range, "ScanAllByEdgeProperty");
  EXPECT_EQ(StringRows(faker, range), (Strings{"e1", "e2", "e3"}));
  const std::vector<std::pair<int64_t, Strings>> probes = {{1, {"e1"}}, {2, {"e2"}}, {3, {"e3"}}, {4, {}}, {20, {}}};
  for (const auto &[w, want] : probes) {
    EXPECT_EQ(StringRows(faker, "MATCH ()-[e]->() WHERE e.w = " + std::to_string(w) + " RETURN e.t"), want)
        << "w = " << w;
  }
}

// Edge-index scans and a full-scan filter agree inside one long transaction and stay on its snapshot while another
// session commits on the branch and main commits more; they agree again after it.
TEST_F(VersioningIndexTest, EdgeIndexScansInOneTransactionStayStableWhileOthersCommit) {
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(1, 12) AS i CREATE (:N {id: i})-[:R {id: i, w: i % 4}]->(:M)");
  Fork();
  auto writers = SessionsOnBranch(1);
  auto &writer = *writers[0];
  InterpreterFaker main_session{&interpreter_context, db};
  interpreter_context.auth_checker = &faker.auth_checker;
  UseMain(main_session);

  auto const typed = "MATCH ()-[r:R]->() WHERE r.w = 2 RETURN r.id ORDER BY r.id";
  auto const global = "MATCH ()-[r]->() WHERE r.w = 2 RETURN r.id ORDER BY r.id";
  auto const by_type = "MATCH ()-[r:R]->() RETURN r.id ORDER BY r.id";
  auto const full_typed = "MATCH ()-[r]->() WHERE type(r) = 'R' AND r.w + 0 = 2 RETURN r.id ORDER BY r.id";
  auto const full_by_type = "MATCH ()-[r]->() WHERE type(r) = 'R' RETURN r.id ORDER BY r.id";
  ExpectPlanUses(faker, typed, "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, global, "ScanAllByEdgeProperty");
  ExpectPlanUses(faker, by_type, "ScanAllByEdgeType");
  auto const expect_scans_agree = [&] {
    EXPECT_EQ(IntRows(faker, typed), IntRows(faker, full_typed));
    EXPECT_EQ(IntRows(faker, global), IntRows(faker, full_typed));
    EXPECT_EQ(IntRows(faker, by_type), IntRows(faker, full_by_type));
  };

  faker.Interpret("BEGIN");
  auto const typed_before = IntRows(faker, typed);
  auto const by_type_before = IntRows(faker, by_type);
  ASSERT_EQ(typed_before, (Rows{{2}, {6}, {10}}));
  ASSERT_EQ(by_type_before.size(), 12U);
  expect_scans_agree();

  writer.Interpret("MATCH ()-[r:R]->() WHERE r.id <= 6 SET r.w = 2");
  writer.Interpret("MATCH ()-[r:R {id: 8}]->() DELETE r");
  writer.Interpret("MATCH ()-[r:R {id: 10}]->() REMOVE r.w");
  writer.Interpret("MATCH (n:N {id: 1}) CREATE (n)-[:R {id: 100, w: 2}]->(:M)");
  main_session.Interpret("MATCH ()-[r:R]->() WHERE r.id > 6 SET r.w = 2");
  main_session.Interpret("MATCH (n:N {id: 1}) CREATE (n)-[:R {id: 200, w: 2}]->(:M)");

  EXPECT_EQ(IntRows(faker, typed), typed_before);
  EXPECT_EQ(IntRows(faker, by_type), by_type_before);
  expect_scans_agree();
  faker.Interpret("COMMIT");

  expect_scans_agree();
  EXPECT_EQ(IntRows(faker, typed), (Rows{{1}, {2}, {3}, {4}, {5}, {6}, {100}}));
  EXPECT_EQ(IntRows(faker, by_type).size(), 12U) << "main's later edges and property writes never reach the branch";
}

// Bounded stress over the edge indexes: one branch writer moves a property flag from one fork edge to the next in a
// single transaction, main keeps flagging every edge, and readers must see, in each transaction, exactly one flagged
// edge from the typed scan, the global scan and a full-scan filter, all the same edge.
TEST_F(VersioningIndexTest, ConcurrentEdgeIndexScansSeeExactlyOneHolderOfAMovingProperty) {
  constexpr int kEdges = 10;
  constexpr int kRounds = 150;
  faker.Interpret("CREATE EDGE INDEX ON :R(flag)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(flag)");
  faker.Interpret("UNWIND range(0, 9) AS i CREATE (:N)-[:R {id: i, flag: 0}]->(:M)");
  faker.Interpret("MATCH ()-[r:R {id: 0}]->() SET r.flag = 1");
  Fork();
  auto sessions = SessionsOnBranch(3);
  InterpreterFaker main_session{&interpreter_context, db};
  interpreter_context.auth_checker = &faker.auth_checker;
  UseMain(main_session);
  std::atomic<int> writers_left{2};

  {
    std::vector<std::jthread> threads;
    threads.emplace_back([&session = *sessions[0], &writers_left] {
      for (int round = 0; round < kRounds; ++round) {
        session.Interpret("BEGIN");
        session.Interpret("MATCH ()-[r:R {id: " + std::to_string(round % kEdges) + "}]->() SET r.flag = 0");
        session.Interpret("MATCH ()-[r:R {id: " + std::to_string((round + 1) % kEdges) + "}]->() SET r.flag = 1");
        session.Interpret("COMMIT");
      }
      --writers_left;
    });
    threads.emplace_back([&session = main_session, &writers_left] {
      for (int round = 0; round < kRounds; ++round) session.Interpret("MATCH ()-[r:R]->() SET r.flag = 1");
      --writers_left;
    });
    for (int r = 1; r < 3; ++r) {
      threads.emplace_back([&session = *sessions[r], &writers_left] {
        do {
          session.Interpret("BEGIN");
          auto const typed = IntRows(session, "MATCH ()-[r:R]->() WHERE r.flag = 1 RETURN r.id");
          auto const global = IntRows(session, "MATCH ()-[r]->() WHERE r.flag = 1 RETURN r.id");
          auto const full = IntRows(session, "MATCH ()-[r]->() WHERE r.flag + 0 = 1 RETURN r.id");
          auto const again = IntRows(session, "MATCH ()-[r:R]->() WHERE r.flag = 1 RETURN r.id");
          session.Interpret("COMMIT");
          ASSERT_EQ(typed.size(), 1U) << "the flag moved atomically, main's copies never leak";
          ASSERT_EQ(global, typed);
          ASSERT_EQ(full, typed);
          ASSERT_EQ(again, typed) << "the snapshot moved inside one transaction";
        } while (writers_left.load() > 0);
      });
    }
  }

  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.flag = 1 RETURN r.id"), (Rows{{kRounds % kEdges}}));
  EXPECT_EQ(IntRows(main_session, "MATCH ()-[r:R]->() WHERE r.flag = 1 RETURN count(r)"), (Rows{{kEdges}}));
}

namespace {

struct ModelEdge {
  int64_t from;
  int64_t to;
  std::string type;
  std::optional<int64_t> w;
};

struct EdgeWorld {
  std::set<int64_t> vertices;
  std::map<int64_t, ModelEdge> edges;
};

Rows EdgeIdRows(EdgeWorld const &world, auto &&keep) {
  Rows rows;
  for (auto const &[id, edge] : world.edges) {
    if (keep(edge)) rows.push_back({id});
  }
  return rows;
}

// One seeded random write on the branch (sometimes rolled back), mirrored into `world`.
void RandomEdgeWrite(InterpreterFaker &session, std::mt19937 &rng, EdgeWorld &world, int64_t &next_native_id) {
  auto const pick = [&](int64_t bound) { return static_cast<int64_t>(rng() % static_cast<uint64_t>(bound)); };
  auto const pick_edge = [&]() -> std::optional<int64_t> {
    if (world.edges.empty()) return std::nullopt;
    auto it = world.edges.begin();
    std::advance(it, pick(static_cast<int64_t>(world.edges.size())));
    return it->first;
  };
  bool const rolled_back = pick(6) == 0;
  auto const run = [&](std::string const &statement) {
    if (!rolled_back) {
      session.Interpret(statement);
      return;
    }
    session.Interpret("BEGIN");
    session.Interpret(statement);
    session.Interpret("ROLLBACK");
  };
  auto const match_edge = [](int64_t id) { return "MATCH ()-[r {id: " + std::to_string(id) + "}]->() "; };

  auto const kind = pick(8);
  auto const id = pick_edge();
  if (kind >= 6 || !id) {
    if (world.vertices.size() < 2) return;
    auto pick_vertex = [&] {
      auto it = world.vertices.begin();
      std::advance(it, pick(static_cast<int64_t>(world.vertices.size())));
      return *it;
    };
    auto const from = pick_vertex();
    auto to = pick_vertex();
    while (to == from) to = pick_vertex();
    auto const native = next_native_id++;
    auto const type = pick(2) == 0 ? "R" : "Q";
    auto const w = pick(3) == 0 ? std::nullopt : std::optional<int64_t>{pick(8)};
    run("MATCH (a {id: " + std::to_string(from) + "}), (b {id: " + std::to_string(to) + "}) CREATE (a)-[:" + type +
        " {id: " + std::to_string(native) + (w ? ", w: " + std::to_string(*w) : "") + "}]->(b)");
    if (!rolled_back) world.edges[native] = ModelEdge{from, to, type, w};
    return;
  }
  auto &edge = world.edges.at(*id);
  switch (kind) {
    case 0:
    case 1: {
      auto const w = pick(8);
      run(match_edge(*id) + "SET r.w = " + std::to_string(w));
      if (!rolled_back) edge.w = w;
      break;
    }
    case 2:
      run(match_edge(*id) + "REMOVE r.w");
      if (!rolled_back) edge.w.reset();
      break;
    case 3:
      run(match_edge(*id) + "DELETE r");
      if (!rolled_back) world.edges.erase(*id);
      break;
    case 4: {
      auto const vertex = pick(2) == 0 ? edge.from : edge.to;
      run("MATCH (n {id: " + std::to_string(vertex) + "}) DETACH DELETE n");
      if (!rolled_back) {
        world.vertices.erase(vertex);
        std::erase_if(world.edges,
                      [&](auto const &entry) { return entry.second.from == vertex || entry.second.to == vertex; });
      }
      break;
    }
    default:
      run(match_edge(*id) + "SET r.z = 1");  // another property: no scan on `w` changes
      break;
  }
}

}  // namespace

// After every random write (some rolled back) the edge-type, typed and global property scans (in index order) and a
// full scan each equal the model, and again inside one explicit transaction.
TEST_F(VersioningIndexTest, BranchEdgeScansEqualAModelOfForkAndNativeWrites) {
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  EdgeWorld world;
  constexpr int64_t kVertices = 8;
  for (int64_t id = 0; id < kVertices; ++id) {
    faker.Interpret("CREATE (:N {id: " + std::to_string(id) + "})");
    world.vertices.insert(id);
  }
  for (int64_t id = 0; id < 18; ++id) {
    ModelEdge edge{id % kVertices,
                   (id * 3 + 1) % kVertices,
                   id % 3 == 0 ? "Q" : "R",
                   id % 4 == 0 ? std::nullopt : std::optional<int64_t>{id % 5}};
    faker.Interpret("MATCH (a:N {id: " + std::to_string(edge.from) + "}), (b:N {id: " + std::to_string(edge.to) +
                    "}) CREATE (a)-[:" + edge.type + " {id: " + std::to_string(id) +
                    (edge.w ? ", w: " + std::to_string(*edge.w) : "") + "}]->(b)");
    world.edges[id] = edge;
  }
  Fork();
  ExpectPlanUses(faker, "MATCH ()-[r:R]->() RETURN r.id", "ScanAllByEdgeType");
  ExpectPlanUses(faker, "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id", "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id", "ScanAllByEdgeProperty");

  auto const expect_model = [&](std::string const &where) {
    auto const is_r = [](ModelEdge const &e) { return e.type == "R"; };
    auto const has_w = [](ModelEdge const &e) { return e.w.has_value(); };
    auto const everything = [](ModelEdge const &) { return true; };
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.id ORDER BY r.id"), EdgeIdRows(world, is_r)) << where;
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.id"),
              EdgeIdRows(world, [&](auto const &e) { return is_r(e) && has_w(e); }))
        << where;
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w >= 2 AND r.w < 6 RETURN r.id ORDER BY r.id"),
              EdgeIdRows(world, [&](auto const &e) { return is_r(e) && has_w(e) && *e.w >= 2 && *e.w < 6; }))
        << where;
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.id"), EdgeIdRows(world, has_w))
        << where;
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() RETURN r.id ORDER BY r.id"), EdgeIdRows(world, everything)) << where;
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE type(r) = 'R' RETURN r.id ORDER BY r.id"), EdgeIdRows(world, is_r))
        << where;
    Rows typed_by_w;
    Rows global_by_w;
    for (auto const &[id, e] : world.edges) {
      if (!has_w(e)) continue;
      global_by_w.push_back({*e.w});
      if (is_r(e)) typed_by_w.push_back({*e.w});
    }
    std::ranges::sort(typed_by_w);
    std::ranges::sort(global_by_w);
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.w ORDER BY r.w"), typed_by_w) << where;
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.w ORDER BY r.w"), global_by_w) << where;
  };

  expect_model("at the fork");
  int64_t next_native_id = 100;
  for (uint32_t seed = 1; seed <= 5; ++seed) {
    std::mt19937 rng(seed);
    for (int step = 0; step < 30; ++step) {
      RandomEdgeWrite(faker, rng, world, next_native_id);
      expect_model("seed " + std::to_string(seed) + " step " + std::to_string(step));
    }
    faker.Interpret("BEGIN");
    expect_model("seed " + std::to_string(seed) + " inside one transaction");
    faker.Interpret("COMMIT");
  }
}

// The scans trust the change records: a write to a fork edge's stub that nothing recorded is not read, and the diff
// engine's property-index hit on a stub with no recorded fork edge is dropped, never yielded as an edge.
TEST_F(VersioningIndexTest, BranchEdgeScansSkipUnrecordedStubsAndNeverYieldAStubHit) {
  namespace ms = memgraph::storage;
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(0, 3) AS i CREATE (:N {id: i})-[:R {id: i, w: i}]->(:M)");
  auto const ids = IntRows(faker, "MATCH (n)-[r:R {id: 1}]->() RETURN id(r), id(n)");
  ASSERT_EQ(ids.size(), 1U);
  Fork();
  faker.Interpret("MATCH (a:N {id: 0}), (b:N {id: 1}) CREATE (a)-[:R {id: 50, w: 50}]->(b)");

  faker.Interpret("BEGIN");
  faker.Interpret("RETURN 1");
  auto *ctx = faker.interpreter.current_db_.branch_context();
  ASSERT_NE(ctx, nullptr);
  auto const gid = ms::Gid::FromUint(static_cast<uint64_t>(ids[0][0]));
  auto fork = ctx->FindForkEdge(gid, ms::Gid::FromUint(static_cast<uint64_t>(ids[0][1])));
  ASSERT_TRUE(fork.has_value());
  faker.Interpret("ROLLBACK");

  // Committed in its own diff transaction, so every later scan of the diff index sees the stub's entries.
  {
    auto acc = ctx->diff_engine().Access(ms::StorageAccessType::WRITE);
    auto stub = ms::EdgeAccessor::Create(ms::EdgeRef{ctx->core()->edge_stub_index().GetOrCreate(gid.AsUint())},
                                         fork->EdgeType(),
                                         fork->from_vertex_,
                                         fork->to_vertex_,
                                         &ctx->diff_engine(),
                                         acc->GetTransaction(),
                                         ms::View::NEW);
    ASSERT_TRUE(stub.has_value());
    ASSERT_TRUE(stub->SetProperty(db->storage()->NameToProperty("w"), ms::PropertyValue(int64_t{99})).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ExpectPlanUses(faker, "MATCH ()-[r:R]->() WHERE r.w = 99 RETURN r.id", "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, "MATCH ()-[r]->() WHERE r.w = 99 RETURN r.id", "ScanAllByEdgeProperty");
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 99 RETURN r.id"), (Rows{}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w = 99 RETURN r.id"), (Rows{}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.id"),
            (Rows{{0}, {1}, {2}, {3}, {50}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.id"),
            (Rows{{0}, {1}, {2}, {3}, {50}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{5}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r {id: 1}]->() RETURN r.w"), (Rows{{1}}));
}

// Edge index scans skip the merge when nothing says the branch changed a hit, so the first write of each kind to a fork
// edge must already be recorded: on a fresh branch each write is the only one, and every scan must show it.
TEST_F(VersioningIndexTest, EdgeIndexScansNeverHideTheFirstStubWriteOfAKind) {
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(0, 3) AS i CREATE (:N {id: i})-[:R {id: i, w: i}]->(:M {id: i})");

  struct Case {
    std::string write;
    Rows type_rows;
    Rows typed_rows;
    Rows global_rows;
  };

  std::vector<Case> const cases{
      {"MATCH ()-[r {id: 1}]->() SET r.w = -5", {{0}, {1}, {2}, {3}}, {{0}, {2}, {3}}, {{0}, {2}, {3}}},
      {"MATCH ()-[r {id: 2}]->() REMOVE r.w", {{0}, {1}, {2}, {3}}, {{0}, {1}, {3}}, {{0}, {1}, {3}}},
      {"MATCH ()-[r {id: 3}]->() DELETE r", {{0}, {1}, {2}}, {{0}, {1}, {2}}, {{0}, {1}, {2}}},
      {"MATCH (m:M {id: 3}) DETACH DELETE m", {{0}, {1}, {2}}, {{0}, {1}, {2}}, {{0}, {1}, {2}}},
      {"MATCH (n:N {id: 0}) DETACH DELETE n", {{1}, {2}, {3}}, {{1}, {2}, {3}}, {{1}, {2}, {3}}},
      {"MATCH (a:N {id: 0}), (b:M {id: 1}) CREATE (a)-[:R {id: 9, w: 9}]->(b)",
       {{0}, {1}, {2}, {3}, {9}},
       {{0}, {1}, {2}, {3}, {9}},
       {{0}, {1}, {2}, {3}, {9}}},
  };
  ExpectPlanUses(faker, "MATCH ()-[r:R]->() RETURN r.id ORDER BY r.id", "ScanAllByEdgeType");
  ExpectPlanUses(faker, "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.id", "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.id", "ScanAllByEdgeProperty");
  for (size_t i = 0; i < cases.size(); ++i) {
    auto const &c = cases[i];
    Fork("b" + std::to_string(i));
    faker.Interpret(c.write);
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.id ORDER BY r.id"), c.type_rows) << c.write;
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.id"), c.typed_rows) << c.write;
    EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.id"), c.global_rows) << c.write;
  }
}

// Every row of the range comes from a stub (main's index holds none in it); the planner drops ORDER BY, so the merge
// alone orders them.
TEST_F(VersioningIndexTest, StreamedEdgeRangeScanYieldsStubsWithNoNativeEdgeOnTheBranch) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(0, 9) AS i CREATE (:N)-[:R {id: i, w: i}]->(:M)");
  Fork();
  faker.Interpret("MATCH ()-[r:R]->() WHERE r.id IN [7, 2, 5] SET r.w = r.id + 100");

  auto const typed = "MATCH ()-[r:R]->() WHERE r.w >= 100 RETURN r.id, r.w ORDER BY r.w";
  auto const global = "MATCH ()-[r]->() WHERE r.w >= 100 RETURN r.id, r.w ORDER BY r.w";
  ExpectPlanUses(faker, typed, "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, global, "ScanAllByEdgeProperty");
  EXPECT_EQ(IntRows(faker, typed), (Rows{{2, 102}, {5, 105}, {7, 107}}));
  EXPECT_EQ(IntRows(faker, global), (Rows{{2, 102}, {5, 105}, {7, 107}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w"),
            (Rows{{0}, {1}, {3}, {4}, {6}, {8}, {9}, {2}, {5}, {7}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w"),
            (Rows{{0}, {1}, {3}, {4}, {6}, {8}, {9}, {2}, {5}, {7}}));
}

// A fork edge set to another value and back is yielded once, at its fork position, by every scan.
TEST_F(VersioningIndexTest, StreamedEdgeScansYieldAKeySetBackToTheForkValueOnce) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(1, 5) AS i CREATE (:N)-[:R {id: i, w: i * 10}]->(:M)");
  Fork();
  faker.Interpret("MATCH ()-[r:R {id: 3}]->() SET r.w = 99");
  faker.Interpret("MATCH ()-[r:R {id: 3}]->() SET r.w = 30");

  auto const typed = "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w";
  auto const global = "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w";
  ExpectPlanUses(faker, typed, "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, global, "ScanAllByEdgeProperty");
  EXPECT_EQ(IntRows(faker, typed), (Rows{{1}, {2}, {3}, {4}, {5}}));
  EXPECT_EQ(IntRows(faker, global), (Rows{{1}, {2}, {3}, {4}, {5}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 30 RETURN r.id"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 99 RETURN r.id"), (Rows{}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN r.id ORDER BY r.id"), (Rows{{1}, {2}, {3}, {4}, {5}}));
}

// The diff engine's index still lists a deleted edge's stub, and the delete marker of an endpoint's DETACH DELETE is
// per edge: the scans must not yield the edge, and must keep its neighbours.
TEST_F(VersioningIndexTest, StreamedEdgeScansDropAnEdgeDeletedItselfOrWithAnEndpoint) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(1, 6) AS i CREATE (:N {id: i})-[:R {id: i, w: i}]->(:M {id: i})");
  Fork();
  faker.Interpret("MATCH ()-[r:R {id: 2}]->() SET r.w = 20");
  faker.Interpret("MATCH ()-[r:R {id: 2}]->() DELETE r");
  faker.Interpret("MATCH ()-[r:R {id: 4}]->() SET r.w = 40");
  faker.Interpret("MATCH (n:N {id: 4}) DETACH DELETE n");  // hides an edge that already has a stub in the diff index
  faker.Interpret("MATCH (m:M {id: 5}) DETACH DELETE m");  // hides an edge nothing else touched

  auto const typed = "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w";
  auto const global = "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w";
  auto const by_type = "MATCH ()-[r:R]->() RETURN r.id ORDER BY r.id";
  ExpectPlanUses(faker, typed, "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, global, "ScanAllByEdgeProperty");
  ExpectPlanUses(faker, by_type, "ScanAllByEdgeType");
  EXPECT_EQ(IntRows(faker, typed), (Rows{{1}, {3}, {6}}));
  EXPECT_EQ(IntRows(faker, global), (Rows{{1}, {3}, {6}}));
  EXPECT_EQ(IntRows(faker, by_type), (Rows{{1}, {3}, {6}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{3}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 40 RETURN count(r)"), (Rows{{0}}));
  UseMain();
  EXPECT_EQ(IntRows(faker, typed).size(), 6U);
}

// Branch-native edges below, between and above the fork's values, and one whose value changes after its creation, sit
// among the fork rows in key order.
TEST_F(VersioningIndexTest, StreamedEdgeScansMergeNativeEdgesAmongForkRows) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(1, 5) AS i CREATE (:N {id: i})-[:R {id: i, w: i * 10}]->(:M {id: i})");
  Fork();
  faker.Interpret("MATCH (a:N {id: 1}), (b:M {id: 1}) CREATE (a)-[:R {id: 100, w: 5}]->(b)");
  faker.Interpret("MATCH (a:N {id: 2}), (b:M {id: 3}) CREATE (a)-[:R {id: 101, w: 25}]->(b)");
  faker.Interpret("MATCH (a:N {id: 3}), (b:M {id: 4}) CREATE (a)-[:R {id: 102, w: 70}]->(b)");
  faker.Interpret("MATCH (a:N {id: 4}), (b:M {id: 5}) CREATE (a)-[:R {id: 103, w: 1}]->(b)");
  faker.Interpret("MATCH ()-[r:R {id: 103}]->() SET r.w = 35");
  faker.Interpret("MATCH (a:N {id: 5}), (b:M {id: 5}) CREATE (a)-[:Q {id: 104, w: 3}]->(b)");

  auto const typed = "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w";
  auto const global = "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w";
  ExpectPlanUses(faker, typed, "ScanAllByEdgeTypeProperty");
  ExpectPlanUses(faker, global, "ScanAllByEdgeProperty");
  ExpectNoOrderBy(faker, typed);
  EXPECT_EQ(IntRows(faker, typed), (Rows{{100}, {1}, {2}, {101}, {3}, {103}, {4}, {5}, {102}}));
  EXPECT_EQ(IntRows(faker, global), (Rows{{104}, {100}, {1}, {2}, {101}, {3}, {103}, {4}, {5}, {102}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() RETURN count(r)"), (Rows{{9}}));
}

// A key-changing edge write that was rolled back, or is still open in another session, leaves main's row where it was.
TEST_F(VersioningIndexTest, StreamedEdgeScansKeepMainsRowForAnAbortedOrUncommittedKeyChange) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(1, 4) AS i CREATE (:N)-[:R {id: i, w: i}]->(:M)");
  Fork();
  auto writers = SessionsOnBranch(1);
  auto &writer = *writers[0];

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH ()-[r:R {id: 2}]->() SET r.w = 50");
  faker.Interpret("ROLLBACK");
  auto const typed = "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w";
  auto const global = "MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w";
  ExpectPlanUses(faker, typed, "ScanAllByEdgeTypeProperty");
  EXPECT_EQ(IntRows(faker, typed), (Rows{{1}, {2}, {3}, {4}}));
  EXPECT_EQ(IntRows(faker, global), (Rows{{1}, {2}, {3}, {4}}));

  writer.Interpret("BEGIN");
  writer.Interpret("MATCH ()-[r:R {id: 3}]->() SET r.w = 60");
  writer.Interpret("MATCH ()-[r:R {id: 4}]->() DELETE r");
  EXPECT_EQ(IntRows(faker, typed), (Rows{{1}, {2}, {3}, {4}})) << "another session's open writes are invisible";
  EXPECT_EQ(IntRows(faker, global), (Rows{{1}, {2}, {3}, {4}}));
  writer.Interpret("COMMIT");
  EXPECT_EQ(IntRows(faker, typed), (Rows{{1}, {2}, {3}}));
  EXPECT_EQ(IntRows(faker, global), (Rows{{1}, {2}, {3}}));
  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w = 60 RETURN r.id"), (Rows{{3}}));
}

// LIMIT stops the merge early: a scan pulls the rows it yields plus a little look-ahead, not all of main's index.
TEST_F(VersioningIndexTest, StreamedEdgeScansPullOnlyWhatALimitConsumes) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(1, 3000) AS i CREATE (:N)-[:R {id: i, w: i}]->(:M)");
  Fork();
  faker.Interpret("MATCH ()-[r:R {id: 2000}]->() SET r.w = 5000");
  faker.Interpret("MATCH ()-[r:R {id: 2500}]->() SET r.w = 0");

  faker.Interpret("BEGIN");
  faker.Interpret("RETURN 1");
  StreamProbe probe(faker, "R", "w");
  auto dba = probe.Dba();
  auto const view = memgraph::storage::View::OLD;
  auto const range = memgraph::storage::PropertyValueRange::IsNotNull();

  auto typed = dba.Edges(view, probe.edge_type(), probe.property(), range);
  EXPECT_EQ(probe.Ids(typed, 3), (std::vector<int64_t>{2500, 1, 2}));
  EXPECT_LE(typed.BranchSourceRows(), 12U) << "an ordered merge pulls a few rows, not the 3000";

  auto global = dba.Edges(view, probe.property(), range);
  EXPECT_EQ(probe.Ids(global, 1), (std::vector<int64_t>{2500}));
  EXPECT_LE(global.BranchSourceRows(), 8U);

  auto by_type = dba.Edges(view, probe.edge_type());
  EXPECT_EQ(probe.Ids(by_type, 1).size(), 1U);
  EXPECT_LE(by_type.BranchSourceRows(), 4U) << "an unordered scan yields main's rows first without touching the diff";
  faker.Interpret("ROLLBACK");

  EXPECT_EQ(IntRows(faker, "MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.w LIMIT 2"), (Rows{{2500}, {1}}));
}

// Under snapshot isolation an edge scan already under way sees the rows of its start, whatever other sessions commit
// while it steps: each row once, none lost to a key that moves ahead of or behind the cursor.
TEST_F(VersioningIndexTest, StreamedEdgeScanInFlightIsStableWhileAnotherSessionCommitsKeyChanges) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(1, 12) AS i CREATE (:N {id: i})-[:R {id: i, w: i}]->(:M)");
  Fork();
  auto writers = SessionsOnBranch(1);
  auto &writer = *writers[0];
  writer.Interpret("MATCH ()-[r:R {id: 4}]->() SET r.w = 40");  // before the scan opens: a D row

  faker.Interpret("BEGIN");
  faker.Interpret("RETURN 1");
  StreamProbe probe(faker, "R", "w");
  auto dba = probe.Dba();
  auto const range = memgraph::storage::PropertyValueRange::IsNotNull();
  auto const view = memgraph::storage::View::OLD;

  auto reference = dba.Edges(view, probe.edge_type(), probe.property(), range);
  auto const expected = probe.Ids(reference);
  ASSERT_EQ(expected, (std::vector<int64_t>{1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 4}));

  auto typed = dba.Edges(view, probe.edge_type(), probe.property(), range);
  auto const seen = probe.Ids(typed, std::numeric_limits<size_t>::max(), 2, [&] {
    // 11 is ahead of the cursor and moves to the front; 1 is behind it and moves to the back.
    writer.Interpret("MATCH ()-[r:R {id: 11}]->() SET r.w = -1");
    writer.Interpret("MATCH ()-[r:R {id: 1}]->() SET r.w = 500");
    writer.Interpret("MATCH ()-[r:R {id: 5}]->() DELETE r");
    writer.Interpret("MATCH (n:N {id: 6}) DETACH DELETE n");
    writer.Interpret("MATCH ()-[r:R {id: 4}]->() SET r.w = 4");
  });
  EXPECT_EQ(seen, expected);

  auto global = dba.Edges(view, probe.property(), range);
  auto const seen_global = probe.Ids(global, std::numeric_limits<size_t>::max(), 3, [&] {
    writer.Interpret("MATCH ()-[r:R {id: 8}]->() SET r.w = 800");
  });
  EXPECT_EQ(seen_global.size(), 12U);
  EXPECT_EQ(std::set<int64_t>(seen_global.begin(), seen_global.end()).size(), 12U);

  auto by_type = dba.Edges(view, probe.edge_type());
  auto const seen_by_type = probe.Ids(
      by_type, std::numeric_limits<size_t>::max(), 3, [&] { writer.Interpret("MATCH ()-[r:R {id: 9}]->() DELETE r"); });
  EXPECT_EQ(seen_by_type.size(), 12U);
  EXPECT_EQ(std::set<int64_t>(seen_by_type.begin(), seen_by_type.end()).size(), 12U);
  faker.Interpret("ROLLBACK");
}

TEST_F(VersioningIndexTest, StreamedEdgeScansEqualFullScansAfterRandomWrites) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(0, 15) AS i CREATE (:N {id: i})-[:R {id: i, w: i % 6}]->(:M {id: i})");
  faker.Interpret("UNWIND range(16, 21) AS i CREATE (:N {id: i})-[:Q {id: i, w: i % 6}]->(:M {id: i})");
  Fork();

  struct Check {
    std::string indexed;
    std::string reference;
    std::string_view op;
  };

  std::vector<Check> const checks{
      {"MATCH ()-[r:R]->() WHERE r.w >= 2 AND r.w <= 4 RETURN r.w ORDER BY r.w",
       "MATCH ()-[r]->() WHERE type(r) = 'R' AND r.w + 0 >= 2 AND r.w + 0 <= 4 RETURN r.w ORDER BY r.w",
       "ScanAllByEdgeTypeProperty"},
      {"MATCH ()-[r:R]->() WHERE r.w >= 0 RETURN r.w ORDER BY r.w",
       "MATCH ()-[r]->() WHERE type(r) = 'R' AND r.w + 0 >= 0 RETURN r.w ORDER BY r.w",
       "ScanAllByEdgeTypeProperty"},
      {"MATCH ()-[r:R]->() WHERE r.w = 3 RETURN r.id ORDER BY r.id",
       "MATCH ()-[r]->() WHERE type(r) = 'R' AND r.w + 0 = 3 RETURN r.id ORDER BY r.id",
       "ScanAllByEdgeTypeProperty"},
      {"MATCH ()-[r]->() WHERE r.w >= 1 RETURN r.w ORDER BY r.w",
       "MATCH ()-[r]->() WHERE r.w + 0 >= 1 RETURN r.w ORDER BY r.w",
       "ScanAllByEdgeProperty"},
      {"MATCH ()-[r]->() WHERE r.w >= 0 RETURN r.id ORDER BY r.id",
       "MATCH ()-[r]->() WHERE r.w + 0 >= 0 RETURN r.id ORDER BY r.id",
       "ScanAllByEdgeProperty"},
      {"MATCH ()-[r:R]->() RETURN r.id ORDER BY r.id",
       "MATCH ()-[r]->() WHERE type(r) = 'R' RETURN r.id ORDER BY r.id",
       "ScanAllByEdgeType"},
  };
  for (auto const &check : checks) ExpectPlanUses(faker, check.indexed, check.op);

  auto const expect_equal = [&](std::string const &where) {
    for (auto const &check : checks) {
      EXPECT_EQ(IntRows(faker, check.indexed), IntRows(faker, check.reference)) << where << ": " << check.indexed;
    }
  };

  expect_equal("at the fork");
  for (uint32_t seed = 1; seed <= 24; ++seed) {
    std::mt19937 rng(seed);
    for (int step = 0; step < 14; ++step) {
      auto const pick = [&](int bound) { return static_cast<int>(rng() % static_cast<uint32_t>(bound)); };
      auto const id = std::to_string(pick(22));
      auto const match = "MATCH ()-[r {id: " + id + "}]->() ";
      std::string statement;
      switch (pick(8)) {
        case 0:
          statement = match + "SET r.w = " + std::to_string(pick(7));
          break;
        case 1:
          statement = match + "REMOVE r.w";
          break;
        case 2:
          statement = match + "DELETE r";
          break;
        case 3:
          statement = "MATCH (n:N {id: " + id + "}) DETACH DELETE n";
          break;
        case 4:
          statement = "MATCH (m:M {id: " + id + "}) DETACH DELETE m";
          break;
        case 5:
          statement = "MATCH (a:N {id: " + id + "}), (b:M {id: " + std::to_string(pick(22)) +
                      "}) CREATE (a)-[:R {id: " + std::to_string(100 + seed * 20 + step) +
                      ", w: " + std::to_string(pick(7)) + "}]->(b)";
          break;
        case 6:
          statement = match + "SET r += {w: " + std::to_string(pick(7)) + "}";
          break;
        default:
          statement = match + "SET r.w = r.w";
          break;
      }
      if (pick(5) == 0) {
        faker.Interpret("BEGIN");
        faker.Interpret(statement);
        faker.Interpret("ROLLBACK");
      } else {
        faker.Interpret(statement);
      }
      expect_equal("seed " + std::to_string(seed) + " after `" + statement + "`");
    }
    faker.Interpret("BEGIN");
    expect_equal("seed " + std::to_string(seed) + " inside one transaction");
    faker.Interpret("COMMIT");
  }
}

// Main is the oracle for edge-property scans over values of every kind: a one-sided range covers only its bound's
// stretch of the order (a string is not > 0, nor a NaN), and null or missing values are never indexed.
TEST_F(VersioningIndexTest, BranchEdgePropertyScansOverValuesOfEveryKindAnswerLikeMain) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(1, 12) AS i CREATE (:N)-[:R {id: i, w: 0}]->(:M)");
  faker.Interpret("CREATE (:N)-[:R {id: 31, w: 'z'}]->(:M), (:N)-[:R {id: 32, w: 3}]->(:M)");
  faker.Interpret("CREATE (:N)-[:S {id: 41, w: 4}]->(:M), (:N)-[:S {id: 42, w: 2}]->(:M)");

  const std::vector<std::string> queries{
      "MATCH ()-[r:R]->() WHERE r.w > 0 RETURN r.id ORDER BY r.id",
      "MATCH ()-[r:R]->() WHERE r.w < 2 RETURN r.id ORDER BY r.id",
      "MATCH ()-[r:R]->() WHERE r.w >= 'a' RETURN r.id ORDER BY r.id",
      "MATCH ()-[r:R]->() WHERE r.w > date('2019-01-01') RETURN r.id ORDER BY r.id",
      "MATCH ()-[r:R]->() WHERE r.w IS NOT NULL RETURN r.id ORDER BY r.id",
      "MATCH ()-[r:R]->() WHERE r.w = 0 RETURN r.id ORDER BY r.id",
      "MATCH ()-[r]->() WHERE r.w > 0 RETURN r.id ORDER BY r.id",
      "MATCH ()-[r]->() WHERE r.w <= 'q' RETURN r.id ORDER BY r.id",
      "MATCH ()-[r]->() WHERE r.w IS NOT NULL RETURN r.id ORDER BY r.id",
  };
  const std::vector<std::string> fork_edge_writes{
      "UNWIND [[1, 1], [2, 2.5], [3, 'a'], [4, true], [5, [1]], [6, 0.0 / 0.0], [9, date('2020-01-01')], [10, -3]] "
      "AS kv MATCH ()-[r:R {id: kv[0]}]->() SET r.w = kv[1]",
      "MATCH ()-[r:R {id: 7}]->() SET r.w = null",
      "MATCH ()-[r:R {id: 8}]->() REMOVE r.w",
      "MATCH ()-[r:R {id: 11}]->() DELETE r",
      "MATCH (a)-[:R {id: 12}]->() DETACH DELETE a",
      "MATCH ()-[r:S {id: 41}]->() SET r.w = 'q'",
  };
  const std::vector<std::string> native_edge_writes{
      "CREATE (:N)-[:R {id: 21, w: 'b'}]->(:M), (:N)-[:R {id: 22, w: 0.5}]->(:M), (:N)-[:R {id: 23, w: false}]->(:M), "
      "(:N)-[:R {id: 24, w: 7}]->(:M), (:N)-[:S {id: 43, w: 'c'}]->(:M)",
  };
  const auto ask_all = [&] {
    std::vector<Rows> rows;
    for (auto const &query : queries) rows.push_back(IntRows(faker, query));
    return rows;
  };
  const auto expect_like_main = [&](std::vector<std::string> const &writes, std::string_view stage) {
    for (auto const &write : writes) faker.Interpret(write);
    const auto branch = ask_all();
    UseMain();
    for (auto const &write : writes) faker.Interpret(write);
    const auto oracle = ask_all();
    Use(faker, "b");
    for (size_t i = 0; i < queries.size(); ++i) {
      EXPECT_EQ(branch[i], oracle[i]) << stage << ": " << queries[i];
    }
    return oracle;
  };

  for (auto const &query : queries) {
    ExpectPlanUses(
        faker, query, query.starts_with("MATCH ()-[r:R]") ? "ScanAllByEdgeTypeProperty" : "ScanAllByEdgeProperty");
  }
  Fork();
  expect_like_main(fork_edge_writes, "fork edges written");
  const auto oracle = expect_like_main(native_edge_writes, "native edges added");
  EXPECT_EQ(oracle[0], (Rows{{1}, {2}, {22}, {24}, {32}})) << "main is the oracle";
  EXPECT_EQ(oracle[2], (Rows{{3}, {21}, {31}})) << "main is the oracle";

  UseMain();
  faker.Interpret("DROP EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("DROP GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  Use(faker, "b");
  for (size_t i = 0; i < queries.size(); ++i) {
    EXPECT_EQ(IntRows(faker, queries[i]), oracle[i]) << "full-scan fallback: " << queries[i];
  }
}

// The same for a vertex property range: a fork vertex the branch rewrote to a string is not > 0.
TEST_F(VersioningIndexTest, BranchVertexPropertyRangeCoversItsBoundsTypeOnlyLikeMain) {
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("CREATE GLOBAL INDEX ON :(p)");
  faker.Interpret("UNWIND range(1, 4) AS i CREATE (:L {id: i, p: i})");
  const std::vector<std::string> queries{
      "MATCH (n:L) WHERE n.p > 1 RETURN n.id ORDER BY n.id",
      "MATCH (n) WHERE n.p < 3 RETURN n.id ORDER BY n.id",
  };
  const std::string write = "MATCH (n:L) WHERE n.id IN [2, 3] SET n.p = CASE n.id WHEN 2 THEN 'a' ELSE true END";
  Fork();
  faker.Interpret(write);
  std::vector<Rows> branch;
  for (auto const &query : queries) branch.push_back(IntRows(faker, query));
  UseMain();
  faker.Interpret(write);
  std::vector<Rows> oracle;
  for (size_t i = 0; i < queries.size(); ++i) {
    ExpectPlanUses(faker, queries[i], i == 0 ? "ScanAllByLabelProperties" : "ScanAllByVertexProperty");
    oracle.push_back(IntRows(faker, queries[i]));
    EXPECT_EQ(branch[i], oracle[i]) << queries[i];
  }
  EXPECT_EQ(oracle[0], (Rows{{4}})) << "main is the oracle";

  faker.Interpret("DROP INDEX ON :L(p)");
  faker.Interpret("CREATE INDEX ON :L(p)");
  faker.Interpret("DROP GLOBAL INDEX ON :(p)");
  faker.Interpret("CREATE GLOBAL INDEX ON :(p)");
  Use(faker, "b");
  for (size_t i = 0; i < queries.size(); ++i) {
    EXPECT_EQ(IntRows(faker, queries[i]), oracle[i]) << "full-scan fallback: " << queries[i];
  }
}

// MERGE matches with View::NEW: a fork edge an earlier statement moved is matched once at its new value, and its old
// value no longer matches it, as on main.
TEST_F(VersioningIndexTest, MergeOverAnEdgeIndexMatchesAMovedForkEdgeLikeMain) {
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("UNWIND range(1, 3) AS i CREATE (:N)-[:R {id: i, w: i}]->(:M)");
  const std::vector<std::string> statements{
      "MATCH ()-[r:R {id: 1}]->() SET r.w = 5",
      "MATCH ()-[r:R {id: 3}]->() DELETE r",
  };
  const std::vector<std::string> merges{
      "MERGE ()-[r:R {w: 5}]->() RETURN coalesce(r.id, -1)",
      "MERGE ()-[r:R {w: 1}]->() RETURN coalesce(r.id, -1)",
      "MERGE ()-[r:R {w: 3}]->() RETURN coalesce(r.id, -1)",
      "MATCH ()-[r:R]->() WHERE r.w >= 1 RETURN coalesce(r.id, -1) ORDER BY r.w",
  };
  ExpectPlanUses(faker, merges[0], "ScanAllByEdgeTypeProperty");
  const auto run = [&] {
    std::vector<Rows> rows;
    for (auto const &statement : statements) faker.Interpret(statement);
    for (auto const &merge : merges) rows.push_back(IntRows(faker, merge));
    return rows;
  };
  Fork();
  const auto branch = run();
  UseMain();
  const auto oracle = run();
  EXPECT_EQ(oracle[0], (Rows{{1}})) << "main is the oracle";
  EXPECT_EQ(oracle[3], (Rows{{-1}, {2}, {-1}, {1}})) << "main is the oracle";
  for (size_t i = 0; i < merges.size(); ++i) EXPECT_EQ(branch[i], oracle[i]) << merges[i];
}

// A scan after a write in the same query reads View::NEW: a fork or native edge the query deleted, or a fork edge it
// moved out of the range, is not yielded by the index scans or the full-scan fallback, as on main.
TEST_F(VersioningIndexTest, NewViewEdgeScansSkipWhatTheSameQueryDeletedOrMovedLikeMain) {
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("UNWIND range(1, 4) AS i CREATE (:N)-[:R {id: i, w: i}]->(:M)");
  const std::string writes =
      "MATCH ()-[x:R]->() WHERE x.id IN [1, 5] DELETE x WITH count(*) AS c "
      "MATCH ()-[y:R {id: 2}]->() SET y.w = 9 WITH count(*) AS c ";
  const std::vector<std::string> queries{
      writes + "MATCH ()-[r:R]->() RETURN r.id ORDER BY r.id",
      writes + "MATCH ()-[r:R]->() WHERE r.w < 5 RETURN r.id ORDER BY r.id",
      writes + "MATCH ()-[r]->() WHERE r.w < 5 RETURN r.id ORDER BY r.id",
  };
  const std::vector<std::string_view> ops{"ScanAllByEdgeType", "ScanAllByEdgeTypeProperty", "ScanAllByEdgeProperty"};
  for (size_t i = 0; i < queries.size(); ++i) ExpectPlanUses(faker, queries[i], ops[i]);
  const auto ask = [&](std::string_view stage) {
    std::vector<Rows> rows;
    for (auto const &query : queries) {
      faker.Interpret("BEGIN");
      faker.Interpret("CREATE (:N)-[:R {id: 5, w: 1}]->(:M)");
      rows.push_back(IntRows(faker, query));
      faker.Interpret("ROLLBACK");
    }
    EXPECT_EQ(rows[0], (Rows{{2}, {3}, {4}})) << stage << ": " << queries[0];
    for (size_t i = 1; i < queries.size(); ++i) EXPECT_EQ(rows[i], (Rows{{3}, {4}})) << stage << ": " << queries[i];
  };
  Fork();
  ask("branch");
  UseMain();
  ask("main is the oracle");
  faker.Interpret("DROP EDGE INDEX ON :R");
  faker.Interpret("CREATE EDGE INDEX ON :R");
  faker.Interpret("DROP EDGE INDEX ON :R(w)");
  faker.Interpret("CREATE EDGE INDEX ON :R(w)");
  faker.Interpret("DROP GLOBAL EDGE INDEX ON :(w)");
  faker.Interpret("CREATE GLOBAL EDGE INDEX ON :(w)");
  Use(faker, "b");
  ask("full-scan fallback");
}
