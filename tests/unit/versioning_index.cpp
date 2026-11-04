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

// Index scans on a branch: main@fork's index merged with the branch's own changes, the index definitions mirrored for
// the planner, and the full-scan fallback for an index that main did not have at the fork. Branches are created and
// attached through the C++ API.

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "flags/run_time_configurable.hpp"
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

// The first column of every row of `query`, as strings.
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

void RunStatements(InterpreterFaker &session, std::string_view script) {
  for (auto stmt : std::views::split(script, ';')) session.Interpret(std::string(std::string_view(stmt)));
}

}  // namespace

TEST_F(VersioningIndexTest, BranchLabelIndexScanMatchesLabelMembership) {
  faker.Interpret("CREATE INDEX ON :User");
  faker.Interpret("CREATE (:User {i:1}), (:User {i:2}), (:User {i:3}), (:User {i:4}), (:User {i:5}), (:Other {i:6})");
  Fork();

  faker.Interpret("MATCH (n:User) WHERE n.i IN [1, 2] SET n:Probe");
  faker.Interpret("MATCH (n:User {i:3}) REMOVE n:User");
  faker.Interpret("MATCH (n:User {i:4}) DETACH DELETE n");
  faker.Interpret("MATCH (n:Other {i:6}) SET n:User");
  faker.Interpret("CREATE (:User {i:7})");

  ExpectPlanUses(faker, "MATCH (n:User) RETURN n.i", "ScanAllByLabel");
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN count(n)"), (Rows{{5}}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {5}, {6}, {7}}));

  faker.Interpret("BEGIN");
  faker.Interpret("MATCH (n:User {i:5}) REMOVE n:User");
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN count(n)"), (Rows{{4}}));
  faker.Interpret("ROLLBACK");
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {5}, {6}, {7}}));

  UseMain();
  EXPECT_EQ(IntRows(faker, "MATCH (n:User) RETURN n.i ORDER BY n.i"), (Rows{{1}, {2}, {3}, {4}, {5}}));
}

// Unindexed label filters over many label writes: HasLabel answers exactly, in any probe order, including the
// session's own uncommitted writes, removals and deletes.
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

// Reading and writing one label per row: the incremental touched-label cache sees the session's own label writes,
// property and label writes in one statement, and removals inside an explicit transaction.
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

  EXPECT_EQ(IntRows(faker, "MATCH (n:Label1|Label2) WHERE n.id < 2 RETURN count(n)"), (Rows{{2}}));
}

namespace {
struct MirroredIndexCase {
  const char *name;
  const char *ddl;
  const char *seed;
  const char *query;
  const char *op;
  // The index scan already yields ascending order, so the planner drops the explicit sort.
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

// Every index main held at the fork is mirrored onto the branch's diff engine, so the planner picks the index operator.
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

TEST_F(VersioningIndexTest, BranchLabelOnlyIndexScanReflectsBranchMutations) {
  faker.Interpret("CREATE INDEX ON :L");
  faker.Interpret("CREATE (:L {n:'a'}), (:L {n:'b'}), (:L {n:'c'}), (:L {n:'e', t:1}), (:Other {n:'x'})");
  Fork();

  faker.Interpret("MATCH (n:L {n:'a'}) DETACH DELETE n");
  faker.Interpret("MATCH (n:L {n:'b'}) REMOVE n:L");
  faker.Interpret("MATCH (n:Other {n:'x'}) SET n:L");
  faker.Interpret("CREATE (:L {n:'d'})");
  faker.Interpret("MATCH (n:L {n:'e'}) SET n.t = 2");

  // 'a' deleted, 'b' lost :L, 'x' gained it, 'd' is branch-native, 'c' untouched, 'e' touched but keeps :L (once).
  ExpectPlanUses(faker, "MATCH (n:L) RETURN n.n", "ScanAllByLabel");
  EXPECT_EQ(StringRows(faker, "MATCH (n:L) RETURN n.n ORDER BY n.n"), (Strings{"c", "d", "e", "x"}));
  EXPECT_EQ(IntRows(faker, "MATCH (n:L) WHERE n.t IS NOT NULL RETURN n.t"), (Rows{{2}}));
}

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
  faker.Interpret("MATCH (n:Q {name: 'F'}) SET n:P");  // the key from the fork
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

// No label or key change: the scan is main's stream, yielded lazily, but an unrelated write still reads through the
// branch and a later key change leaves the fast path.
TEST_F(VersioningIndexTest, BranchLabelPropertySeekUnaffectedByUnrelatedPropertyChange) {
  faker.Interpret("CREATE INDEX ON :P(v)");
  faker.Interpret("CREATE (:P {v: 10, note: 'a'}), (:P {v: 20, note: 'b'}), (:P {v: 30, note: 'c'})");
  Fork();

  const std::string query = "MATCH (n:P) WHERE n.v >= 10 RETURN n.note ORDER BY n.v";
  ExpectPlanUses(faker, query + " DESC LIMIT 2", "ScanAllByLabelProperties");
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
  // The :L indexes exist at the fork and are dropped and re-created on main; otherwise they are first created late.
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
    {"RebuiltNoDrift", true, false, false, false, false, {{"a", 1}, {"c", 3}, {"e", 5}, {"f", 6}}, {1, 3, 5, 6}},
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

// An index main creates after the fork, or drops and re-creates, is not visible at the fork: the branch plans against
// the indexes mirrored when it was built and falls back to a scan for an index main@fork did not have. Either way it
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

// The diff engine mirrors no vector index, so a vertex or edge the branch created still has its vector-indexed
// property omitted from results when the flag asks for it, like a fork object.
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
TEST_F(VersioningIndexTest, BranchEdgePropertyIndexScansOverlayForkEdges) {
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
