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

#include <algorithm>
#include <filesystem>
#include <initializer_list>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "versioning_fixture.hpp"

namespace {

using memgraph::communication::bolt::Value;
using memgraph::tests::versioning_fixture::SortedNames;

struct VisibleGraph {
  std::multiset<std::string> vertices;
  std::multiset<std::string> edges;
};

struct LoadedDump {
  VisibleGraph graph;
  std::vector<std::string> failed_lines;
};

std::string Canonical(const Value &labels, const Value &properties) {
  std::ostringstream out;
  out << "(";
  for (const auto &label : SortedNames(labels)) out << ":" << label;
  out << " " << properties << ")";
  return out.str();
}

VisibleGraph ReadVisibleGraph(InterpreterFaker &session) {
  VisibleGraph graph;
  for (const auto &row : session.Interpret("MATCH (n) RETURN labels(n), properties(n)").GetResults()) {
    graph.vertices.insert(Canonical(row[0], row[1]));
  }
  for (const auto &row : session
                             .Interpret("MATCH (a)-[r]->(b) RETURN labels(a), properties(a), type(r), properties(r), "
                                        "labels(b), properties(b)")
                             .GetResults()) {
    std::ostringstream edge;
    edge << Canonical(row[0], row[1]) << "-[:" << row[2].ValueString() << " " << row[3] << "]->"
         << Canonical(row[4], row[5]);
    graph.edges.insert(edge.str());
  }
  return graph;
}

LoadedDump LoadIntoFreshDatabase(const std::vector<std::string> &lines, const std::filesystem::path &directory) {
  std::filesystem::remove_all(directory);
  LoadedDump loaded;
  {
    memgraph::storage::Config config{};
    config.durability.storage_directory = directory;
    config.durability.snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED;
    config.disk.main_storage_directory = directory / "disk";
    config.salient.items.properties_on_edges = true;

    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
        memgraph::storage::ReplicationStateRootPath(config)};
    memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
    auto db = *db_gk.access();
    memgraph::system::System system_state;
    memgraph::query::InterpreterContext context{{},
                                                nullptr,
                                                nullptr,
                                                memgraph::tests::versioning_fixture::kNoHandler,
                                                &repl_state,
                                                system_state,
                                                nullptr,
                                                nullptr,
                                                nullptr};
    InterpreterFaker oracle{&context, db};
    for (const auto &line : lines) {
      try {
        oracle.Interpret(line);
      } catch (const std::exception &) {
        loaded.failed_lines.push_back(line);
      }
    }
    loaded.graph = ReadVisibleGraph(oracle);
  }
  std::filesystem::remove_all(directory);
  return loaded;
}

class VersioningDumpTest : public VersioningGatedTest {
 protected:
  std::vector<std::string> Dump() {
    std::vector<std::string> lines;
    for (const auto &row : faker.Interpret("DUMP DATABASE").GetResults()) lines.push_back(row[0].ValueString());
    return lines;
  }

  // The expected sizes keep an empty graph from passing vacuously.
  LoadedDump LoadDump(size_t expected_vertices, size_t expected_edges) {
    const auto visible = ReadVisibleGraph(faker);
    EXPECT_EQ(visible.vertices.size(), expected_vertices);
    EXPECT_EQ(visible.edges.size(), expected_edges);
    auto loaded = LoadIntoFreshDatabase(Dump(), data_directory.string() + "_oracle");
    EXPECT_EQ(loaded.graph.vertices, visible.vertices);
    EXPECT_EQ(loaded.graph.edges, visible.edges);
    return loaded;
  }

  static bool HasLineWithAll(const std::vector<std::string> &lines, std::initializer_list<std::string_view> parts) {
    return std::ranges::any_of(lines, [&](const auto &line) {
      return std::ranges::all_of(parts, [&](auto part) { return line.find(part) != std::string::npos; });
    });
  }

  static bool HasLine(const std::vector<std::string> &lines, std::string_view line) {
    return std::ranges::find(lines, line) != lines.end();
  }

  static bool Mentions(const std::vector<std::string> &lines, std::string_view part) {
    return HasLineWithAll(lines, {part});
  }
};

}  // namespace

TEST_F(VersioningDumpTest, DumpOnABranchIsTheBranchsDataWithMainsSchema) {
  faker.Interpret("CREATE INDEX ON :Person(name)");
  faker.Interpret(R"(SET DESCRIPTION ON LABEL :Person "p")");
  faker.Interpret(R"(ENABLE TTL EVERY "1h")");
  faker.Interpret("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)");
  faker.Interpret("CREATE CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE");
  faker.Interpret(
      "CREATE (:Person {name: 'a', id: 1, age: 10}), (:Person {name: 'b', id: 2}), (:Person {name: 'c', id: 3})");
  faker.Interpret("MATCH (a {name: 'a'}), (b {name: 'b'}) CREATE (a)-[:KNOWS {w: 1}]->(b)");

  faker.Interpret("CREATE BRANCH feature");
  faker.Interpret("USE BRANCH feature");
  faker.Interpret("MATCH (a {name: 'a'}) SET a.age = 99, a:Extra");
  faker.Interpret("MATCH (b {name: 'b'}) REMOVE b.id, b:Person SET b:Other");
  faker.Interpret("MATCH (c {name: 'c'}) DETACH DELETE c");
  faker.Interpret("MATCH (a:Extra), (b {name: 'b'}) CREATE (a)-[:NEW {x: 5}]->(b)");
  faker.Interpret("CREATE (:BranchOnly {name: 'native'})");

  const auto lines = Dump();
  EXPECT_TRUE(HasLineWithAll(lines, {"CREATE (:__mg_vertex__", ":`Person`", ":`Extra`", "`age`: 99"}));
  EXPECT_TRUE(HasLineWithAll(lines, {"CREATE (:__mg_vertex__", ":`Other`", "`name`: \"b\""}));
  EXPECT_TRUE(HasLineWithAll(lines, {"CREATE (:__mg_vertex__", ":`BranchOnly`", "`name`: \"native\""}));
  EXPECT_TRUE(HasLineWithAll(lines, {":`NEW`", "`x`: 5"}));
  EXPECT_TRUE(HasLineWithAll(lines, {":`KNOWS`", "`w`: 1"}));
  EXPECT_FALSE(Mentions(lines, "`age`: 10"));
  EXPECT_FALSE(Mentions(lines, "`id`: 2"));
  EXPECT_FALSE(Mentions(lines, "`name`: \"c\""));

  // Main's indexes and constraints; not the label-only index the branch keeps for its own scans.
  EXPECT_TRUE(HasLine(lines, "CREATE INDEX ON :`Person`(`name`);"));
  EXPECT_TRUE(HasLine(lines, "CREATE CONSTRAINT ON (u:`Person`) ASSERT EXISTS (u.`name`);"));
  EXPECT_TRUE(Mentions(lines, "ASSERT u.`id` IS UNIQUE;"));
  EXPECT_FALSE(HasLine(lines, "CREATE INDEX ON :`Person`;"));
  EXPECT_FALSE(Mentions(lines, "CREATE GLOBAL INDEX"));
  EXPECT_TRUE(HasLineWithAll(lines, {"SET DESCRIPTION ON LABEL :`Person`", "\"p\""}));
  EXPECT_TRUE(Mentions(lines, "ENABLE TTL"));

  // An index created on main after the fork is not part of the branch's schema.
  faker.Interpret("USE BRANCH main");
  faker.Interpret("CREATE INDEX ON :Person(age)");
  faker.Interpret("USE BRANCH feature");
  EXPECT_FALSE(HasLine(Dump(), "CREATE INDEX ON :`Person`(`age`);"));
  faker.Interpret("USE BRANCH main");
  EXPECT_TRUE(HasLine(Dump(), "CREATE INDEX ON :`Person`(`age`);"));
  faker.Interpret("USE BRANCH feature");
  EXPECT_TRUE(LoadDump(/*expected_vertices=*/3, /*expected_edges=*/2).failed_lines.empty());

  faker.Interpret("USE BRANCH main");
  const auto main_lines = Dump();
  EXPECT_TRUE(HasLineWithAll(main_lines, {"CREATE (:__mg_vertex__", "`age`: 10"}));
  EXPECT_FALSE(Mentions(main_lines, "`age`: 99"));
  EXPECT_FALSE(Mentions(main_lines, "BranchOnly"));
}

// Values of a vector-indexed property come from main's vector index for fork vertices and from the vertex itself for
// branch-created ones.
TEST_F(VersioningDumpTest, DumpOnABranchWritesVectorIndexedValues) {
  faker.Interpret(R"(CREATE VECTOR INDEX vi ON :Emb(vec) WITH CONFIG {"dimension": 2, "capacity": 10})");
  faker.Interpret("CREATE (:Emb {vec: [1.0, 2.0], name: 'fork'}), (:Emb {vec: [5.0, 6.0], name: 'untouched'})");

  faker.Interpret("CREATE BRANCH feature");
  faker.Interpret("USE BRANCH feature");
  faker.Interpret("MATCH (e:Emb {name: 'fork'}) SET e.tag = 1");
  faker.Interpret("CREATE (:Emb {vec: [3.0, 4.0], name: 'native'})");

  const auto lines = Dump();
  EXPECT_TRUE(HasLineWithAll(lines, {":`Emb`", "`name`: \"fork\"", "`vec`: [1, 2]", "`tag`: 1"}));
  EXPECT_TRUE(HasLineWithAll(lines, {":`Emb`", "`name`: \"untouched\"", "`vec`: [5, 6]"}));
  EXPECT_TRUE(HasLineWithAll(lines, {":`Emb`", "`name`: \"native\"", "`vec`: [3, 4]"}));
  EXPECT_TRUE(Mentions(lines, "CREATE VECTOR INDEX `vi` ON :`Emb`(`vec`)"));
}

// Removed labels and properties and a deleted fork vertex are stubs with reserved marker ids on the diff engine; the
// dump writes the branch's visible state and none of their names.
TEST_F(VersioningDumpTest, DumpOnABranchLeaksNoMarkerNames) {
  faker.Interpret("CREATE INDEX ON :Keep(name)");
  faker.Interpret(
      "CREATE (:Keep:Gone {name: 'a', secret: 1, kept: 2}), (:Keep {name: 'b', secret: 3}), (:Keep {name: 'c'})");
  faker.Interpret("MATCH (a {name: 'a'}), (c {name: 'c'}) CREATE (a)-[:E {w: 1, v: 2}]->(c)");

  faker.Interpret("CREATE BRANCH feature");
  faker.Interpret("USE BRANCH feature");
  faker.Interpret("MATCH (a {name: 'a'}) REMOVE a:Gone, a.secret");
  faker.Interpret("MATCH (a {name: 'a'})-[e:E]->() REMOVE e.w");
  faker.Interpret("MATCH (b {name: 'b'}) DETACH DELETE b");

  const auto lines = Dump();
  EXPECT_FALSE(Mentions(lines, "Gone"));
  EXPECT_FALSE(Mentions(lines, "secret"));
  EXPECT_FALSE(Mentions(lines, "`name`: \"b\""));
  EXPECT_TRUE(HasLineWithAll(lines, {":`Keep`", "`name`: \"a\"", "`kept`: 2"}));
  EXPECT_TRUE(HasLineWithAll(lines, {":`Keep`", "`name`: \"c\""}));
  EXPECT_TRUE(HasLineWithAll(lines, {":`E`", "`v`: 2"}));
  EXPECT_FALSE(HasLineWithAll(lines, {":`E`", "`w`"}));
  EXPECT_TRUE(LoadDump(/*expected_vertices=*/2, /*expected_edges=*/1).failed_lines.empty());
}

TEST_F(VersioningDumpTest, DumpOnAnEmptyBranchRestoresMainsGraph) {
  faker.Interpret("CREATE (:A {k: 1})-[:R {w: 2}]->(:B {k: 2}), (:A {k: 3})");
  const auto main_graph = ReadVisibleGraph(faker);

  faker.Interpret("CREATE BRANCH feature");
  faker.Interpret("USE BRANCH feature");
  const auto loaded = LoadDump(/*expected_vertices=*/3, /*expected_edges=*/1);
  EXPECT_TRUE(loaded.failed_lines.empty());
  EXPECT_EQ(loaded.graph.vertices, main_graph.vertices);
  EXPECT_EQ(loaded.graph.edges, main_graph.edges);
}

TEST_F(VersioningDumpTest, DumpOnABranchWithOnlyDeletionsOmitsTheDeletedObjects) {
  faker.Interpret("CREATE (a:P {k: 1})-[:R]->(b:P {k: 2}), (b)-[:S {w: 1}]->(c:P {k: 3}), (a)-[:T]->(c), (:P {k: 4})");
  const auto main_graph = ReadVisibleGraph(faker);
  ASSERT_EQ(main_graph.vertices.size(), 4);
  ASSERT_EQ(main_graph.edges.size(), 3);

  faker.Interpret("CREATE BRANCH feature");
  faker.Interpret("USE BRANCH feature");
  faker.Interpret("MATCH (n:P {k: 4}) DETACH DELETE n");
  faker.Interpret("MATCH (:P {k: 1})-[t:T]->(:P {k: 3}) DELETE t");

  const auto loaded = LoadDump(/*expected_vertices=*/3, /*expected_edges=*/2);
  EXPECT_TRUE(loaded.failed_lines.empty());
  EXPECT_NE(loaded.graph.vertices, main_graph.vertices);
  EXPECT_NE(loaded.graph.edges, main_graph.edges);
}

// Unique constraints are not enforced on a branch, so a branch can hold data the dumped constraint rejects.
TEST_F(VersioningDumpTest, DumpOnABranchWithAUniqueViolationFailsOnlyTheConstraintLine) {
  faker.Interpret("CREATE CONSTRAINT ON (n:P) ASSERT n.id IS UNIQUE");
  faker.Interpret("CREATE (:P {id: 1}), (:P {id: 2})");

  faker.Interpret("CREATE BRANCH feature");
  faker.Interpret("USE BRANCH feature");
  ASSERT_NO_THROW(faker.Interpret("CREATE (:P {id: 1})"));

  const auto loaded = LoadDump(/*expected_vertices=*/3, /*expected_edges=*/0);
  ASSERT_EQ(loaded.failed_lines.size(), 1);
  EXPECT_NE(loaded.failed_lines[0].find("IS UNIQUE"), std::string::npos) << loaded.failed_lines[0];
}
