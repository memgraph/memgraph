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

// Graph Versioning: DUMP DATABASE on a branch is the branch's data with main's schema.

#include <algorithm>
#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "versioning_fixture.hpp"

namespace {

class VersioningDumpTest : public VersioningGatedTest {
 protected:
  std::vector<std::string> Dump() {
    std::vector<std::string> lines;
    for (const auto &row : faker.Interpret("DUMP DATABASE").GetResults()) lines.push_back(row[0].ValueString());
    return lines;
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
  for (const auto &line : lines) {
    EXPECT_EQ(line.find("__mg_removed"), std::string::npos) << line;
    EXPECT_EQ(line.find("__mg_deleted"), std::string::npos) << line;
  }
  EXPECT_FALSE(Mentions(lines, "Gone"));
  EXPECT_FALSE(Mentions(lines, "secret"));
  EXPECT_FALSE(Mentions(lines, "`name`: \"b\""));
  EXPECT_TRUE(HasLineWithAll(lines, {":`Keep`", "`name`: \"a\"", "`kept`: 2"}));
  EXPECT_TRUE(HasLineWithAll(lines, {":`Keep`", "`name`: \"c\""}));
  EXPECT_TRUE(HasLineWithAll(lines, {":`E`", "`v`: 2"}));
  EXPECT_FALSE(HasLineWithAll(lines, {":`E`", "`w`"}));
}
