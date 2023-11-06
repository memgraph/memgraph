// Copyright 2023 Memgraph Ltd.
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
#include <cstdlib>
#include <filesystem>

#include "communication/bolt/v1/value.hpp"
#include "communication/result_stream_faker.hpp"
#include "csv/parsing.hpp"
#include "disk_test_utils.hpp"
#include "flags/run_time_configurable.hpp"
#include "glue/communication.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "interpreter_faker.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/metadata.hpp"
#include "query/stream.hpp"
#include "query/typed_value.hpp"
#include "query_common.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "utils/logging.hpp"

namespace {

auto ToEdgeList(const memgraph::communication::bolt::Value &v) {
  std::vector<memgraph::communication::bolt::Edge> list;
  for (auto x : v.ValueList()) {
    list.push_back(x.ValueEdge());
  }
  return list;
};

}  // namespace

// TODO: This is not a unit test, but tests/integration dir is chaotic at the
// moment. After tests refactoring is done, move/rename this.

constexpr auto kNoHandler = nullptr;

template <typename StorageType>
class InterpreterTest : public ::testing::Test {
 public:
  const std::string testSuite = "interpreter";
  const std::string testSuiteCsv = "interpreter_csv";
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_interpreter";

  InterpreterTest() : interpreter_context({}, kNoHandler) {}

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
          config.disk = disk_test_utils::GenerateOnDiskConfig(testSuite).disk;
          config.force_on_disk = true;
        }
        return config;
      }()  // iile
  };

  memgraph::dbms::DatabaseAccess db{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                                   ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                                   : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
                  "Wrong storage mode!");
        return db_acc;
      }()  // iile
  };

  memgraph::query::InterpreterContext interpreter_context;

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
      disk_test_utils::RemoveRocksDbDirs(testSuiteCsv);
    }
  }

  InterpreterFaker default_interpreter{&interpreter_context, db};

  auto Prepare(const std::string &query, const std::map<std::string, memgraph::storage::PropertyValue> &params = {}) {
    return default_interpreter.Prepare(query, params);
  }

  void Pull(ResultStreamFaker *stream, std::optional<int> n = {}, std::optional<int> qid = {}) {
    default_interpreter.Pull(stream, n, qid);
  }

  auto Interpret(const std::string &query, const std::map<std::string, memgraph::storage::PropertyValue> &params = {}) {
    return default_interpreter.Interpret(query, params);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(InterpreterTest, StorageTypes);

TYPED_TEST(InterpreterTest, MultiplePulls) {
  {
    auto [stream, qid] = this->Prepare("UNWIND [1,2,3,4,5] as n RETURN n");
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "n");
    this->Pull(&stream, 1);
    ASSERT_EQ(stream.GetSummary().count("has_more"), 1);
    ASSERT_TRUE(stream.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 1);
    this->Pull(&stream, 2);
    ASSERT_EQ(stream.GetSummary().count("has_more"), 1);
    ASSERT_TRUE(stream.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream.GetResults().size(), 3U);
    ASSERT_EQ(stream.GetResults()[1][0].ValueInt(), 2);
    ASSERT_EQ(stream.GetResults()[2][0].ValueInt(), 3);
    this->Pull(&stream);
    ASSERT_EQ(stream.GetSummary().count("has_more"), 1);
    ASSERT_FALSE(stream.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream.GetResults().size(), 5U);
    ASSERT_EQ(stream.GetResults()[3][0].ValueInt(), 4);
    ASSERT_EQ(stream.GetResults()[4][0].ValueInt(), 5);
  }
}

// Run query with different ast twice to see if query executes correctly when
// ast is read from cache.
TYPED_TEST(InterpreterTest, AstCache) {
  {
    auto stream = this->Interpret("RETURN 2 + 3");
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "2 + 3");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 5);
  }
  {
    // Cached ast, different literals.
    auto stream = this->Interpret("RETURN 5 + 4");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 9);
  }
  {
    // Different ast (because of different types).
    auto stream = this->Interpret("RETURN 5.5 + 4");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueDouble(), 9.5);
  }
  {
    // Cached ast, same literals.
    auto stream = this->Interpret("RETURN 2 + 3");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 5);
  }
  {
    // Cached ast, different literals.
    auto stream = this->Interpret("RETURN 10.5 + 1");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueDouble(), 11.5);
  }
  {
    // Cached ast, same literals, different whitespaces.
    auto stream = this->Interpret("RETURN  10.5 + 1");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueDouble(), 11.5);
  }
  {
    // Cached ast, same literals, different named header.
    auto stream = this->Interpret("RETURN  10.5+1");
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "10.5+1");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueDouble(), 11.5);
  }
}

// Run query with same ast multiple times with different parameters.
TYPED_TEST(InterpreterTest, Parameters) {
  {
    auto stream = this->Interpret("RETURN $2 + $`a b`", {{"2", memgraph::storage::PropertyValue(10)},
                                                         {"a b", memgraph::storage::PropertyValue(15)}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 25);
  }
  {
    // Not needed parameter.
    auto stream = this->Interpret("RETURN $2 + $`a b`", {{"2", memgraph::storage::PropertyValue(10)},
                                                         {"a b", memgraph::storage::PropertyValue(15)},
                                                         {"c", memgraph::storage::PropertyValue(10)}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 25);
  }
  {
    // Cached ast, different parameters.
    auto stream = this->Interpret("RETURN $2 + $`a b`", {{"2", memgraph::storage::PropertyValue("da")},
                                                         {"a b", memgraph::storage::PropertyValue("ne")}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueString(), "dane");
  }
  {
    // Non-primitive literal.
    auto stream = this->Interpret("RETURN $2",
                                  {{"2", memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
                                             memgraph::storage::PropertyValue(5), memgraph::storage::PropertyValue(2),
                                             memgraph::storage::PropertyValue(3)})}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    auto result = memgraph::query::test_common::ToIntList(memgraph::glue::ToTypedValue(stream.GetResults()[0][0]));
    ASSERT_THAT(result, testing::ElementsAre(5, 2, 3));
  }
  {
    // Cached ast, unprovided parameter.
    ASSERT_THROW(this->Interpret("RETURN $2 + $`a b`", {{"2", memgraph::storage::PropertyValue("da")},
                                                        {"ab", memgraph::storage::PropertyValue("ne")}}),
                 memgraph::query::UnprovidedParameterError);
  }
}

// Run CREATE/MATCH/MERGE queries with property map
TYPED_TEST(InterpreterTest, ParametersAsPropertyMap) {
  {
    std::map<std::string, memgraph::storage::PropertyValue> property_map{};
    property_map["name"] = memgraph::storage::PropertyValue("name1");
    property_map["age"] = memgraph::storage::PropertyValue(25);
    auto stream =
        this->Interpret("CREATE (n $prop) RETURN n", {
                                                         {"prop", memgraph::storage::PropertyValue(property_map)},
                                                     });
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    ASSERT_EQ(stream.GetHeader()[0], "n");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    auto result = stream.GetResults()[0][0].ValueVertex();
    EXPECT_EQ(result.properties["name"].ValueString(), "name1");
    EXPECT_EQ(result.properties["age"].ValueInt(), 25);
  }
  {
    std::map<std::string, memgraph::storage::PropertyValue> property_map{};
    property_map["name"] = memgraph::storage::PropertyValue("name1");
    property_map["age"] = memgraph::storage::PropertyValue(25);
    this->Interpret("CREATE (:Person)");
    auto stream = this->Interpret("MATCH (m: Person) CREATE (n $prop) RETURN n",
                                  {
                                      {"prop", memgraph::storage::PropertyValue(property_map)},
                                  });
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    ASSERT_EQ(stream.GetHeader()[0], "n");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    auto result = stream.GetResults()[0][0].ValueVertex();
    EXPECT_EQ(result.properties["name"].ValueString(), "name1");
    EXPECT_EQ(result.properties["age"].ValueInt(), 25);
  }
  {
    std::map<std::string, memgraph::storage::PropertyValue> property_map{};
    property_map["name"] = memgraph::storage::PropertyValue("name1");
    property_map["weight"] = memgraph::storage::PropertyValue(121);
    auto stream = this->Interpret("CREATE ()-[r:TO $prop]->() RETURN r",
                                  {
                                      {"prop", memgraph::storage::PropertyValue(property_map)},
                                  });
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    ASSERT_EQ(stream.GetHeader()[0], "r");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    auto result = stream.GetResults()[0][0].ValueEdge();
    EXPECT_EQ(result.properties["name"].ValueString(), "name1");
    EXPECT_EQ(result.properties["weight"].ValueInt(), 121);
  }
  {
    std::map<std::string, memgraph::storage::PropertyValue> property_map{};
    property_map["name"] = memgraph::storage::PropertyValue("name1");
    property_map["age"] = memgraph::storage::PropertyValue(15);
    ASSERT_THROW(this->Interpret("MATCH (n $prop) RETURN n",
                                 {
                                     {"prop", memgraph::storage::PropertyValue(property_map)},
                                 }),
                 memgraph::query::SemanticException);
  }
  {
    std::map<std::string, memgraph::storage::PropertyValue> property_map{};
    property_map["name"] = memgraph::storage::PropertyValue("name1");
    property_map["age"] = memgraph::storage::PropertyValue(15);
    ASSERT_THROW(this->Interpret("MERGE (n $prop) RETURN n",
                                 {
                                     {"prop", memgraph::storage::PropertyValue(property_map)},
                                 }),
                 memgraph::query::SemanticException);
  }
}

// Test bfs end to end.
TYPED_TEST(InterpreterTest, Bfs) {
  srand(0);
  auto kNumLevels = 10;
  auto kNumNodesPerLevel = 100;
  auto kNumEdgesPerNode = 100;
  auto kNumUnreachableNodes = 1000;
  auto kNumUnreachableEdges = 100000;
  auto kResCoeff = 5;
  const auto kReachable = "reachable";
  const auto kId = "id";

  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    kNumLevels = 5;
    kNumNodesPerLevel = 20;
    kNumEdgesPerNode = 20;
    kNumUnreachableNodes = 200;
    kNumUnreachableEdges = 20000;
    kResCoeff = 4;
  }

  std::vector<std::vector<memgraph::query::VertexAccessor>> levels(kNumLevels);
  int id = 0;

  // Set up.
  {
    auto storage_dba = this->db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    auto add_node = [&](int level, bool reachable) {
      auto node = dba.InsertVertex();
      MG_ASSERT(node.SetProperty(dba.NameToProperty(kId), memgraph::storage::PropertyValue(id++)).HasValue());
      MG_ASSERT(
          node.SetProperty(dba.NameToProperty(kReachable), memgraph::storage::PropertyValue(reachable)).HasValue());
      levels[level].push_back(node);
      return node;
    };

    auto add_edge = [&](auto &v1, auto &v2, bool reachable) {
      auto edge = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("edge"));
      MG_ASSERT(
          edge->SetProperty(dba.NameToProperty(kReachable), memgraph::storage::PropertyValue(reachable)).HasValue());
    };

    // Add source node.
    add_node(0, true);

    // Add reachable nodes.
    for (int i = 1; i < kNumLevels; ++i) {
      for (int j = 0; j < kNumNodesPerLevel; ++j) {
        auto node = add_node(i, true);
        for (int k = 0; k < kNumEdgesPerNode; ++k) {
          auto &node2 = levels[i - 1][rand() % levels[i - 1].size()];
          add_edge(node2, node, true);
        }
      }
    }

    // Add unreachable nodes.
    for (int i = 0; i < kNumUnreachableNodes; ++i) {
      auto node = add_node(rand() % kNumLevels,  // Not really important.
                           false);
      for (int j = 0; j < kNumEdgesPerNode; ++j) {
        auto &level = levels[rand() % kNumLevels];
        auto &node2 = level[rand() % level.size()];
        add_edge(node2, node, true);
        add_edge(node, node2, true);
      }
    }

    // Add unreachable edges.
    for (int i = 0; i < kNumUnreachableEdges; ++i) {
      auto &level1 = levels[rand() % kNumLevels];
      auto &node1 = level1[rand() % level1.size()];
      auto &level2 = levels[rand() % kNumLevels];
      auto &node2 = level2[rand() % level2.size()];
      add_edge(node1, node2, false);
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto stream = this->Interpret(
      "MATCH (n {id: 0})-[r *bfs..5 (e, n | n.reachable and "
      "e.reachable)]->(m) RETURN n, r, m");

  ASSERT_EQ(stream.GetHeader().size(), 3U);
  EXPECT_EQ(stream.GetHeader()[0], "n");
  EXPECT_EQ(stream.GetHeader()[1], "r");
  EXPECT_EQ(stream.GetHeader()[2], "m");
  ASSERT_EQ(stream.GetResults().size(), kResCoeff * kNumNodesPerLevel);

  int expected_level = 1;
  int remaining_nodes_in_level = kNumNodesPerLevel;
  std::unordered_set<int64_t> matched_ids;

  for (const auto &result : stream.GetResults()) {
    const auto &begin = result[0].ValueVertex();
    const auto &edges = ToEdgeList(result[1]);
    const auto &end = result[2].ValueVertex();

    // Check that path is of expected length. Returned paths should be from
    // shorter to longer ones.
    EXPECT_EQ(edges.size(), expected_level);
    // Check that starting node is correct.
    EXPECT_EQ(edges.front().from, begin.id);
    EXPECT_EQ(begin.properties.at(kId).ValueInt(), 0);
    for (int i = 1; i < static_cast<int>(edges.size()); ++i) {
      // Check that edges form a connected path.
      EXPECT_EQ(edges[i - 1].to.AsInt(), edges[i].from.AsInt());
    }
    auto matched_id = end.properties.at(kId).ValueInt();
    EXPECT_EQ(edges.back().to, end.id);
    // Check that we didn't match that node already.
    EXPECT_TRUE(matched_ids.insert(matched_id).second);
    // Check that shortest path was found.
    EXPECT_TRUE(matched_id > kNumNodesPerLevel * (expected_level - 1) &&
                matched_id <= kNumNodesPerLevel * expected_level);
    if (!--remaining_nodes_in_level) {
      remaining_nodes_in_level = kNumNodesPerLevel;
      ++expected_level;
    }
  }
}

// Test shortest path end to end.
TYPED_TEST(InterpreterTest, ShortestPath) {
  const auto test_shortest_path = [this](const bool use_duration) {
    const auto get_weight = [use_duration](const auto value) {
      return fmt::format(fmt::runtime(use_duration ? "DURATION('PT{}S')" : "{}"), value);
    };

    this->Interpret(
        fmt::format("CREATE (n:A {{x: 1}}), (m:B {{x: 2}}), (l:C {{x: 1}}), (n)-[:r1 {{w: {} "
                    "}}]->(m)-[:r2 {{w: {}}}]->(l), (n)-[:r3 {{w: {}}}]->(l)",
                    get_weight(1), get_weight(2), get_weight(4)));

    auto stream = this->Interpret("MATCH (n)-[e *wshortest 5 (e, n | e.w) ]->(m) return e");

    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "e");
    ASSERT_EQ(stream.GetResults().size(), 3U);

    std::vector<std::vector<std::string>> expected_results{{"r1"}, {"r2"}, {"r1", "r2"}};

    for (const auto &result : stream.GetResults()) {
      const auto &edges = ToEdgeList(result[0]);

      std::vector<std::string> datum;
      datum.reserve(edges.size());

      for (const auto &edge : edges) {
        datum.push_back(edge.type);
      }

      bool any_match = false;
      for (const auto &expected : expected_results) {
        if (expected == datum) {
          any_match = true;
          break;
        }
      }

      EXPECT_TRUE(any_match);
    }

    this->Interpret("MATCH (n) DETACH DELETE n");
  };

  static constexpr bool kUseNumeric{false};
  static constexpr bool kUseDuration{true};
  {
    SCOPED_TRACE("Test with numeric values");
    test_shortest_path(kUseNumeric);
  }
  {
    SCOPED_TRACE("Test with Duration values");
    test_shortest_path(kUseDuration);
  }
}

TYPED_TEST(InterpreterTest, AllShortestById) {
  auto stream_init = this->Interpret(
      "CREATE (n:A {x: 1}), (m:B {x: 2}), (l:C {x: 3}), (k:D {x: 4}), (n)-[:r1 {w: 1 "
      "}]->(m)-[:r2 {w: 2}]->(l), (n)-[:r3 {w: 4}]->(l), (k)-[:r4 {w: 3}]->(l) return id(n), id(l)");

  auto id_n = stream_init.GetResults().front()[0].ValueInt();
  auto id_l = stream_init.GetResults().front()[1].ValueInt();

  auto stream = this->Interpret(
      fmt::format("MATCH (n)-[e *allshortest 5 (e, n | e.w) ]->(l) WHERE id(n)={} AND id(l)={} return e", id_n, id_l));

  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader()[0], "e");
  ASSERT_EQ(stream.GetResults().size(), 1U);

  std::vector<std::string> expected_result = {"r1", "r2"};

  const auto &result = stream.GetResults()[0];
  const auto &edges = ToEdgeList(result[0]);

  std::vector<std::string> datum;
  datum.reserve(edges.size());

  for (const auto &edge : edges) {
    datum.push_back(edge.type);
  }

  EXPECT_TRUE(expected_result == datum);

  this->Interpret("MATCH (n) DETACH DELETE n");
}

TYPED_TEST(InterpreterTest, CreateLabelIndexInMulticommandTransaction) {
  this->Interpret("BEGIN");
  ASSERT_THROW(this->Interpret("CREATE INDEX ON :X"), memgraph::query::IndexInMulticommandTxException);
  this->Interpret("ROLLBACK");
}

TYPED_TEST(InterpreterTest, CreateLabelPropertyIndexInMulticommandTransaction) {
  this->Interpret("BEGIN");
  ASSERT_THROW(this->Interpret("CREATE INDEX ON :X(y)"), memgraph::query::IndexInMulticommandTxException);
  this->Interpret("ROLLBACK");
}

TYPED_TEST(InterpreterTest, CreateExistenceConstraintInMulticommandTransaction) {
  this->Interpret("BEGIN");
  ASSERT_THROW(this->Interpret("CREATE CONSTRAINT ON (n:A) ASSERT EXISTS (n.a)"),
               memgraph::query::ConstraintInMulticommandTxException);
  this->Interpret("ROLLBACK");
}

TYPED_TEST(InterpreterTest, CreateUniqueConstraintInMulticommandTransaction) {
  this->Interpret("BEGIN");
  ASSERT_THROW(this->Interpret("CREATE CONSTRAINT ON (n:A) ASSERT n.a, n.b IS UNIQUE"),
               memgraph::query::ConstraintInMulticommandTxException);
  this->Interpret("ROLLBACK");
}

TYPED_TEST(InterpreterTest, ShowIndexInfoInMulticommandTransaction) {
  this->Interpret("BEGIN");
  ASSERT_THROW(this->Interpret("SHOW INDEX INFO"), memgraph::query::InfoInMulticommandTxException);
  this->Interpret("ROLLBACK");
}

TYPED_TEST(InterpreterTest, ShowConstraintInfoInMulticommandTransaction) {
  this->Interpret("BEGIN");
  ASSERT_THROW(this->Interpret("SHOW CONSTRAINT INFO"), memgraph::query::InfoInMulticommandTxException);
  this->Interpret("ROLLBACK");
}

TYPED_TEST(InterpreterTest, ShowStorageInfoInMulticommandTransaction) {
  this->Interpret("BEGIN");
  ASSERT_THROW(this->Interpret("SHOW STORAGE INFO"), memgraph::query::InfoInMulticommandTxException);
  this->Interpret("ROLLBACK");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(InterpreterTest, ExistenceConstraintTest) {
  this->Interpret("CREATE CONSTRAINT ON (n:A) ASSERT EXISTS (n.a);");
  this->Interpret("CREATE (:A{a:1})");
  this->Interpret("CREATE (:A{a:2})");
  ASSERT_THROW(this->Interpret("CREATE (:A)"), memgraph::query::QueryException);
  this->Interpret("MATCH (n:A{a:2}) SET n.a=3");
  this->Interpret("CREATE (:A{a:2})");
  this->Interpret("MATCH (n:A{a:2}) DETACH DELETE n");
  this->Interpret("CREATE (n:A{a:2})");
  ASSERT_THROW(this->Interpret("CREATE CONSTRAINT ON (n:A) ASSERT EXISTS (n.b);"),
               memgraph::query::QueryRuntimeException);
}

TYPED_TEST(InterpreterTest, UniqueConstraintTest) {
  // Empty property list should result with syntax exception.
  ASSERT_THROW(this->Interpret("CREATE CONSTRAINT ON (n:A) ASSERT IS UNIQUE;"), memgraph::query::SyntaxException);
  ASSERT_THROW(this->Interpret("DROP CONSTRAINT ON (n:A) ASSERT IS UNIQUE;"), memgraph::query::SyntaxException);

  // Too large list of properties should also result with syntax exception.
  {
    std::stringstream stream;
    stream << " ON (n:A) ASSERT ";
    for (size_t i = 0; i < 33; ++i) {
      if (i > 0) stream << ", ";
      stream << "n.prop" << i;
    }
    stream << " IS UNIQUE;";
    std::string create_query = "CREATE CONSTRAINT" + stream.str();
    std::string drop_query = "DROP CONSTRAINT" + stream.str();
    ASSERT_THROW(this->Interpret(create_query), memgraph::query::SyntaxException);
    ASSERT_THROW(this->Interpret(drop_query), memgraph::query::SyntaxException);
  }

  // Providing property list with duplicates results with syntax exception.
  ASSERT_THROW(this->Interpret("CREATE CONSTRAINT ON (n:A) ASSERT n.a, n.b, n.a IS UNIQUE;"),
               memgraph::query::SyntaxException);
  ASSERT_THROW(this->Interpret("DROP CONSTRAINT ON (n:A) ASSERT n.a, n.b, n.a IS UNIQUE;"),
               memgraph::query::SyntaxException);

  // Commit of vertex should fail if a constraint is violated.
  this->Interpret("CREATE CONSTRAINT ON (n:A) ASSERT n.a, n.b IS UNIQUE;");
  this->Interpret("CREATE (:A{a:1, b:2})");
  this->Interpret("CREATE (:A{a:1, b:3})");
  ASSERT_THROW(this->Interpret("CREATE (:A{a:1, b:2})"), memgraph::query::QueryException);

  // Attempt to create a constraint should fail if it's violated.
  this->Interpret("CREATE (:A{a:1, c:2})");
  this->Interpret("CREATE (:A{a:1, c:2})");
  ASSERT_THROW(this->Interpret("CREATE CONSTRAINT ON (n:A) ASSERT n.a, n.c IS UNIQUE;"),
               memgraph::query::QueryRuntimeException);

  this->Interpret("MATCH (n:A{a:2, b:2}) SET n.a=1");
  this->Interpret("CREATE (:A{a:2})");
  this->Interpret("MATCH (n:A{a:2}) DETACH DELETE n");
  this->Interpret("CREATE (n:A{a:2})");

  // Show constraint info.
  {
    auto stream = this->Interpret("SHOW CONSTRAINT INFO");
    ASSERT_EQ(stream.GetHeader().size(), 3U);
    const auto &header = stream.GetHeader();
    ASSERT_EQ(header[0], "constraint type");
    ASSERT_EQ(header[1], "label");
    ASSERT_EQ(header[2], "properties");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    const auto &result = stream.GetResults().front();
    ASSERT_EQ(result.size(), 3U);
    ASSERT_EQ(result[0].ValueString(), "unique");
    ASSERT_EQ(result[1].ValueString(), "A");
    const auto &properties = result[2].ValueList();
    ASSERT_EQ(properties.size(), 2U);
    ASSERT_EQ(properties[0].ValueString(), "a");
    ASSERT_EQ(properties[1].ValueString(), "b");
  }

  // Drop constraint.
  this->Interpret("DROP CONSTRAINT ON (n:A) ASSERT n.a, n.b IS UNIQUE;");
  // Removing the same constraint twice should not throw any exception.
  this->Interpret("DROP CONSTRAINT ON (n:A) ASSERT n.a, n.b IS UNIQUE;");
}

TYPED_TEST(InterpreterTest, ExplainQuery) {
  EXPECT_EQ(this->db->plan_cache()->size(), 0U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 0U);
  auto stream = this->Interpret("EXPLAIN MATCH (n) RETURN *;");
  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader().front(), "QUERY PLAN");
  std::vector<std::string> expected_rows{" * Produce {n}", " * ScanAll (n)", " * Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 1U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  // We should have AST cache for EXPLAIN ... and for inner MATCH ...
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
  this->Interpret("MATCH (n) RETURN *;");
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
}

TYPED_TEST(InterpreterTest, ExplainQueryMultiplePulls) {
  EXPECT_EQ(this->db->plan_cache()->size(), 0U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 0U);
  auto [stream, qid] = this->Prepare("EXPLAIN MATCH (n) RETURN *;");
  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader().front(), "QUERY PLAN");
  std::vector<std::string> expected_rows{" * Produce {n}", " * ScanAll (n)", " * Once"};
  this->Pull(&stream, 1);
  ASSERT_EQ(stream.GetResults().size(), 1);
  auto expected_it = expected_rows.begin();
  ASSERT_EQ(stream.GetResults()[0].size(), 1U);
  EXPECT_EQ(stream.GetResults()[0].front().ValueString(), *expected_it);
  ++expected_it;

  this->Pull(&stream, 1);
  ASSERT_EQ(stream.GetResults().size(), 2);
  ASSERT_EQ(stream.GetResults()[1].size(), 1U);
  EXPECT_EQ(stream.GetResults()[1].front().ValueString(), *expected_it);
  ++expected_it;

  this->Pull(&stream);
  ASSERT_EQ(stream.GetResults().size(), 3);
  ASSERT_EQ(stream.GetResults()[2].size(), 1U);
  EXPECT_EQ(stream.GetResults()[2].front().ValueString(), *expected_it);
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  // We should have AST cache for EXPLAIN ... and for inner MATCH ...
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
  this->Interpret("MATCH (n) RETURN *;");
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
}

TYPED_TEST(InterpreterTest, ExplainQueryInMulticommandTransaction) {
  EXPECT_EQ(this->db->plan_cache()->size(), 0U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 0U);
  this->Interpret("BEGIN");
  auto stream = this->Interpret("EXPLAIN MATCH (n) RETURN *;");
  this->Interpret("COMMIT");
  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader().front(), "QUERY PLAN");
  std::vector<std::string> expected_rows{" * Produce {n}", " * ScanAll (n)", " * Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 1U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  // We should have AST cache for EXPLAIN ... and for inner MATCH ...
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
  this->Interpret("MATCH (n) RETURN *;");
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
}

TYPED_TEST(InterpreterTest, ExplainQueryWithParams) {
  EXPECT_EQ(this->db->plan_cache()->size(), 0U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 0U);
  auto stream =
      this->Interpret("EXPLAIN MATCH (n) WHERE n.id = $id RETURN *;", {{"id", memgraph::storage::PropertyValue(42)}});
  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader().front(), "QUERY PLAN");
  std::vector<std::string> expected_rows{" * Produce {n}", " * Filter {n.id}", " * ScanAll (n)", " * Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 1U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  // We should have AST cache for EXPLAIN ... and for inner MATCH ...
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
  this->Interpret("MATCH (n) WHERE n.id = $id RETURN *;", {{"id", memgraph::storage::PropertyValue("something else")}});
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
}

TYPED_TEST(InterpreterTest, ProfileQuery) {
  EXPECT_EQ(this->db->plan_cache()->size(), 0U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 0U);
  auto stream = this->Interpret("PROFILE MATCH (n) RETURN *;");
  std::vector<std::string> expected_header{"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"};
  EXPECT_EQ(stream.GetHeader(), expected_header);
  std::vector<std::string> expected_rows{"* Produce {n}", "* ScanAll (n)", "* Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 4U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  // We should have AST cache for PROFILE ... and for inner MATCH ...
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
  this->Interpret("MATCH (n) RETURN *;");
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
}

TYPED_TEST(InterpreterTest, ProfileQueryMultiplePulls) {
  EXPECT_EQ(this->db->plan_cache()->size(), 0U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 0U);
  auto [stream, qid] = this->Prepare("PROFILE MATCH (n) RETURN *;");
  std::vector<std::string> expected_header{"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"};
  EXPECT_EQ(stream.GetHeader(), expected_header);

  std::vector<std::string> expected_rows{"* Produce {n}", "* ScanAll (n)", "* Once"};
  auto expected_it = expected_rows.begin();

  this->Pull(&stream, 1);
  ASSERT_EQ(stream.GetResults().size(), 1U);
  ASSERT_EQ(stream.GetResults()[0].size(), 4U);
  ASSERT_EQ(stream.GetResults()[0][0].ValueString(), *expected_it);
  ++expected_it;

  this->Pull(&stream, 1);
  ASSERT_EQ(stream.GetResults().size(), 2U);
  ASSERT_EQ(stream.GetResults()[1].size(), 4U);
  ASSERT_EQ(stream.GetResults()[1][0].ValueString(), *expected_it);
  ++expected_it;

  this->Pull(&stream);
  ASSERT_EQ(stream.GetResults().size(), 3U);
  ASSERT_EQ(stream.GetResults()[2].size(), 4U);
  ASSERT_EQ(stream.GetResults()[2][0].ValueString(), *expected_it);

  // We should have a plan cache for MATCH ...
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  // We should have AST cache for PROFILE ... and for inner MATCH ...
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
  this->Interpret("MATCH (n) RETURN *;");
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
}

TYPED_TEST(InterpreterTest, ProfileQueryInMulticommandTransaction) {
  this->Interpret("BEGIN");
  ASSERT_THROW(this->Interpret("PROFILE MATCH (n) RETURN *;"), memgraph::query::ProfileInMulticommandTxException);
  this->Interpret("ROLLBACK");
}

TYPED_TEST(InterpreterTest, ProfileQueryWithParams) {
  EXPECT_EQ(this->db->plan_cache()->size(), 0U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 0U);
  auto stream =
      this->Interpret("PROFILE MATCH (n) WHERE n.id = $id RETURN *;", {{"id", memgraph::storage::PropertyValue(42)}});
  std::vector<std::string> expected_header{"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"};
  EXPECT_EQ(stream.GetHeader(), expected_header);
  std::vector<std::string> expected_rows{"* Produce {n}", "* Filter {n.id}", "* ScanAll (n)", "* Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 4U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  // We should have AST cache for PROFILE ... and for inner MATCH ...
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
  this->Interpret("MATCH (n) WHERE n.id = $id RETURN *;", {{"id", memgraph::storage::PropertyValue("something else")}});
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
}

TYPED_TEST(InterpreterTest, ProfileQueryWithLiterals) {
  EXPECT_EQ(this->db->plan_cache()->size(), 0U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 0U);
  auto stream = this->Interpret("PROFILE UNWIND range(1, 1000) AS x CREATE (:Node {id: x});", {});
  std::vector<std::string> expected_header{"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"};
  EXPECT_EQ(stream.GetHeader(), expected_header);
  std::vector<std::string> expected_rows{"* EmptyResult", "* CreateNode", "* Unwind", "* Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 4U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for UNWIND ...
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  // We should have AST cache for PROFILE ... and for inner UNWIND ...
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
  this->Interpret("UNWIND range(42, 4242) AS x CREATE (:Node {id: x});", {});
  EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  EXPECT_EQ(this->interpreter_context.ast_cache.size(), 2U);
}

TYPED_TEST(InterpreterTest, Transactions) {
  auto &interpreter = this->default_interpreter.interpreter;
  {
    ASSERT_THROW(interpreter.CommitTransaction(), memgraph::query::ExplicitTransactionUsageException);
    ASSERT_THROW(interpreter.RollbackTransaction(), memgraph::query::ExplicitTransactionUsageException);
    interpreter.BeginTransaction();
    ASSERT_THROW(interpreter.BeginTransaction(), memgraph::query::ExplicitTransactionUsageException);
    auto [stream, qid] = this->Prepare("RETURN 2");
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "2");
    this->Pull(&stream, 1);
    ASSERT_EQ(stream.GetSummary().count("has_more"), 1);
    ASSERT_FALSE(stream.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
    interpreter.CommitTransaction();
  }
  {
    interpreter.BeginTransaction();
    auto [stream, qid] = this->Prepare("RETURN 2");
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "2");
    this->Pull(&stream, 1);
    ASSERT_EQ(stream.GetSummary().count("has_more"), 1);
    ASSERT_FALSE(stream.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 2);
    interpreter.RollbackTransaction();
  }
}

TYPED_TEST(InterpreterTest, Qid) {
  auto &interpreter = this->default_interpreter.interpreter;
  {
    interpreter.BeginTransaction();
    auto [stream, qid] = this->Prepare("RETURN 2");
    ASSERT_TRUE(qid);
    ASSERT_THROW(this->Pull(&stream, {}, *qid + 1), memgraph::query::InvalidArgumentsException);
    interpreter.RollbackTransaction();
  }
  {
    interpreter.BeginTransaction();
    auto [stream1, qid1] = this->Prepare("UNWIND(range(1,3)) as n RETURN n");
    ASSERT_TRUE(qid1);
    ASSERT_EQ(stream1.GetHeader().size(), 1U);
    EXPECT_EQ(stream1.GetHeader()[0], "n");

    auto [stream2, qid2] = this->Prepare("UNWIND(range(4,6)) as n RETURN n");
    ASSERT_TRUE(qid2);
    ASSERT_EQ(stream2.GetHeader().size(), 1U);
    EXPECT_EQ(stream2.GetHeader()[0], "n");

    this->Pull(&stream1, 1, qid1);
    ASSERT_EQ(stream1.GetSummary().count("has_more"), 1);
    ASSERT_TRUE(stream1.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream1.GetResults().size(), 1U);
    ASSERT_EQ(stream1.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream1.GetResults()[0][0].ValueInt(), 1);

    auto [stream3, qid3] = this->Prepare("UNWIND(range(7,9)) as n RETURN n");
    ASSERT_TRUE(qid3);
    ASSERT_EQ(stream3.GetHeader().size(), 1U);
    EXPECT_EQ(stream3.GetHeader()[0], "n");

    this->Pull(&stream2, {}, qid2);
    ASSERT_EQ(stream2.GetSummary().count("has_more"), 1);
    ASSERT_FALSE(stream2.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream2.GetResults().size(), 3U);
    ASSERT_EQ(stream2.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream2.GetResults()[0][0].ValueInt(), 4);
    ASSERT_EQ(stream2.GetResults()[1][0].ValueInt(), 5);
    ASSERT_EQ(stream2.GetResults()[2][0].ValueInt(), 6);

    this->Pull(&stream3, 1, qid3);
    ASSERT_EQ(stream3.GetSummary().count("has_more"), 1);
    ASSERT_TRUE(stream3.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream3.GetResults().size(), 1U);
    ASSERT_EQ(stream3.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream3.GetResults()[0][0].ValueInt(), 7);

    this->Pull(&stream1, {}, qid1);
    ASSERT_EQ(stream1.GetSummary().count("has_more"), 1);
    ASSERT_FALSE(stream1.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream1.GetResults().size(), 3U);
    ASSERT_EQ(stream1.GetResults()[1].size(), 1U);
    ASSERT_EQ(stream1.GetResults()[1][0].ValueInt(), 2);
    ASSERT_EQ(stream1.GetResults()[2][0].ValueInt(), 3);

    this->Pull(&stream3);
    ASSERT_EQ(stream3.GetSummary().count("has_more"), 1);
    ASSERT_FALSE(stream3.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream3.GetResults().size(), 3U);
    ASSERT_EQ(stream3.GetResults()[1].size(), 1U);
    ASSERT_EQ(stream3.GetResults()[1][0].ValueInt(), 8);
    ASSERT_EQ(stream3.GetResults()[2][0].ValueInt(), 9);

    interpreter.CommitTransaction();
  }
}

namespace {
// copied from utils_csv_parsing.cpp - tmp dir management and csv file writer
class TmpDirManager final {
 public:
  explicit TmpDirManager(const std::string_view directory)
      : tmp_dir_{std::filesystem::temp_directory_path() / directory} {
    CreateDir();
  }
  ~TmpDirManager() { Clear(); }

  const std::filesystem::path &Path() const { return tmp_dir_; }

 private:
  std::filesystem::path tmp_dir_;

  void CreateDir() {
    if (!std::filesystem::exists(tmp_dir_)) {
      std::filesystem::create_directory(tmp_dir_);
    }
  }

  void Clear() {
    if (!std::filesystem::exists(tmp_dir_)) return;
    std::filesystem::remove_all(tmp_dir_);
  }
};

class FileWriter {
 public:
  explicit FileWriter(const std::filesystem::path path) { stream_.open(path); }

  FileWriter(const FileWriter &) = delete;
  FileWriter &operator=(const FileWriter &) = delete;

  FileWriter(FileWriter &&) = delete;
  FileWriter &operator=(FileWriter &&) = delete;

  void Close() { stream_.close(); }

  size_t WriteLine(const std::string_view line) {
    if (!stream_.is_open()) {
      return 0;
    }

    stream_ << line << std::endl;

    // including the newline character
    return line.size() + 1;
  }

 private:
  std::ofstream stream_;
};

std::string CreateRow(const std::vector<std::string> &columns, const std::string_view delim) {
  return memgraph::utils::Join(columns, delim);
}
}  // namespace

TYPED_TEST(InterpreterTest, LoadCsvClause) {
  auto dir_manager = TmpDirManager("csv_directory");
  const auto csv_path = dir_manager.Path() / "file.csv";
  auto writer = FileWriter(csv_path);

  const std::string delimiter{"|"};

  const std::vector<std::string> header{"A", "B", "C"};
  writer.WriteLine(CreateRow(header, delimiter));

  const std::vector<std::string> good_columns_1{"a", "b", "c"};
  writer.WriteLine(CreateRow(good_columns_1, delimiter));

  const std::vector<std::string> bad_columns{"\"\"1", "2", "3"};
  writer.WriteLine(CreateRow(bad_columns, delimiter));

  const std::vector<std::string> good_columns_2{"d", "e", "f"};
  writer.WriteLine(CreateRow(good_columns_2, delimiter));

  writer.Close();

  {
    const std::string query = fmt::format(R"(LOAD CSV FROM "{}" WITH HEADER IGNORE BAD DELIMITER "{}" AS x RETURN x.A)",
                                          csv_path.string(), delimiter);
    auto [stream, qid] = this->Prepare(query);
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "x.A");

    this->Pull(&stream, 1);
    ASSERT_EQ(stream.GetSummary().count("has_more"), 1);
    ASSERT_TRUE(stream.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueString(), "a");

    this->Pull(&stream, 1);
    ASSERT_EQ(stream.GetSummary().count("has_more"), 1);
    ASSERT_FALSE(stream.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream.GetResults().size(), 2U);
    ASSERT_EQ(stream.GetResults()[1][0].ValueString(), "d");
  }

  {
    const std::string query = fmt::format(R"(LOAD CSV FROM "{}" WITH HEADER IGNORE BAD DELIMITER "{}" AS x RETURN x.C)",
                                          csv_path.string(), delimiter);
    auto [stream, qid] = this->Prepare(query);
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "x.C");

    this->Pull(&stream);
    ASSERT_EQ(stream.GetSummary().count("has_more"), 1);
    ASSERT_FALSE(stream.GetSummary().at("has_more").ValueBool());
    ASSERT_EQ(stream.GetResults().size(), 2U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueString(), "c");
    ASSERT_EQ(stream.GetResults()[1][0].ValueString(), "f");
  }
}

TYPED_TEST(InterpreterTest, CacheableQueries) {
  // This should be cached
  {
    SCOPED_TRACE("Cacheable query");
    this->Interpret("RETURN 1");
    EXPECT_EQ(this->interpreter_context.ast_cache.size(), 1U);
    EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  }

  {
    SCOPED_TRACE("Uncacheable query");
    // Queries which are calling procedure should not be cached because the
    // result signature could be changed
    this->Interpret("CALL mg.load_all()");
    EXPECT_EQ(this->interpreter_context.ast_cache.size(), 1U);
    EXPECT_EQ(this->db->plan_cache()->size(), 1U);
  }
}

TYPED_TEST(InterpreterTest, AllowLoadCsvConfig) {
  const auto check_load_csv_queries = [&](const bool allow_load_csv) {
    TmpDirManager directory_manager{"allow_load_csv"};
    const auto csv_path = directory_manager.Path() / "file.csv";
    auto writer = FileWriter(csv_path);
    const std::vector<std::string> data{"A", "B", "C"};
    writer.WriteLine(CreateRow(data, ","));
    writer.Close();

    const std::array<std::string, 2> queries = {
        fmt::format("LOAD CSV FROM \"{}\" WITH HEADER AS row RETURN row", csv_path.string()),
        "CREATE TRIGGER trigger ON CREATE BEFORE COMMIT EXECUTE LOAD CSV FROM 'file.csv' WITH HEADER AS row RETURN "
        "row"};

    memgraph::storage::Config config2{};
    config2.durability.storage_directory = directory_manager.Path();
    config2.disk.main_storage_directory = config2.durability.storage_directory / "disk";
    if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
      config2.disk = disk_test_utils::GenerateOnDiskConfig(this->testSuiteCsv).disk;
      config2.force_on_disk = true;
    }

    memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk2(config2);
    auto db_acc_opt = db_gk2.access();
    ASSERT_TRUE(db_acc_opt) << "Failed to access db2";
    auto &db_acc = *db_acc_opt;
    ASSERT_TRUE(db_acc->GetStorageMode() == (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>
                                                 ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                                 : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL))
        << "Wrong storage mode!";

    memgraph::query::InterpreterContext csv_interpreter_context{{.query = {.allow_load_csv = allow_load_csv}}, nullptr};
    InterpreterFaker interpreter_faker{&csv_interpreter_context, db_acc};
    for (const auto &query : queries) {
      if (allow_load_csv) {
        SCOPED_TRACE(fmt::format("'{}' should not throw because LOAD CSV is allowed", query));
        ASSERT_NO_THROW(interpreter_faker.Interpret(query));
      } else {
        SCOPED_TRACE(fmt::format("'{}' should throw becuase LOAD CSV is not allowed", query));
        ASSERT_THROW(interpreter_faker.Interpret(query), memgraph::utils::BasicException);
      }
      SCOPED_TRACE(fmt::format("Normal query should not throw (allow_load_csv: {})", allow_load_csv));
      ASSERT_NO_THROW(interpreter_faker.Interpret("RETURN 1"));
    }
  };

  check_load_csv_queries(true);
  check_load_csv_queries(false);
}

void AssertAllValuesAreZero(const std::map<std::string, memgraph::communication::bolt::Value> &map,
                            const std::vector<std::string> &exceptions) {
  for (const auto &[key, value] : map) {
    if (const auto it = std::find(exceptions.begin(), exceptions.end(), key); it != exceptions.end()) continue;
    ASSERT_EQ(value.ValueInt(), 0);
  }
}

TYPED_TEST(InterpreterTest, ExecutionStatsIsValid) {
  {
    auto [stream, qid] = this->Prepare("MATCH (n) DELETE n;");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("stats"), 0);
  }
  {
    std::array stats_keys{"nodes-created",  "nodes-deleted", "relationships-created", "relationships-deleted",
                          "properties-set", "labels-added",  "labels-removed"};
    auto [stream, qid] = this->Prepare("CREATE ();");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("stats"), 1);
    ASSERT_TRUE(stream.GetSummary().at("stats").IsMap());
    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_TRUE(
        std::all_of(stats_keys.begin(), stats_keys.end(), [&stats](const auto &key) { return stats.contains(key); }));
    AssertAllValuesAreZero(stats, {"nodes-created"});
  }
}

TYPED_TEST(InterpreterTest, ExecutionStatsValues) {
  {
    auto [stream, qid] = this->Prepare("CREATE (),(),(),();");

    this->Pull(&stream);
    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_EQ(stats["nodes-created"].ValueInt(), 4);
    AssertAllValuesAreZero(stats, {"nodes-created"});
  }
  {
    auto [stream, qid] = this->Prepare("MATCH (n) DELETE n;");
    this->Pull(&stream);

    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_EQ(stats["nodes-deleted"].ValueInt(), 4);
    AssertAllValuesAreZero(stats, {"nodes-deleted"});
  }
  {
    auto [stream, qid] = this->Prepare("CREATE (n)-[:TO]->(m), (n)-[:TO]->(m), (n)-[:TO]->(m);");
    this->Pull(&stream);

    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_EQ(stats["nodes-created"].ValueInt(), 2);
    ASSERT_EQ(stats["relationships-created"].ValueInt(), 3);
    AssertAllValuesAreZero(stats, {"nodes-created", "relationships-created"});
  }
  {
    auto [stream, qid] = this->Prepare("MATCH (n) DETACH DELETE n;");
    this->Pull(&stream);

    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_EQ(stats["nodes-deleted"].ValueInt(), 2);
    ASSERT_EQ(stats["relationships-deleted"].ValueInt(), 3);
    AssertAllValuesAreZero(stats, {"nodes-deleted", "relationships-deleted"});
  }
  {
    auto [stream, qid] = this->Prepare("CREATE (n)-[:TO]->(m);");
    this->Pull(&stream);

    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_EQ(stats["nodes-created"].ValueInt(), 2);
    ASSERT_EQ(stats["relationships-created"].ValueInt(), 1);
    AssertAllValuesAreZero(stats, {"nodes-created", "relationships-created"});
  }
  {
    auto [stream, qid] = this->Prepare("MATCH (n)-[r]->(m) DELETE r;");
    this->Pull(&stream);

    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_EQ(stats["relationships-deleted"].ValueInt(), 1);
    AssertAllValuesAreZero(stats, {"nodes-deleted", "relationships-deleted"});
  }
  {
    auto [stream, qid] = this->Prepare("MATCH (n) DELETE n;");
    this->Pull(&stream);

    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_EQ(stats["nodes-deleted"].ValueInt(), 2);
    AssertAllValuesAreZero(stats, {"nodes-deleted", ""});
  }
  {
    auto [stream, qid] = this->Prepare("CREATE (:L1:L2:L3), (:L1), (:L1), (:L2);");
    this->Pull(&stream);

    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_EQ(stats["nodes-created"].ValueInt(), 4);
    ASSERT_EQ(stats["labels-added"].ValueInt(), 6);
    AssertAllValuesAreZero(stats, {"nodes-created", "labels-added"});
  }
  {
    auto [stream, qid] = this->Prepare("MATCH (n:L1) SET n.name='test';");
    this->Pull(&stream);

    auto stats = stream.GetSummary().at("stats").ValueMap();
    ASSERT_EQ(stats["properties-set"].ValueInt(), 3);
    AssertAllValuesAreZero(stats, {"properties-set"});
  }
}

TYPED_TEST(InterpreterTest, ExecutionStatsValuesPropertiesSet) {
  {
    auto [stream, qid] = this->Prepare(
        "CREATE (u:Employee {Uid: 'EMP_AAAAA', FirstName: 'Bong', LastName: 'Revilla'}) RETURN u.name AS name;");
    this->Pull(&stream);
  }
  {
    auto [stream, qid] = this->Prepare(
        "MATCH (node:Employee) WHERE node.Uid='EMP_AAAAA' SET node={FirstName: 'James', LastName: 'Revilla', Uid: "
        "'EMP_AAAAA', CreatedOn: 'null', CreatedBy: 'null', LastModifiedOn: '1698226931701', LastModifiedBy: 'null', "
        "Description: 'null'};");
    this->Pull(&stream);
    auto stats = stream.GetSummary().at("stats").ValueMap();
    auto key = memgraph::query::ExecutionStatsKeyToString(memgraph::query::ExecutionStats::Key::UPDATED_PROPERTIES);
    ASSERT_EQ(stats[key].ValueInt(), 8);
  }
}

TYPED_TEST(InterpreterTest, NotificationsValidStructure) {
  {
    auto [stream, qid] = this->Prepare("MATCH (n) DELETE n;");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 0);
  }
  {
    auto [stream, qid] = this->Prepare("CREATE INDEX ON :Person(id);");
    this->Pull(&stream);

    // Assert notifications list
    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    ASSERT_TRUE(stream.GetSummary().at("notifications").IsList());
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    // Assert one notification structure
    ASSERT_EQ(notifications.size(), 1);
    ASSERT_TRUE(notifications[0].IsMap());
    auto notification = notifications[0].ValueMap();
    ASSERT_TRUE(notification.contains("severity"));
    ASSERT_TRUE(notification.contains("code"));
    ASSERT_TRUE(notification.contains("title"));
    ASSERT_TRUE(notification.contains("description"));
    ASSERT_TRUE(notification["severity"].IsString());
    ASSERT_TRUE(notification["code"].IsString());
    ASSERT_TRUE(notification["title"].IsString());
    ASSERT_TRUE(notification["description"].IsString());
  }
}

TYPED_TEST(InterpreterTest, IndexInfoNotifications) {
  {
    auto [stream, qid] = this->Prepare("CREATE INDEX ON :Person;");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "CreateIndex");
    ASSERT_EQ(notification["title"].ValueString(), "Created index on label Person on properties .");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("CREATE INDEX ON :Person(id);");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "CreateIndex");
    ASSERT_EQ(notification["title"].ValueString(), "Created index on label Person on properties id.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("CREATE INDEX ON :Person(id);");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "IndexAlreadyExists");
    ASSERT_EQ(notification["title"].ValueString(), "Index on label Person on properties id already exists.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("DROP INDEX ON :Person(id);");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "DropIndex");
    ASSERT_EQ(notification["title"].ValueString(), "Dropped index on label Person on properties id.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("DROP INDEX ON :Person(id);");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "IndexDoesNotExist");
    ASSERT_EQ(notification["title"].ValueString(), "Index on label Person on properties id doesn't exist.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
}

TYPED_TEST(InterpreterTest, ConstraintUniqueInfoNotifications) {
  {
    auto [stream, qid] = this->Prepare("CREATE CONSTRAINT ON (n:Person) ASSERT n.email, n.id IS UNIQUE;");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "CreateConstraint");
    ASSERT_EQ(notification["title"].ValueString(),
              "Created UNIQUE constraint on label Person on properties email, id.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("CREATE CONSTRAINT ON (n:Person) ASSERT n.email, n.id IS UNIQUE;");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "ConstraintAlreadyExists");
    ASSERT_EQ(notification["title"].ValueString(),
              "Constraint UNIQUE on label Person on properties email, id already exists.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("DROP CONSTRAINT ON (n:Person) ASSERT n.email, n.id IS UNIQUE;");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "DropConstraint");
    ASSERT_EQ(notification["title"].ValueString(),
              "Dropped UNIQUE constraint on label Person on properties email, id.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("DROP CONSTRAINT ON (n:Person) ASSERT n.email, n.id IS UNIQUE;");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "ConstraintDoesNotExist");
    ASSERT_EQ(notification["title"].ValueString(),
              "Constraint UNIQUE on label Person on properties email, id doesn't exist.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
}

TYPED_TEST(InterpreterTest, ConstraintExistsInfoNotifications) {
  {
    auto [stream, qid] = this->Prepare("CREATE CONSTRAINT ON (n:L1) ASSERT EXISTS (n.name);");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "CreateConstraint");
    ASSERT_EQ(notification["title"].ValueString(), "Created EXISTS constraint on label L1 on properties name.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("CREATE CONSTRAINT ON (n:L1) ASSERT EXISTS (n.name);");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "ConstraintAlreadyExists");
    ASSERT_EQ(notification["title"].ValueString(), "Constraint EXISTS on label L1 on properties name already exists.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("DROP CONSTRAINT ON (n:L1) ASSERT EXISTS (n.name);");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "DropConstraint");
    ASSERT_EQ(notification["title"].ValueString(), "Dropped EXISTS constraint on label L1 on properties name.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("DROP CONSTRAINT ON (n:L1) ASSERT EXISTS (n.name);");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "ConstraintDoesNotExist");
    ASSERT_EQ(notification["title"].ValueString(), "Constraint EXISTS on label L1 on properties name doesn't exist.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
}

TYPED_TEST(InterpreterTest, TriggerInfoNotifications) {
  {
    auto [stream, qid] = this->Prepare(
        "CREATE TRIGGER bestTriggerEver ON  CREATE AFTER COMMIT EXECUTE "
        "CREATE ();");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "CreateTrigger");
    ASSERT_EQ(notification["title"].ValueString(), "Created trigger bestTriggerEver.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
  {
    auto [stream, qid] = this->Prepare("DROP TRIGGER bestTriggerEver;");
    this->Pull(&stream);

    ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
    auto notifications = stream.GetSummary().at("notifications").ValueList();

    auto notification = notifications[0].ValueMap();
    ASSERT_EQ(notification["severity"].ValueString(), "INFO");
    ASSERT_EQ(notification["code"].ValueString(), "DropTrigger");
    ASSERT_EQ(notification["title"].ValueString(), "Dropped trigger bestTriggerEver.");
    ASSERT_EQ(notification["description"].ValueString(), "");
  }
}

TYPED_TEST(InterpreterTest, LoadCsvClauseNotification) {
  auto dir_manager = TmpDirManager("csv_directory");
  const auto csv_path = dir_manager.Path() / "file.csv";
  auto writer = FileWriter(csv_path);

  const std::string delimiter{"|"};

  const std::vector<std::string> header{"A", "B", "C"};
  writer.WriteLine(CreateRow(header, delimiter));

  const std::vector<std::string> good_columns_1{"a", "b", "c"};
  writer.WriteLine(CreateRow(good_columns_1, delimiter));

  writer.Close();

  const std::string query = fmt::format(R"(LOAD CSV FROM "{}" WITH HEADER IGNORE BAD DELIMITER "{}" AS x RETURN x;)",
                                        csv_path.string(), delimiter);
  auto [stream, qid] = this->Prepare(query);
  this->Pull(&stream);

  ASSERT_EQ(stream.GetSummary().count("notifications"), 1);
  auto notifications = stream.GetSummary().at("notifications").ValueList();

  auto notification = notifications[0].ValueMap();
  ASSERT_EQ(notification["severity"].ValueString(), "INFO");
  ASSERT_EQ(notification["code"].ValueString(), "LoadCSVTip");
  ASSERT_EQ(notification["title"].ValueString(),
            "It's important to note that the parser parses the values as strings. It's up to the user to "
            "convert the parsed row values to the appropriate type. This can be done using the built-in "
            "conversion functions such as ToInteger, ToFloat, ToBoolean etc.");
  ASSERT_EQ(notification["description"].ValueString(), "");
}
