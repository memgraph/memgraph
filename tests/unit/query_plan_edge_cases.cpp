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

#include <filesystem>
#include <memory>
#include <optional>

#include "disk_test_utils.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cache_properties_flag_guard.hpp"
#include "communication/result_stream_faker.hpp"
#include "query/auth_checker.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/stream/streams.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

DECLARE_bool(query_cost_planner);
DECLARE_bool(query_cache_properties);

template <typename StorageType>
class QueryExecution : public testing::Test {
 protected:
  const std::string testSuite = "query_plan_edge_cases";
  std::optional<memgraph::dbms::DatabaseAccess> db_acc_;
  std::optional<memgraph::query::InterpreterContext> interpreter_context_;
  std::optional<memgraph::query::AllowEverythingAuthChecker> auth_checker_;
  std::optional<memgraph::query::Interpreter> interpreter_;

  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_query_plan_edge_cases"};

  std::optional<memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock>>
      repl_state;
  std::optional<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> db_gk;
  std::optional<memgraph::system::System> system_state;

  void SetUp() override {
    auto config = [&]() {
      memgraph::storage::Config config{};
      config.durability.storage_directory = data_directory;
      config.disk.main_storage_directory = config.durability.storage_directory / "disk";
      if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
        config.disk = disk_test_utils::GenerateOnDiskConfig(testSuite).disk;
        config.force_on_disk = true;
      }
      return config;
    }();  // iile

    repl_state.emplace(memgraph::storage::ReplicationStateRootPath(config));
    db_gk.emplace(config);
    auto db_acc_opt = db_gk->access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;
    MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                               ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                               : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
              "Wrong storage mode!");
    db_acc_ = std::move(db_acc);
    system_state.emplace();
    interpreter_context_.emplace(memgraph::query::InterpreterConfig{},
                                 nullptr,
                                 nullptr,
                                 nullptr,
                                 &repl_state.value(),
                                 *system_state,
                                 nullptr
#ifdef MG_ENTERPRISE
                                 ,
                                 nullptr,
                                 nullptr
#endif
    );
    auth_checker_.emplace();
    interpreter_.emplace(&*interpreter_context_, *db_acc_);
    interpreter_->SetUser(auth_checker_->GenQueryUser(std::nullopt, {}));
  }

  void TearDown() override {
    interpreter_ = std::nullopt;
    auth_checker_.reset();
    interpreter_context_ = std::nullopt;
    system_state.reset();
    db_acc_.reset();
    db_gk.reset();
    repl_state.reset();
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    std::filesystem::remove_all(data_directory);
  }

  /**
   * Execute the given query and commit the transaction.
   *
   * Return the query results.
   */
  auto Execute(const std::string &query) {
    ResultStreamFaker stream(this->db_acc_->get()->storage());

    auto [header, _1, qid, _2] = interpreter_->Prepare(query, memgraph::query::no_params_fn, {});
    stream.Header(header);
    auto summary = interpreter_->PullAll(&stream);
    stream.Summary(summary);

    return stream.GetResults();
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
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

/// The queries the property cache is allowed to change the plan of, plus the ones it must leave alone. Each fixture
/// gets a fresh plan cache, so the flag has to be compared across two tests rather than toggled inside one.
#define CHECK_CACHE_PROPERTIES_RESULTS()                                                                     \
  do {                                                                                                       \
    using BoltValue = memgraph::communication::bolt::Value;                                                  \
    this->Execute("CREATE (:P {id: 1, a: 1, b: 'x'}), (:P {id: 2, a: 2})");                                  \
    {                                                                                                        \
      /* Two properties on one symbol, the second missing on one of the nodes. */                            \
      auto results = this->Execute("MATCH (n:P) RETURN n.a AS a, n.b AS b ORDER BY a");                      \
      ASSERT_EQ(results.size(), 2);                                                                          \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                \
      EXPECT_EQ(results[0][1].ValueString(), "x");                                                           \
      EXPECT_EQ(results[1][0].ValueInt(), 2);                                                                \
      EXPECT_EQ(results[1][1].type(), BoltValue::Type::Null);                                                \
    }                                                                                                        \
    {                                                                                                        \
      /* A single lookup must not be cached, but must still answer the same. */                              \
      auto results = this->Execute("MATCH (n:P) RETURN n.a AS a ORDER BY a");                                \
      ASSERT_EQ(results.size(), 2);                                                                          \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                \
      EXPECT_EQ(results[1][0].ValueInt(), 2);                                                                \
    }                                                                                                        \
    {                                                                                                        \
      /* One lookup each is not worth a pass of its own, whether or not they share a symbol. */              \
      auto results = this->Execute("MATCH (n:P), (m:P) RETURN n.id AS x, m.a AS y ORDER BY x, y");           \
      ASSERT_EQ(results.size(), 4);                                                                          \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                \
      EXPECT_EQ(results[0][1].ValueInt(), 1);                                                                \
      EXPECT_EQ(results[3][0].ValueInt(), 2);                                                                \
      EXPECT_EQ(results[3][1].ValueInt(), 2);                                                                \
    }                                                                                                        \
    {                                                                                                        \
      /* The cached symbol is Null for every row here. */                                                    \
      auto results =                                                                                         \
          this->Execute("MATCH (n:P) OPTIONAL MATCH (n)-[:R]->(o) RETURN o.a AS a, o.b AS b ORDER BY n.id"); \
      ASSERT_EQ(results.size(), 2);                                                                          \
      for (const auto &row : results) {                                                                      \
        EXPECT_EQ(row[0].type(), BoltValue::Type::Null);                                                     \
        EXPECT_EQ(row[1].type(), BoltValue::Type::Null);                                                     \
      }                                                                                                      \
    }                                                                                                        \
  } while (false)

TYPED_TEST(QueryExecution, CachePropertiesResultsWithFlagOff) {
  CachePropertiesFlagGuard guard{false};
  CHECK_CACHE_PROPERTIES_RESULTS();
}

TYPED_TEST(QueryExecution, CachePropertiesResultsWithFlagOn) {
  CachePropertiesFlagGuard guard{true};
  CHECK_CACHE_PROPERTIES_RESULTS();
}

TYPED_TEST(QueryExecution, CachePropertiesReadsARelationshipsProperties) {
  // A relationship keeps its properties in the same store a node does, so a projection of two of
  // them is the same shape the rewrite is for. The second is missing on one relationship, so the
  // slots have to answer Null there rather than drop the column.
  CachePropertiesFlagGuard guard{true};
  using BoltValue = memgraph::communication::bolt::Value;

  this->Execute("CREATE (:P)-[:R {a: 1, b: 'x'}]->(:P), (:P)-[:R {a: 2}]->(:P)");

  auto results = this->Execute("MATCH ()-[r:R]->() RETURN r.a AS a, r.b AS b ORDER BY a");
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0][0].ValueInt(), 1);
  EXPECT_EQ(results[0][1].ValueString(), "x");
  EXPECT_EQ(results[1][0].ValueInt(), 2);
  EXPECT_EQ(results[1][1].type(), BoltValue::Type::Null);
}

TYPED_TEST(QueryExecution, CachePropertiesReachesTheRealPlanForARelationship) {
  // Guards the test above from passing because the rewrite never fired. The two runs must not share
  // a plan cache entry, so they differ in the returned column names.
  auto explained = [this](const std::string &query) {
    auto results = this->Execute(query);
    std::string joined;
    for (const auto &row : results) joined += row[0].ValueString();
    return joined;
  };

  {
    CachePropertiesFlagGuard guard{false};
    EXPECT_EQ(explained("EXPLAIN MATCH ()-[r:R]->() RETURN r.a AS a, r.b AS b").find("CacheProperties"),
              std::string::npos);
  }
  {
    CachePropertiesFlagGuard guard{true};
    EXPECT_NE(explained("EXPLAIN MATCH ()-[r:R]->() RETURN r.a AS c, r.b AS d").find("CacheProperties"),
              std::string::npos);
  }
}

TYPED_TEST(QueryExecution, CachePropertiesUnderASunkOrderByWithALimit) {
  // An ORDER BY with a LIMIT sinks below the projection so that only the rows it keeps are
  // projected, and it has to carry forward everything above it still reads. The cache reads the
  // record, so what it needs carried is the symbol itself rather than the properties the
  // projection used to read. An omission here does not fail the query: the rows come back Null.
  CachePropertiesFlagGuard guard{true};

  this->Execute("CREATE (:P {id: 1, a: 10, b: 'x'}), (:P {id: 2, a: 20, b: 'y'}), (:P {id: 3, a: 30, b: 'z'})");

  auto results = this->Execute("MATCH (n:P) RETURN n.a AS a, n.b AS b ORDER BY n.id LIMIT 2");
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0][0].ValueInt(), 10);
  EXPECT_EQ(results[0][1].ValueString(), "x");
  EXPECT_EQ(results[1][0].ValueInt(), 20);
  EXPECT_EQ(results[1][1].ValueString(), "y");
}

TYPED_TEST(QueryExecution, CachePropertiesGroupsTwoSymbols) {
  // Two symbols are two records and so two reads whichever way the plan is built. Gathering them
  // into one operator keeps that to one pull per row, and a symbol with only one lookup of its own
  // is left alone, since one pass for one property is what a lookup already does.
  CachePropertiesFlagGuard guard{true};
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

// AND stops on a false first operand and OR on a true one, so whether the second is read at all is
// decided while the row is evaluated. A pass reads what it was asked for before that happens, so a
// lookup reachable only through one of them is left to the lookup.
TYPED_TEST(QueryExecution, CachePropertiesLeavesLookupsBehindAShortCircuit) {
  this->Execute("CREATE (:Q {a: 1, b: 2}), (:G {t: true})");

  auto explained = [this](const std::string &query) {
    auto results = this->Execute(query);
    std::string joined;
    for (const auto &row : results) joined += row[0].ValueString();
    return joined;
  };

  CachePropertiesFlagGuard guard{true};
  // The first operand is read from another node, so it is not folded away before planning.
  EXPECT_EQ(explained("EXPLAIN MATCH (n:Q), (g:G) RETURN g.t OR n.a AS x, g.t OR n.b AS y").find("CacheProperties"),
            std::string::npos);
  EXPECT_EQ(explained("EXPLAIN MATCH (n:Q), (g:G) RETURN g.t AND n.a AS p, g.t AND n.b AS q").find("CacheProperties"),
            std::string::npos);

  // A projection reading the same properties outside a short circuit still gets its pass.
  EXPECT_NE(explained("EXPLAIN MATCH (n:Q) RETURN n.a AS r, n.b AS s").find("CacheProperties"), std::string::npos);
}

// The body of a reduce or an extract runs once per element of its list, so an empty or Null list
// runs it no times. A lookup there is conditional in the same way as one behind an AND or an OR.
TYPED_TEST(QueryExecution, CachePropertiesLeavesLookupsInsideAListBody) {
  this->Execute("CREATE (:Q {a: 1, b: 2})");

  auto explained = [this](const std::string &query) {
    std::string joined;
    for (const auto &row : this->Execute(query)) joined += row[0].ValueString();
    return joined;
  };

  CachePropertiesFlagGuard guard{true};
  EXPECT_EQ(explained("EXPLAIN MATCH (n:Q) RETURN reduce(s = 0, x IN [] | s + n.a) AS u, "
                      "reduce(s = 0, y IN [] | s + n.b) AS v")
                .find("CacheProperties"),
            std::string::npos);
  EXPECT_EQ(explained("EXPLAIN MATCH (n:Q) RETURN extract(x IN [] | n.a) AS p, extract(y IN [] | n.b) AS q")
                .find("CacheProperties"),
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
  CachePropertiesFlagGuard guard{true};

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
  CachePropertiesFlagGuard guard{true};

  this->Execute("CREATE (:Person)-[:PAID {amount: 10, fee: 1}]->(:Person)");

  EXPECT_THROW(this->Execute("MATCH (p:Person) RETURN "
                             "COLLECT { MATCH (p)-[r:PAID*1..2]->(q) RETURN r.amount + r.fee } AS c"),
               memgraph::query::QueryRuntimeException);
}

TYPED_TEST(QueryExecution, CachePropertiesReadsAGroupWhoseOrderIsNotTheRecordsOrder) {
  // One pass over a record finds each property from where the last one stopped, so it cannot go
  // back for one it has already passed. A projection names its properties in whatever order the
  // query was written in, which has nothing to do with the order the record holds them. Asking both
  // ways means one of these two is against the record's order whichever way the names interned.
  CachePropertiesFlagGuard guard{true};

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

TYPED_TEST(QueryExecution, CachePropertiesGroupsTwoSymbolsInTheRealPlan) {
  // Guards the test above from passing because the rewrite never fired.
  auto explained = [this](const std::string &query) {
    auto results = this->Execute(query);
    std::string joined;
    for (const auto &row : results) joined += row[0].ValueString();
    return joined;
  };

  {
    CachePropertiesFlagGuard guard{false};
    EXPECT_EQ(
        explained("EXPLAIN MATCH (n:P), (m:P) RETURN n.a AS a, n.b AS b, m.a AS c, m.b AS d").find("CacheProperties"),
        std::string::npos);
  }
  {
    CachePropertiesFlagGuard guard{true};
    EXPECT_NE(
        explained("EXPLAIN MATCH (n:P), (m:P) RETURN n.a AS e, n.b AS f, m.a AS g, m.b AS h").find("CacheProperties"),
        std::string::npos);
  }
}

TYPED_TEST(QueryExecution, CachePropertiesReachesTheRealPlan) {
  // Guards the results tests above from passing because the rewrite never fired.
  // The two runs must not share a plan cache entry, so they differ in the returned column names.
  auto explained = [this](const std::string &query) {
    auto results = this->Execute(query);
    std::string joined;
    for (const auto &row : results) joined += row[0].ValueString();
    return joined;
  };

  {
    CachePropertiesFlagGuard guard{false};
    EXPECT_EQ(explained("EXPLAIN MATCH (n:P) RETURN n.a AS a, n.b AS b").find("CacheProperties"), std::string::npos);
  }
  {
    CachePropertiesFlagGuard guard{true};
    EXPECT_NE(explained("EXPLAIN MATCH (n:P) RETURN n.a AS c, n.b AS d").find("CacheProperties"), std::string::npos);
  }
}
