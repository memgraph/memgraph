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

/// The queries the property cache is allowed to change the plan of, plus the ones it must leave alone. Each fixture
/// gets a fresh plan cache, so the flag has to be compared across two tests rather than toggled inside one.
#define CHECK_CACHE_PROPERTIES_RESULTS()                                                                       \
  do {                                                                                                         \
    using BoltValue = memgraph::communication::bolt::Value;                                                    \
    this->Execute("CREATE (:P {id: 1, a: 1, b: 'x'}), (:P {id: 2, a: 2})");                                    \
    {                                                                                                          \
      /* Two properties on one symbol, the second missing on one of the nodes. */                              \
      auto results = this->Execute("MATCH (n:P) RETURN n.a AS a, n.b AS b ORDER BY a");                        \
      ASSERT_EQ(results.size(), 2);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                  \
      EXPECT_EQ(results[0][1].ValueString(), "x");                                                             \
      EXPECT_EQ(results[1][0].ValueInt(), 2);                                                                  \
      EXPECT_EQ(results[1][1].type(), BoltValue::Type::Null);                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* A single lookup must not be cached, but must still answer the same. */                                \
      auto results = this->Execute("MATCH (n:P) RETURN n.a AS a ORDER BY a");                                  \
      ASSERT_EQ(results.size(), 2);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                  \
      EXPECT_EQ(results[1][0].ValueInt(), 2);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* Lookups spread over two symbols are out of scope for the rewrite. */                                  \
      auto results = this->Execute("MATCH (n:P), (m:P) RETURN n.id AS x, m.a AS y ORDER BY x, y");             \
      ASSERT_EQ(results.size(), 4);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                  \
      EXPECT_EQ(results[0][1].ValueInt(), 1);                                                                  \
      EXPECT_EQ(results[3][0].ValueInt(), 2);                                                                  \
      EXPECT_EQ(results[3][1].ValueInt(), 2);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* The cached symbol is Null for every row here. */                                                      \
      auto results =                                                                                           \
          this->Execute("MATCH (n:P) OPTIONAL MATCH (n)-[:R]->(o) RETURN o.a AS a, o.b AS b ORDER BY n.id");   \
      ASSERT_EQ(results.size(), 2);                                                                            \
      for (const auto &row : results) {                                                                        \
        EXPECT_EQ(row[0].type(), BoltValue::Type::Null);                                                       \
        EXPECT_EQ(row[1].type(), BoltValue::Type::Null);                                                       \
      }                                                                                                        \
    }                                                                                                          \
    {                                                                                                          \
      /* Two aggregation arguments on one symbol, evaluated by the Aggregate rather than the Produce. */       \
      auto results = this->Execute("MATCH (n:P) RETURN sum(n.id) AS s, sum(n.a) AS t");                        \
      ASSERT_EQ(results.size(), 1);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 3);                                                                  \
      EXPECT_EQ(results[0][1].ValueInt(), 3);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* Group-by expression plus an aggregation argument, over a property one node lacks. */                  \
      auto results = this->Execute("MATCH (n:P) RETURN n.id AS id, count(n.b) AS c ORDER BY id");              \
      ASSERT_EQ(results.size(), 2);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                  \
      EXPECT_EQ(results[0][1].ValueInt(), 1);                                                                  \
      EXPECT_EQ(results[1][0].ValueInt(), 2);                                                                  \
      EXPECT_EQ(results[1][1].ValueInt(), 0);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* count(*) carries no argument, so the one remaining lookup must not be cached. */                      \
      auto results = this->Execute("MATCH (n:P) RETURN count(*) AS c, sum(n.a) AS s");                         \
      ASSERT_EQ(results.size(), 1);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 2);                                                                  \
      EXPECT_EQ(results[0][1].ValueInt(), 3);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* Both rewrites fire on one plan: a cache beneath the Aggregate and another beneath the Produce. */     \
      auto results = this->Execute("MATCH (n:P) RETURN n.id + n.a AS k, sum(n.a) AS s ORDER BY k");            \
      ASSERT_EQ(results.size(), 2);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 2);                                                                  \
      EXPECT_EQ(results[0][1].ValueInt(), 1);                                                                  \
      EXPECT_EQ(results[1][0].ValueInt(), 4);                                                                  \
      EXPECT_EQ(results[1][1].ValueInt(), 2);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* DISTINCT dedupes on the argument value, which the cache must not change. */                           \
      auto results = this->Execute("MATCH (n:P) RETURN count(DISTINCT n.a) AS c, sum(n.id) AS s");             \
      ASSERT_EQ(results.size(), 1);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 2);                                                                  \
      EXPECT_EQ(results[0][1].ValueInt(), 3);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* A lookup reachable only through a conditional must not be cached, and must still answer. */           \
      auto results =                                                                                           \
          this->Execute("MATCH (n:P) RETURN sum(n.id) AS s, sum(CASE WHEN n.a > 1 THEN 10 ELSE 20 END) AS t"); \
      ASSERT_EQ(results.size(), 1);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 3);                                                                  \
      EXPECT_EQ(results[0][1].ValueInt(), 30);                                                                 \
    }                                                                                                          \
    {                                                                                                          \
      /* Aggregating over an optional match whose symbol is Null on every row. */                              \
      auto results =                                                                                           \
          this->Execute("MATCH (n:P) OPTIONAL MATCH (n)-[:R]->(o) RETURN count(o.a) AS c, count(o.b) AS d");   \
      ASSERT_EQ(results.size(), 1);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 0);                                                                  \
      EXPECT_EQ(results[0][1].ValueInt(), 0);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* A row reading what this same transaction just wrote. The values live in the delta chain rather        \
       * than in the record, which is the one case a read cannot build straight into the frame and must        \
       * replay storage values first. Getting that boundary wrong returns the pre-write value.                 \
       * Idempotent, so running this macro once per flag setting asserts the same thing twice. */              \
      auto results = this->Execute("MATCH (n:P) SET n.w = n.id * 100 RETURN n.w AS w, n.id AS i ORDER BY i");  \
      ASSERT_EQ(results.size(), 2);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 100);                                                                \
      EXPECT_EQ(results[0][1].ValueInt(), 1);                                                                  \
      EXPECT_EQ(results[1][0].ValueInt(), 200);                                                                \
      EXPECT_EQ(results[1][1].ValueInt(), 2);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* An edge read twice: a relationship holds a record like a vertex does, and reads from it the same      \
       * way. Idempotent for the same reason as the write above. */                                            \
      this->Execute("MATCH (x:P {id: 1}), (y:P {id: 2}) MERGE (x)-[:E {p: 7, q: 9}]->(y)");                    \
      auto results = this->Execute("MATCH (:P)-[e:E]->(:P) RETURN e.p AS p, e.q AS q");                        \
      ASSERT_EQ(results.size(), 1);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 7);                                                                  \
      EXPECT_EQ(results[0][1].ValueInt(), 9);                                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* An edge property the relationship does not carry has to read back as Null, not as a stale slot. */    \
      auto results = this->Execute("MATCH (:P)-[e:E]->(:P) RETURN e.p AS p, e.absent AS a");                   \
      ASSERT_EQ(results.size(), 1);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 7);                                                                  \
      EXPECT_EQ(results[0][1].type(), BoltValue::Type::Null);                                                  \
    }                                                                                                          \
    {                                                                                                          \
      /* The same for a vertex, and read once so the cache does not serve it: a lookup the operator            \
       * skips reads through the single-property path, where a property the record does not carry is           \
       * the one case that produces a value without decoding anything. */                                      \
      auto results = this->Execute("MATCH (n:P) RETURN n.id AS i, n.absent AS a ORDER BY i");                  \
      ASSERT_EQ(results.size(), 2);                                                                            \
      EXPECT_EQ(results[0][0].ValueInt(), 1);                                                                  \
      EXPECT_EQ(results[0][1].type(), BoltValue::Type::Null);                                                  \
      EXPECT_EQ(results[1][1].type(), BoltValue::Type::Null);                                                  \
    }                                                                                                          \
  } while (false)

TYPED_TEST(QueryExecution, CachePropertiesResultsWithFlagOff) {
  CachePropertiesFlagGuard guard{false};
  CHECK_CACHE_PROPERTIES_RESULTS();
}

TYPED_TEST(QueryExecution, CachePropertiesResultsWithFlagOn) {
  CachePropertiesFlagGuard guard{true};
  CHECK_CACHE_PROPERTIES_RESULTS();
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
  {
    // The Produce above an Aggregate reads only the aggregate's outputs, so this can only come from the Aggregate.
    CachePropertiesFlagGuard guard{true};
    auto const plan = explained("EXPLAIN MATCH (n:P) RETURN sum(n.a) AS e, sum(n.b) AS f");
    auto const cache_at = plan.find("CacheProperties");
    ASSERT_NE(cache_at, std::string::npos);
    // Plans print root first, so the cache appearing after the Aggregate is the cache sitting beneath it.
    EXPECT_LT(plan.find("Aggregate"), cache_at);
  }
}
