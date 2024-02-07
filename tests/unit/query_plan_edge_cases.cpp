// Copyright 2024 Memgraph Ltd.
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

#include "communication/result_stream_faker.hpp"
#include "query/auth_checker.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/stream/streams.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

DECLARE_bool(query_cost_planner);

template <typename StorageType>
class QueryExecution : public testing::Test {
 protected:
  const std::string testSuite = "query_plan_edge_cases";
  std::optional<memgraph::dbms::DatabaseAccess> db_acc_;
  std::optional<memgraph::query::InterpreterContext> interpreter_context_;
  std::optional<memgraph::query::AllowEverythingAuthChecker> auth_checker_;
  std::optional<memgraph::query::Interpreter> interpreter_;

  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_query_plan_edge_cases"};

  std::optional<memgraph::replication::ReplicationState> repl_state;
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
    db_gk.emplace(config, *repl_state);
    auto db_acc_opt = db_gk->access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;
    MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                               ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                               : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
              "Wrong storage mode!");
    db_acc_ = std::move(db_acc);
    system_state.emplace();
    interpreter_context_.emplace(memgraph::query::InterpreterConfig{}, nullptr, &repl_state.value(), *system_state
#ifdef MG_ENTERPRISE
                                 ,
                                 nullptr
#endif
    );
    auth_checker_.emplace();
    interpreter_.emplace(&*interpreter_context_, *db_acc_);
    interpreter_->SetUser(auth_checker_->GenQueryUser(std::nullopt));
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

    auto [header, _1, qid, _2] = interpreter_->Prepare(query, {}, {});
    stream.Header(header);
    auto summary = interpreter_->PullAll(&stream);
    stream.Summary(summary);

    return stream.GetResults();
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(QueryExecution, StorageTypes);

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
