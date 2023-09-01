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
#include "query/interpreter.hpp"
#include "query/stream/streams.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

DECLARE_bool(query_cost_planner);

template <typename StorageType>
class QueryExecution : public testing::Test {
 protected:
  const std::string testSuite = "query_plan_edge_cases";
  std::shared_ptr<memgraph::dbms::Database> db_;
  std::optional<memgraph::query::InterpreterContext> interpreter_context_;
  std::optional<memgraph::query::Interpreter> interpreter_;

  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_query_plan_edge_cases"};

  void SetUp() {
    db_ = [&]() {
      if constexpr (std::is_same_v<StorageType, memgraph::storage::InMemoryStorage>) {
        return std::make_shared<memgraph::dbms::Database>(
            memgraph::storage::Config{.durability.storage_directory = data_directory});
      } else {
        auto tmp = disk_test_utils::GenerateOnDiskConfig(testSuite);
        tmp.force_on_disk = true;
        tmp.durability.storage_directory = data_directory;
        return std::make_shared<memgraph::dbms::Database>(tmp);
      }
    }();  // iile

    interpreter_context_.emplace(memgraph::query::InterpreterConfig{}, nullptr);
    interpreter_.emplace(&*interpreter_context_, db_);
  }

  void TearDown() {
    db_.reset();
    interpreter_ = std::nullopt;
    interpreter_context_ = std::nullopt;

    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  /**
   * Execute the given query and commit the transaction.
   *
   * Return the query results.
   */
  auto Execute(const std::string &query) {
    ResultStreamFaker stream(this->db_->storage());

    auto [header, _1, qid, _2] = interpreter_->Prepare(query, {}, nullptr);
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
