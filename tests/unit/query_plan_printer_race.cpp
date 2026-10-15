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

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "interpreter_faker.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"

// Concurrent EXPLAIN of one plan-cache entry.
//
// CypherQueryToPlan returns the cached shared_ptr, so every session prints the
// same LogicalOperator tree. Plan printing must therefore treat that tree as
// read-only and carry the DbAccessor itself; an operator that stores the
// accessor lets one session's teardown strand another session mid-print.
//
// The label index is required: without it MATCH (n:Node) plans to ScanAll plus
// Filter, whose printing needs no accessor at all.

namespace {
constexpr int kThreads = 4;
constexpr int kExplainsPerThread = 5000;
constexpr char kExplainQuery[] = "EXPLAIN MATCH (n:Node) RETURN n;";
}  // namespace

class PlanPrinterRaceTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_plan_printer_race"};

  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        return config;
      }()  // iile
  };

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        return *db_acc_opt;
      }()  // iile
  };

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };

  void TearDown() override { std::filesystem::remove_all(data_directory); }

  static bool PlanMentions(const ResultStreamFaker &stream, std::string_view needle) {
    for (const auto &row : stream.GetResults()) {
      if (row.front().ValueString().find(needle) != std::string::npos) return true;
    }
    return false;
  }

  size_t PlanCacheSize() {
    return db->plan_cache()->WithLock([](auto &cache) { return cache.size(); });
  }
};

TEST_F(PlanPrinterRaceTest, ConcurrentExplainOfSharedCachedPlan) {
  InterpreterFaker setup{&interpreter_context, db};
  setup.Interpret("CREATE INDEX ON :Node;");

  // Prime the cache, and confirm the plan really is an indexed scan. Without
  // this the cached tree could contain no operator that reads dba_, and the
  // test would pass while exercising nothing.
  auto primed = setup.Interpret(kExplainQuery);
  ASSERT_TRUE(PlanMentions(primed, "ScanAllByLabel (n :Node)"))
      << "expected an indexed scan; the label index was not picked up";
  ASSERT_EQ(PlanCacheSize(), 1U);

  // One interpreter per thread: each supplies its own DbAccessor, so the
  // threads write different pointers into the same shared node.
  std::vector<std::unique_ptr<InterpreterFaker>> interpreters;
  interpreters.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    interpreters.push_back(std::make_unique<InterpreterFaker>(&interpreter_context, db));
  }

  std::atomic<int> label_missing{0};
  auto hammer = [&](int index) {
    for (int i = 0; i < kExplainsPerThread; ++i) {
      auto stream = interpreters[index]->Interpret(kExplainQuery);
      if (!PlanMentions(stream, ":Node")) label_missing.fetch_add(1, std::memory_order_relaxed);
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) threads.emplace_back(hammer, i);
  for (auto &thread : threads) thread.join();

  EXPECT_EQ(label_missing.load(), 0) << "a plan was printed without the label name";
  EXPECT_EQ(PlanCacheSize(), 1U) << "the plan was not shared across sessions";
}
