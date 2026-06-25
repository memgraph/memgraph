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

// COROUTINE ROOT-DRIVE SMOKE TEST (coroutine cursors v2, PR-3).
//
// Exercises the coroutine root-drive seam added to PullPlan::Pull: with the test-only force hook ON,
// the interpreter drives the plan through PullCo() + ResumePullStep instead of the synchronous Pull().
//
// In this PR NO cursor is converted yet (every cursor is Sync, so PullCo() == Immediate(Pull())), so
// coroutine-driven execution must be BYTE-IDENTICAL to synchronous execution. That is exactly what this
// test asserts across a spread of read-only queries (Produce / Unwind / Once / aggregation / OrderBy /
// Limit / Skip / list ops). It is the dormant-correctness gate for the driver; the full per-cursor
// parity corpus (driven through this same hook) lands with the cursor-body PRs.

#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "disk_test_utils.hpp"
#include "glue/communication.hpp"
#include "interpreter_faker.hpp"
#include "query/interpreter_context.hpp"
#include "query/plan/cursor_awaitable.hpp"
#include "replication/state.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "system/system.hpp"

// DEBUG-ONLY: drives the root via the force-coro hook, which only exists when NDEBUG is undefined
// (Debug builds). Compiled out in Release/RelWithDebInfo; replaced by a single skipped test below.
#ifndef NDEBUG

namespace {

// Render a result stream (header + rows) to a stable string for equality comparison.
std::string Render(const ResultStreamFaker &stream) {
  std::ostringstream os;
  os << "HEADER:";
  for (const auto &col : stream.GetHeader()) os << ' ' << col;
  os << '\n';
  for (const auto &row : stream.GetResults()) {
    os << "ROW:";
    for (const auto &val : row) os << " | " << val;
    os << '\n';
  }
  return os.str();
}

class CursorCoroDriverTest : public ::testing::Test {
 protected:
  const std::string testSuite = "cursor_coro_driver";
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_cursor_coro_driver";

  memgraph::storage::Config config{[&]() {
    memgraph::storage::Config c{};
    c.durability.storage_directory = data_directory;
    c.disk.main_storage_directory = c.durability.storage_directory / "disk";
    return c;
  }()};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{[&]() {
    auto db_acc_opt = db_gk.access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    return *db_acc_opt;
  }()};
  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,  // DbmsHandler*
                                                          &repl_state,
                                                          system_state,
                                                          nullptr  // ServerContext*
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };
  InterpreterFaker interpreter{&interpreter_context, db};

  void TearDown() override {
    memgraph::query::plan::SetForceCoroRootDriveForTesting(false);
    std::filesystem::remove_all(data_directory);
  }

  // Run `query` synchronously and then with the coroutine root-drive forced ON; assert identical renders.
  void ExpectCoroDriveParity(const std::string &query) {
    memgraph::query::plan::SetForceCoroRootDriveForTesting(false);
    auto sync = interpreter.Interpret(query);

    memgraph::query::plan::SetForceCoroRootDriveForTesting(true);
    auto coro = interpreter.Interpret(query);

    memgraph::query::plan::SetForceCoroRootDriveForTesting(false);

    EXPECT_EQ(Render(sync), Render(coro)) << "coro root-drive mismatch for query: " << query;
  }
};

TEST_F(CursorCoroDriverTest, ReadOnlyQueriesByteIdentical) {
  // No converted cursors yet → coro-driven must equal sync-driven.
  ExpectCoroDriveParity("RETURN 1 AS a, 'x' AS b, true AS c");
  ExpectCoroDriveParity("UNWIND [3, 1, 2, 1] AS x RETURN x ORDER BY x");
  ExpectCoroDriveParity("UNWIND range(1, 20) AS x WITH x WHERE x % 2 = 0 RETURN x SKIP 2 LIMIT 5");
  ExpectCoroDriveParity("UNWIND [1, 2, 3, 4] AS x RETURN sum(x) AS s, count(x) AS c, avg(x) AS a");
  ExpectCoroDriveParity("UNWIND [1, 1, 2, 3, 3, 3] AS x RETURN DISTINCT x ORDER BY x DESC");
  ExpectCoroDriveParity("WITH [1, 2, 3] AS l RETURN [e IN l WHERE e > 1 | e * 10] AS mapped");
  ExpectCoroDriveParity("UNWIND range(1, 5) AS x RETURN x, x * x AS sq ORDER BY sq DESC LIMIT 3");
}

TEST_F(CursorCoroDriverTest, MultiPullBatchingByteIdentical) {
  // Larger result set to exercise PullPlan's n-batching + has_unsent_results look-ahead under coro drive.
  ExpectCoroDriveParity("UNWIND range(1, 100) AS x RETURN x");
  ExpectCoroDriveParity("UNWIND range(1, 100) AS x WITH x WHERE x > 40 RETURN x ORDER BY x DESC");
}

TEST_F(CursorCoroDriverTest, WritesAndReadbackByteIdentical) {
  // Exercise write cursors + a read-back, both under coro drive (still Sync bodies → identical).
  ExpectCoroDriveParity("CREATE (n:N {id: 1}), (m:N {id: 2}) RETURN n.id AS a, m.id AS b");
  ExpectCoroDriveParity("MATCH (n:N) RETURN n.id AS id ORDER BY id");
}

}  // namespace

#else  // NDEBUG

TEST(CursorCoroDriver, DebugOnly) {
  GTEST_SKIP() << "coroutine root-drive smoke test is Debug-only (NDEBUG gates the force-coro seam)";
}

#endif  // NDEBUG
