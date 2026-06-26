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

// KNOB TEST — the PRODUCTION coroutine split-point path (coroutine cursors v2 / PR-13).
//
// Unlike cursor_parity (DEBUG-only, force-drives EVERY plan coroutine via the test hook), this test
// exercises the REAL production knob `--query-coroutine-yield-ops` and therefore runs in every build,
// including Release/RelWithDebInfo. Setting the flag designates operator kinds as coroutine split
// points: a plan runs coroutine from the root down to (and including) the deepest such operator, and
// synchronously below it. With the flag empty (the default) every cursor is Sync — byte-identical to
// master — so the assertion here is that turning the knob ON changes HOW the plan is pulled but never
// WHAT it produces. We compare each query's rendered result with the knob OFF (sync baseline) against
// the same query with the knob ON.

#include <sstream>
#include <string>

#include <gflags/gflags.h>
#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "disk_test_utils.hpp"
#include "glue/communication.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "system/system.hpp"
#include "utils/priority_thread_pool.hpp"

DECLARE_string(query_coroutine_yield_ops);

namespace {

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

class CursorKnobTest : public ::testing::Test {
 protected:
  const std::string testSuite = "cursor_knob";
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_cursor_knob";

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

  void SetUp() override { FLAGS_query_coroutine_yield_ops = ""; }

  void TearDown() override {
    FLAGS_query_coroutine_yield_ops = "";
    std::filesystem::remove_all(data_directory);
  }

  // Run `query` with the knob OFF (sync baseline) and again with `knob` set; assert identical renders.
  // The knob only changes the pull strategy, never the result.
  void ExpectKnobInvariant(const std::string &query, const std::string &knob) {
    FLAGS_query_coroutine_yield_ops = "";
    auto baseline = Render(interpreter.Interpret(query));

    FLAGS_query_coroutine_yield_ops = knob;
    auto with_knob = Render(interpreter.Interpret(query));

    FLAGS_query_coroutine_yield_ops = "";
    EXPECT_EQ(baseline, with_knob) << "KNOB CHANGED RESULT for query: " << query << "  (knob=\"" << knob << "\")";
  }
};

TEST_F(CursorKnobTest, ResultsInvariantUnderKnob) {
  // Deterministic seed.
  interpreter.Interpret("CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}), (d:N {id: 4})");
  interpreter.Interpret("MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:E {w: 10}]->(b)");
  interpreter.Interpret("MATCH (b:N {id: 2}), (c:N {id: 3}) CREATE (b)-[:E {w: 20}]->(c)");

  // Aggregate split point: Produce <- Aggregate(Coro) <- ScanAll(Sync). Root goes Coro and is driven
  // via PullCo + ResumePullStep; the result must be identical to the sync baseline.
  ExpectKnobInvariant("MATCH (n:N) RETURN count(n) AS c, sum(n.id) AS s", "Aggregate");
  ExpectKnobInvariant("MATCH (n:N) RETURN count(n) AS c, sum(n.id) AS s, avg(n.id) AS a", "Aggregate");

  // OrderBy split point.
  ExpectKnobInvariant("MATCH (n:N) RETURN n.id AS id ORDER BY n.id DESC", "OrderBy");

  // Combined knob: whichever split point appears in the plan drives the coroutine region.
  ExpectKnobInvariant("MATCH (n:N) WITH n.id AS id ORDER BY id RETURN count(id) AS c", "Aggregate,OrderBy");
  ExpectKnobInvariant("MATCH (n:N) RETURN n.id AS id ORDER BY n.id", "Aggregate,OrderBy");

  // A pure scan/expand query with the Aggregate knob: the plan has no split point, so the root stays
  // Sync (== master). Confirms the knob is inert when its op-kind is absent.
  ExpectKnobInvariant("MATCH (n:N) RETURN n.id AS id ORDER BY id", "Aggregate");
  ExpectKnobInvariant("MATCH (a:N)-[r:E]->(b:N) RETURN a.id AS a, b.id AS b, r.w AS w ORDER BY a", "Aggregate");

  // Whitespace + case tolerance and an unknown op (ignored with a warning -> empty policy -> sync).
  ExpectKnobInvariant("MATCH (n:N) RETURN count(n) AS c", " aggregate ");
  ExpectKnobInvariant("MATCH (n:N) RETURN count(n) AS c", "Bogus");

  // Whole-plan coroutine ("All" / "*"): every cursor Coro (the benchmark worst-case arm). Results must
  // still be identical to the sync baseline across scan, expand, aggregate and order-by plans.
  ExpectKnobInvariant("MATCH (n:N) RETURN count(n) AS c, sum(n.id) AS s", "All");
  ExpectKnobInvariant("MATCH (a:N)-[r:E]->(b:N) RETURN a.id AS a, b.id AS b, r.w AS w ORDER BY a", "All");
  ExpectKnobInvariant("MATCH (n:N) RETURN n.id AS id ORDER BY id", "*");
}

// The coroutine pull path is ON by default (flip-default): the compiled default of the knob is the
// validated split policy, not empty. Empty remains the kill switch. Locks the flip against accidental
// revert. (A plain TEST, not in the fixture, so its SetUp does not overwrite the flag first.)
TEST(CursorKnobDefault, DefaultIsSplitPolicy) {
  gflags::CommandLineFlagInfo info;
  ASSERT_TRUE(gflags::GetCommandLineFlagInfo("query_coroutine_yield_ops", &info));
  EXPECT_EQ(info.default_value, "Aggregate,OrderBy")
      << "the coroutine pull path is expected ON by default (split policy); empty is the kill switch";
}

#ifdef MG_ENTERPRISE
// Enterprise PARALLEL cursors under the coroutine drive. The parallel cursors (ScanParallel,
// ParallelMerge, AggregateParallel, OrderByParallel, ...) intentionally have NO coroutine body — they
// ride the base PullCo()=Immediate(Pull()) and drive their branch sub-cursors synchronously (see the
// seam note on ParallelBranchCursor in operator.cpp). This fixture proves that is coro-drive-safe:
// `USING PARALLEL EXECUTION` queries produce identical results whether driven synchronously (knob off)
// or with a Coro root over the parallel subtree (knob=All). Requires a real worker_pool + an enterprise
// license; without them the planner falls back to single-threaded and the parallel cursors never run.
class CursorKnobParallelTest : public ::testing::Test {
 protected:
  const std::string testSuite = "cursor_knob_parallel";
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_cursor_knob_parallel";

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
  // Must be constructed BEFORE interpreter_context (which takes a pointer to it) and torn down after.
  memgraph::utils::PriorityThreadPool worker_pool{/* mixed */ 4, /* high prio */ 1, nullptr};
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,  // DbmsHandler*
                                                          &repl_state,
                                                          system_state,
                                                          nullptr,  // ServerContext*
                                                          nullptr,  // CoordinatorState*
                                                          nullptr,  // ResourceMonitoring*
                                                          nullptr,  // AuthQueryHandler*
                                                          nullptr,  // AuthChecker*
                                                          nullptr,  // ReplicationQueryHandler*
                                                          &worker_pool};
  InterpreterFaker interpreter{&interpreter_context, db};

  void SetUp() override {
    FLAGS_query_coroutine_yield_ops = "";
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
  }

  void TearDown() override {
    FLAGS_query_coroutine_yield_ops = "";
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(data_directory);
  }

  void ExpectKnobInvariant(const std::string &query, const std::string &knob) {
    FLAGS_query_coroutine_yield_ops = "";
    auto baseline = Render(interpreter.Interpret(query));
    FLAGS_query_coroutine_yield_ops = knob;
    auto with_knob = Render(interpreter.Interpret(query));
    FLAGS_query_coroutine_yield_ops = "";
    EXPECT_EQ(baseline, with_knob) << "KNOB CHANGED RESULT for query: " << query << "  (knob=\"" << knob << "\")";
  }
};

TEST_F(CursorKnobParallelTest, ParallelResultsInvariantUnderKnob) {
  // Enough rows (and groups) that the planner parallelizes the aggregation across the 4 workers.
  for (int b = 0; b < 800; b += 200) {
    interpreter.Interpret("UNWIND range(" + std::to_string(b + 1) + ", " + std::to_string(b + 200) +
                          ") AS i CREATE (:N {id: i, g: i % 4})");
  }

  // NON-VACUITY GUARD: confirm the plan actually contains a parallel cursor. If parallelization didn't
  // happen (no license / no worker_pool / planner fallback), the parity below would pass vacuously on a
  // serial plan and prove nothing about the parallel cursors.
  auto plan = Render(interpreter.Interpret("EXPLAIN USING PARALLEL EXECUTION 4 MATCH (n:N) RETURN sum(n.id) AS s"));
  // The plan render marks parallelized operators with a "threads: N" annotation on the scan (and a 'P'
  // branch prefix). Its presence proves the query parallelized -> the parallel cursors actually run.
  ASSERT_NE(plan.find("threads:"), std::string::npos)
      << "expected a parallelized plan (ScanAll with threads: N); got:\n"
      << plan;

  // Coro-drive safety: a Coro root (knob=All) over the synchronous parallel subtree must match the
  // fully-synchronous baseline. Exercises AggregateParallel + OrderByParallel + ScanParallel.
  ExpectKnobInvariant("USING PARALLEL EXECUTION 4 MATCH (n:N) RETURN sum(n.id) AS s", "All");
  ExpectKnobInvariant("USING PARALLEL EXECUTION 4 MATCH (n:N) RETURN n.g AS g, count(*) AS c ORDER BY g", "All");
  ExpectKnobInvariant("USING PARALLEL EXECUTION 4 MATCH (n:N) RETURN sum(n.id) AS s", "Aggregate,OrderBy");
}
#endif  // MG_ENTERPRISE

}  // namespace
