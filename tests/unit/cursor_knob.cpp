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
#include "query/plan/cursor_awaitable.hpp"
#include "replication/state.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "system/system.hpp"
#include "utils/memory_tracker.hpp"
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

  // Distinct split point (the broadened default also includes Accumulate/HashJoin; all blocking ops are
  // already proven coro==sync by the all-coro parity harness, this exercises the production knob path).
  ExpectKnobInvariant("MATCH (n:N) RETURN DISTINCT n.id % 2 AS p", "Distinct");
  ExpectKnobInvariant("MATCH (n:N) RETURN DISTINCT n.id % 2 AS p", "Aggregate,OrderBy,Accumulate,Distinct,HashJoin");

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
  EXPECT_EQ(info.default_value, "Aggregate,OrderBy,Accumulate,Distinct,HashJoin")
      << "the coroutine pull path is expected ON by default (split at the blocking operators); empty is "
         "the kill switch";
}

// The coroutine-region-size tally that feeds the observability metric (memgraph_coroutine_region_cursors).
// CoroSelectedCount() is reset before each plan's MakeCursor and incremented once per cursor selected
// Coro, so after a query it holds that plan's coroutine-region size. A split-point query has a non-empty
// region; a pure-scan query with the knob off has none. (Verifies the metric's input directly; the
// Prometheus counter wiring follows the existing per-DB pattern.)
TEST_F(CursorKnobTest, CoroRegionTallyReflectsSplit) {
  interpreter.Interpret("CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3})");

  // Aggregate under the (broadened) split set: Produce <- Aggregate(Coro) <- ScanAll(Sync) -> region > 0.
  FLAGS_query_coroutine_yield_ops = "Aggregate,OrderBy,Accumulate,Distinct,HashJoin";
  interpreter.Interpret("MATCH (n:N) RETURN count(n) AS c");
  EXPECT_GT(memgraph::query::plan::CoroSelectedCount(), 0u) << "aggregate plan should have a coroutine region";

  // Pure scan with the knob off: no split point -> no coroutine region.
  FLAGS_query_coroutine_yield_ops = "";
  interpreter.Interpret("MATCH (n:N) RETURN n.id AS id");
  EXPECT_EQ(memgraph::query::plan::CoroSelectedCount(), 0u) << "knob-off plan should be fully synchronous";
}

// PROFILE annotates each operator with its coroutine/synchronous mode ("[coro]" suffix), so the split
// point is visible per operator. Profile queries drive synchronously, but the cursor modes are still
// selected by the policy, so the annotation reflects what the (non-profile) run would do.
TEST_F(CursorKnobTest, ProfileAnnotatesCoroOperators) {
  interpreter.Interpret("CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3})");

  // Returns the single line of `text` that contains `needle` (or "" if none).
  auto line_with = [](const std::string &text, const std::string &needle) -> std::string {
    auto pos = text.find(needle);
    if (pos == std::string::npos) return "";
    auto start = text.rfind('\n', pos);
    start = (start == std::string::npos) ? 0 : start + 1;
    auto end = text.find('\n', pos);
    return text.substr(start, end == std::string::npos ? std::string::npos : end - start);
  };

  // Split policy: Aggregate (split point) + Produce (above it) run coroutine; the scan (below) stays sync.
  FLAGS_query_coroutine_yield_ops = "Aggregate,OrderBy,Accumulate,Distinct,HashJoin";
  auto split = Render(interpreter.Interpret("PROFILE MATCH (n:N) RETURN count(n) AS c"));
  FLAGS_query_coroutine_yield_ops = "";

  EXPECT_NE(split.find("[coro]"), std::string::npos) << "split policy should mark some operators coro:\n" << split;
  EXPECT_NE(line_with(split, "Aggregate").find("[coro]"), std::string::npos)
      << "Aggregate is the split point -> coro:\n"
      << split;
  auto scan_line = line_with(split, "ScanAll");
  ASSERT_FALSE(scan_line.empty()) << split;
  EXPECT_EQ(scan_line.find("[coro]"), std::string::npos) << "the scan is below the split point -> sync:\n" << split;

  // Knob off: every operator synchronous -> no annotation anywhere.
  auto sync = Render(interpreter.Interpret("PROFILE MATCH (n:N) RETURN count(n) AS c"));
  EXPECT_EQ(sync.find("[coro]"), std::string::npos) << "knob off -> no coroutine operators:\n" << sync;
}

#ifndef NDEBUG
// ─────────────────────────────────────────────────────────────────────────────
// S1 / c-core-2 DEBUG test helpers
// ─────────────────────────────────────────────────────────────────────────────

// Pull all results from a prepared query, looping through any Parked returns.
// n=nullopt → pull all; n=k → batched pull of size k, re-entered while parked.
// Returns the completed stream.
static ResultStreamFaker InterpretWithParkLoop(memgraph::query::Interpreter &interp,
                                               memgraph::storage::Storage *storage, const std::string &query,
                                               std::optional<int> n = {}) {
  const auto [header, _1, qid, _2] =
      interp.Prepare(query, [](auto *) { return memgraph::storage::ExternalPropertyValue::map_t{}; }, {});
  ResultStreamFaker stream(storage);
  stream.Header(header);
  bool parked = false;
  bool has_more = false;
  std::map<std::string, memgraph::query::TypedValue> summary;
  do {
    parked = false;
    summary = interp.Pull(&stream, n, qid, &parked);
    // A batched pull returns has_more (BatchContinues) — NOT a park. Re-pull on either, mirroring the
    // Bolt client (which re-sends PULL on has_more) and the A/B task driver (which re-enters on park).
    auto it = summary.find("has_more");
    has_more = it != summary.end() && it->second.IsBool() && it->second.ValueBool();
  } while (parked || has_more);
  stream.Summary(summary);
  return stream;
}

// S1 (coroutine-native scheduler): the production drive yields + resumes correctly. Force EVERY
// throttled checkpoint to yield and assert the coroutine drive still produces results identical to the
// no-yield baseline — proving PullPlan's Enabled PullDriverScope + the Yielded re-resume loop are
// correct on a real query through the interpreter.
//
// c-core-2: under the park behaviour the drive no longer re-resumes inline on Yielded; instead it
// returns PullOutcome::Parked and the caller must re-enter.  The `run` lambda uses InterpretWithParkLoop
// which re-enters until Finished, so the assertion is unchanged: park-drive == no-park-drive.
//
// Debug-only: the force-yield seam compiles out under NDEBUG (Release/RelWithDebInfo).
TEST_F(CursorKnobTest, ForcedYieldDriveIsResultInvariant) {
  for (int b = 0; b < 200; b += 50) {
    interpreter.Interpret("UNWIND range(" + std::to_string(b + 1) + ", " + std::to_string(b + 50) +
                          ") AS i CREATE (:N {id: i, g: i % 4})");
  }
  FLAGS_query_coroutine_yield_ops = "Aggregate,OrderBy,Accumulate,Distinct,HashJoin";

  auto storage_ptr = interpreter.interpreter.current_db_.db_acc_->get()->storage();

  auto run = [&](const std::string &q) {
    memgraph::query::plan::SetForceYieldForTesting(false);
    auto baseline = Render(interpreter.Interpret(q));
    memgraph::query::plan::SetForceYieldForTesting(true);
    // Use the park-loop helper: re-enters Pull() after each Parked return until Finished.
    auto yielded = Render(InterpretWithParkLoop(interpreter.interpreter, storage_ptr, q));
    memgraph::query::plan::SetForceYieldForTesting(false);
    EXPECT_EQ(baseline, yielded) << "forced-yield/park drive changed results for: " << q;
  };

  run("MATCH (n:N) RETURN n.id AS id ORDER BY n.id");            // OrderBy split, scan below
  run("MATCH (n:N) RETURN n.g AS g, count(*) AS c ORDER BY g");  // Aggregate + OrderBy
  run("MATCH (n:N) RETURN DISTINCT n.g AS g ORDER BY g");        // Distinct + OrderBy
  FLAGS_query_coroutine_yield_ops = "";
}

// ─────────────────────────────────────────────────────────────────────────────
// c-core-2: park/resume parity test.
//
// Force-park ON; a driver loops Interpreter::Pull re-entering while parked until done.
// Asserts:
//   (a) rows + order + summary identical to force-park-OFF run
//   (b) the park path fired N > 0 times  (non-vacuous; throttle=1 ensures small datasets park)
//   (c) n=7 batched PULL (S-2 over-stream trap)
//   (d) park-in-probe (has_unsent_results_ both entry states)
//   (e) OOM counter == baseline immediately after a park-return
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(CursorKnobTest, ParkResumeParity) {
  // Insert 20 nodes so batched pulls of n=7 force the probe to fire and park.
  for (int i = 1; i <= 20; ++i) {
    interpreter.Interpret("CREATE (:N {id: " + std::to_string(i) + ", g: " + std::to_string(i % 4) + "})");
  }
  // Use the broadened default split policy so the root is Coro-driven.
  FLAGS_query_coroutine_yield_ops = "Aggregate,OrderBy,Accumulate,Distinct,HashJoin";

  auto storage_ptr = interpreter.interpreter.current_db_.db_acc_->get()->storage();

  // run_with_park: pull all via the park-loop driver; count parks; return rendered stream + park_count.
  auto run_with_park = [&](const std::string &q, std::optional<int> batch_n) -> std::pair<std::string, int> {
    const auto [header, _1, qid, _2] = interpreter.interpreter.Prepare(
        q, [](auto *) { return memgraph::storage::ExternalPropertyValue::map_t{}; }, {});
    ResultStreamFaker stream(storage_ptr);
    stream.Header(header);

    memgraph::query::plan::SetForceYieldForTesting(true);
    int park_count = 0;
    bool parked = false;
    bool has_more = false;
    std::map<std::string, memgraph::query::TypedValue> summary;
    do {
      parked = false;
      // (e) OOM counter must equal the baseline (0 from the driver's frame-free POV) right after
      // any Parked return — the save/restore mechanism keeps the worker thread clean during park.
      const auto pre_oom = memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler::GetCounter();
      summary = interpreter.interpreter.Pull(&stream, batch_n, qid, &parked);
      if (parked) {
        ++park_count;
        const auto post_oom = memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler::GetCounter();
        EXPECT_EQ(post_oom, pre_oom) << "OOM counter drifted after park-return (park #" << park_count << ")";
      }
      // Re-pull on has_more (BatchContinues) too — a batched pull is not a park. Mirrors the Bolt
      // client re-sending PULL on has_more.
      auto it = summary.find("has_more");
      has_more = it != summary.end() && it->second.IsBool() && it->second.ValueBool();
    } while (parked || has_more);
    memgraph::query::plan::SetForceYieldForTesting(false);

    stream.Summary(summary);
    return {Render(stream), park_count};
  };

  // baseline: force-park OFF, pull all
  auto run_baseline = [&](const std::string &q) -> std::string {
    memgraph::query::plan::SetForceYieldForTesting(false);
    return Render(interpreter.Interpret(q));
  };

  const std::vector<std::string> queries = {
      "MATCH (n:N) RETURN n.id AS id ORDER BY n.id",            // (a) read + OrderBy
      "MATCH (n:N) RETURN n.g AS g, count(*) AS c ORDER BY g",  // Aggregate + OrderBy
      "MATCH (n:N) RETURN DISTINCT n.g AS g ORDER BY g",        // Distinct
  };
  const std::vector<std::optional<int>> batch_sizes = {
      1,             // n=1: parks frequently
      2,             // n=2
      7,             // n=7: (c) S-2 over-stream trap
      std::nullopt,  // pull all at once
  };

  for (const auto &q : queries) {
    const auto baseline = run_baseline(q);
    for (const auto &bn : batch_sizes) {
      auto [result, parks] = run_with_park(q, bn);
      // (a) result parity
      EXPECT_EQ(baseline, result) << "park-drive changed result for: " << q
                                  << " batch_n=" << (bn ? std::to_string(*bn) : "all");
      // (b) non-vacuous: at least one park must have fired
      EXPECT_GT(parks, 0) << "zero parks fired — force-park seam may be broken (query: " << q
                          << ", batch_n=" << (bn ? std::to_string(*bn) : "all") << ")";
    }
  }

  FLAGS_query_coroutine_yield_ops = "";
}
#endif

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
