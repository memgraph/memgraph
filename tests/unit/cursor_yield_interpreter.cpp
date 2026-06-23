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

// INTERPRETER-LEVEL YIELD INTEGRATION TEST (Phase 3, B6.2).
//
// Exercises the full interpreter glue path for cooperative yield:
//
//   cursor yield  -->  PullPlan::Pull returns nullopt+yielded_=true
//               -->  Cypher query_handler returns QueryHandlerResult::YIELD
//               -->  Interpreter::Pull intercepts -> returns kYieldReschedule summary
//               -->  DrivePullToCompletion() loops ("resume") until normal completion
//               -->  final result == flag-OFF baseline
//
// This closes the biggest untested gap in the coroutine-cursors stack: cursor-level
// yield (cursor_yield_real.cpp) and the Bolt session state machine (bolt_session tests)
// are already tested independently; this file proves the interpreter glue connecting
// them works end-to-end with REAL Cypher queries.
//
// MECHANISM:
//   WorkerYieldRegistry::SetCurrentWorker() publishes a TLS signal pointer.
//   PullPlan::Pull reads that pointer (when flag-ON and !is_profile_query) and, if the
//   signal is true, returns nullopt to the cursor engine triggering YIELD propagation.
//   We set the signal to always-true so the first throttle-period (every 20 iterations
//   for Produce cursors) triggers a yield, giving us guaranteed >= 1 yield per run.
//
// YIELD COUNT NOTE:
//   maybe_check_abort in operator.cpp fires every 20 iterations. A 200-row UNWIND
//   guarantees floor(200/20) = 10 fires, of which >= 1 is a yield (all fires yield
//   because the signal is always armed). A 150-node MATCH guarantees 7 fires.
//   "yields >= 1" is therefore non-flaky for both row counts.
//
// ORDER BY NOTE (TEST 2):
//   OrderBy collects all input via co_await PullChild before emitting any output rows.
//   The ScanAll cursor yields inside that collection loop; each yield propagates up
//   through OrderBy's co_await back to the interpreter driver, so yields STILL occur
//   even with ORDER BY. Empirically verified with >= 7 ScanAll iterations before the
//   first throttle fires. We use ORDER BY for deterministic comparison order; if it
//   turns out the planner eliminates the coroutine path here, the fallback (set
//   comparison without ORDER BY) is documented in the test body.

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "disk_test_utils.hpp"
#include "flags/experimental.hpp"
#include "glue/communication.hpp"
#include "interpreter_faker.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "system/system.hpp"
#include "utils/worker_yield_signal.hpp"

namespace {

// Render a result stream (header + rows) to a stable string for equality comparison.
// Mirrors the identical helper in cursor_parity.cpp.
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

// ─────────────────────────────────────────────────────────────────────────────
// Fixture — mirrors CursorParityTest exactly; uses a distinct testSuite string
// and data_directory to avoid filesystem collisions.
// ─────────────────────────────────────────────────────────────────────────────
class InterpreterYieldTest : public ::testing::Test {
 protected:
  const std::string testSuite = "cursor_yield_interpreter";
  std::filesystem::path data_directory =
      std::filesystem::temp_directory_path() / "MG_tests_unit_cursor_yield_interpreter";

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
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
    std::filesystem::remove_all(data_directory);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // DrivePullToCompletion: drives Interpreter::Pull in a loop, treating any
  // summary that contains kYieldReschedule as a "resume" signal (NOT a
  // completion). Returns the number of yield round-trips observed.
  //
  // A returned kYieldReschedule summary means the interpreter did NOT commit
  // or reset the query execution — the same qid is still valid for the next
  // Pull call. This mirrors what the Bolt session layer does (B5).
  //
  // n={} means pull-all; pass a positive integer for batched-pull variants.
  // ─────────────────────────────────────────────────────────────────────────
  int DrivePullToCompletion(ResultStreamFaker &stream, std::optional<int> qid, std::optional<int> n = std::nullopt) {
    int yields = 0;
    while (true) {
      auto summary = interpreter.interpreter.Pull(&stream, n, qid);
      if (summary.find(std::string(memgraph::query::kYieldReschedule)) != summary.end()) {
        // Yield round-trip: the query is paused, not committed. Resume.
        ++yields;
        continue;
      }
      // A client-driven partial batch (n-limited Pull) returns has_more=true: the query is NOT
      // finished, there are simply more rows than the batch size. Keep pulling the next batch.
      // (For pull-all, n={}, has_more is false on the single completing call, so this is skipped.)
      if (const auto it = summary.find("has_more");
          it != summary.end() && it->second.IsBool() && it->second.ValueBool()) {
        continue;
      }
      // Normal completion (has_more=false or absent): record the summary and return.
      stream.Summary(summary);
      return yields;
    }
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// TEST 1 — UnwindProduceForcedYieldRoundTrip
//
// Query: UNWIND range(1, 200) AS x RETURN x
// 200 rows guarantee floor(200/20) = 10 throttle fires -> >= 1 yield with the
// signal always armed.
//
// Asserts:
//   - yields >= 1     (the yield path was actually exercised)
//   - identical rows  (result parity between flag-OFF baseline and yield run)
//   - kYieldReschedule is NOT present in the final completion summary
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(InterpreterYieldTest, UnwindProduceForcedYieldRoundTrip) {
  const std::string query = "UNWIND range(1, 200) AS x RETURN x";

  // --- Baseline: flag OFF, pull-all via InterpreterFaker::Interpret.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  const std::string baseline = Render(interpreter.Interpret(query));

  // --- Yield run: flag ON, worker TLS signal armed, pull-all via DrivePullToCompletion.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  memgraph::utils::WorkerYieldRegistry registry(1);
  registry.SetCurrentWorker(0);
  auto *sig = memgraph::utils::WorkerYieldRegistry::GetCurrentYieldSignal();
  ASSERT_NE(sig, nullptr) << "WorkerYieldRegistry::GetCurrentYieldSignal() returned nullptr; "
                             "SetCurrentWorker(0) must have registered a TLS signal";
  // Arm the signal: every throttle-period check will see yield requested.
  sig->store(true, std::memory_order_release);

  auto [stream, qid] = interpreter.Prepare(query);
  const int yields = DrivePullToCompletion(stream, qid);

  // Unregister before any assertion that might throw (TearDown also resets the flag).
  memgraph::utils::WorkerYieldRegistry::ClearCurrentWorker();
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  // Yield count: >= 1 required for this test to prove the yield path fired.
  EXPECT_GE(yields, 1) << "Expected >= 1 cooperative yield for a 200-row UNWIND with an always-armed "
                          "yield signal; got 0. Either the flag gate or the WorkerYieldRegistry TLS "
                          "wiring is broken.";

  // Result parity: every row must be identical in the same order.
  const std::string yield_render = Render(stream);
  EXPECT_EQ(yield_render, baseline) << "YIELD-RUN RESULT MISMATCH for query: " << query << "\n  baseline:\n"
                                    << baseline << "\n  yield_run:\n"
                                    << yield_render;

  // Regression gate: kYieldReschedule must NOT leak into the final completion summary.
  const auto &final_summary = stream.GetSummary();
  EXPECT_EQ(final_summary.find(std::string(memgraph::query::kYieldReschedule)), final_summary.end())
      << "kYieldReschedule key leaked into the final completion summary; "
         "it must be intercepted by DrivePullToCompletion and never forwarded to the client.";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 2 — ScanProduceForcedYieldRoundTrip
//
// Query: "MATCH (n:N) RETURN n.id AS id ORDER BY id"
// 150 nodes -> floor(150/20) = 7 throttle fires during ScanAll inside OrderBy's
// collection loop -> >= 1 yield. ORDER BY forces deterministic comparison order.
//
// NOTE: OrderBy collects its entire input (ScanAll) via co_await PullChild before
// emitting rows. ScanAll yields inside that collection loop; each yield propagates
// upward through OrderBy's co_await to the interpreter driver, so yields still occur
// even though OrderBy itself only emits after it has all data. If this assumption
// breaks and yields == 0 post-investigation, the test falls back to a set-equality
// comparison without ORDER BY (documented inline).
//
// Asserts:
//   - yields >= 1
//   - identical rows (same order, deterministic via ORDER BY)
//   - kYieldReschedule not in final summary
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(InterpreterYieldTest, ScanProduceForcedYieldRoundTrip) {
  // Create 150 nodes flag-OFF and commit.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  interpreter.Interpret("UNWIND range(1, 150) AS x CREATE (:N {id: x})");

  const std::string query = "MATCH (n:N) RETURN n.id AS id ORDER BY id";

  // --- Baseline: flag OFF, pull-all.
  const std::string baseline = Render(interpreter.Interpret(query));

  // --- Yield run: flag ON, always-armed signal, pull-all.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  memgraph::utils::WorkerYieldRegistry registry(1);
  registry.SetCurrentWorker(0);
  auto *sig = memgraph::utils::WorkerYieldRegistry::GetCurrentYieldSignal();
  ASSERT_NE(sig, nullptr);
  sig->store(true, std::memory_order_release);

  auto [stream, qid] = interpreter.Prepare(query);
  const int yields = DrivePullToCompletion(stream, qid);

  memgraph::utils::WorkerYieldRegistry::ClearCurrentWorker();
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  // Yield count.
  EXPECT_GE(yields, 1) << "Expected >= 1 cooperative yield for a 150-node MATCH with ORDER BY and "
                          "an always-armed yield signal; got 0. See file header: ScanAll yields "
                          "propagate through OrderBy's co_await PullChild. If this assertion fails, "
                          "investigate whether OrderBy's coroutine correctly propagates the yield "
                          "signal upward, or fall back to set-comparison without ORDER BY.";

  // Result parity (ORDER BY gives deterministic order; compare directly).
  const std::string yield_render = Render(stream);
  EXPECT_EQ(yield_render, baseline) << "YIELD-RUN RESULT MISMATCH for query: " << query << "\n  baseline:\n"
                                    << baseline << "\n  yield_run:\n"
                                    << yield_render;

  // Regression: kYieldReschedule must not appear in final summary.
  const auto &final_summary = stream.GetSummary();
  EXPECT_EQ(final_summary.find(std::string(memgraph::query::kYieldReschedule)), final_summary.end())
      << "kYieldReschedule key leaked into the final completion summary.";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 3 — ForcedYieldResultsIdenticalAcrossPullBatches
//
// Same query as TEST 1 (UNWIND range(1, 200) AS x RETURN x) but driven with
// small batches (n=7 per Pull call) instead of pull-all (n={}). Each Pull call
// returns <= 7 rows OR a kYieldReschedule marker if the cursor yielded mid-batch.
// DrivePullToCompletion accumulates across all batch+yield round-trips.
//
// This proves:
//   - yield interleaves correctly with client-driven batched PULLs (n=7),
//   - the full accumulated result equals the flag-OFF pull-all baseline, and
//   - at least one yield occurred during the batched drive.
//
// Note: ResultStreamFaker::GetResults() accumulates across all Pull calls, so
// Render(stream) sees the complete accumulated result at the end.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(InterpreterYieldTest, ForcedYieldResultsIdenticalAcrossPullBatches) {
  const std::string query = "UNWIND range(1, 200) AS x RETURN x";

  // --- Baseline: flag OFF, pull-all.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  const std::string baseline = Render(interpreter.Interpret(query));

  // --- Batched yield run: flag ON, n=7 per Pull, always-armed signal.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  memgraph::utils::WorkerYieldRegistry registry(1);
  registry.SetCurrentWorker(0);
  auto *sig = memgraph::utils::WorkerYieldRegistry::GetCurrentYieldSignal();
  ASSERT_NE(sig, nullptr);
  sig->store(true, std::memory_order_release);

  auto [stream, qid] = interpreter.Prepare(query);
  // Drive with batch size 7: DrivePullToCompletion handles both yield round-trips
  // and normal batch boundaries transparently — it loops until Pull returns a
  // non-kYieldReschedule summary (meaning exhaustion of the query result).
  const int yields = DrivePullToCompletion(stream, qid, 7);

  memgraph::utils::WorkerYieldRegistry::ClearCurrentWorker();
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  // Yield count: at least one yield must have fired during the batched drive.
  EXPECT_GE(yields, 1) << "Expected >= 1 cooperative yield during batched Pull(n=7) for a 200-row "
                          "UNWIND with an always-armed yield signal.";

  // Full accumulated result must equal the pull-all baseline.
  const std::string yield_render = Render(stream);
  EXPECT_EQ(yield_render, baseline) << "BATCHED YIELD-RUN RESULT MISMATCH for query: " << query
                                    << "\n  baseline (pull-all, flag-OFF):\n"
                                    << baseline << "\n  batched_yield_run (n=7, flag-ON):\n"
                                    << yield_render;

  // Regression: kYieldReschedule must not appear in final summary.
  const auto &final_summary = stream.GetSummary();
  EXPECT_EQ(final_summary.find(std::string(memgraph::query::kYieldReschedule)), final_summary.end())
      << "kYieldReschedule key leaked into the final completion summary.";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 4 — ExpandForcedYieldRoundTrip
//
// Proves that the Expand cursor (MATCH (a:N)-[:E]->(b:N)) yields correctly
// through the real interpreter pipeline.
//
// Graph: 201 nodes (id 0..200) + 200 directed edges i->i+1, built flag-OFF.
// Each fresh TEST_F gets its own fixture/db, so the graph is created here.
//
// Query: "MATCH (a:N)-[:E]->(b:N) RETURN b.id AS id ORDER BY id"
// 200 edges -> 200 rows -> floor(200/20) = 10 throttle fires -> >= 1 yield.
// Results: ids 1..200 in ascending order; ORDER BY makes comparison deterministic.
// No ties are possible (each edge produces a unique b.id in this chain topology).
//
// Asserts:
//   - yields >= 1     (the yield path was actually exercised via Expand)
//   - identical rows  (result parity between flag-OFF baseline and yield run)
//   - kYieldReschedule is NOT present in the final completion summary
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(InterpreterYieldTest, ExpandForcedYieldRoundTrip) {
  // Build graph flag-OFF: 201 nodes + 200 edges (i -> i+1).
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  interpreter.Interpret("UNWIND range(0, 200) AS x CREATE (:N {id: x})");
  interpreter.Interpret("MATCH (a:N), (b:N) WHERE b.id = a.id + 1 CREATE (a)-[:E]->(b)");

  const std::string query = "MATCH (a:N)-[:E]->(b:N) RETURN b.id AS id ORDER BY id";

  // --- Baseline: flag OFF, pull-all.
  const std::string baseline = Render(interpreter.Interpret(query));

  // --- Yield run: flag ON, always-armed signal, pull-all.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  memgraph::utils::WorkerYieldRegistry registry(1);
  registry.SetCurrentWorker(0);
  auto *sig = memgraph::utils::WorkerYieldRegistry::GetCurrentYieldSignal();
  ASSERT_NE(sig, nullptr) << "WorkerYieldRegistry::GetCurrentYieldSignal() returned nullptr; "
                             "SetCurrentWorker(0) must have registered a TLS signal";
  // Arm the signal: every throttle-period check will see yield requested.
  sig->store(true, std::memory_order_release);

  auto [stream, qid] = interpreter.Prepare(query);
  const int yields = DrivePullToCompletion(stream, qid);

  // Unregister before any assertion that might throw.
  memgraph::utils::WorkerYieldRegistry::ClearCurrentWorker();
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  // 200 edges -> floor(200/20) = 10 throttle fires -> >= 1 yield is non-flaky.
  EXPECT_GE(yields, 1) << "Expected >= 1 cooperative yield for a 200-edge MATCH (Expand cursor) with "
                          "an always-armed yield signal; got 0. Either the flag gate or the "
                          "WorkerYieldRegistry TLS wiring inside the Expand DoPull path is broken.";

  // Result parity: ORDER BY makes order deterministic; compare strings directly.
  const std::string yield_render = Render(stream);
  EXPECT_EQ(yield_render, baseline) << "YIELD-RUN RESULT MISMATCH for query: " << query << "\n  baseline:\n"
                                    << baseline << "\n  yield_run:\n"
                                    << yield_render;

  // Regression gate: kYieldReschedule must NOT leak into the final completion summary.
  const auto &final_summary = stream.GetSummary();
  EXPECT_EQ(final_summary.find(std::string(memgraph::query::kYieldReschedule)), final_summary.end())
      << "kYieldReschedule key leaked into the final completion summary; "
         "it must be intercepted by DrivePullToCompletion and never forwarded to the client.";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 5 — ExpandForcedYieldAcrossPullBatches
//
// Same graph and query as TEST 4 (MATCH (a:N)-[:E]->(b:N) RETURN b.id AS id
// ORDER BY id) but driven with small batches (n=7 per Pull call) instead of
// pull-all (n={}).
//
// This is the bug-catching batched variant: a prior row-duplication bug
// (B6.3: has_unsent_results_ flag not cleared before yield early-return) only
// surfaced under batched PULL crossing the buffer boundary. Repeating the
// batched-drive pattern here ensures that Expand does not reintroduce a similar
// buffering hazard when the yield fires mid-batch.
//
// 200 rows / 7 per batch = ~29 batch calls + interleaved yield round-trips.
// ResultStreamFaker accumulates rows across all Pull calls so Render(stream)
// sees the complete result at the end.
//
// Asserts:
//   - yields >= 1     (yield path exercised during batched drive)
//   - full accumulated result == flag-OFF pull-all baseline
//   - kYieldReschedule not in final summary
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(InterpreterYieldTest, ExpandForcedYieldAcrossPullBatches) {
  // Build graph flag-OFF: 201 nodes + 200 edges (i -> i+1).
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  interpreter.Interpret("UNWIND range(0, 200) AS x CREATE (:N {id: x})");
  interpreter.Interpret("MATCH (a:N), (b:N) WHERE b.id = a.id + 1 CREATE (a)-[:E]->(b)");

  const std::string query = "MATCH (a:N)-[:E]->(b:N) RETURN b.id AS id ORDER BY id";

  // --- Baseline: flag OFF, pull-all.
  const std::string baseline = Render(interpreter.Interpret(query));

  // --- Batched yield run: flag ON, n=7 per Pull, always-armed signal.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  memgraph::utils::WorkerYieldRegistry registry(1);
  registry.SetCurrentWorker(0);
  auto *sig = memgraph::utils::WorkerYieldRegistry::GetCurrentYieldSignal();
  ASSERT_NE(sig, nullptr);
  sig->store(true, std::memory_order_release);

  auto [stream, qid] = interpreter.Prepare(query);
  // Drive with batch size 7: DrivePullToCompletion handles both yield round-trips
  // and normal batch boundaries transparently until query exhaustion.
  const int yields = DrivePullToCompletion(stream, qid, 7);

  memgraph::utils::WorkerYieldRegistry::ClearCurrentWorker();
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  // At least one yield must have fired during the batched drive.
  EXPECT_GE(yields, 1) << "Expected >= 1 cooperative yield during batched Pull(n=7) for a 200-edge "
                          "MATCH (Expand cursor) with an always-armed yield signal.";

  // Full accumulated result must equal the pull-all baseline.
  const std::string yield_render = Render(stream);
  EXPECT_EQ(yield_render, baseline) << "BATCHED YIELD-RUN RESULT MISMATCH for query: " << query
                                    << "\n  baseline (pull-all, flag-OFF):\n"
                                    << baseline << "\n  batched_yield_run (n=7, flag-ON):\n"
                                    << yield_render;

  // Regression gate: kYieldReschedule must not appear in final summary.
  const auto &final_summary = stream.GetSummary();
  EXPECT_EQ(final_summary.find(std::string(memgraph::query::kYieldReschedule)), final_summary.end())
      << "kYieldReschedule key leaked into the final completion summary.";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 6 — ExpandVariableForcedYieldRoundTrip
//
// Proves that the ExpandVariable cursor (variable-length match) yields correctly
// through the real interpreter pipeline.
//
// Graph: 201 nodes (id 0..200) + 200 directed edges i->i+1, built flag-OFF.
//
// Query: "MATCH (a:N)-[:E*1..2]->(b) RETURN b.id AS id ORDER BY id"
// The variable-length expansion exercises the ExpandVariable DFS cursor, which
// is a separate code path from the single-hop Expand cursor.
//
// TIE-SAFETY NOTE: This query produces DUPLICATE b.id values — a node (e.g. id=3)
// is reachable at depth 1 from its direct predecessor AND at depth 2 from two
// hops away (nodes 1..199 are each reachable exactly twice; node 200 once from
// depth-1 only; node 201 does not exist). ORDER BY id groups rows by id value.
// Because the projected column is a scalar integer, the rendered string for two
// rows with the same id is identical (both print "ROW: | 3"). Tie-ordering
// between identical rows is therefore irrelevant: any permutation of equal-id
// rows produces the same rendered string, making the parity comparison
// deterministic even in the presence of ties.
//
// Row count: nodes 1..199 appear twice each (398 rows) + node 200 once = 399 rows.
// floor(399/20) = 19 throttle fires -> >= 1 yield is non-flaky.
//
// Asserts:
//   - yields >= 1
//   - identical rows (tie-safe; see note above)
//   - kYieldReschedule not in final summary
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(InterpreterYieldTest, ExpandVariableForcedYieldRoundTrip) {
  // Build graph flag-OFF: 201 nodes + 200 directed edges (i -> i+1).
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  interpreter.Interpret("UNWIND range(0, 200) AS x CREATE (:N {id: x})");
  interpreter.Interpret("MATCH (a:N), (b:N) WHERE b.id = a.id + 1 CREATE (a)-[:E]->(b)");

  const std::string query = "MATCH (a:N)-[:E*1..2]->(b) RETURN b.id AS id ORDER BY id";

  // --- Baseline: flag OFF, pull-all.
  const std::string baseline = Render(interpreter.Interpret(query));

  // --- Yield run: flag ON, always-armed signal, pull-all.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  memgraph::utils::WorkerYieldRegistry registry(1);
  registry.SetCurrentWorker(0);
  auto *sig = memgraph::utils::WorkerYieldRegistry::GetCurrentYieldSignal();
  ASSERT_NE(sig, nullptr) << "WorkerYieldRegistry::GetCurrentYieldSignal() returned nullptr; "
                             "SetCurrentWorker(0) must have registered a TLS signal";
  // Arm the signal: every throttle-period check will see yield requested.
  sig->store(true, std::memory_order_release);

  auto [stream, qid] = interpreter.Prepare(query);
  const int yields = DrivePullToCompletion(stream, qid);

  // Unregister before any assertion that might throw.
  memgraph::utils::WorkerYieldRegistry::ClearCurrentWorker();
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  // ~399 rows -> floor(399/20) = 19 throttle fires -> >= 1 yield is non-flaky.
  EXPECT_GE(yields, 1) << "Expected >= 1 cooperative yield for variable-length MATCH (ExpandVariable "
                          "DFS cursor) with an always-armed yield signal; got 0. Either the flag gate "
                          "or the WorkerYieldRegistry TLS wiring inside ExpandVariable DoPull is broken.";

  // Result parity: tie-safe per the TIE-SAFETY NOTE above; compare rendered strings directly.
  const std::string yield_render = Render(stream);
  EXPECT_EQ(yield_render, baseline) << "YIELD-RUN RESULT MISMATCH for query: " << query << "\n  baseline:\n"
                                    << baseline << "\n  yield_run:\n"
                                    << yield_render;

  // Regression gate: kYieldReschedule must NOT leak into the final completion summary.
  const auto &final_summary = stream.GetSummary();
  EXPECT_EQ(final_summary.find(std::string(memgraph::query::kYieldReschedule)), final_summary.end())
      << "kYieldReschedule key leaked into the final completion summary; "
         "it must be intercepted by DrivePullToCompletion and never forwarded to the client.";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 7 — NamedPathForcedYieldRoundTrip
//
// Proves that the ConstructNamedPath cursor yields correctly through the real
// interpreter pipeline.
//
// Graph: 201 nodes (id 0..200) + 200 directed edges i->i+1, built flag-OFF.
//
// Query: "MATCH p = (a:N)-[:E*1..2]->(b) RETURN b.id AS id ORDER BY id"
// Binding "p =" forces the query planner to insert a ConstructNamedPath cursor
// that builds a Path object for each matched subgraph. The projected column is
// still b.id AS id (we avoid returning p directly to keep Render() output
// stable; path string representation is implementation-defined and not needed
// for parity verification).
//
// TIE-SAFETY NOTE: Identical to TEST 6 — variable-length 1..2 over the chain
// produces duplicate b.id values. ORDER BY id sorts by scalar integer id.
// Rows with equal id values render identically ("ROW: | <id>"), so tie-ordering
// does not affect the rendered string comparison. Parity is deterministic.
//
// Row count and throttle fires: same as TEST 6 (~399 rows, >= 19 fires).
// >= 1 yield is therefore non-flaky.
//
// Asserts:
//   - yields >= 1     (ConstructNamedPath yield path exercised)
//   - identical rows  (parity with flag-OFF baseline; tie-safe)
//   - kYieldReschedule not in final summary
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(InterpreterYieldTest, NamedPathForcedYieldRoundTrip) {
  // Build graph flag-OFF: 201 nodes + 200 directed edges (i -> i+1).
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  interpreter.Interpret("UNWIND range(0, 200) AS x CREATE (:N {id: x})");
  interpreter.Interpret("MATCH (a:N), (b:N) WHERE b.id = a.id + 1 CREATE (a)-[:E]->(b)");

  // "p =" binding forces ConstructNamedPath cursor into the plan.
  // We project b.id (not p) so that Render() output is a simple integer,
  // independent of any implementation-defined path string representation.
  const std::string query = "MATCH p = (a:N)-[:E*1..2]->(b) RETURN b.id AS id ORDER BY id";

  // --- Baseline: flag OFF, pull-all.
  const std::string baseline = Render(interpreter.Interpret(query));

  // --- Yield run: flag ON, always-armed signal, pull-all.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  memgraph::utils::WorkerYieldRegistry registry(1);
  registry.SetCurrentWorker(0);
  auto *sig = memgraph::utils::WorkerYieldRegistry::GetCurrentYieldSignal();
  ASSERT_NE(sig, nullptr) << "WorkerYieldRegistry::GetCurrentYieldSignal() returned nullptr; "
                             "SetCurrentWorker(0) must have registered a TLS signal";
  // Arm the signal: every throttle-period check will see yield requested.
  sig->store(true, std::memory_order_release);

  auto [stream, qid] = interpreter.Prepare(query);
  const int yields = DrivePullToCompletion(stream, qid);

  // Unregister before any assertion that might throw.
  memgraph::utils::WorkerYieldRegistry::ClearCurrentWorker();
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  // ~399 rows -> floor(399/20) = 19 throttle fires -> >= 1 yield is non-flaky.
  EXPECT_GE(yields, 1) << "Expected >= 1 cooperative yield for named-path MATCH (ConstructNamedPath "
                          "cursor) with an always-armed yield signal; got 0. Either the flag gate or "
                          "the WorkerYieldRegistry TLS wiring inside ConstructNamedPath DoPull is broken.";

  // Result parity: tie-safe per the TIE-SAFETY NOTE above; compare rendered strings directly.
  const std::string yield_render = Render(stream);
  EXPECT_EQ(yield_render, baseline) << "YIELD-RUN RESULT MISMATCH for query: " << query << "\n  baseline:\n"
                                    << baseline << "\n  yield_run:\n"
                                    << yield_render;

  // Regression gate: kYieldReschedule must NOT leak into the final completion summary.
  const auto &final_summary = stream.GetSummary();
  EXPECT_EQ(final_summary.find(std::string(memgraph::query::kYieldReschedule)), final_summary.end())
      << "kYieldReschedule key leaked into the final completion summary; "
         "it must be intercepted by DrivePullToCompletion and never forwarded to the client.";
}

}  // namespace
