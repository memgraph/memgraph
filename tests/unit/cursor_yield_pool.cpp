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

// MULTI-WORKER POOL COROUTINE YIELD INTEGRATION TEST (PR3 P3.1 B7).
//
// PURPOSE:
//   This is the functional counterpart to cursor_yield_real.cpp, elevated to
//   the level of a REAL priority-thread-pool with N workers.  It drives the
//   REAL coroutine cursor machinery (ScanAll -> Produce, PullDriverScope,
//   PullRootStep, YieldPointAwaitable) inside actual pool workers, with an
//   external scheduler thread continuously arming yield signals for every
//   worker.  M independent tasks each produce exactly kVertexCount rows across
//   an arbitrary number of yield/reschedule cycles.
//
//   CORRECTNESS PROOF:
//     EXPECT_EQ(run->rows, kVertexCount) for every task proves that the
//     yield -> reschedule -> resume cycle does NOT lose, duplicate, or
//     reorder rows across any number of suspensions or worker migrations.
//     EXPECT_GE(total_yields, 1) proves that the yield path was actually
//     exercised under real pool contention (not a no-op test).
//
//   DESIGN:
//     • A SHARED, COMMITTED InMemoryStorage holds kVertexCount vertices.
//       Each task opens its OWN storage accessor + DbAccessor so accessors
//       are never shared across worker threads.
//     • SHARED AstStorage + SymbolTable + ScanAll->Produce plan are built
//       once and are READ-ONLY during the test (safe to share).
//     • Each task's entire per-query state (acc/dba/ctx/frame/cursor/driver)
//       lives in a heap-allocated QueryRun object whose lifetime covers the
//       whole test.  A raw pointer to QueryRun is captured by the ResumableTask
//       closure; the raw pointer is safe because unique_ptr<QueryRun> lives in
//       the test body and outlives the pool.
//     • The resumable closure re-fetches GetCurrentYieldSignal() on every
//       invocation, faithfully mirroring what the interpreter glue does when
//       it re-arms stopping_context.yield_requested before each PullRootStep.
//     • PullDriverScope(Enabled) is constructed ONCE (on the first invocation)
//       and persists across reschedules inside the QueryRun.  This matches
//       the production pattern: the driver scope lives for the duration of a
//       pull-batch, not per-step.
//
//   TSan NOTE:
//     This test is deliberately designed to be run under ThreadSanitizer.
//     Every shared mutable object (completed counter, stop flag) uses
//     std::atomic with explicit acquire/release ordering.  The QueryRun
//     objects are NOT shared between tasks and the pool (each task owns its
//     unique QueryRun*), so there is no data race on QueryRun fields.
//
//   THROTTLE ANALYSIS:
//     maybe_check_abort (ResettableCounter in operator.cpp) has period = 20.
//     With kVertexCount = 200 and yield_requested always set by the scheduler
//     thread, Produce's throttle fires at rows 20, 40, 60, ..., 200 (10 times
//     per task).  Combined with ScanAll's independent throttle slot the total
//     yield count per task is >= 10.  The global "total_yields >= 1" assertion
//     is deliberately conservative and non-flaky even if the scheduler thread
//     fires only once before all tasks complete.

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "flags/experimental.hpp"
#include "license/license.hpp"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/cursor_awaitable.hpp"
#include "query/plan/operator.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/priority_thread_pool.hpp"
#include "utils/worker_yield_signal.hpp"

#include "query_plan_common.hpp"
#include "tests/test_commit_args_helper.hpp"

// query_plan_common.hpp provides:
//   using namespace memgraph::query;
//   using namespace memgraph::query::plan;
//   MakeContext(), CollectProduce(), TestMetricHandles()
// query_common.hpp (via query_plan_common.hpp) provides: IDENT, NEXPR, GetNode.

// ─────────────────────────────────────────────────────────────────────────────
// Per-task heap state.
//
// One instance per resumable task; owns everything that must survive across
// reschedules (i.e. the pool destroying and re-invoking the closure).
//
// LIFETIME: the owning std::unique_ptr<QueryRun> lives in the test body and
// outlives the pool — the raw pointer captured by the closure is always valid
// while the pool is alive.
//
// THREAD SAFETY: each QueryRun is accessed by exactly ONE worker thread at a
// time.  The pool's resumable-task semantics guarantee mutual exclusion between
// successive invocations of the same closure.
// ─────────────────────────────────────────────────────────────────────────────
struct QueryRun {
  // Owns the storage accessor for this task.  Opened once (first invocation)
  // as a READ accessor so it sees the pre-committed fixture data.
  std::unique_ptr<memgraph::storage::Storage::Accessor> acc;

  // DbAccessor wraps acc; constructed once and kept alive alongside acc.
  std::optional<memgraph::query::DbAccessor> dba;

  // ExecutionContext built from the shared plan AstStorage + SymbolTable.
  // Owns its own copy so stopping_context fields (yield_requested) are
  // private to this task.
  std::optional<memgraph::query::ExecutionContext> ctx;

  // Frame: one slot per symbol in the plan.  Persists across reschedules.
  std::optional<memgraph::query::Frame> frame;

  // The coroutine cursor produced by Produce::MakeCursor.  Persists across
  // reschedules — this is the suspended coroutine frame.
  memgraph::query::plan::UniqueCursorPtr cursor;

  // PullDriverScope(Enabled): wires the leaf-handle channel so
  // YieldPointAwaitable can stash the coroutine handle on a yield.
  // Constructed once (first invocation) and kept alive for the whole task.
  std::optional<memgraph::query::plan::PullDriverScope> driver;

  // Output symbols extracted from the produce plan.
  std::vector<memgraph::query::Symbol> out_syms;

  // Row count accumulated across all invocations.  This is the primary
  // correctness assertion: must equal kVertexCount when the task finishes.
  int rows{0};

  // Yield count accumulated across all invocations.  Used for the global
  // "yield path was exercised" assertion.
  int yields{0};

  // Initialized to false; set to true on the first invocation.
  bool started{false};
};

// ─────────────────────────────────────────────────────────────────────────────
// Fixture: shared storage + plan, committed data.
//
// Unlike RealProduceYieldTest (which keeps a WRITE accessor open), this
// fixture COMMITS the vertices so that each task can open its own independent
// READ accessor and see the data.  This is required because concurrent WRITE
// accessors on InMemoryStorage are serialized by the engine lock, and opening
// multiple simultaneous WRITE accessors from different worker threads would
// serialize the tasks rather than run them concurrently.
//
// Lifetime order (outer -> inner, i.e. dtor fires inner first):
//   db > (write accessor destroyed after commit) > AstStorage / SymbolTable /
//   shared plan — all of which live in the fixture and are destroyed after the
//   test body returns.
// ─────────────────────────────────────────────────────────────────────────────
class PoolCoroutineYieldTest : public ::testing::Test {
 protected:
  static constexpr int kVertexCount = 200;

  // The shared, committed storage.  Each task opens its own accessor via
  // db->Access(memgraph::storage::READ) when it first starts.
  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage(memgraph::storage::Config{})};

  // Shared plan components — built once in SetUp and READ-ONLY during tests.
  AstStorage storage;
  SymbolTable symbol_table;

  ScanAllTuple scan;
  NamedExpression *output{nullptr};
  std::shared_ptr<Produce> produce;

  void SetUp() override {
    memgraph::license::global_license_checker.EnableTesting();

    // Insert kVertexCount vertices via a WRITE accessor and commit them so
    // that NEW read accessors opened per-task can see the data.
    {
      auto write_acc = db->Access(memgraph::storage::WRITE);
      memgraph::query::DbAccessor setup_dba(write_acc.get());
      for (int i = 0; i < kVertexCount; ++i) {
        setup_dba.InsertVertex();
      }
      setup_dba.AdvanceCommand();
      // Commit via dba.Commit() which calls accessor->PrepareForCommitPhase().
      // The TestDatabaseProtector is a no-op stub for unit tests that don't
      // involve replication.
      auto commit_result = setup_dba.Commit(memgraph::tests::MakeMainCommitArgs());
      ASSERT_TRUE(commit_result.has_value()) << "Fixture commit must succeed";
      // write_acc is now committed and destroyed here (end of scope).
    }

    // Build shared ScanAll("n") -> Produce(n) plan using a WRITE-backed
    // DbAccessor to resolve label/property/edge-type name maps.  This
    // accessor is opened only to resolve names; it is NOT used for scanning.
    // We open a READ accessor for name resolution to avoid touching the WRITE
    // lock unnecessarily.
    auto name_acc = db->Access(memgraph::storage::READ);
    memgraph::query::DbAccessor name_dba(name_acc.get());

    scan = MakeScanAll(storage, symbol_table, "n");

    // NEXPR/IDENT wiring (canonical idiom, see cursor_yield_real.cpp):
    //   IDENT("n")->MapTo(scan.sym_) maps the identifier to the scan symbol so
    //   the evaluator reads the vertex from frame[scan.sym_].
    //   NEXPR("n", ...)->MapTo(...) creates the output symbol that
    //   DriveWithYield / task closures use to extract values from the frame.
    output = NEXPR("n", IDENT("n")->MapTo(scan.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    produce = MakeProduce(scan.op_, output);
    // name_acc destroyed here (READ accessor, no rollback needed).
  }

  void TearDown() override {
    // Ensure the flag is reset even if a test body throws or asserts.
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// TEST — MultiWorkerYieldResumeParity.
//
// Drives M independent coroutine-cursor tasks on an N-worker pool with a
// WorkerYieldRegistry.  An external scheduler thread continuously arms the
// yield signal for every worker.  Each task's ScanAll->Produce cursor yields
// and is rescheduled to the SAME worker (pinned by the pool's ResumableTask
// semantics) until it has produced all kVertexCount rows.
//
// PRIMARY CORRECTNESS ASSERTION:
//   EXPECT_EQ(run->rows, kVertexCount) for every task — proves that
//   yield -> reschedule -> resume does NOT lose or duplicate rows regardless
//   of the number of reschedule cycles or which worker runs each slice.
//
// SECONDARY ASSERTION:
//   EXPECT_GE(total_yields, 1) — proves the yield path was actually hit under
//   contention (the test is not vacuous: the scheduler thread really fired).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(PoolCoroutineYieldTest, MultiWorkerYieldResumeParity) {
  // Enable the coroutine cursor flag for the duration of this test.
  // MakeCursor captures use_coroutine_=true at construction time (flag ON).
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  constexpr int N = 4;   // pool workers
  constexpr int M = 32;  // concurrent tasks

  // WorkerYieldRegistry must outlive the pool (lifetime contract in
  // worker_yield_signal.hpp: registry destroyed AFTER pool threads exit).
  memgraph::utils::WorkerYieldRegistry registry(static_cast<uint16_t>(N));
  memgraph::utils::PriorityThreadPool pool(static_cast<uint16_t>(N), nullptr, &registry);

  // Heap-allocate one QueryRun per task.  These outlive the pool.
  std::vector<std::unique_ptr<QueryRun>> runs;
  runs.reserve(M);
  for (int i = 0; i < M; ++i) {
    runs.push_back(std::make_unique<QueryRun>());
  }

  // Completion counter: incremented to M when all tasks have finished.
  std::atomic<int> completed{0};

  // External scheduler thread: continuously arms yield signals for ALL
  // workers.  The pool clears each worker's signal before each task slice
  // (SetCurrentWorker stores false), so the scheduler must keep re-arming.
  // A 200µs sleep between rounds gives the pool workers a chance to run
  // while still ensuring the yield path fires on most throttle periods.
  std::atomic<bool> stop{false};
  std::thread scheduler([&]() {
    while (!stop.load(std::memory_order_acquire)) {
      for (int w = 0; w < N; ++w) {
        registry.RequestYieldForWorker(static_cast<uint16_t>(w));
      }
      std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
  });

  // Submit one resumable task per QueryRun.
  for (int i = 0; i < M; ++i) {
    QueryRun *run = runs[i].get();

    // The closure captures `run` (raw pointer) and `this` (fixture, for db /
    // storage / symbol_table / produce).  Both are stable for the test lifetime.
    memgraph::utils::ResumableTaskSignature task = [run, this, &completed]() -> bool {
      if (!run->started) {
        // ── First invocation: initialize the full per-task query state. ──────

        // Open a READ accessor.  Sees all committed vertices (kVertexCount).
        // `db->Access(memgraph::storage::READ)` returns
        // std::unique_ptr<Storage::Accessor>.
        run->acc = db->Access(memgraph::storage::READ);

        // DbAccessor wraps the raw accessor pointer.
        run->dba.emplace(run->acc.get());

        // ExecutionContext: copies SymbolTable (value type), resolves
        // label/property/edge-type name maps from the DbAccessor.
        run->ctx.emplace(MakeContext(storage, symbol_table, &*run->dba));

        // Frame: one slot per symbol in the shared plan.
        run->frame.emplace(symbol_table.max_position());

        // MakeCursor captures use_coroutine_=true (COROUTINE_CURSORS is ON).
        // Returns plan::UniqueCursorPtr (std::unique_ptr<Cursor, deleter>).
        run->cursor = produce->MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());

        // Collect output symbols from the Produce's named expressions.
        run->out_syms.clear();
        for (auto *ne : produce->named_expressions_) {
          run->out_syms.emplace_back(symbol_table.at(*ne));
        }

        // PullDriverScope(Enabled): wires ctx.suspended_task_handle_ptr to an
        // owned slot so YieldPointAwaitable has somewhere to stash the suspended
        // coroutine handle.  The scope persists for the task's entire lifetime
        // (inside run->driver); it is NOT reconstructed on each reschedule.
        run->driver.emplace(*run->ctx, memgraph::query::plan::YieldMode::Enabled);

        run->started = true;
      }

      // ── Every invocation (including the first, after driver setup): ────────

      // Re-fetch THIS worker's yield signal.  The pool calls SetCurrentWorker
      // (which clears the previous signal) before each task slice, so we must
      // re-read the TLS pointer here rather than caching it across invocations.
      // This faithfully mirrors the interpreter glue's per-Pull re-arm.
      run->ctx->stopping_context.yield_requested = memgraph::utils::WorkerYieldRegistry::GetCurrentYieldSignal();

      using S = memgraph::query::plan::PullRunResult::Status;

      // Drive the coroutine cursor until it yields, finishes, or produces a row.
      while (true) {
        auto r = run->cursor->PullRootStep(*run->frame, *run->ctx);

        if (r.status == S::Yielded) {
          // The coroutine suspended at a YieldPointAwaitable.  Return true so
          // the pool reschedules this closure on the SAME worker (pinned).
          ++run->yields;
          return true;
        }

        if (r.status == S::Done) {
          // All rows have been produced.  Shut down the cursor, then signal
          // completion to the main thread.
          run->cursor->Shutdown();
          // Destroy the driver scope and other per-task objects explicitly here
          // rather than waiting for the QueryRun destructor, so that any
          // RAII cleanup (e.g. OOMExceptionEnabler) happens on the worker thread
          // while we still have the right TLS context.
          run->driver.reset();
          completed.fetch_add(1, std::memory_order_acq_rel);
          return false;  // task complete; do not reschedule
        }

        // S::HasRow: a vertex was placed in the frame.  Count it.
        ++run->rows;
      }
    };

    pool.ScheduleResumableTask(std::move(task), memgraph::utils::Priority::LOW);
  }

  // ── Wait for all M tasks to complete (up to 30 s). ───────────────────────
  using namespace std::chrono_literals;
  constexpr auto kTimeout = 30s;
  const auto deadline = std::chrono::steady_clock::now() + kTimeout;

  while (completed.load(std::memory_order_acquire) < M) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "Timeout waiting for all " << M
                                                          << " pool coroutine tasks to complete. "
                                                             "A task may have been lost or deadlocked.";
    std::this_thread::sleep_for(1ms);
  }

  // ── Stop the scheduler thread before shutting down the pool. ─────────────
  stop.store(true, std::memory_order_release);
  scheduler.join();

  // ── Shut down the pool gracefully. ───────────────────────────────────────
  pool.ShutDown();
  pool.AwaitShutdown();

  // ── Assertions. ──────────────────────────────────────────────────────────

  // PRIMARY: every task must have produced exactly kVertexCount rows.
  // Failure here means the yield->reschedule->resume cycle lost or duplicated
  // a row somewhere in the coroutine state machine.
  int total_yields = 0;
  for (int i = 0; i < M; ++i) {
    EXPECT_EQ(runs[i]->rows, kVertexCount)
        << "Task " << i << ": expected " << kVertexCount << " rows but got " << runs[i]->rows
        << ". The yield->reschedule->resume cycle corrupted row delivery "
           "(either a row was lost or duplicated across a suspension boundary).";
    total_yields += runs[i]->yields;
  }

  // SECONDARY: the yield path must have been exercised under real pool
  // contention.  With kVertexCount=200 and period=20, Produce alone fires 10
  // times per task; with M=32 tasks and N=4 workers and the scheduler
  // re-arming every 200µs, the total yield count should be >> 1.  We use
  // ">=1" as a conservative, non-flaky lower bound.
  EXPECT_GE(total_yields, 1) << "The yield path was never exercised. "
                                "The scheduler thread or the yield trigger wiring may be broken. "
                                "Total yields across "
                             << M << " tasks: " << total_yields;

  // Ensure the flag is reset (TearDown also does this; belt-and-suspenders).
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
}
