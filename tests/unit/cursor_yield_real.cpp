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

// REAL PRODUCE CURSOR YIELD TEST (Phase 3 gate).
//
// Exercises the newly-wired cooperative-yield trigger inside
// Produce::ProduceCursor::DoPull (co_await YieldPointAwaitable replaces AbortCheck,
// flag-ON path only) using a REAL InMemoryStorage, a real ScanAll + Produce plan, and
// a real DbAccessor.
//
// DESIGN:
//   A FIXTURE creates 50 in-memory vertices, builds a ScanAll->Produce plan, and provides:
//   - a "legacy baseline" run via CollectProduce() (flag OFF, standard Pull() path),
//   - a "yield-aware" run via DriveWithYield() (flag ON, PullRootStep loop).
//
//   DriveWithYield():
//     • Installs PullDriverScope(Enabled) to wire the leaf-handle channel.
//     • Arms ctx.stopping_context.yield_requested = &yield_sig (always true) so
//       every throttle-firing (every 20th Produce iteration) returns Yield.
//     • Loops cursor->PullRootStep():
//         - Yielded  → count yield, continue (re-arm, same coroutine resumes next step).
//         - HasRow   → capture frame symbols into output.
//         - Done     → break.
//     • Returns rows + yield count.
//
// TEST 1 — RealProduceYieldParity:
//   Baseline (flag OFF) and yield-run (flag ON) must produce identical row counts (50)
//   and identical vertex GIDs.  The yield-run must show >= 1 Yielded step.
//   OOM counter must be rebalanced to 0 after the complete drive.
//
// TEST 2 — RealProduceAbandonMidYieldNoLeak (TLS-hazard gate):
//   Drive until the FIRST Yielded step, then ABANDON (destroy cursor) without resuming.
//   At the suspension point OOMExceptionEnabler is alive on the coroutine frame
//   (it is constructed inside DoPull's while-loop before co_await YieldPointAwaitable).
//   Frame destruction must run the in-scope dtor, rebalancing counter_ to 0.
//
// NOTE: maybe_check_abort (the shared throttle counter in operator.cpp) has period=20.
// With 50 vertices and yield_requested always armed, floor(50/20) = 2 guaranteed yields,
// so "yields >= 1" is a non-flaky assertion even at the minimum vertex count.
// TEST 2 uses the same vertex count; the coroutine is guaranteed to yield on the 20th
// iteration (where the throttle fires for the first time).

#include <atomic>
#include <memory>
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
#include "utils/memory_tracker.hpp"

#include "query_plan_common.hpp"

// query_plan_common.hpp provides: using namespace memgraph::query;
//                                 using namespace memgraph::query::plan;
// query_common.hpp (included by query_plan_common.hpp) provides: IDENT, NEXPR, GetNode.

// ─────────────────────────────────────────────────────────────────────────────
// Fixture: 50 in-memory vertices, ScanAll -> Produce plan.
//
// IDENT/NEXPR symbol wiring (canonical idiom from query_plan_match_filter_return.cpp):
//
//   auto scan  = MakeScanAll(storage, symbol_table, "n");
//   // scan.sym_ is the Symbol for "n" (already MapTo'd onto the node atom's identifier).
//
//   auto output = NEXPR("n", IDENT("n")->MapTo(scan.sym_))
//                     ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
//   // IDENT("n") creates an Identifier; ->MapTo(scan.sym_) maps it to the scan symbol
//   //   so the evaluator reads the vertex from the correct frame slot.
//   // NEXPR("n", ...) wraps it in a NamedExpression; ->MapTo(...) creates the output
//   //   symbol whose name CollectProduce/DriveWithYield reads from frame[symbol].
//
//   auto produce = MakeProduce(scan.op_, output);
//
// The output symbol (result of the NEXPR->MapTo) is what context->symbol_table.at(*ne)
// resolves to for each NamedExpression* in produce.named_expressions_.
// ─────────────────────────────────────────────────────────────────────────────
class RealProduceYieldTest : public ::testing::Test {
 protected:
  static constexpr int kVertexCount = 50;

  // Storage / accessor lifetime: storage > storage_dba > dba.
  // dba must be destroyed before storage_dba, which must be destroyed before db.
  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage(memgraph::storage::Config{})};
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{db->Access(memgraph::storage::WRITE)};
  memgraph::query::DbAccessor dba{storage_dba.get()};

  AstStorage storage;
  SymbolTable symbol_table;

  // Plan components — built in SetUp after vertices are inserted.
  ScanAllTuple scan;
  NamedExpression *output{nullptr};
  std::shared_ptr<Produce> produce;

  void SetUp() override {
    memgraph::license::global_license_checker.EnableTesting();

    // Insert kVertexCount vertices and commit them so View::OLD can see them.
    for (int i = 0; i < kVertexCount; ++i) {
      dba.InsertVertex();
    }
    dba.AdvanceCommand();

    // Build plan: ScanAll("n") -> Produce(n).
    scan = MakeScanAll(storage, symbol_table, "n");

    // NEXPR/IDENT wiring: IDENT("n") mapped to scan.sym_ so the evaluator reads
    // the vertex from frame[scan.sym_]; the NEXPR output symbol is what
    // CollectProduce and DriveWithYield use to extract the value.
    output = NEXPR("n", IDENT("n")->MapTo(scan.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

    produce = MakeProduce(scan.op_, output);
  }

  void TearDown() override {
    // Ensure the flag is reset even if a test body throws.
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// YieldDrive: result of DriveWithYield.
// ─────────────────────────────────────────────────────────────────────────────
struct YieldDrive {
  std::vector<std::vector<TypedValue>> rows;
  int yields{0};
};

// ─────────────────────────────────────────────────────────────────────────────
// DriveWithYield: runs a Produce cursor to completion under the yield-enabled
// coroutine path, collecting rows and counting Yielded steps.
//
// Precondition: COROUTINE_CURSORS flag must already be ON at call time (so that
// produce.MakeCursor() captures use_coroutine_=true in the Cursor ctor).
//
// yield_sig must remain alive for the duration of the call.
// ─────────────────────────────────────────────────────────────────────────────
static YieldDrive DriveWithYield(const Produce &produce, ExecutionContext *ctx, std::atomic<bool> *yield_sig) {
  Frame frame(ctx->symbol_table.max_position());

  // Collect the output symbols from the Produce's named expressions.
  std::vector<Symbol> symbols;
  symbols.reserve(produce.named_expressions_.size());
  for (auto *ne : produce.named_expressions_) {
    symbols.emplace_back(ctx->symbol_table.at(*ne));
  }

  // MakeCursor captures use_coroutine_ from the COROUTINE_CURSORS flag at ctor-time.
  // The flag MUST be ON before this call.
  auto cursor = produce.MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());

  // Wire the yield channel: the scheduler normally does this per-worker.
  // For the unit test we do it manually before the driver scope activates.
  ctx->stopping_context.yield_requested = yield_sig;

  // PullDriverScope(Enabled): installs the leaf-handle slot so YieldPointAwaitable
  // has somewhere to write the suspended frame handle.
  memgraph::query::plan::PullDriverScope driver(*ctx, memgraph::query::plan::YieldMode::Enabled);

  YieldDrive out;

  while (true) {
    auto r = cursor->PullRootStep(frame, *ctx);
    using S = memgraph::query::plan::PullRunResult::Status;

    if (r.status == S::Yielded) {
      ++out.yields;
      continue;  // same coroutine resumes on the next PullRootStep call
    }
    if (r.status == S::Done) {
      break;
    }
    // S::HasRow: capture frame values.
    std::vector<TypedValue> vals;
    vals.reserve(symbols.size());
    for (auto &s : symbols) {
      vals.emplace_back(frame[s]);
    }
    out.rows.emplace_back(std::move(vals));
  }

  cursor->Shutdown();
  return out;
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 1 — RealProduceYieldParity.
//
// Baseline (flag OFF): CollectProduce via the legacy Pull() path → 50 rows.
// Yield run (flag ON): DriveWithYield via PullRootStep → 50 rows, same GIDs.
//
// Assertions:
//   - Both runs produce exactly kVertexCount rows.
//   - Every row's single TypedValue is a vertex, and GIDs match in order.
//   - The yield run shows >= 1 Yielded step (throttle period=20, 50 vertices
//     → guaranteed 2 fires at rows 20 and 40; ">=1" keeps the assertion robust).
//   - OOM counter is rebalanced to 0 after the complete drive (no enabler leaks).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RealProduceYieldTest, RealProduceYieldParity) {
  // ── Baseline: legacy path (COROUTINE_CURSORS OFF). ──────────────────────
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  std::vector<memgraph::storage::Gid> baseline_gids;
  {
    auto ctx = MakeContext(storage, symbol_table, &dba);
    auto baseline = CollectProduce(*produce, &ctx);
    ASSERT_EQ(static_cast<int>(baseline.size()), kVertexCount)
        << "Legacy path must produce exactly " << kVertexCount << " rows";

    baseline_gids.reserve(baseline.size());
    for (const auto &row : baseline) {
      ASSERT_EQ(row.size(), 1u) << "Each row must have exactly one value";
      ASSERT_TRUE(row[0].IsVertex()) << "Row value must be a vertex";
      baseline_gids.push_back(row[0].ValueVertex().Gid());
    }
  }

  // ── Yield run: coroutine path (COROUTINE_CURSORS ON). ──────────────────
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  std::atomic<bool> yield_sig{true};  // always armed: every throttle-fire → Yield
  auto ctx = MakeContext(storage, symbol_table, &dba);

  auto result = DriveWithYield(*produce, &ctx, &yield_sig);

  ASSERT_EQ(static_cast<int>(result.rows.size()), kVertexCount)
      << "Coroutine path must produce exactly " << kVertexCount << " rows";

  // GID parity: same order, same vertex GIDs.
  for (int i = 0; i < kVertexCount; ++i) {
    ASSERT_EQ(result.rows[i].size(), 1u) << "Row " << i << " must have exactly one value";
    ASSERT_TRUE(result.rows[i][0].IsVertex()) << "Row " << i << " value must be a vertex";
    EXPECT_EQ(result.rows[i][0].ValueVertex().Gid(), baseline_gids[i])
        << "Row " << i << ": GID mismatch between legacy and coroutine paths";
  }

  // Yield count: throttle period=20, 50 vertices → 2 fires guaranteed.
  EXPECT_GE(result.yields, 1) << "Coroutine path with yield_requested=true must yield at least once "
                                 "(throttle period=20, "
                              << kVertexCount << " vertices → >=1 Yielded step guaranteed)";

  // OOM counter rebalance: after a complete drive, no OOMExceptionEnabler must
  // be alive on this thread.  The enabler is scoped inside DoPull's while-body;
  // each co_yield/co_return closes the scope, so the counter is 0 at Done.
  EXPECT_FALSE(memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler::CanThrow())
      << "OOM counter must be rebalanced to 0 after a complete coroutine drive "
         "(no OOMExceptionEnabler alive on this thread)";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 2 — RealProduceAbandonMidYieldNoLeak (TLS-hazard gate).
//
// The hazard: Produce::ProduceCursor::DoPull holds an OOMExceptionEnabler alive
// inside the while-loop scope (constructed before co_await YieldPointAwaitable).
// If the cursor is destroyed while the coroutine is suspended at the yield point,
// the coroutine frame must be cleaned up (via the PullAwaitable dtor in Cursor::~gen_),
// running the in-scope OOMExceptionEnabler dtor and decrementing counter_.
//
// If the frame dtor does NOT run (leak), counter_ stays elevated → CanThrow()
// returns true after destruction → a future spurious OOM exception could fire on
// an unrelated allocation.
//
// Protocol:
//   1. Assert precondition: CanThrow() == false at entry.
//   2. Drive until the FIRST Yielded step.
//   3. Assert: CanThrow() == true (enabler alive mid-yield on coroutine frame).
//   4. Destroy the cursor (cursor.reset()).
//   5. Assert: CanThrow() == false (frame dtor ran, counter decremented).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RealProduceYieldTest, RealProduceAbandonMidYieldNoLeak) {
  using OOMEnabler = memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler;

  // Precondition: no OOM enabler must be alive on this thread at test entry.
  // If this fires, a prior test leaked an enabler — the EXPECT_TRUE/FALSE logic
  // below would be unreliable.
  ASSERT_FALSE(OOMEnabler::CanThrow()) << "Precondition violated: OOMExceptionEnabler counter must be 0 at test entry. "
                                          "A prior test leaked an OOMExceptionEnabler on this thread.";

  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  std::atomic<bool> yield_sig{true};  // always armed
  auto ctx = MakeContext(storage, symbol_table, &dba);

  Frame frame(ctx.symbol_table.max_position());

  // MakeCursor captures use_coroutine_=true (flag is ON).
  auto cursor = produce->MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());

  ctx.stopping_context.yield_requested = &yield_sig;

  // PullDriverScope: wires the leaf-handle channel (required by YieldPointAwaitable).
  // We use a raw scope (not the DriveWithYield helper) so we control the stopping point.
  memgraph::query::plan::PullDriverScope driver(ctx, memgraph::query::plan::YieldMode::Enabled);

  // Drive until the FIRST Yielded step.
  // With maybe_check_abort period=20 and 50 vertices, the coroutine yields on
  // the 20th iteration (throttle fires for the first time).
  bool found_yield = false;
  for (int step = 0; step < kVertexCount + 100; ++step) {
    auto r = cursor->PullRootStep(frame, ctx);
    using S = memgraph::query::plan::PullRunResult::Status;

    if (r.status == S::Yielded) {
      found_yield = true;
      break;
    }
    if (r.status == S::Done) {
      break;  // exhausted without yielding — OOM precondition will catch this
    }
    // S::HasRow: continue driving to reach the first yield.
  }

  ASSERT_TRUE(found_yield) << "The coroutine must have yielded at least once before exhaustion "
                              "(yield_requested=true always, throttle period=20, "
                           << kVertexCount
                           << " vertices). "
                              "If this fails, the yield trigger is not wired in DoPull.";

  // At this point the coroutine is suspended mid-loop with OOMExceptionEnabler
  // alive on its frame (constructed inside DoPull's while(true){} before co_await).
  EXPECT_TRUE(OOMEnabler::CanThrow()) << "OOMExceptionEnabler must be alive on the suspended coroutine frame "
                                         "(CanThrow() must be true while the coroutine is parked mid-yield).";

  // ABANDON: destroy the cursor without resuming.
  // UniqueCursorPtr dtor calls the deleter, which calls ~ProduceCursor,
  // which calls ~Cursor, which destroys gen_ (the PullAwaitable),
  // which calls coroutine_handle::destroy() on the suspended frame,
  // running all dtors of in-scope locals — including OOMExceptionEnabler.
  cursor.reset();

  // After frame destruction the OOM counter must be back to 0.
  EXPECT_FALSE(OOMEnabler::CanThrow())
      << "OOMExceptionEnabler counter must be 0 after cursor destruction "
         "(coroutine frame dtor must have run the in-scope OOMExceptionEnabler dtor). "
         "If this fails, the coroutine frame leaked (frame dtor did not run on cursor.reset()).";
}
