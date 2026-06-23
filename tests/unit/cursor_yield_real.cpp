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
#include "query/exceptions.hpp"
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

// ─────────────────────────────────────────────────────────────────────────────
// TEST 3 — ScanFilterProduceYieldParity (deep chain, key yield-trigger gate).
//
// Plan: ScanAll("n") -> Filter(LITERAL(true)) -> Produce(n).
//
// Filter predicate: LITERAL(true) — always-true.
// Rationale: property plumbing in a TEST_F body (vertices already inserted in
// SetUp) would require opening a second write accessor, careful AdvanceCommand
// sequencing, and TLS-scope ownership, all orthogonal to the yield-trigger
// correctness under test.  An always-true Filter still exercises
// Filter::FilterCursor::DoPull's cooperative-yield point (the co_await
// YieldPointAwaitable that replaced AbortCheck in the DoPull body), which is
// the actual gate.
//
// Assertions:
//   - Baseline (flag OFF) and yield run (flag ON) produce identical row counts
//     (kVertexCount) and identical vertex GIDs (order-stable InMemoryStorage).
//   - Yield run yields >= 1 times (ScanAll + Filter + Produce each hold a
//     throttle slot; with 50 vertices and period=20 the combined count is >= 2).
//   - OOM counter rebalanced to 0 after complete drive (ScanAll + Filter +
//     Produce each hold an OOMExceptionEnabler; all must be released at Done).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RealProduceYieldTest, ScanFilterProduceYieldParity) {
  // ── Build deep chain plan in test body (separate from fixture's scan/produce). ──
  //
  // We reuse the fixture's storage, symbol_table, and dba.  The 50 vertices are
  // already inserted and committed by SetUp.  We build a fresh symbol + ScanAll
  // -> Filter -> Produce triple so this test is self-contained.
  AstStorage local_ast;

  // ScanAll("m") — separate symbol to avoid colliding with the fixture's "n".
  auto local_scan = MakeScanAll(local_ast, symbol_table, "m");

  // Filter(LITERAL(true)): always passes all rows; exercises FilterCursor::DoPull.
  auto *always_true = local_ast.Create<PrimitiveLiteral>(true);
  auto filter_op =
      std::make_shared<Filter>(local_scan.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, always_true);

  // Produce("m"): wire the output symbol.
  auto *local_output =
      local_ast.Create<NamedExpression>("m", local_ast.Create<Identifier>("m")->MapTo(local_scan.sym_));
  auto out_sym = symbol_table.CreateSymbol("named_expression_m", true);
  local_output->MapTo(out_sym);
  auto local_produce = MakeProduce(filter_op, local_output);

  // ── Baseline: legacy path (COROUTINE_CURSORS OFF). ──────────────────────
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  std::vector<memgraph::storage::Gid> baseline_gids;
  {
    auto ctx = MakeContext(local_ast, symbol_table, &dba);
    auto baseline = CollectProduce(*local_produce, &ctx);
    ASSERT_EQ(static_cast<int>(baseline.size()), kVertexCount)
        << "Legacy path must produce exactly " << kVertexCount << " rows through Filter(LITERAL(true))";

    baseline_gids.reserve(baseline.size());
    for (const auto &row : baseline) {
      ASSERT_EQ(row.size(), 1u);
      ASSERT_TRUE(row[0].IsVertex());
      baseline_gids.push_back(row[0].ValueVertex().Gid());
    }
  }

  // ── Yield run: coroutine path (COROUTINE_CURSORS ON). ──────────────────
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  std::atomic<bool> yield_sig{true};
  auto ctx = MakeContext(local_ast, symbol_table, &dba);

  auto result = DriveWithYield(*local_produce, &ctx, &yield_sig);

  ASSERT_EQ(static_cast<int>(result.rows.size()), kVertexCount)
      << "Coroutine path must produce exactly " << kVertexCount << " rows through Filter(LITERAL(true))";

  // GID parity: ScanAll over InMemoryStorage is deterministic for a fixed insertion order.
  // Compare ordered — same ordering is guaranteed across both runs for the same storage.
  for (int i = 0; i < kVertexCount; ++i) {
    ASSERT_EQ(result.rows[i].size(), 1u) << "Row " << i << " must have exactly one value";
    ASSERT_TRUE(result.rows[i][0].IsVertex()) << "Row " << i << " value must be a vertex";
    EXPECT_EQ(result.rows[i][0].ValueVertex().Gid(), baseline_gids[i])
        << "Row " << i << ": GID mismatch between legacy and coroutine paths (ScanAll+Filter+Produce)";
  }

  // Yield count: ScanAll, Filter, and Produce each have an independent throttle
  // slot (maybe_check_abort is shared thread_local with period=20).  With 50
  // vertices the combined throttle fires at least 2 times; ">=1" is conservative.
  EXPECT_GE(result.yields, 1) << "Deep chain (ScanAll+Filter+Produce) must yield at least once "
                                 "with yield_requested=true and 50 vertices";

  // OOM counter: all three cursors hold OOMExceptionEnabler inside DoPull;
  // all must release it on normal completion.
  EXPECT_FALSE(memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler::CanThrow())
      << "OOM counter must be rebalanced to 0 after a complete ScanAll+Filter+Produce drive";

  // Safety: reset flag for TearDown.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 4 — YieldEnabledAbortStillThrows.
//
// Both yield_requested (always-true) and transaction_status (TERMINATED) are
// set simultaneously.  The CheckAbortOrYield contract mandates that Abort is
// evaluated BEFORE Yield, so the cursor must throw HintedAbortError rather than
// quietly suspending.
//
// This proves the yield-wiring did not break transaction-abort priority.
//
// Protocol:
//   1. flag ON.
//   2. Set transaction_status = TERMINATED and yield_requested = true.
//   3. Drive PullRootStep in a loop inside EXPECT_THROW.
//   4. Expect HintedAbortError to be thrown on the first throttle-fire (row 20).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RealProduceYieldTest, YieldEnabledAbortStillThrows) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  std::atomic<bool> yield_sig{true};  // yield armed
  std::atomic<memgraph::query::TransactionStatus> tx_status{memgraph::query::TransactionStatus::TERMINATED};

  auto ctx = MakeContext(storage, symbol_table, &dba);
  ctx.stopping_context.yield_requested = &yield_sig;
  ctx.stopping_context.transaction_status = &tx_status;

  Frame frame(ctx.symbol_table.max_position());

  // MakeCursor captures use_coroutine_=true (flag is ON).
  auto cursor = produce->MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());

  // PullDriverScope: wires the leaf-handle channel (required by YieldPointAwaitable).
  memgraph::query::plan::PullDriverScope driver(ctx, memgraph::query::plan::YieldMode::Enabled);

  // Drive until HintedAbortError is thrown (expected at first throttle-fire,
  // i.e., row 20).  The lambda captures by reference; EXPECT_THROW evaluates it.
  EXPECT_THROW(
      {
        for (int step = 0; step < kVertexCount + 100; ++step) {
          // PullRootStep throws HintedAbortError when abort precedes yield.
          [[maybe_unused]] auto r = cursor->PullRootStep(frame, ctx);
        }
      },
      memgraph::query::HintedAbortError)
      << "With transaction_status=TERMINATED and yield_requested=true, "
         "PullRootStep must throw HintedAbortError (abort precedes yield). "
         "If this fails, the yield path incorrectly swallowed the abort signal.";

  // Safety: reset flag for TearDown.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 5 — DeepChainAbandonMidYieldNoLeak (TLS-hazard gate, deep chain).
//
// Plan: ScanAll("p") -> Filter(LITERAL(true)) -> Produce(p).
//
// All three cursors (ScanAll, Filter, Produce) hold an OOMExceptionEnabler on
// their suspended coroutine frame when the chain is parked mid-yield.
// Destroying the cursor without resuming must run all three dtors via the
// coroutine frame destruction cascade.
//
// If any frame's dtor does NOT run, counter_ stays elevated after cursor.reset()
// and CanThrow() returns true — exposing a TLS-side-effect leak that could fire
// a spurious OOM on an unrelated allocation in a future query.
//
// Protocol:
//   1. Assert CanThrow()==false at entry (precondition).
//   2. Drive until the FIRST Yielded step.
//   3. Assert CanThrow()==true (>=1 enabler alive on suspended frames).
//   4. cursor.reset() — destroy without resuming.
//   5. Assert CanThrow()==false (all frame dtors ran).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RealProduceYieldTest, DeepChainAbandonMidYieldNoLeak) {
  using OOMEnabler = memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler;

  // Precondition: no stale enabler from a prior test.
  ASSERT_FALSE(OOMEnabler::CanThrow()) << "Precondition violated: OOM counter must be 0 at test entry.";

  // Build deep chain plan: ScanAll("p") -> Filter(LITERAL(true)) -> Produce(p).
  AstStorage local_ast;

  auto local_scan = MakeScanAll(local_ast, symbol_table, "p");

  auto *always_true = local_ast.Create<PrimitiveLiteral>(true);
  auto filter_op =
      std::make_shared<Filter>(local_scan.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, always_true);

  auto *local_output =
      local_ast.Create<NamedExpression>("p", local_ast.Create<Identifier>("p")->MapTo(local_scan.sym_));
  auto out_sym = symbol_table.CreateSymbol("named_expression_p", true);
  local_output->MapTo(out_sym);
  auto local_produce = MakeProduce(filter_op, local_output);

  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  std::atomic<bool> yield_sig{true};
  auto ctx = MakeContext(local_ast, symbol_table, &dba);
  ctx.stopping_context.yield_requested = &yield_sig;

  Frame frame(ctx.symbol_table.max_position());

  auto cursor = local_produce->MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());

  memgraph::query::plan::PullDriverScope driver(ctx, memgraph::query::plan::YieldMode::Enabled);

  // Drive until the FIRST Yielded step.
  bool found_yield = false;
  for (int step = 0; step < kVertexCount + 100; ++step) {
    auto r = cursor->PullRootStep(frame, ctx);
    using S = memgraph::query::plan::PullRunResult::Status;

    if (r.status == S::Yielded) {
      found_yield = true;
      break;
    }
    if (r.status == S::Done) {
      break;
    }
    // S::HasRow: continue to reach first yield.
  }

  ASSERT_TRUE(found_yield) << "Deep chain (ScanAll+Filter+Produce) must yield at least once "
                              "(yield_requested=true always, throttle period=20, "
                           << kVertexCount
                           << " vertices). "
                              "If this fails, the yield trigger is not wired in one of the DoPull bodies.";

  // At least one OOMExceptionEnabler is alive on a suspended frame in the chain
  // (Produce, Filter, or ScanAll — whichever was inside DoPull's while-body
  // when the co_await fired).
  EXPECT_TRUE(OOMEnabler::CanThrow())
      << "OOMExceptionEnabler must be alive on a suspended coroutine frame in the chain "
         "(CanThrow() must be true while the deep chain is parked mid-yield).";

  // ABANDON: destroy all cursors via the top-level unique_ptr.
  // The cursor dtor cascade calls ~ProduceCursor -> ~Cursor -> destroys gen_,
  // which calls coroutine_handle::destroy on the suspended frames,
  // running all in-scope dtors including OOMExceptionEnabler instances.
  cursor.reset();

  // All OOMExceptionEnabler dtors must have run.
  EXPECT_FALSE(OOMEnabler::CanThrow())
      << "OOM counter must be 0 after deep-chain cursor destruction "
         "(all suspended-frame OOMExceptionEnabler dtors must have run on cursor.reset()). "
         "If this fails, a coroutine frame in the chain leaked its OOMExceptionEnabler.";

  // Safety: reset flag for TearDown.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 6 — ManyVerticesMultiYieldParity.
//
// Uses a LOCAL storage with 100 vertices (built entirely in the test body,
// not the fixture's 50-vertex db) to guarantee >= 2 Yielded steps.
//
// Throttle period=20, 100 vertices: Produce's throttle fires at rows 20, 40, 60,
// 80 — that alone gives 4 fires. With ScanAll also having a throttle slot the
// combined count is higher. ">=2" is a conservative lower bound.
//
// GID ordering: InMemoryStorage is deterministic for a fixed insertion order;
// we compare GIDs in order between baseline and yield run.
//
// Assertions:
//   - 100 rows, same GIDs in same order, between flag-OFF and flag-ON.
//   - yields >= 2.
//   - OOM counter rebalanced to 0 after complete drive.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RealProduceYieldTest, ManyVerticesMultiYieldParity) {
  static constexpr int kLocalVertexCount = 100;

  // Build a completely local storage/dba to avoid disturbing the fixture's db.
  std::unique_ptr<memgraph::storage::Storage> local_db{
      new memgraph::storage::InMemoryStorage(memgraph::storage::Config{})};
  auto local_storage_dba = local_db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor local_dba(local_storage_dba.get());

  for (int i = 0; i < kLocalVertexCount; ++i) {
    local_dba.InsertVertex();
  }
  local_dba.AdvanceCommand();

  // Build ScanAll("q") -> Produce(q) plan using a local AstStorage and a fresh
  // SymbolTable (independent from the fixture's symbol_table).
  AstStorage local_ast;
  SymbolTable local_sym_table;

  auto local_scan = MakeScanAll(local_ast, local_sym_table, "q");
  auto *local_output =
      local_ast.Create<NamedExpression>("q", local_ast.Create<Identifier>("q")->MapTo(local_scan.sym_));
  local_output->MapTo(local_sym_table.CreateSymbol("named_expression_q", true));
  auto local_produce = MakeProduce(local_scan.op_, local_output);

  // ── Baseline: legacy path (COROUTINE_CURSORS OFF). ──────────────────────
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

  std::vector<memgraph::storage::Gid> baseline_gids;
  {
    auto ctx = MakeContext(local_ast, local_sym_table, &local_dba);
    auto baseline = CollectProduce(*local_produce, &ctx);
    ASSERT_EQ(static_cast<int>(baseline.size()), kLocalVertexCount)
        << "Legacy path must produce exactly " << kLocalVertexCount << " rows";

    baseline_gids.reserve(baseline.size());
    for (const auto &row : baseline) {
      ASSERT_EQ(row.size(), 1u);
      ASSERT_TRUE(row[0].IsVertex());
      baseline_gids.push_back(row[0].ValueVertex().Gid());
    }
  }

  // ── Yield run: coroutine path (COROUTINE_CURSORS ON). ──────────────────
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  std::atomic<bool> yield_sig{true};
  auto ctx = MakeContext(local_ast, local_sym_table, &local_dba);

  auto result = DriveWithYield(*local_produce, &ctx, &yield_sig);

  ASSERT_EQ(static_cast<int>(result.rows.size()), kLocalVertexCount)
      << "Coroutine path must produce exactly " << kLocalVertexCount << " rows";

  // GID parity in order.
  for (int i = 0; i < kLocalVertexCount; ++i) {
    ASSERT_EQ(result.rows[i].size(), 1u) << "Row " << i << " must have exactly one value";
    ASSERT_TRUE(result.rows[i][0].IsVertex()) << "Row " << i << " value must be a vertex";
    EXPECT_EQ(result.rows[i][0].ValueVertex().Gid(), baseline_gids[i])
        << "Row " << i << ": GID mismatch between legacy and coroutine paths (100-vertex run)";
  }

  // Multi-yield: with 100 vertices and period=20 the throttle fires at rows
  // 20/40/60/80 in Produce alone → 4 yields minimum.  ">=2" is conservative.
  EXPECT_GE(result.yields, 2) << "100-vertex run must produce >= 2 Yielded steps "
                                 "(throttle period=20, 100 rows → 4 Produce fires alone)";

  // OOM counter rebalance.
  EXPECT_FALSE(memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler::CanThrow())
      << "OOM counter must be rebalanced to 0 after a complete 100-vertex coroutine drive";

  // Safety: reset flag for TearDown.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 7 — SuppressedScopePreventsSynchronousPullCollapse (EXISTS-island gate).
//
// MOTIVATION (EXISTS callback scenario):
//   EvaluatePatternFilterCursor::DoPull invokes a synchronous bool Cursor::Pull()
//   on a yield-capable subtree — exactly the same call as `cursor->Pull(frame, ctx)`
//   in this test.  Cursor::Pull() collapses Yielded → false (line 36 of
//   cursor_awaitable.cpp): `return ResumePullStep(ra, ctx).status == HasRow;`.
//   A Yielded step from ScanAll's co_await YieldPointAwaitable therefore appears
//   as "no row found" to the EXISTS callback, corrupting the EXISTS result.
//
//   The fix wraps the EXISTS callback in PullDriverScope(Suppressed).
//   Suppressed sets ctx.stopping_context.yield_requested = nullptr, so
//   YieldPointAwaitable can only ever Continue — it never writes the stash slot —
//   so ResumePullStep never returns Yielded, so Pull() never collapses.
//
// STRUCTURE:
//   Outer context: COROUTINE_CURSORS flag ON, yield_sig armed (always true),
//   outer PullDriverScope(Enabled) — models the running query's driver that
//   already armed the yield channel.
//
//   PART A (with the fix):
//     A fresh ScanAll("s") cursor + Frame + ExecutionContext.
//     Inside PullDriverScope(Suppressed) — the fix.
//     50 synchronous Pull() calls; expects exactly 50 trues.
//     Rationale: Suppressed nullifies yield_requested → no throttle-fire ever
//     becomes Yielded → all 50 rows surface as true.
//
//   PART B (without the fix — demonstrates the bug):
//     A separate ScanAll("sb") cursor + Frame + ExecutionContext.
//     NO Suppressed scope; the outer Enabled scope + armed sig remain.
//     50 synchronous Pull() calls; expects fewer than 50 trues.
//     Rationale: throttle period=20, sig always true → at least 2 throttle fires
//     in 50 iterations → >=2 Yielded collapses → trues_b <= 48 < 50.
//
// INDEPENDENCE:
//   Part A and Part B use entirely separate AstStorage, ScanAllTuple, Frame,
//   and ExecutionContext instances — Part A's cursor is destroyed before Part B
//   constructs anything.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(RealProduceYieldTest, SuppressedScopePreventsSynchronousPullCollapse) {
  // ── Common setup: flag ON and a shared yield signal. ──────────────────────
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);

  // yield_sig is always true: every throttle-fire (period=20) would trigger a
  // yield in an unsuppressed context.  Both parts share the same atomic because
  // they share the flag; the signal is never reset between parts.
  std::atomic<bool> yield_sig{true};

  // ── PART A: with PullDriverScope(Suppressed) — the EXISTS fix. ───────────
  //
  // Goal: prove that 50 synchronous Pull() calls over a yield-capable ScanAll
  // cursor ALL return true when the EXISTS callback is correctly wrapped in a
  // Suppressed scope.
  {
    // Independent AstStorage and ScanAll("s") — distinct identifier from the
    // fixture's "n" and from Part B's "sb" to avoid symbol_table collisions.
    AstStorage ast_a;
    auto scan_a = MakeScanAll(ast_a, symbol_table, "s");

    auto ctx_a = MakeContext(ast_a, symbol_table, &dba);

    // Wire yield_sig on the context — this models the scheduler having armed
    // the yield channel on the worker thread.
    ctx_a.stopping_context.yield_requested = &yield_sig;

    // Outer Enabled scope: models the running query's top-level driver region.
    // It sets ctx_a.enabled_driver_active = true and wires the handle channel.
    memgraph::query::plan::PullDriverScope outer_a{ctx_a, memgraph::query::plan::YieldMode::Enabled};

    Frame frame_a(ctx_a.symbol_table.max_position());

    // MakeCursor captures use_coroutine_=true because COROUTINE_CURSORS is ON.
    auto cursor_a = scan_a.op_->MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());

    int trues_a = 0;
    {
      // PullDriverScope(Suppressed): the EXISTS fix.
      //   - Sets ctx_a.stopping_context.yield_requested = nullptr for this scope.
      //   - YieldPointAwaitable therefore always continues (never stashes the handle).
      //   - ResumePullStep never sees a stashed handle → never returns Yielded.
      //   - Cursor::Pull() therefore never collapses a Yielded to false.
      memgraph::query::plan::PullDriverScope suppress_a{ctx_a, memgraph::query::plan::YieldMode::Suppressed};

      for (int i = 0; i < kVertexCount; ++i) {
        if (cursor_a->Pull(frame_a, ctx_a)) ++trues_a;
      }
    }  // Suppressed scope exits; yield_requested is restored to &yield_sig.

    EXPECT_EQ(trues_a, kVertexCount)
        << "PART A (Suppressed scope / EXISTS fix): every one of the " << kVertexCount
        << " synchronous Pull() calls must return true. "
           "Under PullDriverScope(Suppressed), yield_requested is set to nullptr, so "
           "YieldPointAwaitable can only Continue — no throttle-fire ever becomes Yielded — "
           "so Cursor::Pull() (which collapses Yielded→false) sees only HasRow or Done. "
           "All "
        << kVertexCount << " ScanAll rows must surface as true in " << kVertexCount << " pulls.";

    cursor_a->Shutdown();
  }  // cursor_a, frame_a, ctx_a, outer_a all destroyed here — completely independent from Part B.

  // ── PART B: WITHOUT Suppressed scope — demonstrates the bug. ──────────────
  //
  // Goal: prove that without the fix, the armed yield signal causes at least one
  // synchronous Pull() to collapse a Yielded to false, so trues_b < kVertexCount.
  //
  // Throttle analysis: maybe_check_abort has period=20.  With kVertexCount=50
  // and yield_sig always true:
  //   - Throttle fires at iterations 20 and 40 (0-indexed) inside ScanAll's
  //     DoPull while-loop → 2 Yielded returns from ResumePullStep.
  //   - Cursor::Pull() collapses each to false → trues_b <= 48.
  //   - Starting phase is deterministic (fresh cursor, counter starts at 0).
  //   - EXPECT_LT(trues_b, 50) is guaranteed non-flaky.
  {
    // Fully separate AstStorage and ScanAll("sb") — different identifier string
    // to avoid any symbol collision with Part A's "s" in the shared symbol_table.
    AstStorage ast_b;
    auto scan_b = MakeScanAll(ast_b, symbol_table, "sb");

    auto ctx_b = MakeContext(ast_b, symbol_table, &dba);

    // Arm yield_sig on the context — same as Part A, modelling the running query.
    ctx_b.stopping_context.yield_requested = &yield_sig;

    // Outer Enabled scope only — NO Suppressed inner scope.
    // This models calling cursor->Pull() from an EXISTS callback without the fix.
    memgraph::query::plan::PullDriverScope outer_b{ctx_b, memgraph::query::plan::YieldMode::Enabled};

    Frame frame_b(ctx_b.symbol_table.max_position());

    // MakeCursor captures use_coroutine_=true (flag still ON from Part A).
    auto cursor_b = scan_b.op_->MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());

    int trues_b = 0;
    // No Suppressed scope here — yield_requested remains &yield_sig.
    // ScanAll's DoPull co_await YieldPointAwaitable fires at each throttle tick,
    // stashing the leaf handle.  ResumePullStep returns Yielded.
    // Cursor::Pull() returns (Yielded == HasRow) == false — a spurious "no row".
    for (int i = 0; i < kVertexCount; ++i) {
      if (cursor_b->Pull(frame_b, ctx_b)) ++trues_b;
    }

    EXPECT_LT(trues_b, kVertexCount)
        << "PART B (no Suppressed scope / bug reproduction): with yield_requested=true always "
           "and throttle period=20, at least 2 of the "
        << kVertexCount
        << " synchronous Pull() calls must collapse a Yielded to false (returning false). "
           "Specifically: throttle fires at ScanAll iterations 20 and 40, each producing "
           "Yielded from ResumePullStep, which Cursor::Pull() collapses to false — exactly "
           "the EXISTS corruption this fix prevents. "
           "Expected: trues_b <= "
        << (kVertexCount - 2) << "; got: " << trues_b << ".";

    cursor_b->Shutdown();
  }  // cursor_b, frame_b, ctx_b, outer_b all destroyed here.

  // TearDown also calls SetExperimental(NONE); explicit reset here for clarity.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
}
