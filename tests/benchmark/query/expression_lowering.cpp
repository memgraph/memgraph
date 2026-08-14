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

/// How much is left on the table by evaluating an expression as a visitor walk over the AST?
///
/// The row expressions here are the ones the aggregation benchmark's q12 runs once per row, over
/// properties a `CacheProperties` operator has already put in frame slots:
///
///   WITH i.ts / 2592000 AS month_bucket, i.value * i.quantity AS revenue
///   WHERE revenue > 500.0
///
/// Four ways of computing exactly that, on the same frame:
///
///   Interpreter  what runs today: `Expression::Accept(ExpressionEvaluator&)`, two indirect calls
///                per node (the virtual `Accept`, then the virtual `Visit`), a `TypedValue` built
///                per node, and every identifier read copying its frame slot.
///   Threaded     the same tree, lowered once into nodes carrying a function pointer. One indirect
///                call per node instead of two, and no evaluator state to carry.
///   Fused        lowered into one node per expression, reading its operands straight out of the
///                frame. The intermediate `TypedValue`s of the leaves are gone; a lowering pass can
///                do this because it sees the whole expression, a tree-walk cannot because it only
///                ever sees one node.
///   Typed        the ceiling: the same arithmetic with the tag checks resolved at compile time.
///                Not reachable without a type guarantee, but it says how much of the cost is
///                dispatch and how much is the tagged union itself.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <vector>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace {

using memgraph::query::AstStorage;
using memgraph::query::Expression;
using memgraph::query::Frame;
using memgraph::query::Symbol;
using memgraph::query::SymbolTable;
using memgraph::query::TypedValue;

constexpr int64_t kMonthSeconds = 2592000;
constexpr double kRevenueFloor = 500.0;
constexpr int64_t kRows = 4096;

/// The three slots a `CacheProperties` above this expression would have filled.
struct Slots {
  int64_t value{};
  int64_t quantity{};
  int64_t ts{};
};

/// Everything the four variants share: a frame, and the row values to walk it over.
struct Fixture {
  Fixture()
      : db_(std::make_unique<memgraph::storage::InMemoryStorage>()),
        storage_dba_(db_->Access(memgraph::storage::WRITE)),
        dba_(storage_dba_.get()) {
    auto const &value_sym = symbol_table_.CreateSymbol("value", true);
    auto const &quantity_sym = symbol_table_.CreateSymbol("quantity", true);
    auto const &ts_sym = symbol_table_.CreateSymbol("ts", true);
    slots_ = {value_sym.position(), quantity_sym.position(), ts_sym.position()};

    value_ident_ = ast_.Create<memgraph::query::Identifier>("value", true);
    value_ident_->symbol_pos_ = static_cast<int32_t>(slots_.value);
    quantity_ident_ = ast_.Create<memgraph::query::Identifier>("quantity", true);
    quantity_ident_->symbol_pos_ = static_cast<int32_t>(slots_.quantity);
    ts_ident_ = ast_.Create<memgraph::query::Identifier>("ts", true);
    ts_ident_->symbol_pos_ = static_cast<int32_t>(slots_.ts);

    revenue_ = ast_.Create<memgraph::query::MultiplicationOperator>(value_ident_, quantity_ident_);
    bucket_ = ast_.Create<memgraph::query::DivisionOperator>(
        ts_ident_, ast_.Create<memgraph::query::PrimitiveLiteral>(kMonthSeconds));
    keep_ = ast_.Create<memgraph::query::GreaterOperator>(
        revenue_, ast_.Create<memgraph::query::PrimitiveLiteral>(kRevenueFloor));

    frame_ = std::make_unique<Frame>(symbol_table_.max_position(), memory_.get());

    // Values with the spread of the real dataset, so branchy paths are exercised both ways.
    rows_.reserve(kRows);
    for (int64_t row = 0; row != kRows; ++row) {
      rows_.push_back(
          {.value = 1.0 + static_cast<double>(row % 97), .quantity = 1 + row % 31, .ts = 1600000000 + row * 7919});
    }
  }

  struct Row {
    double value;
    int64_t quantity;
    int64_t ts;
  };

  /// The frame is written the way an operator above would have written it, then read back.
  void PutRow(Row const &row) {
    auto &elems = const_cast<memgraph::utils::pmr::vector<TypedValue> &>(frame_->elems());
    elems[slots_.value] = TypedValue(row.value, memory_.get());
    elems[slots_.quantity] = TypedValue(row.quantity, memory_.get());
    elems[slots_.ts] = TypedValue(row.ts, memory_.get());
  }

  struct MemoryHandle {
    static memgraph::utils::MemoryResource *get() { return memgraph::utils::NewDeleteResource(); }
  } memory_;

  AstStorage ast_;
  SymbolTable symbol_table_;
  std::unique_ptr<memgraph::storage::Storage> db_;
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba_;
  memgraph::query::DbAccessor dba_;
  std::unique_ptr<Frame> frame_;
  Slots slots_;
  memgraph::query::Identifier *value_ident_{};
  memgraph::query::Identifier *quantity_ident_{};
  memgraph::query::Identifier *ts_ident_{};
  Expression *revenue_{};
  Expression *bucket_{};
  Expression *keep_{};
  std::vector<Row> rows_;
};

// ---------------------------------------------------------------------------------------------
// Variant 2: the tree lowered into function-pointer nodes.

struct ThreadedNode;
using EvalFn = TypedValue (*)(ThreadedNode const &, Frame const &);

struct ThreadedNode {
  EvalFn fn;
  ThreadedNode const *lhs{nullptr};
  ThreadedNode const *rhs{nullptr};
  int64_t slot{-1};
  TypedValue literal;

  TypedValue Eval(Frame const &frame) const { return fn(*this, frame); }
};

TypedValue EvalSlot(ThreadedNode const &node, Frame const &frame) { return frame.elems()[node.slot]; }

TypedValue EvalLiteral(ThreadedNode const &node, Frame const & /*frame*/) { return node.literal; }

TypedValue EvalMul(ThreadedNode const &node, Frame const &frame) {
  return node.lhs->Eval(frame) * node.rhs->Eval(frame);
}

TypedValue EvalDiv(ThreadedNode const &node, Frame const &frame) {
  return node.lhs->Eval(frame) / node.rhs->Eval(frame);
}

TypedValue EvalGreater(ThreadedNode const &node, Frame const &frame) {
  return node.lhs->Eval(frame) > node.rhs->Eval(frame);
}

// ---------------------------------------------------------------------------------------------
// Variant 3: one node per expression, operands read from the frame in place.

struct FusedNode {
  int64_t lhs_slot{-1};
  int64_t rhs_slot{-1};
  TypedValue literal;
};

TypedValue FusedMulSlots(FusedNode const &node, Frame const &frame) {
  return frame.elems()[node.lhs_slot] * frame.elems()[node.rhs_slot];
}

TypedValue FusedDivLiteral(FusedNode const &node, Frame const &frame) {
  return frame.elems()[node.lhs_slot] / node.literal;
}

TypedValue FusedGreaterLiteral(FusedNode const &node, TypedValue const &lhs) { return lhs > node.literal; }

}  // namespace

// ---------------------------------------------------------------------------------------------

static void BenchInterpreter(benchmark::State &state) {
  Fixture fixture;
  memgraph::query::ExecutionContext ctx;
  ctx.db_accessor = &fixture.dba_;
  ctx.symbol_table = fixture.symbol_table_;
  ctx.evaluation_context = memgraph::query::EvaluationContext{fixture.memory_.get()};
  memgraph::query::ExpressionEvaluator evaluator(fixture.frame_.get(), ctx, memgraph::storage::View::NEW);

  while (state.KeepRunning()) {
    for (auto const &row : fixture.rows_) {
      fixture.PutRow(row);
      auto revenue = fixture.revenue_->Accept(evaluator);
      auto bucket = fixture.bucket_->Accept(evaluator);
      auto keep = fixture.keep_->Accept(evaluator);
      benchmark::DoNotOptimize(revenue);
      benchmark::DoNotOptimize(bucket);
      benchmark::DoNotOptimize(keep);
    }
  }
  state.SetItemsProcessed(state.iterations() * kRows);
}

static void BenchThreaded(benchmark::State &state) {
  Fixture fixture;
  auto *memory = fixture.memory_.get();

  ThreadedNode value_node{.fn = EvalSlot, .slot = fixture.slots_.value};
  ThreadedNode quantity_node{.fn = EvalSlot, .slot = fixture.slots_.quantity};
  ThreadedNode ts_node{.fn = EvalSlot, .slot = fixture.slots_.ts};
  ThreadedNode month_node{.fn = EvalLiteral, .literal = TypedValue(kMonthSeconds, memory)};
  ThreadedNode floor_node{.fn = EvalLiteral, .literal = TypedValue(kRevenueFloor, memory)};
  ThreadedNode revenue_node{.fn = EvalMul, .lhs = &value_node, .rhs = &quantity_node};
  ThreadedNode bucket_node{.fn = EvalDiv, .lhs = &ts_node, .rhs = &month_node};
  ThreadedNode keep_node{.fn = EvalGreater, .lhs = &revenue_node, .rhs = &floor_node};

  while (state.KeepRunning()) {
    for (auto const &row : fixture.rows_) {
      fixture.PutRow(row);
      auto revenue = revenue_node.Eval(*fixture.frame_);
      auto bucket = bucket_node.Eval(*fixture.frame_);
      auto keep = keep_node.Eval(*fixture.frame_);
      benchmark::DoNotOptimize(revenue);
      benchmark::DoNotOptimize(bucket);
      benchmark::DoNotOptimize(keep);
    }
  }
  state.SetItemsProcessed(state.iterations() * kRows);
}

static void BenchFused(benchmark::State &state) {
  Fixture fixture;
  auto *memory = fixture.memory_.get();

  FusedNode revenue_node{.lhs_slot = fixture.slots_.value, .rhs_slot = fixture.slots_.quantity};
  FusedNode bucket_node{.lhs_slot = fixture.slots_.ts, .literal = TypedValue(kMonthSeconds, memory)};
  FusedNode keep_node{.literal = TypedValue(kRevenueFloor, memory)};

  while (state.KeepRunning()) {
    for (auto const &row : fixture.rows_) {
      fixture.PutRow(row);
      auto revenue = FusedMulSlots(revenue_node, *fixture.frame_);
      auto bucket = FusedDivLiteral(bucket_node, *fixture.frame_);
      auto keep = FusedGreaterLiteral(keep_node, revenue);
      benchmark::DoNotOptimize(revenue);
      benchmark::DoNotOptimize(bucket);
      benchmark::DoNotOptimize(keep);
    }
  }
  state.SetItemsProcessed(state.iterations() * kRows);
}

static void BenchTyped(benchmark::State &state) {
  Fixture fixture;

  while (state.KeepRunning()) {
    for (auto const &row : fixture.rows_) {
      fixture.PutRow(row);
      auto const &value = fixture.frame_->elems()[fixture.slots_.value];
      auto const &quantity = fixture.frame_->elems()[fixture.slots_.quantity];
      auto const &ts = fixture.frame_->elems()[fixture.slots_.ts];
      auto revenue = value.UnsafeValueDouble() * static_cast<double>(quantity.UnsafeValueInt());
      auto bucket = ts.UnsafeValueInt() / kMonthSeconds;
      auto keep = revenue > kRevenueFloor;
      benchmark::DoNotOptimize(revenue);
      benchmark::DoNotOptimize(bucket);
      benchmark::DoNotOptimize(keep);
    }
  }
  state.SetItemsProcessed(state.iterations() * kRows);
}

/// Every variant above writes the row into the frame first, exactly as the operator above it would
/// have. This measures that write on its own, so the expression cost can be read net of it.
static void BenchFrameOnly(benchmark::State &state) {
  Fixture fixture;
  while (state.KeepRunning()) {
    for (auto const &row : fixture.rows_) {
      fixture.PutRow(row);
      benchmark::DoNotOptimize(fixture.frame_->elems()[fixture.slots_.value]);
    }
  }
  state.SetItemsProcessed(state.iterations() * kRows);
}

BENCHMARK(BenchFrameOnly)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchInterpreter)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchThreaded)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchFused)->Unit(benchmark::kMicrosecond);
BENCHMARK(BenchTyped)->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
