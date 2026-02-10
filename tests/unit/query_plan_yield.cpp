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

#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "query/exceptions.hpp"
#include "query/plan/cursor_awaitable.hpp"
#include "query/plan/operator.hpp"
#include "query_plan_common.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using memgraph::query::test_common::GetPropertyLookup;

class YieldFixture : public testing::Test {
 protected:
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig("yield_test");
  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage(config)};
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{db->Access(memgraph::storage::WRITE)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  AstStorage storage;
  SymbolTable symbol_table;

  void SetUp() override { memgraph::license::global_license_checker.EnableTesting(); }

  void AddVertices(int count) {
    for (int i = 0; i < count; i++) dba.InsertVertex();
  }
};

TEST_F(YieldFixture, ScanAllYields) {
  // Add enough vertices to trigger the throttle (default is 20)
  AddVertices(30);
  dba.AdvanceCommand();

  ExecutionContext context = MakeContext(storage, symbol_table, &dba);
  std::atomic<bool> yield_req{true};
  context.stopping_context.yield_requested = &yield_req;
  std::coroutine_handle<> stored;
  context.suspended_task_handle_ptr = &stored;

  // Setup ScanAll
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto cursor = scan_all.op_->MakeCursor(memgraph::utils::NewDeleteResource());
  Frame frame(symbol_table.max_position());

  // We loop to consume rows until yield happens
  int rows = 0;
  bool yielded = false;

  // Use a safety limit to prevent infinite loops if it never yields
  const int kMaxIterations = 1000;
  int iterations = 0;

  // Initial pull
  auto awaitable = cursor->Pull(frame, context);

  while (iterations++ < kMaxIterations) {
    auto result = RunPullToCompletion(awaitable, context);

    if (result.status == PullRunResult::Status::Yielded) {
      yielded = true;
      // If yielded, we must RESUME the SAME awaitable.
      // So we loop back and call RunPullToCompletion(awaitable) again.
      // We break here to verify yield state, then resume in the next block.
      break;
    }
    if (result.status == PullRunResult::Status::HasRow) {
      rows++;
      // If HasRow, this awaitable is DONE (it produced one row).
      // We need a NEW awaitable for the next row.
      awaitable = cursor->Pull(frame, context);
    } else {
      // Done before yield?
      break;
    }
  }

  EXPECT_TRUE(yielded) << "Should have yielded after processing some rows (throttle limit reached)";
  EXPECT_LT(rows, 30) << "Should have yielded before processing all rows";

  // Disable yield to finish
  yield_req = false;

  // Resume and finish
  // The 'awaitable' is currently in Yielded state (suspended) OR we broke the loop.
  // We continue consuming.

  while (iterations++ < kMaxIterations) {
    auto result = RunPullToCompletion(awaitable, context);
    if (result.status == PullRunResult::Status::Yielded) {
      FAIL() << "Should not yield anymore";
    }
    if (result.status == PullRunResult::Status::HasRow) {
      rows++;
      // Get next awaitable for next row
      awaitable = cursor->Pull(frame, context);
    } else {
      // Done
      break;
    }
  }

  EXPECT_EQ(rows, 30);
}

TEST_F(YieldFixture, ScanAllAborts) {
  AddVertices(30);
  dba.AdvanceCommand();

  ExecutionContext context = MakeContext(storage, symbol_table, &dba);
  std::atomic<TransactionStatus> transaction_status = TransactionStatus::TERMINATED;
  context.stopping_context.transaction_status = &transaction_status;
  std::coroutine_handle<> stored;
  context.suspended_task_handle_ptr = &stored;

  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto cursor = scan_all.op_->MakeCursor(memgraph::utils::NewDeleteResource());
  Frame frame(symbol_table.max_position());

  EXPECT_THROW(
      {
        // Initial pull
        auto awaitable = cursor->Pull(frame, context);
        int iterations = 0;
        const int kMaxIterations = 1000;
        while (iterations++ < kMaxIterations) {
          auto result = RunPullToCompletion(awaitable, context);
          if (result.status == PullRunResult::Status::Yielded) {
            // If yielded, we must RESUME the SAME awaitable.
            // So we loop back and call RunPullToCompletion(awaitable) again.
            // We break here to verify yield state, then resume in the next block.
            continue;
          }
          if (result.status == PullRunResult::Status::HasRow) {
            // If HasRow, this awaitable is DONE (it produced one row).
            // We need a NEW awaitable for the next row.
            awaitable = cursor->Pull(frame, context);
          } else {
            // Done
            break;
          }
        }
      },
      memgraph::query::HintedAbortError);
}

TEST_F(YieldFixture, MultipleYields) {
  AddVertices(200);
  dba.AdvanceCommand();

  ExecutionContext context = MakeContext(storage, symbol_table, &dba);
  std::atomic<bool> yield_req{false};
  context.stopping_context.yield_requested = &yield_req;
  std::atomic<TransactionStatus> transaction_status = TransactionStatus::ACTIVE;
  context.stopping_context.transaction_status = &transaction_status;
  std::coroutine_handle<> stored;
  context.suspended_task_handle_ptr = &stored;

  // ScanAll
  auto scan_all = MakeScanAll(storage, symbol_table, "n");

  // Filter (WHERE true)
  auto true_expr = storage.Create<memgraph::query::PrimitiveLiteral>(true);
  auto filter_op = std::make_shared<memgraph::query::plan::Filter>(
      scan_all.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, true_expr);

  // Limit 1000
  auto limit_expr = storage.Create<memgraph::query::PrimitiveLiteral>(1000);
  auto limit_op = std::make_shared<memgraph::query::plan::Limit>(filter_op, limit_expr);

  auto cursor = limit_op->MakeCursor(memgraph::utils::NewDeleteResource());
  Frame frame(symbol_table.max_position());

  // 1. Verify Yielding
  int yields = 0;
  int rows = 0;

  // First pull
  auto awaitable = cursor->Pull(frame, context);
  int iterations = 0;
  const int kMaxIterations = 1000;
  while (iterations++ < kMaxIterations) {
    auto result = RunPullToCompletion(awaitable, context);
    if (result.status == PullRunResult::Status::Yielded) {
      yields++;
      yield_req = false;
      continue;  // Resume same awaitable; next RunPullToCompletion should see TERMINATED and throw
    }
    if (result.status == PullRunResult::Status::HasRow) {
      rows++;
      if (!yield_req) {
        // First yield
        if (rows == 10) yield_req = true;
        // Second yield
        if (rows == 150) yield_req = true;
      }
      awaitable = cursor->Pull(frame, context);
    } else {
      break;  // Done
    }
  }

  EXPECT_EQ(yields, 2);
  EXPECT_EQ(rows, 200);
}

TEST_F(YieldFixture, ProduceLimitFilterScanAllYields) {
  AddVertices(200);
  dba.AdvanceCommand();

  ExecutionContext context = MakeContext(storage, symbol_table, &dba);
  std::atomic<bool> yield_req{true};
  context.stopping_context.yield_requested = &yield_req;
  std::atomic<TransactionStatus> transaction_status = TransactionStatus::ACTIVE;
  context.stopping_context.transaction_status = &transaction_status;
  std::coroutine_handle<> stored;
  context.suspended_task_handle_ptr = &stored;

  // ScanAll
  auto scan_all = MakeScanAll(storage, symbol_table, "n");

  // Filter (WHERE true)
  auto true_expr = storage.Create<memgraph::query::PrimitiveLiteral>(true);
  auto filter_op = std::make_shared<memgraph::query::plan::Filter>(
      scan_all.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, true_expr);

  // Limit 1000
  auto limit_expr = storage.Create<memgraph::query::PrimitiveLiteral>(1000);
  auto limit_op = std::make_shared<memgraph::query::plan::Limit>(filter_op, limit_expr);

  auto cursor = limit_op->MakeCursor(memgraph::utils::NewDeleteResource());
  Frame frame(symbol_table.max_position());

  // 1. Verify Yielding
  int yields = 0;
  int rows = 0;

  EXPECT_THROW(
      {
        // First pull
        auto awaitable = cursor->Pull(frame, context);
        int iterations = 0;
        const int kMaxIterations = 1000;
        while (iterations++ < kMaxIterations) {
          auto result = RunPullToCompletion(awaitable, context);
          if (result.status == PullRunResult::Status::Yielded) {
            yields++;
            yield_req = false;
            if (yields == 2) {
              transaction_status = TransactionStatus::TERMINATED;
            }
            continue;  // Resume same awaitable; next RunPullToCompletion should see TERMINATED and throw
          }
          if (result.status == PullRunResult::Status::HasRow) {
            rows++;
            if (!yield_req && rows > 50) yield_req = true;
            awaitable = cursor->Pull(frame, context);
          } else {
            break;  // Done
          }
        }
      },
      memgraph::query::HintedAbortError);

  EXPECT_EQ(yields, 2);
  EXPECT_GT(rows, 0);
}

// MATCH (n) RETURN max(n.p) with mixed property types (int vs string) triggers QueryRuntimeException during
// aggregation.
TEST_F(YieldFixture, ExceptionDuringPullMaxMixedTypes) {
  auto prop_id = dba.NameToProperty("p");
  // 200 vertices: all have int p except vertex at index 99 has string p -> max(n.p) throws when comparing
  for (int i = 0; i < 200; ++i) {
    auto vertex = dba.InsertVertex();
    if (i == 99) {
      ASSERT_TRUE(vertex.SetProperty(prop_id, memgraph::storage::PropertyValue("string")).has_value());
    } else {
      ASSERT_TRUE(vertex.SetProperty(prop_id, memgraph::storage::PropertyValue(i)).has_value());
    }
  }
  dba.AdvanceCommand();

  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto *n_p = GetPropertyLookup(storage, dba, scan_all.node_->identifier_, prop_id);

  Symbol max_sym = symbol_table.CreateSymbol("max_p", true);
  auto aggregate_op =
      std::make_shared<Aggregate>(scan_all.op_,
                                  std::vector<Aggregate::Element>{{n_p, nullptr, Aggregation::Op::MAX, max_sym, false}},
                                  std::vector<Expression *>{},
                                  std::vector<Symbol>{});

  auto *max_ident = storage.Create<Identifier>("max_p");
  max_ident->MapTo(max_sym);
  auto *named_expr = storage.Create<NamedExpression>("max_p", max_ident);
  named_expr->MapTo(symbol_table.CreateSymbol("named", true));

  auto produce = std::make_shared<Produce>(aggregate_op, std::vector<NamedExpression *>{named_expr});

  // MakeContext must be called after building the plan so storage.properties_ contains "p" (from GetPropertyLookup).
  ExecutionContext context = MakeContext(storage, symbol_table, &dba);
  std::coroutine_handle<> stored;
  context.suspended_task_handle_ptr = &stored;

  Frame frame(symbol_table.max_position());
  auto cursor = produce->MakeCursor(memgraph::utils::NewDeleteResource());

  EXPECT_THROW(
      {
        auto awaitable = cursor->Pull(frame, context);
        while (true) {
          auto result = RunPullToCompletion(awaitable, context);
          if (result.status == PullRunResult::Status::Yielded) continue;
          if (result.status != PullRunResult::Status::HasRow) break;
          awaitable = cursor->Pull(frame, context);
        }
      },
      QueryRuntimeException);
}
