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

// CURSOR MODE-SEAM UNIT TEST (coroutine cursors v2, PR-2).
//
// Validates the per-cursor mode seam added to the base Cursor:
//   • Sync (default): a cursor is pulled via its ordinary virtual bool Pull().
//   • PullCo() default = Immediate(Pull()): a coroutine PARENT pulls a Sync child
//     frame-lessly (await_ready()==true, no suspend) — this is the hybrid that lets
//     a Coro cursor drive an arbitrary Sync subtree at synchronous speed.
//   • A converted (Coro) cursor (MG_COROUTINE_CURSOR_PULLCO + DoPull) composes with
//     PullChild() over BOTH a Sync child (Immediate path) and a Coro child (real
//     persistent gen_ frame), producing the same row sequence as the sync drive.
//
// No DB/storage/interpreter. Minimal fake Cursor subclasses on the real seam types,
// driven by the public ResumePullStep. Yield is OFF (no PullDriverScope), so this
// exercises only the mode/composition seam, not the cooperative-yield protocol
// (covered by cursor_yield.cpp).

#include <cstdint>
#include <vector>

#include "gtest/gtest.h"

#include "query/context.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/cursor_awaitable.hpp"
#include "query/plan/cursor_awaitable_core.hpp"
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {
namespace {

// A Sync leaf: emits `n` rows (writing 0..n-1 into `last`) then exhausts. No PullCo
// override → driven through the base default PullCo() == Immediate(Pull()).
struct SyncLeaf : Cursor {
  int n_;
  int i_{0};
  int64_t *last_;

  SyncLeaf(int n, int64_t *last) : n_(n), last_(last) {}

  bool Pull(Frame & /*f*/, ExecutionContext & /*ctx*/) override {
    if (i_ >= n_) return false;
    *last_ = i_++;
    return true;
  }

  void Reset() override { i_ = 0; }

  void Shutdown() override {}
};

// A Coro leaf: same contract as SyncLeaf but as a persistent coroutine generator —
// exercises the real frame path (gen_ created, co_yield per row).
struct CoroLeaf : Cursor {
  int n_;
  int64_t *last_;

  CoroLeaf(int n, int64_t *last) : n_(n), last_(last) { set_mode(CursorMode::Coro); }

  MG_COROUTINE_CURSOR_PULLCO

  bool Pull(Frame & /*f*/, ExecutionContext & /*ctx*/) override { LOG_FATAL("CoroLeaf is Coro-only in this test"); }

  void Reset() override { ResetGen(); }

  void Shutdown() override {}

 protected:
  PullAwaitable DoPull(Frame & /*f*/, ExecutionContext & /*ctx*/) override {
    for (int i = 0; i < n_; ++i) {
      *last_ = i;
      co_yield true;
    }
    co_return false;
  }
};

// A Coro parent (1:1 passthrough, like Produce): pulls its child via co_await PullChild,
// relays each row up. The child may be Sync (Immediate hop, no frame) or Coro (real hop).
struct CoroParent : Cursor {
  Cursor *child_;
  int64_t *seen_;  // mirrors the child's last value, proving the row flowed through
  int64_t *child_last_;

  explicit CoroParent(Cursor *child, int64_t *seen, int64_t *child_last)
      : child_(child), seen_(seen), child_last_(child_last) {
    set_mode(CursorMode::Coro);
  }

  MG_COROUTINE_CURSOR_PULLCO

  bool Pull(Frame & /*f*/, ExecutionContext & /*ctx*/) override { LOG_FATAL("CoroParent is Coro-only in this test"); }

  void Reset() override {
    ResetGen();
    child_->Reset();
  }

  void Shutdown() override {}

 protected:
  PullAwaitable DoPull(Frame &f, ExecutionContext &ctx) override {
    while (true) {
      if (!co_await PullChild(*child_, f, ctx)) co_return false;
      *seen_ = *child_last_;
      co_yield true;
    }
  }
};

// Drive a cursor as a coroutine root: ResumePullStep until Done, collecting the
// `probe` value at each produced row. Yield is OFF (no PullDriverScope).
std::vector<int64_t> DriveCoro(Cursor &root, const int64_t *probe) {
  Frame frame{0};
  ExecutionContext ctx;
  std::vector<int64_t> rows;
  while (true) {
    auto ra = root.PullCo(frame, ctx);
    auto r = ResumePullStep(ra, ctx);
    if (r.status == PullRunResult::Status::Done) break;
    EXPECT_EQ(r.status, PullRunResult::Status::HasRow);
    rows.push_back(*probe);
  }
  return rows;
}

// Drive a cursor synchronously (the master path), collecting `probe` at each row.
std::vector<int64_t> DriveSync(Cursor &root, const int64_t *probe) {
  Frame frame{0};
  ExecutionContext ctx;
  std::vector<int64_t> rows;
  while (root.Pull(frame, ctx)) rows.push_back(*probe);
  return rows;
}

constexpr int kRows = 5;
const std::vector<int64_t> kExpected{0, 1, 2, 3, 4};

// A bare Sync cursor defaults to Sync mode (== master) with empty gen_.
TEST(CursorSeam, DefaultModeIsSync) {
  int64_t last = -1;
  SyncLeaf leaf{kRows, &last};
  EXPECT_EQ(leaf.mode(), CursorMode::Sync);
  EXPECT_EQ(DriveSync(leaf, &last), kExpected);
}

// PullCo() default wraps Pull() as an Immediate result: driving a Sync leaf through the
// coroutine entry yields the same rows, with no coroutine frame allocated for it.
TEST(CursorSeam, SyncLeafDrivenThroughPullCoIsImmediate) {
  int64_t last = -1;
  SyncLeaf leaf{kRows, &last};
  // First step returns an Immediate ResumeAwaitable (handle-less, already ready).
  Frame frame{0};
  ExecutionContext ctx;
  auto ra = leaf.PullCo(frame, ctx);
  EXPECT_TRUE(ra.Done());        // immediate: no suspension/frame
  EXPECT_FALSE(ra.GetHandle());  // no coroutine handle allocated
  // Driving to completion produces the full sequence.
  SyncLeaf leaf2{kRows, &last};
  EXPECT_EQ(DriveCoro(leaf2, &last), kExpected);
}

// Hybrid: a Coro parent over a SYNC child — the child hop is frame-less, rows match.
TEST(CursorSeam, CoroParentOverSyncChild) {
  int64_t child_last = -1, seen = -1;
  SyncLeaf child{kRows, &child_last};
  CoroParent parent{&child, &seen, &child_last};
  EXPECT_EQ(DriveCoro(parent, &seen), kExpected);
}

// Coro parent over a CORO child — real frame composition through PullChild.
TEST(CursorSeam, CoroParentOverCoroChild) {
  int64_t child_last = -1, seen = -1;
  CoroLeaf child{kRows, &child_last};
  CoroParent parent{&child, &seen, &child_last};
  EXPECT_EQ(DriveCoro(parent, &seen), kExpected);
}

// Reset re-runs a converted cursor from the start (gen_ destroyed + rebuilt).
TEST(CursorSeam, ResetRerunsCoroChain) {
  int64_t child_last = -1, seen = -1;
  CoroLeaf child{kRows, &child_last};
  CoroParent parent{&child, &seen, &child_last};
  EXPECT_EQ(DriveCoro(parent, &seen), kExpected);
  parent.Reset();
  EXPECT_EQ(DriveCoro(parent, &seen), kExpected);
}

}  // namespace
}  // namespace memgraph::query::plan
