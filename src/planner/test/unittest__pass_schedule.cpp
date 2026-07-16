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

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/rewrite/pass_schedule.hpp"
#include "test_rewriter_fixture.hpp"
#include "test_rules.hpp"

// The pass-schedule seam viewed from the loop's side: the saturation loop is
// schedule-agnostic and drives begin -> (before_pass -> after_pass)* in a fixed
// order. These tests inject a recording schedule to pin that order - in
// particular that the terminating zero-rewrite pass runs before_pass but never
// after_pass. RuleLatch's own behaviour (what arming decides) is tested below
// the seam in unittest__rule_latch.cpp; here the schedule arms nothing, so the
// hook sequence is a pure observation of the loop.

namespace memgraph::planner::core {

using namespace test;
using rewrite::FullSchedule;
using rewrite::IncrementalSchedule;
using rewrite::PassSchedule;
using rewrite::RewriteConfig;

namespace {

/// The lifecycle hooks the loop drives, in observed order.
enum class Hook : std::uint8_t { Begin, BeforePass, AfterPass };

/// Records the loop's hook calls and otherwise behaves as Full (arms nothing,
/// restricts nothing), so a run's hook sequence is independent of any arming.
class SpySchedule {
 public:
  explicit SpySchedule(std::vector<Hook> &log) : log_(&log) {}

  void begin(TestEGraph & /*egraph*/) { log_->push_back(Hook::Begin); }

  void before_pass(TestEGraph & /*egraph*/) { log_->push_back(Hook::BeforePass); }

  void after_pass(TestEGraph & /*egraph*/) { log_->push_back(Hook::AfterPass); }

  static auto armed() -> std::vector<std::uint8_t> const * { return nullptr; }

  static auto active() -> boost::unordered_flat_set<EClassId> const * { return nullptr; }

 private:
  std::vector<Hook> *log_;
};

static_assert(PassSchedule<SpySchedule, TestEGraph>);
static_assert(PassSchedule<FullSchedule, TestEGraph>);
static_assert(PassSchedule<IncrementalSchedule<Op, NoAnalysis>, TestEGraph>);

}  // namespace

TEST_F(Rewrite, SpySchedule_HookOrderPinsTheLifecycle) {
  // Neg(Neg(x)) collapses in one pass, then the next pass finds nothing: begin
  // once, one productive before_pass -> after_pass, and a trailing before_pass
  // for the fixpoint pass with no after_pass following it.
  use_rules(make_double_neg_rule());
  auto x = leaf(Op::Var, 1);
  auto neg_neg_x = node(Op::Neg, node(Op::Neg, x));
  rebuild_index();

  std::vector<Hook> log;
  SpySchedule spy{log};
  auto const result = rewriter().saturate(RewriteConfig::Unlimited(), spy);

  EXPECT_EQ(egraph.find(x), egraph.find(neg_neg_x));
  EXPECT_EQ(result.rewrites_applied, 1U);
  EXPECT_EQ(result.iterations, 2U);
  EXPECT_EQ(log, (std::vector<Hook>{Hook::Begin, Hook::BeforePass, Hook::AfterPass, Hook::BeforePass}));
}

TEST_F(Rewrite, SpySchedule_ImmediateFixpointSkipsAfterPass) {
  // No Neg(Neg(...)) to match, so the first pass is already a fixpoint: begin
  // plus exactly one before_pass, and never an after_pass.
  use_rules(make_double_neg_rule());
  leaf(Op::Var, 1);
  rebuild_index();

  std::vector<Hook> log;
  SpySchedule spy{log};
  auto const result = rewriter().saturate(RewriteConfig::Unlimited(), spy);

  EXPECT_TRUE(result.saturated());
  EXPECT_EQ(result.rewrites_applied, 0U);
  EXPECT_EQ(log, (std::vector<Hook>{Hook::Begin, Hook::BeforePass}));
}

}  // namespace memgraph::planner::core
