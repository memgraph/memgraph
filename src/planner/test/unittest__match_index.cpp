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

// Tests for the symbol index the VM iterates for root candidates.
//
// Inclusion criterion: a test here pins a property of the index's contents that
// the matcher relies on. The matcher's own behaviour is covered by
// unittest__vm_matching.cpp.

#include <gtest/gtest.h>

#include <array>

#include <boost/unordered/unordered_flat_set.hpp>

#include <span>
#include <vector>

#include <algorithm>

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "test_egraph_fixture.hpp"
#include "test_support/patterns.hpp"

namespace memgraph::planner::core {

using namespace test;

namespace {

/// Every id the index reports for `sym`, in no particular order.
auto EntriesFor(TestMatcherIndex const &index, Op sym) -> std::vector<EClassId> {
  auto const *set = index.eclasses_set_for_symbol(sym);
  if (set == nullptr) return {};
  return {set->begin(), set->end()};
}

class MatchIndexTest : public EGraphTestBase {};

// A merge leaves one of the two ids naming nothing. An index that keeps it
// reports the surviving class twice over, once under each id, so a root
// iteration visits that class twice. Driven through the incremental update,
// which is the path saturation takes; a full rebuild is canonical anyway.
TEST_F(MatchIndexTest, MergedAwayIdIsNotReportedAfterIncrementalUpdate) {
  auto const f_a = node(Op::F, leaf(Op::A));
  auto const f_b = node(Op::F, leaf(Op::B));
  matcher.rebuild_index();  // in sync with the graph before the merge
  ASSERT_EQ(EntriesFor(matcher, Op::F).size(), 2U);

  auto const survivor = merge(f_a, f_b);
  egraph.rebuild(proc_ctx_);

  // The sequence saturation runs: rebuild, retire what the merge absorbed, add
  // what it created.
  boost::unordered_flat_set<EClassId> retired;
  egraph.retired_eclasses_into(retired);
  auto const retired_ids = std::vector<EClassId>{retired.begin(), retired.end()};
  matcher.retire(retired_ids);
  auto const updated = std::array{survivor};
  matcher.rebuild_index(std::span<EClassId const>{updated});

  auto const entries = EntriesFor(matcher, Op::F);
  ASSERT_EQ(entries.size(), 1U);
  EXPECT_EQ(entries.front(), egraph.find(f_a));
}

// The consequence the matcher depends on: a merged class is one root candidate,
// so a symbol-rooted pattern reports it once. Without this the root level has to
// recognise the repeat while enumerating.
TEST_F(MatchIndexTest, MergedClassIsOneRootCandidate) {
  auto const a = leaf(Op::A);
  auto const b = leaf(Op::B);
  auto const f_a = node(Op::F, a);
  auto const f_b = node(Op::F, b);
  matcher.rebuild_index();

  auto const survivor = merge(f_a, f_b);
  egraph.rebuild(proc_ctx_);
  boost::unordered_flat_set<EClassId> retired;
  egraph.retired_eclasses_into(retired);
  auto const retired_ids = std::vector<EClassId>{retired.begin(), retired.end()};
  matcher.retire(retired_ids);
  auto const updated = std::array{survivor};
  matcher.rebuild_index(std::span<EClassId const>{updated});

  // F(?x) over the merged class: one match per distinct ?x, each reported once.
  pattern::vm::PatternsCompiler<Op> compiler;
  auto const compiled = compiler.compile(std::array{TestPattern::build(Op::F, {pattern::dsl::Var{kVarX}})});

  pattern::EMatchContext ctx;
  std::vector<pattern::PatternMatch> matches;
  pattern::vm::VMExecutor<Op, NoAnalysis> executor{egraph};
  executor.execute(compiled, matcher, ctx.arena(), matches);

  pattern::MatchBindings bindings(compiled.var_slots(), ctx.arena());
  std::vector<EClassId> bound;
  for (auto const &match : matches) bound.push_back(bindings.match(match)[kVarX]);
  std::ranges::sort(bound);

  EXPECT_EQ(
      bound,
      (std::vector<EClassId>{std::min(egraph.find(a), egraph.find(b)), std::max(egraph.find(a), egraph.find(b))}));
}

}  // namespace
}  // namespace memgraph::planner::core
