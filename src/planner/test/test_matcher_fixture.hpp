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

#pragma once

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "test_binding_helpers.hpp"
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"

namespace memgraph::planner::core::test {

using pattern::EMatchContext;
using pattern::MatchBindings;
using pattern::PatternVar;

// ============================================================================
// PatternVM Test Fixture
// ============================================================================
//
// Provides:
//   - use_patterns(...)      to store one or more patterns
//   - run_compiled()         to compile and execute patterns via the VM
//   - rebuild_index()        to rebuild the matcher index
//   - verify(...)            to check matches against expected bindings
//   - verify_empty()         to assert no matches
//   - Low-level VM state (matches, ctx, index, compiled_) for direct access

class PatternVM_Matching : public EGraphTestBase {
 public:
  std::vector<TestPattern> patterns_;
  TestVMExecutor vm_executor{egraph};
  TestMatcherIndex index{egraph};
  EMatchContext ctx;
  TestMatches matches;
  std::optional<TestCompiledPattern> compiled_;

  // ---------------------------------------------------------------------------
  // Pattern Setup
  // ---------------------------------------------------------------------------

  template <typename... Patterns>
  void use_patterns(Patterns &&...ps) {
    patterns_.clear();
    (patterns_.push_back(std::forward<Patterns>(ps)), ...);
    TestPatternCompiler compiler;
    compiled_.emplace(compiler.compile(patterns_));
  }

  void use_patterns(std::vector<TestPattern> pats) {
    patterns_ = std::move(pats);
    TestPatternCompiler compiler;
    compiled_.emplace(compiler.compile(patterns_));
  }

  // ---------------------------------------------------------------------------
  // Index Management
  // ---------------------------------------------------------------------------

  void rebuild_index() { index.rebuild_index(); }

  template <typename... Ids>
  void rebuild_index_with(Ids... ids) {
    std::array<EClassId, sizeof...(Ids)> updated{ids...};
    index.rebuild_index(updated);
  }

  // ---------------------------------------------------------------------------
  // Execution
  // ---------------------------------------------------------------------------

  /// Execute the compiled patterns via the VM.
  void run_compiled() {
    ASSERT_TRUE(compiled_.has_value());
    matches.clear();
    vm_executor.execute(*compiled_, index, ctx.arena(), matches);
  }

  // ---------------------------------------------------------------------------
  // Assertions
  // ---------------------------------------------------------------------------

  /// Verify matches against expected bindings (order-independent).
  void verify(std::initializer_list<Bindings> expected) { verify_bindings({expected.begin(), expected.end()}); }

  void verify(std::vector<Bindings> const &expected) { verify_bindings(expected); }

  /// Verify no matches found.
  void verify_empty() const { EXPECT_TRUE(matches.empty()) << "Expected no matches, got " << matches.size(); }

 private:
  void verify_bindings(std::vector<Bindings> const &exp) {
    ASSERT_TRUE(compiled_.has_value());
    MatchBindings mb(compiled_->var_slots(), ctx.arena());

    ASSERT_EQ(matches.size(), exp.size()) << "Expected " << exp.size() << " matches, got " << matches.size();

    std::vector<Bindings const *> remaining;
    remaining.reserve(exp.size());
    for (auto const &e : exp) remaining.push_back(&e);

    for (auto pattern_match : matches) {
      auto match = mb.match(pattern_match);
      auto it = std::ranges::find_if(remaining, [&](auto const *b) { return b->satisfied_by(match); });
      if (it != remaining.end()) remaining.erase(it);
    }

    for (auto const *miss : remaining) {
      ADD_FAILURE() << "Expected match not found: " << miss->format();
    }
  }
};

}  // namespace memgraph::planner::core::test
