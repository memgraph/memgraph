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
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"

namespace memgraph::planner::core::test {

// ============================================================================
// PatternVM MatcherIndex Test Fixture
// ============================================================================

class PatternVM_MatcherIndex : public EGraphTestBase {
 protected:
  TestMatcherIndex index{egraph};
  EMatchContext ctx;
  TestMatches matches;
  std::optional<TestPattern> pattern_;

  // ---------------------------------------------------------------------------
  // Pattern Matching
  // ---------------------------------------------------------------------------

  void use_pattern(TestPattern p) { pattern_.emplace(std::move(p)); }

  auto pattern() const -> TestPattern const & { return *pattern_; }

  // ---------------------------------------------------------------------------
  // Matcher Index Rebuild
  // ---------------------------------------------------------------------------

  void rebuild_index() { index.rebuild_index(); }

  template <typename... Ids>
  void rebuild_index_with(Ids... ids) {
    std::array<EClassId, sizeof...(Ids)> updated{ids...};
    index.rebuild_index(updated);
  }

  // ---------------------------------------------------------------------------
  // Assertions
  // ---------------------------------------------------------------------------

  using Bindings = std::initializer_list<std::pair<PatternVar, EClassId>>;

  void expect_matches(std::initializer_list<Bindings> expected_matches) {
    auto const num_vars = pattern_->num_vars();

    // Verify each expected binding specifies exactly the right number of variables
    std::size_t binding_idx = 0;
    for (auto const &expected : expected_matches) {
      ASSERT_EQ(expected.size(), num_vars) << "Expected binding " << binding_idx << " has " << expected.size()
                                           << " variables, but pattern has " << num_vars;
      ++binding_idx;
    }

    matches.clear();
    vm::PatternCompiler<Op> compiler;
    auto compiled = compiler.compile(*pattern_);
    ASSERT_TRUE(compiled.has_value()) << "Pattern compilation failed";
    vm::VMExecutor<Op, NoAnalysis> vm_executor(egraph);
    vm_executor.execute(*compiled, index, ctx, matches);

    ASSERT_EQ(matches.size(), expected_matches.size())
        << "Expected " << expected_matches.size() << " matches, got " << matches.size();

    std::vector<bool> found(expected_matches.size(), false);
    for (std::size_t mi = 0; mi < matches.size(); ++mi) {
      std::size_t ei = 0;
      for (auto const &expected : expected_matches) {
        if (!found[ei]) {
          bool all_match = true;
          for (auto [var, expected_id] : expected) {
            if (binding(mi, var) != expected_id) {
              all_match = false;
              break;
            }
          }
          if (all_match) {
            found[ei] = true;
            break;
          }
        }
        ++ei;
      }
    }

    for (std::size_t i = 0; i < found.size(); ++i) {
      EXPECT_TRUE(found[i]) << "Expected match " << i << " not found";
    }
  }

  void expect_no_matches() {
    matches.clear();
    // Patterns with no variables have no meaningful bindings to return
    if (pattern_->num_vars() == 0) {
      return;
    }
    vm::PatternCompiler<Op> compiler;
    auto compiled = compiler.compile(*pattern_);
    if (compiled.has_value()) {
      vm::VMExecutor<Op, NoAnalysis> vm_executor(egraph);
      vm_executor.execute(*compiled, index, ctx, matches);
    }
    EXPECT_TRUE(matches.empty()) << "Expected no matches, got " << matches.size();
  }

 private:
  auto binding(std::size_t match_idx, PatternVar var) -> EClassId {
    auto bindings = ctx.arena().bindings(matches.at(match_idx), pattern_->num_vars());
    return bindings[pattern_->var_slot(var)];
  }
};

}  // namespace memgraph::planner::core::test
