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

#include <vector>

#include "planner/pattern/match_index.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "test_symbols.hpp"

namespace memgraph::planner::core::test {

// ============================================================================
// E-Graph Test Types
// ============================================================================

using TestEGraph = EGraph<Op, NoAnalysis>;
using TestMatcherIndex = pattern::MatcherIndex<Op, NoAnalysis>;
using TestMatches = std::vector<pattern::PatternMatch>;
using TestVMExecutor = pattern::vm::VMExecutor<Op, NoAnalysis>;
using TestPatternCompiler = pattern::vm::PatternCompiler<Op>;
using TestCompiledPattern = pattern::vm::CompiledPattern<Op>;
using TestProcessingContext = ProcessingContext<Op>;
using TestPattern = pattern::Pattern<Op>;

}  // namespace memgraph::planner::core::test
