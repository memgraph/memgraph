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

// Explicit template instantiation definitions for test types.
// These match the extern template declarations in test_egraph.hpp,
// so each template is instantiated exactly once across all test TUs.

#include "test_egraph.hpp"

namespace {
using memgraph::planner::core::test::NoAnalysis;
using memgraph::planner::core::test::Op;
}  // namespace

template struct memgraph::planner::core::EGraph<Op, NoAnalysis>;
template class memgraph::planner::core::pattern::vm::PatternsCompiler<Op>;
template class memgraph::planner::core::pattern::vm::VMExecutor<Op, NoAnalysis>;
template class memgraph::planner::core::pattern::vm::VMExecutor<Op, NoAnalysis, true>;
template class memgraph::planner::core::pattern::vm::CompiledMatcher<Op>;
template class memgraph::planner::core::pattern::MatcherIndex<Op, NoAnalysis>;
