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

// Explicit template instantiation definitions matching the extern template
// declarations in test_support/types.hpp. Compiled into both the unit-test
// and benchmark binaries so each binary instantiates the templates once.

#include "test_support/types.hpp"

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
