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

// Explicit template instantiation definitions for bench types.
// These match the extern template declarations in bench_common.hpp,
// so each template is instantiated exactly once across all bench TUs.

#include "bench_common.hpp"

namespace {
using memgraph::planner::bench::NoAnalysis;
using memgraph::planner::bench::Op;
}  // namespace

template struct memgraph::planner::core::EGraph<Op, NoAnalysis>;
template class memgraph::planner::core::pattern::vm::PatternCompiler<Op>;
template class memgraph::planner::core::pattern::vm::VMExecutor<Op, NoAnalysis>;
template class memgraph::planner::core::pattern::vm::VMExecutor<Op, NoAnalysis, true>;
template class memgraph::planner::core::pattern::vm::CompiledPattern<Op>;
template class memgraph::planner::core::pattern::MatcherIndex<Op, NoAnalysis>;
