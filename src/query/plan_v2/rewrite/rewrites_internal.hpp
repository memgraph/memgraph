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

// NOTE: This header should NOT be included by public API consumers. It exposes
// the rule set behind the egraph pimpl. Production code uses ApplyAllRewrites /
// ApplyInlineRewrite (rewrites.hpp); this back-door exists for benchmarks and
// tests that need to drive a persistent Rewriter directly.

#include "planner/rewrite/rule_set.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

/// The default plan_v2 rewrite rule set: the inline rule plus one constant-fold
/// rule per binary and unary operator. A process-wide singleton (cheap to copy:
/// a shared_ptr increment).
auto DefaultRules() -> planner::core::rewrite::RuleSet<typed_egraph> const &;

}  // namespace memgraph::query::plan::v2
