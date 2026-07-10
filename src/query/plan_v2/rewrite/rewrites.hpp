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

#include "planner/rewrite/rewriter.hpp"
#include "query/plan_v2/egraph/egraph.hpp"

namespace memgraph::query::plan::v2 {

// Import generic rewrite types from planner/core/rewrite
using planner::core::rewrite::RewriteConfig;
using planner::core::rewrite::RewriteResult;

/// Inline bound identifiers: for each `Bind(input, sym, expr)`, merge every
/// `Identifier(sym)` e-class into expr's, so the bound expression is shared
/// wherever the identifier is referenced. Returns the number of merges.
auto ApplyInlineRewrite(egraph &eg) -> std::size_t;

/// Run all rewrites to a fixed point or until `config`'s limits are hit;
/// returns the run statistics and stop reason.
auto ApplyAllRewrites(egraph &eg, RewriteConfig const &config = RewriteConfig::Default()) -> RewriteResult;

}  // namespace memgraph::query::plan::v2
