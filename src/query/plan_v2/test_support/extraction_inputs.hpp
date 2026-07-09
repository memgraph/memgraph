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

#include "query/plan_v2/cost/builtin_estimator.hpp"
#include "query/plan_v2/cost/cost_model.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/resolve/extraction_env.hpp"
#include "query/plan_v2/resolve/pre_extraction.hpp"

namespace memgraph::query::plan::v2::test {

/// The production-mirrored inputs every extraction stage reads: the core
/// e-graph, the builtin cardinality estimator, the pre-extraction data, and the
/// SymbolContext over them. `CostHarness` and `ResolveHarness` both stand this
/// up the way `ConvertToLogicalOperator` does, so the harnesses run the real
/// pipeline rather than a re-implementation of its setup. Members initialise in
/// declaration order (`syms` holds a reference into `pre`); construct on the
/// stack and do not move while in use.
struct ExtractionInputs {
  explicit ExtractionInputs(egraph const &e)
      : core{impl_of(e).graph.core()},
        estimator{e},
        pre{BuildPreExtractionData(core)},
        syms{.egraph = core,
             .variable_index = pre.variable_index,
             .outer_scope = {},
             .referenced_syms = pre.referenced_syms} {}

  ExtractionInputs(ExtractionInputs const &) = delete;
  ExtractionInputs(ExtractionInputs &&) = delete;
  auto operator=(ExtractionInputs const &) -> ExtractionInputs & = delete;
  auto operator=(ExtractionInputs &&) -> ExtractionInputs & = delete;
  ~ExtractionInputs() = default;

  /// The cost model the frontier pass is driven with.
  [[nodiscard]] auto cost_ctx() const -> CostCtx { return CostCtx{.estimator = estimator, .syms = syms}; }

  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
  EGraph const &core;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  BuiltinEstimator estimator;
  PreExtractionData pre;
  SymbolContext syms;
};

}  // namespace memgraph::query::plan::v2::test
