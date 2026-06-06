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

#include <cstdint>
#include <span>

#include "planner/extract/extractor.hpp"
#include "query/plan_v2/cost/cost_model.hpp"
#include "query/plan_v2/egraph/alternative.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/test_support/extraction_inputs.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::plan::v2::test {

/// Runs only the cost pass over an e-graph and exposes each e-class's pareto
/// frontier, so a test can assert a cost trait's emitted alternatives directly
/// (their `required` / `introduces` / `cardinality`) rather than inferring them
/// from the built v1 plan. Construct on the stack; it owns everything the
/// frontier views reference, so it must not be moved while in use.
class CostHarness {
 public:
  CostHarness(egraph const &e, eclass root) : in_{e} {
    (void)planner::core::extract::ComputeFrontiers(in_.core, in_.cost_ctx(), to_core(root), frontier_context_);
  }

  CostHarness(CostHarness const &) = delete;
  CostHarness(CostHarness &&) = delete;
  auto operator=(CostHarness const &) -> CostHarness & = delete;
  auto operator=(CostHarness &&) -> CostHarness & = delete;
  ~CostHarness() = default;

  /// The pareto frontier computed for `c`. Asserts one exists (a missing or
  /// in-progress frontier is a test-setup error, not an expected outcome).
  [[nodiscard]] auto frontier(eclass c) const -> CostFrontier const & {
    auto const it = frontier_context_.frontier_map.find(to_core(c));
    MG_ASSERT(it != frontier_context_.frontier_map.end() && it->second.has_value(),
              "CostHarness: no frontier for eclass {} (not reachable from root?)",
              c.value_of());
    return *it->second;
  }

  [[nodiscard]] auto alts(eclass c) const -> std::span<Alternative const> { return frontier(c).alts(); }

  /// The VariableIndex bit a Symbol e-class was assigned, for asserting against
  /// an alt's `introduces` / `required`.
  [[nodiscard]] auto bit_of(eclass sym) const -> uint16_t { return in_.pre.variable_index.bit_of(to_core(sym)); }

 private:
  ExtractionInputs in_;
  planner::core::extract::FrontierContext<CostFrontier> frontier_context_{};
};

}  // namespace memgraph::query::plan::v2::test
