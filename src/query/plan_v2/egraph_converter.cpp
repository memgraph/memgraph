// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
#include "query/plan_v2/egraph_converter.hpp"

#include "planner/core/extractor.hpp"
#include "query/plan/operator.hpp"
#include "query/plan_v2/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

struct CostModel {
  struct CostResult {
    double cost{};
    double cardinality{};

    friend auto operator<(CostResult const &lhs, CostResult const &rhs) { return lhs.cost < rhs.cost; }
  };

  static auto operator()(planner::core::ENode<symbol> const &current, std::span<CostResult const> children)
      -> CostResult {
    // TODO: build a better cost calculator
    auto children_sum =
        std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val.cost; });
    return {1.0 + children_sum, 1.0};
  }
};

// TODO: make a concept to check cost model is valid
//  static_assert(some_concept<CostModel>);

auto ConvertToLogicalOperator(egraph const &e, eclass root) -> std::tuple<std::unique_ptr<LogicalOperator>, double> {
  // Access the internal egraph through the accessor
  auto const &internal_egraph = internal::get_egraph(e);

  auto extractor = planner::core::Extractor{internal_egraph, CostModel{}};
  auto thing = extractor.Extract(internal::to_core_id(root));

  // TODO: build LogicalOperator

  // egraph extraction -> subgraph of the egraph (one Enode per EClass)
  // start for a root
  // must be able to handle cycles -> Extraction should have no cycles (cycles only useful to aid rewrites)

  // Subgraph -> LogicalOperator + AST Expressions + Symbol table
  // NOTE: we lost a NamedExpressions name when we did BIND for WITH
  //       that shoudl be tracked for the enode (not the disambiguator)
  //       so we can use it again when

  // WITH 1 AS X, 1 AS Y WITH Y AS RES

  //  (BIND (0) input_1 (SYM 0) (LITERAL 1))
  //  (BIND (1) input_2 (SYM 1) (LITERAL 1))
  //  (IDENT (SYM 1))

  // $a=(IDENT X), (BIND _ X $b) -> MERGE $a, $b

  return {nullptr, 0.0};
}
}  // namespace memgraph::query::plan::v2
