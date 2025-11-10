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

#include "planner/core/egraph.hpp"
#include "query/plan/operator.hpp"

#include "private_analysis.hpp"
#include "private_symbol.hpp"

// #include "egraph.cpp"  //TODO: fix C++20 module

namespace memgraph::query::plan::v2 {
auto ConvertToLogicalOperator(egraph const &e, eclass root) -> std::tuple<std::unique_ptr<LogicalOperator>, double> {
  auto cost_func = [](planner::core::ENode<symbol> const &) -> double { return 1.0; };
  auto extractor = memgraph::planner::core::Extractor{e.pimpl_->egraph_, cost_func};
  auto thing = extractor.Extract(planner::core::EClassId{root.value_of()});

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
