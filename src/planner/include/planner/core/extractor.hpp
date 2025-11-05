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

#pragma once
#include "enode.hpp"

namespace memgraph::planner::core {

template <typename Symbol>
using CostFunction = std::function<double(ENode<Symbol> const &)>;

template <typename Symbol, typename Analysis>
struct Extractor {
  Extractor(EGraph<Symbol, Analysis> const &egraph, CostFunction<Symbol> cost_function)
      : egraph_(egraph), cost_function_(std::move(cost_function)) {}

  auto Extract(EClass<Analysis> eclass) -> void;

  auto GetResult() -> std::vector<std::pair<EClassId, ENodeId>> const & { return result_; }

 private:
  EGraph<Symbol, Analysis> const &egraph_;
  CostFunction<Symbol> cost_function_;
  std::vector<std::pair<EClassId, ENodeId>> result_;
};

}  // namespace memgraph::planner::core
