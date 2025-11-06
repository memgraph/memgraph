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

#include <gtest/gtest.h>

#include "planner/core/egraph.hpp"
#include "planner/core/extractor.hpp"

using namespace memgraph::planner::core;

enum struct symbol : std::uint8_t { A, B };

struct analysis {};

TEST(Extractor, Thing) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [root_class, root_enode] = egraph.emplace(symbol::A);
  auto cost_function = [](ENode<symbol> const &) -> double { return 1.0; };
  auto extractor = Extractor{egraph, cost_function};
  auto extracted = extractor.Extract(root_class);
  ASSERT_EQ(extracted.size(), 1);
  ASSERT_EQ(extracted[0].first, root_class);
  ASSERT_EQ(extracted[0].second, root_enode);
}

TEST(Extractor, CheapestRootSelected) {
  auto egraph = EGraph<symbol, analysis>{};
  auto [aclass, anode] = egraph.emplace(symbol::A);
  auto [bclass, bnode] = egraph.emplace(symbol::B);
  auto root = egraph.merge(aclass, bclass);
  auto cost_function = [](ENode<symbol> const &node) -> double { return (node.symbol() == symbol::A) ? 1.0 : 2.0; };
  auto extractor = Extractor{egraph, cost_function};
  auto extracted = extractor.Extract(root);
  ASSERT_EQ(extracted.size(), 1);
  ASSERT_EQ(extracted[0].first, root);
  ASSERT_EQ(extracted[0].second, anode);
}
