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

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_ref.hpp"
#include "versioning/hidden_fork_edges.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::HiddenForkEdges;

namespace {

std::vector<std::unique_ptr<ms::Edge>> MakeEdges(size_t count) {
  std::vector<std::unique_ptr<ms::Edge>> edges;
  for (size_t i = 0; i < count; ++i) edges.push_back(std::make_unique<ms::Edge>(ms::Gid::FromUint(i), nullptr));
  return edges;
}

}  // namespace

TEST(VersioningHiddenForkEdges, ContainsExactlyWhatWasAddedWhetherSearchedLinearlyOrSorted) {
  for (size_t const count : {size_t{0}, size_t{1}, size_t{8}, size_t{9}, size_t{300}}) {
    SCOPED_TRACE(count);
    auto const edges = MakeEdges(2 * count + 1);
    HiddenForkEdges hidden;
    // Added in descending address order, so a sorted lookup cannot rely on insertion order.
    for (size_t i = count; i-- > 0;) hidden.Add(ms::EdgeRef{edges[2 * i].get()});
    hidden.Seal();
    EXPECT_EQ(hidden.size(), count);
    EXPECT_EQ(hidden.empty(), count == 0);
    for (size_t i = 0; i < edges.size(); ++i) {
      EXPECT_EQ(hidden.Contains(ms::EdgeRef{edges[i].get()}), i % 2 == 0 && i / 2 < count) << i;
    }
  }
}

TEST(VersioningHiddenForkEdges, ABareGidIsAnEdgeRefLikeAnyOther) {
  HiddenForkEdges hidden;
  hidden.Add(ms::EdgeRef{ms::Gid::FromUint(5)});
  hidden.Seal();
  EXPECT_TRUE(hidden.Contains(ms::EdgeRef{ms::Gid::FromUint(5)}));
  EXPECT_FALSE(hidden.Contains(ms::EdgeRef{ms::Gid::FromUint(6)}));
}
