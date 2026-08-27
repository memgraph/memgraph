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

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

#include <gtest/gtest.h>

#include "query/value_relations.hpp"

namespace memgraph::query::tests {
namespace {

using memgraph::query::TypedValue;
namespace orderability = memgraph::query::relations::orderability;

/// The types orderability places, one value of each, written in the order the
/// relation is meant to put them. Graph, VirtualGraph and Function are absent
/// because no order is defined over them.
std::vector<TypedValue> OrderedSample() {
  return {
      TypedValue(std::vector<TypedValue>{TypedValue(1)}),  // List
      TypedValue("string"),
      TypedValue(true),
      TypedValue(int64_t{42}),
      TypedValue(),  // Null
  };
}

TEST(Orderability, PlacesEveryPairOfUnlikeTypes) {
  auto const sample = OrderedSample();
  for (auto const &a : sample) {
    for (auto const &b : sample) {
      // Nothing is left unplaced; a weak ordering has no such state to return.
      auto const order = orderability::Compare(a, b);
      EXPECT_TRUE(std::is_lt(order) || std::is_gt(order) || order == std::weak_ordering::equivalent);
    }
  }
}

TEST(Orderability, SampleIsInAscendingOrder) {
  auto const sample = OrderedSample();
  for (size_t i = 1; i < sample.size(); ++i) {
    EXPECT_TRUE(std::is_lt(orderability::Compare(sample[i - 1], sample[i])))
        << "index " << i - 1 << " should sort before " << i;
  }
}

TEST(Orderability, IsAntisymmetric) {
  auto const sample = OrderedSample();
  for (auto const &a : sample) {
    for (auto const &b : sample) {
      auto const forward = orderability::Compare(a, b);
      auto const backward = orderability::Compare(b, a);
      EXPECT_EQ(std::is_lt(forward), std::is_gt(backward));
      EXPECT_EQ(std::is_gt(forward), std::is_lt(backward));
    }
  }
}

TEST(Orderability, IsTransitive) {
  auto const sample = OrderedSample();
  for (auto const &a : sample) {
    for (auto const &b : sample) {
      for (auto const &c : sample) {
        if (std::is_lt(orderability::Compare(a, b)) && std::is_lt(orderability::Compare(b, c))) {
          EXPECT_TRUE(std::is_lt(orderability::Compare(a, c)));
        }
      }
    }
  }
}

TEST(Orderability, PlacesNaNAfterEveryNumber) {
  auto const nan = TypedValue(std::numeric_limits<double>::quiet_NaN());
  for (auto const &number :
       {TypedValue(int64_t{0}), TypedValue(-1.5), TypedValue(std::numeric_limits<double>::infinity())}) {
    EXPECT_TRUE(std::is_gt(orderability::Compare(nan, number)));
    EXPECT_TRUE(std::is_lt(orderability::Compare(number, nan)));
  }
}

TEST(Orderability, PlacesTwoNaNsAlongsideOneAnother) {
  auto const nan = TypedValue(std::numeric_limits<double>::quiet_NaN());
  EXPECT_EQ(orderability::Compare(nan, nan), std::weak_ordering::equivalent);
}

TEST(Orderability, PlacesNullAfterEverything) {
  auto const null = TypedValue();
  for (auto const &value : OrderedSample()) {
    if (value.IsNull()) continue;
    EXPECT_TRUE(std::is_gt(orderability::Compare(null, value)));
  }
  EXPECT_EQ(orderability::Compare(null, null), std::weak_ordering::equivalent);
}

TEST(Orderability, OrdersIntegersAgainstDoublesAsNumbers) {
  // Both share one rank, so the ordering reads their values rather than their
  // types.
  EXPECT_TRUE(std::is_lt(orderability::Compare(TypedValue(int64_t{1}), TypedValue(1.5))));
  EXPECT_TRUE(std::is_gt(orderability::Compare(TypedValue(int64_t{2}), TypedValue(1.5))));
  EXPECT_EQ(orderability::Compare(TypedValue(int64_t{2}), TypedValue(2.0)), std::weak_ordering::equivalent);
}

TEST(Orderability, SortsAMixedColumnWithoutRaising) {
  auto values = OrderedSample();
  std::ranges::reverse(values);
  // Sorting is the property the relation exists for, and it is undefined
  // behaviour unless the relation is a strict weak ordering.
  std::ranges::sort(values,
                    [](TypedValue const &a, TypedValue const &b) { return std::is_lt(orderability::Compare(a, b)); });
  auto const expected = OrderedSample();
  for (size_t i = 0; i != values.size(); ++i) {
    EXPECT_EQ(orderability::Compare(values[i], expected[i]), std::weak_ordering::equivalent) << "at index " << i;
  }
}

}  // namespace
}  // namespace memgraph::query::tests
