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

#include "gtest/gtest.h"

#include "storage/v2/interesting_ids.hpp"

#include <vector>

using memgraph::storage::InterestingIds;
using memgraph::storage::InterestingProperties;
using memgraph::storage::LabelId;
using memgraph::storage::PropertyId;

namespace {

PropertyId Prop(uint64_t const id) { return PropertyId::FromUint(id); }

LabelId Label(uint64_t const id) { return LabelId::FromUint(id); }

}  // namespace

TEST(InterestingIds, EverythingIsInterestedInAnyProperty) {
  auto const interesting = InterestingProperties::Everything();

  EXPECT_TRUE(interesting.IsInteresting(Prop(0)));
  EXPECT_TRUE(interesting.IsInteresting(Prop(7)));
  EXPECT_TRUE(interesting.IsInteresting(Prop(1'000'000)));
}

TEST(InterestingIds, ANarrowSetAnswersByMembership) {
  auto const properties = std::vector{Prop(2), Prop(5), Prop(9)};
  auto const interesting = InterestingProperties::Only(properties);

  EXPECT_TRUE(interesting.IsInteresting(Prop(2)));
  EXPECT_TRUE(interesting.IsInteresting(Prop(5)));
  EXPECT_TRUE(interesting.IsInteresting(Prop(9)));

  EXPECT_FALSE(interesting.IsInteresting(Prop(1)));
  EXPECT_FALSE(interesting.IsInteresting(Prop(6)));
  EXPECT_FALSE(interesting.IsInteresting(Prop(10)));
}

// The two ways of holding nothing must stay distinguishable: a holder with no constrained
// properties narrows everything away, while one that cannot enumerate its properties must
// narrow nothing.
TEST(InterestingIds, AnEmptyNarrowSetIsNotEverything) {
  auto const none = InterestingProperties::Only({});

  EXPECT_FALSE(none.IsInteresting(Prop(0)));
  EXPECT_FALSE(none.IsInteresting(Prop(3)));

  EXPECT_TRUE(InterestingProperties::Everything().IsInteresting(Prop(0)));
}

// The default has to be the safe one: a value nobody filled in reports every write.
TEST(InterestingIds, TheDefaultNarrowsNothing) {
  auto const interesting = InterestingProperties{};

  EXPECT_TRUE(interesting.IsInteresting(Prop(0)));
  EXPECT_TRUE(interesting.IsInteresting(Prop(42)));
}

TEST(InterestingIds, ASingleMemberSetAnswersOnBothSides) {
  auto const properties = std::vector{Prop(4)};
  auto const interesting = InterestingProperties::Only(properties);

  EXPECT_TRUE(interesting.IsInteresting(Prop(4)));
  EXPECT_FALSE(interesting.IsInteresting(Prop(3)));
  EXPECT_FALSE(interesting.IsInteresting(Prop(5)));
}

// The same set over a different id type, because the label channel narrows by label.
TEST(InterestingIds, ANarrowLabelSetAnswersByMembership) {
  auto const labels = std::vector{Label(1), Label(4)};
  auto const interesting = InterestingIds<LabelId>::Only(labels);

  EXPECT_TRUE(interesting.IsInteresting(Label(1)));
  EXPECT_TRUE(interesting.IsInteresting(Label(4)));
  EXPECT_FALSE(interesting.IsInteresting(Label(0)));
  EXPECT_FALSE(interesting.IsInteresting(Label(2)));
  EXPECT_FALSE(interesting.IsInteresting(Label(5)));
}

TEST(InterestingIds, EverythingAndTheDefaultAgreeForLabels) {
  EXPECT_TRUE(InterestingIds<LabelId>::Everything().IsInteresting(Label(9)));
  EXPECT_TRUE((InterestingIds<LabelId>{}).IsInteresting(Label(9)));
  EXPECT_FALSE(InterestingIds<LabelId>::Only({}).IsInteresting(Label(9)));
}
