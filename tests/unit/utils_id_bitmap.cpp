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

#include "utils/id_bitmap.hpp"

#include <cstdint>

using memgraph::utils::IdBitmap;

namespace {

// Stands in for LabelId and friends without dragging storage in: the bitmap asks an id for
// nothing but its number.
struct TestId {
  uint64_t value;

  uint64_t AsUint() const { return value; }
};

// The bitmap holds a word per sixty-four ids, so the boundaries between words are where an
// off-by-one in the addressing shows up.
constexpr auto kBitsPerWord = uint64_t{64};

}  // namespace

TEST(IdBitmap, AnEmptyBitmapHoldsNothing) {
  auto bitmap = IdBitmap<TestId>{};

  EXPECT_FALSE(bitmap.any());
  // Asking about an id past the end must answer rather than read off the end of the storage.
  EXPECT_FALSE(bitmap.test(TestId{0}));
  EXPECT_FALSE(bitmap.test(TestId{1'000'000}));
}

TEST(IdBitmap, SetIdsAreTheOnlyOnesHeld) {
  auto bitmap = IdBitmap<TestId>{};
  bitmap.set(TestId{3});
  bitmap.set(TestId{200});

  EXPECT_TRUE(bitmap.any());
  EXPECT_TRUE(bitmap.test(TestId{3}));
  EXPECT_TRUE(bitmap.test(TestId{200}));
  EXPECT_FALSE(bitmap.test(TestId{2}));
  EXPECT_FALSE(bitmap.test(TestId{4}));
  EXPECT_FALSE(bitmap.test(TestId{199}));
  EXPECT_FALSE(bitmap.test(TestId{201}));
}

TEST(IdBitmap, IdsEitherSideOfAWordBoundaryAreDistinct) {
  auto bitmap = IdBitmap<TestId>{};
  bitmap.set(TestId{kBitsPerWord});

  EXPECT_FALSE(bitmap.test(TestId{kBitsPerWord - 1}));
  EXPECT_TRUE(bitmap.test(TestId{kBitsPerWord}));
  EXPECT_FALSE(bitmap.test(TestId{kBitsPerWord + 1}));
  // The same bit within the next word along, which a missing word offset would confuse for it.
  EXPECT_FALSE(bitmap.test(TestId{2 * kBitsPerWord}));
}

TEST(IdBitmap, SettingAnIdTwiceChangesNothing) {
  auto bitmap = IdBitmap<TestId>{};
  bitmap.set(TestId{7});
  bitmap.set(TestId{7});

  EXPECT_TRUE(bitmap.test(TestId{7}));
  EXPECT_FALSE(bitmap.test(TestId{6}));
  EXPECT_FALSE(bitmap.test(TestId{8}));
}

TEST(IdBitmap, GrowingForAHighIdKeepsTheLowOnes) {
  auto bitmap = IdBitmap<TestId>{};
  bitmap.set(TestId{1});
  bitmap.set(TestId{5'000});

  EXPECT_TRUE(bitmap.test(TestId{1}));
  EXPECT_TRUE(bitmap.test(TestId{5'000}));
}

TEST(IdBitmap, ResetEmptiesWithoutLosingWhatWasGrown) {
  auto bitmap = IdBitmap<TestId>{};
  bitmap.set(TestId{500});
  ASSERT_TRUE(bitmap.any());

  bitmap.reset();

  EXPECT_FALSE(bitmap.any());
  EXPECT_FALSE(bitmap.test(TestId{500}));

  // A holder reused across collection cycles refills after resetting, and the words it kept must
  // still be the words it addresses.
  bitmap.set(TestId{500});
  EXPECT_TRUE(bitmap.test(TestId{500}));
  EXPECT_FALSE(bitmap.test(TestId{499}));
}

TEST(IdBitmap, MergingTakesTheUnion) {
  auto lhs = IdBitmap<TestId>{};
  lhs.set(TestId{1});
  auto rhs = IdBitmap<TestId>{};
  rhs.set(TestId{2});

  lhs |= rhs;

  EXPECT_TRUE(lhs.test(TestId{1}));
  EXPECT_TRUE(lhs.test(TestId{2}));
  // The right-hand side is read, not emptied: a collection cycle merges several holders in turn.
  EXPECT_TRUE(rhs.test(TestId{2}));
  EXPECT_FALSE(rhs.test(TestId{1}));
}

TEST(IdBitmap, MergingAWiderBitmapGrowsTheNarrowerOne) {
  auto narrow = IdBitmap<TestId>{};
  narrow.set(TestId{1});
  auto wide = IdBitmap<TestId>{};
  wide.set(TestId{5'000});

  narrow |= wide;

  EXPECT_TRUE(narrow.test(TestId{1}));
  EXPECT_TRUE(narrow.test(TestId{5'000}));
}

TEST(IdBitmap, MergingANarrowerBitmapKeepsTheWiderOnesHighIds) {
  auto wide = IdBitmap<TestId>{};
  wide.set(TestId{5'000});
  auto narrow = IdBitmap<TestId>{};
  narrow.set(TestId{1});

  wide |= narrow;

  EXPECT_TRUE(wide.test(TestId{1}));
  EXPECT_TRUE(wide.test(TestId{5'000}));
}

TEST(IdBitmap, MergingAnEmptyBitmapHoldsNothing) {
  auto lhs = IdBitmap<TestId>{};
  auto rhs = IdBitmap<TestId>{};

  lhs |= rhs;

  EXPECT_FALSE(lhs.any());
}

// A reset bitmap keeps its words, all of them zero, so a merge from one must not report the
// bitmap it merged into as holding something.
TEST(IdBitmap, MergingAResetBitmapHoldsNothing) {
  auto emptied = IdBitmap<TestId>{};
  emptied.set(TestId{5'000});
  emptied.reset();

  auto bitmap = IdBitmap<TestId>{};
  bitmap |= emptied;

  EXPECT_FALSE(bitmap.any());
}
