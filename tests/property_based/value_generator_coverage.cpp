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

// What the generator reaches, measured rather than assumed.
//
// A generator that never builds the shape that breaks makes every property
// above it pass, and nothing about a green run says which shapes it drew. So the
// spread is sampled here and asserted: a cell of the histogram left empty fails
// the run, and says which cell.

#include <gtest/gtest.h>
#include <rapidcheck.h>

#include <array>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "tests/property_based/value_generators.hpp"
#include "tests/unit/value_shapes.hpp"

using memgraph::storage::PropertyValue;
using memgraph::storage::PropertyValueType;

namespace generators = memgraph::test::generators;
namespace shapes = memgraph::test::shapes;

namespace {

constexpr std::size_t kTypeCount = shapes::kEveryType.size();
constexpr int kDrawCount = 20'000;

/// How deeply these tests require a drawn value to nest, stated here rather than
/// read from the generator: a check that asks for whatever the generator was
/// configured to give passes at any setting, including one too shallow to reach
/// the case it is there for. A walk that forgets to recurse still looks right
/// one level down.
constexpr int kRequiredDepth = 3;

std::size_t Index(PropertyValueType type) { return static_cast<std::size_t>(type); }

/// How deeply the value nests, counting itself as zero.
int NestingDepth(PropertyValue const &value) {
  switch (value.type()) {
    case PropertyValueType::List: {
      auto deepest = 0;
      for (auto const &element : value.ValueList()) deepest = std::max(deepest, NestingDepth(element));
      return 1 + deepest;
    }
    case PropertyValueType::Map: {
      auto deepest = 0;
      for (auto const &entry : value.ValueMap()) deepest = std::max(deepest, NestingDepth(entry.second));
      return 1 + deepest;
    }
    default:
      return 0;
  }
}

/// The shallowest depth at which the value answers the predicate, if any.
template <typename Predicate>
std::optional<int> DepthOf(PropertyValue const &value, Predicate const &holds, int depth = 0) {
  if (holds(value)) return depth;

  auto shallowest = std::optional<int>{};
  auto const consider = [&](PropertyValue const &inner) {
    auto const found = DepthOf(inner, holds, depth + 1);
    if (found && (!shallowest || *found < *shallowest)) shallowest = found;
  };

  if (value.type() == PropertyValueType::List) {
    for (auto const &element : value.ValueList()) consider(element);
  } else if (value.type() == PropertyValueType::Map) {
    for (auto const &entry : value.ValueMap()) consider(entry.second);
  }
  return shallowest;
}

bool IsNaN(PropertyValue const &value) {
  return value.type() == PropertyValueType::Double && std::isnan(value.ValueDouble());
}

bool IsNull(PropertyValue const &value) { return value.type() == PropertyValueType::Null; }

/// Everything one sampling run records, so that one run answers every question
/// below rather than each test drawing its own values.
struct Coverage {
  std::array<int, kTypeCount> by_type{};
  std::array<std::array<int, kTypeCount>, kTypeCount> by_pair{};
  std::array<int, 8> by_nesting_depth{};
  std::array<int, 8> nan_at_depth{};
  std::array<int, 8> null_at_depth{};
  int drawn = 0;
};

Coverage const &Sampled() {
  static Coverage const coverage = [] {
    auto measured = Coverage{};
    auto const generator = generators::AnyValue();

    auto const record = [&measured](PropertyValue const &value) {
      ++measured.by_type[Index(value.type())];
      ++measured.by_nesting_depth[std::min<std::size_t>(NestingDepth(value), measured.by_nesting_depth.size() - 1)];
      if (auto const depth = DepthOf(value, IsNaN)) ++measured.nan_at_depth[std::min<std::size_t>(*depth, 7)];
      if (auto const depth = DepthOf(value, IsNull)) ++measured.null_at_depth[std::min<std::size_t>(*depth, 7)];
    };

    for (auto draw = 0; draw < kDrawCount; ++draw) {
      // A fresh seed per draw: `operator()` is const, so one Random handed to it
      // twice answers the same value twice.
      auto const left = generator(rc::Random(static_cast<std::uint64_t>(draw) * 2), rc::kNominalSize).value();
      auto const right = generator(rc::Random(static_cast<std::uint64_t>(draw) * 2 + 1), rc::kNominalSize).value();

      record(left);
      record(right);
      ++measured.by_pair[Index(left.type())][Index(right.type())];
      measured.drawn += 2;
    }
    return measured;
  }();
  return coverage;
}

}  // namespace

TEST(ValueGeneratorCoverage, DrawsEveryType) {
  auto const &coverage = Sampled();
  ASSERT_EQ(coverage.drawn, kDrawCount * 2);

  for (auto const type : shapes::kEveryType) {
    EXPECT_GT(coverage.by_type[Index(type)], 0) << "no value of type " << static_cast<unsigned>(type) << " was drawn";
  }
}

TEST(ValueGeneratorCoverage, DrawsEveryTypeAgainstEveryOther) {
  // A relation is asked about a pair, so a run that draws every type and never
  // puts two given types together has not asked most of the questions.
  auto const &coverage = Sampled();

  auto empty_cells = 0;
  for (auto const left : shapes::kEveryType) {
    for (auto const right : shapes::kEveryType) {
      if (coverage.by_pair[Index(left)][Index(right)] == 0) {
        ++empty_cells;
        ADD_FAILURE() << "no pair drawn with types " << static_cast<unsigned>(left) << " and "
                      << static_cast<unsigned>(right);
      }
    }
  }
  EXPECT_EQ(empty_cells, 0) << empty_cells << " of " << kTypeCount * kTypeCount << " cells of the type matrix";
}

TEST(ValueGeneratorCoverage, DrawsValuesThatNestAsDeeplyAsTheseTestsRequire) {
  EXPECT_GE(generators::kDefaultDepth, kRequiredDepth)
      << "the generator is asked for less nesting than the cases below need";

  auto const &coverage = Sampled();
  for (auto depth = 0; depth <= kRequiredDepth; ++depth) {
    EXPECT_GT(coverage.by_nesting_depth[depth], 0) << "nothing drawn nesting " << depth << " deep";
  }
}

TEST(ValueGeneratorCoverage, DrawsNothingNestedDeeperThanItWasAskedFor) {
  // A separate claim from the one above: that the generator honours its own
  // bound, whatever that bound is set to.
  auto const &coverage = Sampled();
  EXPECT_EQ(coverage.by_nesting_depth[generators::kDefaultDepth + 1], 0)
      << "something nested deeper than the generator was asked for";
}

TEST(ValueGeneratorCoverage, DrawsANaNAtEveryDepthAValueCanHoldOne) {
  // The case this whole area keeps getting wrong. A NaN at the top exercises a
  // different route from one inside a list, which is different again from one
  // inside a list inside a map.
  auto const &coverage = Sampled();
  for (auto depth = 0; depth < kRequiredDepth; ++depth) {
    EXPECT_GT(coverage.nan_at_depth[depth], 0) << "no NaN drawn at depth " << depth;
  }
}

TEST(ValueGeneratorCoverage, DrawsANullAtEveryDepthAValueCanHoldOne) {
  // The depth that decides whether a uniqueness constraint accepts two rows.
  auto const &coverage = Sampled();
  for (auto depth = 0; depth < kRequiredDepth; ++depth) {
    EXPECT_GT(coverage.null_at_depth[depth], 0) << "no Null drawn at depth " << depth;
  }
}

TEST(ValueGeneratorCoverage, DrawsTheAwkwardDoublesTheShapesName) {
  // The shapes are one of the generator's two sources, and a change that stopped
  // reading them would leave a run that still looks well spread.
  auto const generator = generators::ValueOfType(PropertyValueType::Double, 0);
  auto seen_nan = false;
  auto seen_infinity = false;
  auto seen_negative_zero = false;

  for (auto draw = 0; draw < 2'000; ++draw) {
    auto const held = generator(rc::Random(static_cast<std::uint64_t>(draw)), rc::kNominalSize).value().ValueDouble();
    seen_nan = seen_nan || std::isnan(held);
    seen_infinity = seen_infinity || std::isinf(held);
    seen_negative_zero = seen_negative_zero || (held == 0.0 && std::signbit(held));
  }

  EXPECT_TRUE(seen_nan) << "no NaN drawn";
  EXPECT_TRUE(seen_infinity) << "no infinity drawn";
  EXPECT_TRUE(seen_negative_zero) << "no negative zero drawn";
}

TEST(ValueGeneratorCoverage, DrawsEachTypeAboutAsOftenAsEveryOther) {
  // Type coverage that is technically complete but wildly skewed hides a type
  // behind a handful of draws, so the matrix above passes while that type is
  // barely asked about.
  auto const &coverage = Sampled();
  auto const expected = static_cast<double>(coverage.drawn) / static_cast<double>(kTypeCount);

  for (auto const type : shapes::kEveryType) {
    auto const share = static_cast<double>(coverage.by_type[Index(type)]) / expected;
    EXPECT_GT(share, 0.5) << "type " << static_cast<unsigned>(type) << " is drawn far less than its share";
    EXPECT_LT(share, 2.0) << "type " << static_cast<unsigned>(type) << " is drawn far more than its share";
  }
}
