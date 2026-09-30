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

// What the query-side generator reaches. The stored generator's own coverage is
// measured beside it; this one answers the same questions over the types only
// the query layer has.

#include <gtest/gtest.h>
#include <rapidcheck.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <map>
#include <optional>
#include <vector>

#include "tests/property_based/typed_value_generators.hpp"
#include "tests/unit/typed_value_shapes.hpp"

using memgraph::query::TypedValue;

namespace generators = memgraph::test::generators;
namespace shapes = memgraph::test::shapes;

namespace {

constexpr int kDrawCount = 20'000;
constexpr int kRequiredDepth = 3;

/// The shallowest depth at which the value answers the predicate, if any.
template <typename Predicate>
std::optional<int> DepthOf(TypedValue const &value, Predicate const &holds, int depth = 0) {
  if (holds(value)) return depth;

  auto shallowest = std::optional<int>{};
  auto const consider = [&](TypedValue const &inner) {
    auto const found = DepthOf(inner, holds, depth + 1);
    if (found && (!shallowest || *found < *shallowest)) shallowest = found;
  };

  if (value.type() == TypedValue::Type::List) {
    for (auto const &element : value.ValueList()) consider(element);
  } else if (value.type() == TypedValue::Type::Map) {
    for (auto const &entry : value.ValueMap()) consider(entry.second);
  }
  return shallowest;
}

bool IsNaN(TypedValue const &value) {
  return value.type() == TypedValue::Type::Double && std::isnan(value.ValueDouble());
}

bool IsNull(TypedValue const &value) { return value.IsNull(); }

struct Coverage {
  std::map<TypedValue::Type, int> by_type;
  std::map<std::pair<TypedValue::Type, TypedValue::Type>, int> by_pair;
  std::array<int, 8> nan_at_depth{};
  std::array<int, 8> null_at_depth{};
  int drawn = 0;
};

Coverage const &Sampled() {
  static Coverage const coverage = [] {
    auto measured = Coverage{};
    auto const generator = generators::AnyTypedValue();

    auto const record = [&measured](TypedValue const &value) {
      ++measured.by_type[value.type()];
      if (auto const depth = DepthOf(value, IsNaN)) ++measured.nan_at_depth[std::min(*depth, 7)];
      if (auto const depth = DepthOf(value, IsNull)) ++measured.null_at_depth[std::min(*depth, 7)];
    };

    for (auto draw = 0; draw < kDrawCount; ++draw) {
      auto const left = generator(rc::Random(static_cast<std::uint64_t>(draw) * 2), rc::kNominalSize).value();
      auto const right = generator(rc::Random(static_cast<std::uint64_t>(draw) * 2 + 1), rc::kNominalSize).value();

      record(left);
      record(right);
      ++measured.by_pair[{left.type(), right.type()}];
      measured.drawn += 2;
    }
    return measured;
  }();
  return coverage;
}

}  // namespace

TEST(TypedValueGeneratorCoverage, DrawsEveryTypeItClaimsTo) {
  auto const &coverage = Sampled();
  ASSERT_EQ(coverage.drawn, kDrawCount * 2);

  for (auto const type : generators::GraphFreeTypes()) {
    EXPECT_GT(coverage.by_type.contains(type) ? coverage.by_type.at(type) : 0, 0)
        << "no value of type " << static_cast<unsigned>(type) << " was drawn";
  }
}

TEST(TypedValueGeneratorCoverage, DrawsNoTypeItCannotMakeWithoutTheGraph) {
  auto const &coverage = Sampled();
  for (auto const type : shapes::kGraphTypedValueTypes) {
    EXPECT_FALSE(coverage.by_type.contains(type))
        << "type " << static_cast<unsigned>(type) << " was drawn with no accessor to make it";
  }
  for (auto const type : shapes::kUnshapedTypedValueTypes) {
    EXPECT_FALSE(coverage.by_type.contains(type))
        << "type " << static_cast<unsigned>(type) << " has no shapes but was drawn";
  }
}

TEST(TypedValueGeneratorCoverage, DrawsEveryTypeAgainstEveryOther) {
  auto const &coverage = Sampled();
  auto const types = generators::GraphFreeTypes();

  auto empty_cells = 0;
  for (auto const left : types) {
    for (auto const right : types) {
      if (!coverage.by_pair.contains({left, right})) {
        ++empty_cells;
        ADD_FAILURE() << "no pair drawn with types " << static_cast<unsigned>(left) << " and "
                      << static_cast<unsigned>(right);
      }
    }
  }
  EXPECT_EQ(empty_cells, 0) << empty_cells << " of " << types.size() * types.size() << " cells of the type matrix";
}

TEST(TypedValueGeneratorCoverage, DrawsANaNAtEveryDepthAValueCanHoldOne) {
  auto const &coverage = Sampled();
  for (auto depth = 0; depth < kRequiredDepth; ++depth) {
    EXPECT_GT(coverage.nan_at_depth[depth], 0) << "no NaN drawn at depth " << depth;
  }
}

TEST(TypedValueGeneratorCoverage, DrawsANullAtEveryDepthAValueCanHoldOne) {
  auto const &coverage = Sampled();
  for (auto depth = 0; depth < kRequiredDepth; ++depth) {
    EXPECT_GT(coverage.null_at_depth[depth], 0) << "no Null drawn at depth " << depth;
  }
}
