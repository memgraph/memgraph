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

#include <gtest/gtest.h>

#include <algorithm>
#include <barrier>
#include <thread>
#include <vector>

#include "storage/v2/property_manifest.hpp"

using memgraph::storage::ManifestEntry;
using memgraph::storage::ManifestRegistry;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyStoreType;
using memgraph::storage::StoredType;

namespace {

PropertyId Prop(uint32_t id) { return PropertyId::FromUint(id); }

StoredType Int(uint8_t width) { return StoredType::Fixed(PropertyStoreType::INT, width); }

StoredType Double() { return StoredType::Fixed(PropertyStoreType::DOUBLE, 8); }

StoredType Bool() { return StoredType::Fixed(PropertyStoreType::BOOL, 1); }

StoredType String() { return StoredType::Variable(PropertyStoreType::STRING); }

ManifestEntry Entry(uint32_t id, StoredType type) { return ManifestEntry{.property = Prop(id), .stored_type = type}; }

}  // namespace

TEST(PropertyManifest, EmptyShapeInterns) {
  ManifestRegistry registry;
  auto const id = registry.Intern({});
  auto const &manifest = registry.Resolve(id);

  EXPECT_EQ(manifest.size(), 0);
  EXPECT_EQ(manifest.fixed_region_size(), 0);
  EXPECT_EQ(manifest.variable_count(), 0);
  EXPECT_FALSE(manifest.Find(Prop(1)).has_value());
}

TEST(PropertyManifest, SameShapeInternsToSameId) {
  ManifestRegistry registry;
  auto const shape = std::vector{Entry(1, Int(4)), Entry(7, String())};

  auto const first = registry.Intern(shape);
  auto const second = registry.Intern(shape);

  EXPECT_EQ(first, second);
  EXPECT_EQ(registry.size(), 1);
}

TEST(PropertyManifest, ShapesDifferingByPropertySetAreDistinct) {
  ManifestRegistry registry;

  auto const with_seven = registry.Intern(std::vector{Entry(1, Int(4)), Entry(7, String())});
  auto const with_eight = registry.Intern(std::vector{Entry(1, Int(4)), Entry(8, String())});
  auto const only_one = registry.Intern(std::vector{Entry(1, Int(4))});

  EXPECT_NE(with_seven, with_eight);
  EXPECT_NE(with_seven, only_one);
  EXPECT_EQ(registry.size(), 3);
}

TEST(PropertyManifest, ShapesDifferingOnlyByTypeAreDistinct) {
  ManifestRegistry registry;

  auto const as_int = registry.Intern(std::vector{Entry(1, Int(8))});
  auto const as_double = registry.Intern(std::vector{Entry(1, Double())});

  EXPECT_NE(as_int, as_double);
}

// Width class is part of the shape: keeping int compression means a value that grows past
// its width class lands in a different manifest.
TEST(PropertyManifest, ShapesDifferingOnlyByWidthClassAreDistinct) {
  ManifestRegistry registry;

  auto const narrow = registry.Intern(std::vector{Entry(1, Int(1))});
  auto const wide = registry.Intern(std::vector{Entry(1, Int(8))});

  EXPECT_NE(narrow, wide);
  EXPECT_EQ(registry.Resolve(narrow).fixed_region_size(), 1);
  EXPECT_EQ(registry.Resolve(wide).fixed_region_size(), 8);
}

TEST(PropertyManifest, FixedValuesGetPrefixSumOffsetsInPropertyOrder) {
  ManifestRegistry registry;
  auto const id = registry.Intern(std::vector{Entry(1, Bool()), Entry(2, Int(4)), Entry(3, Double())});
  auto const &manifest = registry.Resolve(id);

  auto const first = manifest.Find(Prop(1));
  auto const second = manifest.Find(Prop(2));
  auto const third = manifest.Find(Prop(3));

  ASSERT_TRUE(first && second && third);
  EXPECT_TRUE(first->is_fixed);
  EXPECT_EQ(first->offset, 0);
  EXPECT_EQ(second->offset, 1);
  EXPECT_EQ(third->offset, 5);
  EXPECT_EQ(manifest.fixed_region_size(), 13);
  EXPECT_EQ(manifest.variable_count(), 0);
}

TEST(PropertyManifest, VariableValuesGetSequentialIndicesAndDoNotConsumeFixedSpace) {
  ManifestRegistry registry;
  auto const id = registry.Intern(std::vector{Entry(1, String()), Entry(2, Int(4)), Entry(3, String())});
  auto const &manifest = registry.Resolve(id);

  auto const first_string = manifest.Find(Prop(1));
  auto const the_int = manifest.Find(Prop(2));
  auto const second_string = manifest.Find(Prop(3));

  ASSERT_TRUE(first_string && the_int && second_string);
  EXPECT_FALSE(first_string->is_fixed);
  EXPECT_EQ(first_string->offset, 0);  // index into the variable region, not a byte offset
  EXPECT_FALSE(second_string->is_fixed);
  EXPECT_EQ(second_string->offset, 1);

  EXPECT_TRUE(the_int->is_fixed);
  EXPECT_EQ(the_int->offset, 0);  // the only fixed value, so it starts the fixed region

  EXPECT_EQ(manifest.fixed_region_size(), 4);
  EXPECT_EQ(manifest.variable_count(), 2);
}

TEST(PropertyManifest, FindReportsTheStoredType) {
  ManifestRegistry registry;
  auto const id = registry.Intern(std::vector{Entry(1, Int(2)), Entry(2, String())});
  auto const &manifest = registry.Resolve(id);

  EXPECT_EQ(manifest.Find(Prop(1))->stored_type, Int(2));
  EXPECT_EQ(manifest.Find(Prop(2))->stored_type, String());
}

TEST(PropertyManifest, FindReturnsNulloptForAbsentProperty) {
  ManifestRegistry registry;
  auto const id = registry.Intern(std::vector{Entry(2, Int(4))});
  auto const &manifest = registry.Resolve(id);

  EXPECT_FALSE(manifest.Find(Prop(1)).has_value());
  EXPECT_FALSE(manifest.Find(Prop(3)).has_value());
}

// A resolved manifest is held across many record reads, so interning must never move one.
TEST(PropertyManifest, ResolvedManifestSurvivesFurtherInterning) {
  ManifestRegistry registry;
  auto const id = registry.Intern(std::vector{Entry(1, Int(4))});
  auto const *manifest = &registry.Resolve(id);

  for (uint32_t i = 2; i < 1000; ++i) {
    registry.Intern(std::vector{Entry(i, Int(4))});
  }

  EXPECT_EQ(manifest, &registry.Resolve(id));
  ASSERT_TRUE(manifest->Find(Prop(1)).has_value());
  EXPECT_EQ(manifest->Find(Prop(1))->stored_type, Int(4));
}

TEST(PropertyManifest, EntriesAreCanonicalSoWriteOrderDoesNotMatter) {
  ManifestRegistry registry;

  auto const ascending = registry.Intern(std::vector{Entry(1, Int(4)), Entry(2, String()), Entry(3, Bool())});
  auto const descending = registry.Intern(std::vector{Entry(3, Bool()), Entry(2, String()), Entry(1, Int(4))});

  EXPECT_EQ(ascending, descending);
  EXPECT_EQ(registry.size(), 1);
}

TEST(PropertyManifest, ConcurrentInterningOfOneShapeYieldsOneManifest) {
  ManifestRegistry registry;
  constexpr auto kThreads = 8;
  constexpr auto kRounds = 500;

  std::barrier start{kThreads};
  std::vector<std::vector<memgraph::storage::ManifestId>> seen(kThreads);
  std::vector<std::jthread> threads;
  threads.reserve(kThreads);

  for (auto t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      start.arrive_and_wait();
      for (auto round = 0; round < kRounds; ++round) {
        seen[t].push_back(registry.Intern(std::vector{Entry(1, Int(4)), Entry(2, String())}));
      }
    });
  }
  threads.clear();  // join

  EXPECT_EQ(registry.size(), 1);
  auto const expected = seen[0][0];
  for (auto const &per_thread : seen) {
    EXPECT_TRUE(std::ranges::all_of(per_thread, [&](auto id) { return id == expected; }));
  }
}

TEST(PropertyManifest, ConcurrentInterningOfDistinctShapesKeepsThemDistinct) {
  ManifestRegistry registry;
  constexpr auto kThreads = 8;
  constexpr auto kShapesPerThread = 100;

  std::barrier start{kThreads};
  std::vector<std::jthread> threads;
  threads.reserve(kThreads);

  for (auto t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      start.arrive_and_wait();
      for (auto i = 0; i < kShapesPerThread; ++i) {
        registry.Intern(std::vector{Entry(t * kShapesPerThread + i + 1, Int(4))});
      }
    });
  }
  threads.clear();  // join

  EXPECT_EQ(registry.size(), kThreads * kShapesPerThread);
}
