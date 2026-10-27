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
#include <atomic>
#include <barrier>
#include <thread>
#include <vector>

#include "storage/v2/property_manifest.hpp"

using memgraph::storage::ManifestId;
using memgraph::storage::ManifestRegistry;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyStoreType;
using memgraph::storage::RecordShape;
using memgraph::storage::StoredType;

namespace {

PropertyId Prop(uint32_t id) { return PropertyId::FromUint(id); }

StoredType Int(uint8_t width) { return StoredType::Fixed(PropertyStoreType::INT, width); }

StoredType Double() { return StoredType::Fixed(PropertyStoreType::DOUBLE, 8); }

StoredType Bool() { return StoredType::Fixed(PropertyStoreType::BOOL, 1); }

StoredType String() { return StoredType::Variable(PropertyStoreType::STRING); }

RecordShape Shape(std::initializer_list<std::pair<uint32_t, StoredType>> entries) {
  RecordShape shape;
  for (auto const &[id, type] : entries) shape.emplace(Prop(id), type);
  return shape;
}

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
  auto const shape = Shape({{1, Int(4)}, {7, String()}});

  auto const first = registry.Intern(shape);
  auto const second = registry.Intern(shape);

  EXPECT_EQ(first, second);
  EXPECT_EQ(registry.size(), 1);
}

TEST(PropertyManifest, ShapesDifferingByPropertySetAreDistinct) {
  ManifestRegistry registry;

  auto const with_seven = registry.Intern(Shape({{1, Int(4)}, {7, String()}}));
  auto const with_eight = registry.Intern(Shape({{1, Int(4)}, {8, String()}}));
  auto const only_one = registry.Intern(Shape({{1, Int(4)}}));

  EXPECT_NE(with_seven, with_eight);
  EXPECT_NE(with_seven, only_one);
  EXPECT_EQ(registry.size(), 3);
}

TEST(PropertyManifest, ShapesDifferingOnlyByTypeAreDistinct) {
  ManifestRegistry registry;

  auto const as_int = registry.Intern(Shape({{1, Int(8)}}));
  auto const as_double = registry.Intern(Shape({{1, Double()}}));

  EXPECT_NE(as_int, as_double);
}

// Width class is part of the shape: keeping int compression means a value that grows past
// its width class lands in a different manifest.
TEST(PropertyManifest, ShapesDifferingOnlyByWidthClassAreDistinct) {
  ManifestRegistry registry;

  auto const narrow = registry.Intern(Shape({{1, Int(1)}}));
  auto const wide = registry.Intern(Shape({{1, Int(8)}}));

  EXPECT_NE(narrow, wide);
  EXPECT_EQ(registry.Resolve(narrow).fixed_region_size(), 1);
  EXPECT_EQ(registry.Resolve(wide).fixed_region_size(), 8);
}

TEST(PropertyManifest, FixedValuesGetPrefixSumOffsetsInPropertyOrder) {
  ManifestRegistry registry;
  auto const id = registry.Intern(Shape({{1, Bool()}, {2, Int(4)}, {3, Double()}}));
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
  auto const id = registry.Intern(Shape({{1, String()}, {2, Int(4)}, {3, String()}}));
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
  auto const id = registry.Intern(Shape({{1, Int(2)}, {2, String()}}));
  auto const &manifest = registry.Resolve(id);

  EXPECT_EQ(manifest.Find(Prop(1))->stored_type, Int(2));
  EXPECT_EQ(manifest.Find(Prop(2))->stored_type, String());
}

TEST(PropertyManifest, LocationAtAgreesWithFindForEveryEntry) {
  ManifestRegistry registry;
  auto const id = registry.Intern(Shape({{1, Int(2)}, {2, String()}, {3, Bool()}, {4, String()}, {5, Double()}}));
  auto const &manifest = registry.Resolve(id);

  auto const entries = manifest.entries();
  for (size_t position = 0; position != entries.size(); ++position) {
    auto const found = manifest.Find(entries[position].property);
    ASSERT_TRUE(found.has_value());
    auto const at = manifest.LocationAt(position);
    EXPECT_EQ(at.is_fixed, found->is_fixed);
    EXPECT_EQ(at.offset, found->offset);
    EXPECT_EQ(at.stored_type, found->stored_type);
  }
}

TEST(PropertyManifest, FindReturnsNulloptForAbsentProperty) {
  ManifestRegistry registry;
  auto const id = registry.Intern(Shape({{2, Int(4)}}));
  auto const &manifest = registry.Resolve(id);

  EXPECT_FALSE(manifest.Find(Prop(1)).has_value());
  EXPECT_FALSE(manifest.Find(Prop(3)).has_value());
}

// A resolved manifest is held across many record reads, so interning must never move one.
TEST(PropertyManifest, ResolvedManifestSurvivesFurtherInterning) {
  ManifestRegistry registry;
  auto const id = registry.Intern(Shape({{1, Int(4)}}));
  auto const *manifest = &registry.Resolve(id);

  for (uint32_t i = 2; i < 1000; ++i) {
    registry.Intern(Shape({{i, Int(4)}}));
  }

  EXPECT_EQ(manifest, &registry.Resolve(id));
  ASSERT_TRUE(manifest->Find(Prop(1)).has_value());
  EXPECT_EQ(manifest->Find(Prop(1))->stored_type, Int(4));
}

TEST(PropertyManifest, EntriesAreCanonicalSoWriteOrderDoesNotMatter) {
  ManifestRegistry registry;

  auto const ascending = registry.Intern(Shape({{1, Int(4)}, {2, String()}, {3, Bool()}}));
  auto const descending = registry.Intern(Shape({{3, Bool()}, {2, String()}, {1, Int(4)}}));

  EXPECT_EQ(ascending, descending);
  EXPECT_EQ(registry.size(), 1);
}

// Ids are handed out densely from zero and the registry keeps every manifest it issued, so
// each one resolves to the shape it was interned for. That is the property the debug-only
// bounds check in Resolve relies on rather than enforces.
TEST(PropertyManifest, EveryIdTheRegistryIssuedResolvesToItsOwnShape) {
  ManifestRegistry registry;
  constexpr auto kShapes = 32;

  auto ids = std::vector<ManifestId>{};
  for (auto i = 0; i != kShapes; ++i) {
    ids.push_back(registry.Intern(Shape({{static_cast<uint32_t>(i + 1), Int(4)}})));
  }

  ASSERT_EQ(registry.size(), kShapes);
  for (auto i = 0; i != kShapes; ++i) {
    EXPECT_LT(ids[i].value, registry.size()) << "an issued id has to be resolvable";
    auto const &manifest = registry.Resolve(ids[i]);
    ASSERT_EQ(manifest.size(), 1);
    EXPECT_EQ(manifest.entries()[0].property, Prop(i + 1));
  }
}

// One stored type per property is all a shape can hold, so the last type given for a
// property is the one the manifest carries and the shape stays one entry wide.
TEST(PropertyManifest, APropertyCanOnlyBeTypedOnceInAShape) {
  ManifestRegistry registry;

  auto shape = RecordShape{};
  shape.insert_or_assign(Prop(1), Int(4));
  shape.insert_or_assign(Prop(1), Int(8));

  auto const &manifest = registry.Resolve(registry.Intern(shape));
  ASSERT_EQ(manifest.size(), 1);
  EXPECT_EQ(manifest.entries()[0].stored_type, Int(8));
  EXPECT_EQ(manifest.fixed_region_size(), 8) << "space for one value, not two";
}

// A resolved manifest is read while other threads are still interning, which is what makes
// the registry's growth visible to a reader mid-read. Every id already issued has to keep
// resolving to its own shape throughout.
TEST(PropertyManifest, ResolvingWhileOtherThreadsInternKeepsEveryShape) {
  ManifestRegistry registry;
  constexpr auto kSeeded = 64;
  constexpr auto kReaders = 4;
  constexpr auto kWriters = 4;
  constexpr auto kRounds = 2000;

  auto seeded = std::vector<ManifestId>{};
  for (auto i = 0; i != kSeeded; ++i) {
    seeded.push_back(registry.Intern(Shape({{static_cast<uint32_t>(i + 1), Int(4)}})));
  }

  auto mismatches = std::atomic<int>{0};
  auto start = std::barrier{kReaders + kWriters};
  auto threads = std::vector<std::jthread>{};

  for (auto r = 0; r != kReaders; ++r) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (auto round = 0; round != kRounds; ++round) {
        for (auto i = 0; i != kSeeded; ++i) {
          auto const &manifest = registry.Resolve(seeded[i]);
          if (manifest.size() != 1 || manifest.entries()[0].property != Prop(i + 1)) {
            mismatches.fetch_add(1, std::memory_order_relaxed);
          }
        }
      }
    });
  }

  // Each writer interns shapes no other writer does, so the registry keeps growing under
  // the readers rather than settling after the first round.
  for (auto w = 0; w != kWriters; ++w) {
    threads.emplace_back([&, w] {
      start.arrive_and_wait();
      for (auto round = 0; round != kRounds; ++round) {
        auto const id = static_cast<uint32_t>(kSeeded + 1 + (w * kRounds) + round);
        registry.Intern(Shape({{id, Int(8)}}));
      }
    });
  }

  threads.clear();  // join

  EXPECT_EQ(mismatches.load(), 0);
  EXPECT_EQ(registry.size(), kSeeded + (kWriters * kRounds));
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
        seen[t].push_back(registry.Intern(Shape({{1, Int(4)}, {2, String()}})));
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
        registry.Intern(Shape({{t * kShapesPerThread + i + 1, Int(4)}}));
      }
    });
  }
  threads.clear();  // join

  EXPECT_EQ(registry.size(), kThreads * kShapesPerThread);
}
