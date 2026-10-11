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

#include "storage/v2/delta_container.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/small_vector.hpp"

#include <optional>
#include <ranges>
#include <string_view>

using namespace memgraph::storage;

TEST(DeltaContainer, Empty) {
  auto container = delta_container{};
  EXPECT_TRUE(std::ranges::empty(container));
  EXPECT_EQ(container.size(), 0);
  EXPECT_EQ(std::distance(container.begin(), container.end()), 0);
}

TEST(DeltaContainer, Emplace) {
  auto container = delta_container{};
  container.emplace(Delta::DeleteObjectTag{}, (CommitInfo *)nullptr, 0);
  EXPECT_FALSE(std::ranges::empty(container));
  EXPECT_EQ(container.size(), 1);
  EXPECT_EQ(std::distance(container.begin(), container.end()), 1);
}

TEST(DeltaContainer, Move) {
  auto container = delta_container{};
  container.emplace(Delta::DeleteObjectTag{}, (CommitInfo *)nullptr, 0);
  auto container2 = std::move(container);

  EXPECT_TRUE(std::ranges::empty(container));
  EXPECT_EQ(container.size(), 0);
  EXPECT_EQ(std::distance(container.begin(), container.end()), 0);

  EXPECT_FALSE(std::ranges::empty(container2));
  EXPECT_EQ(container2.size(), 1);
  EXPECT_EQ(std::distance(container2.begin(), container2.end()), 1);
}

TEST(DeltaContainer, MoveWithPageSlabMemoryResource) {
  auto container = delta_container{};
  container.emplace(Delta::DeleteDeserializedObjectTag{}, 42U, std::optional<std::string_view>{"disk-key"});
  auto container2 = std::move(container);

  auto container3 = delta_container{};
  container3 = std::move(container2);

  EXPECT_TRUE(std::ranges::empty(container));
  EXPECT_EQ(container.size(), 0);
  EXPECT_TRUE(std::ranges::empty(container2));
  EXPECT_EQ(container2.size(), 0);

  EXPECT_FALSE(std::ranges::empty(container3));
  EXPECT_EQ(container3.size(), 1);
  EXPECT_EQ(std::distance(container3.begin(), container3.end()), 1);
}

namespace {
PropertyValue MakeSpilledVectorIndexValue() {
  // ids spill to the heap at >=2 entries and floats at >=3.
  return PropertyValue(
      PropertyValue::VectorIndexIdData{.ids = memgraph::utils::small_vector<uint64_t>{1, 2},
                                       .vector = memgraph::utils::small_vector<float>{1.0F, 2.0F, 3.0F}});
}

void EmplaceVectorIndexBeforeImage(delta_container &container) {
  container.emplace(
      Delta::SetPropertyTag{}, PropertyId::FromUint(1), MakeSpilledVectorIndexValue(), (CommitInfo *)nullptr, 0);
}
}  // namespace

// The leak assertion is LeakSanitizer (ASan CI): every release path must free the heap-spilled before-images.
TEST(DeltaContainer, VectorIndexBeforeImagesAreFreedOnEveryRelease) {
  {
    auto container = delta_container{};
    EmplaceVectorIndexBeforeImage(container);
    container.clear();
    EXPECT_EQ(container.size(), 0);
    EmplaceVectorIndexBeforeImage(container);
    EXPECT_EQ(container.size(), 1);
  }  // destructor

  auto container = delta_container{};
  EmplaceVectorIndexBeforeImage(container);
  auto moved = std::move(container);
  EXPECT_EQ(moved.size(), 1);
  EXPECT_EQ(container.size(), 0);

  auto assigned = delta_container{};
  EmplaceVectorIndexBeforeImage(assigned);
  assigned = std::move(moved);
  EXPECT_EQ(assigned.size(), 1);
  EXPECT_EQ(moved.size(), 1);  // swapped: the old `assigned` content, freed when `moved` dies
}
