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

// Cancellation of the full-scan schema operations (index population, constraint validation). The parallel variants
// matter most: a cancel check throwing inside a worker used to escape the thread function and terminate the process,
// so these tests pin that it surfaces as PopulateCancel on the calling thread instead.

#include "gtest/gtest.h"

#include <atomic>
#include <optional>
#include <set>

#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/constraints/existence_constraints.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/text_edge_index.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/edge_property_index.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/edge_type_property_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"
#include "storage/v2/inmemory/vertex_property_index.hpp"
#include "storage/v2/property_value.hpp"

using memgraph::storage::ActiveIndices;
using memgraph::storage::ActiveIndicesPtr;
using memgraph::storage::ActiveIndicesStore;
using memgraph::storage::ActiveIndicesUpdater;
using memgraph::storage::ExistenceConstraints;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryEdgePropertyIndex;
using memgraph::storage::InMemoryEdgeTypeIndex;
using memgraph::storage::InMemoryEdgeTypePropertyIndex;
using memgraph::storage::InMemoryLabelIndex;
using memgraph::storage::InMemoryLabelPropertyIndex;
using memgraph::storage::InMemoryUniqueConstraints;
using memgraph::storage::LabelId;
using memgraph::storage::PopulateCancel;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyValue;
using memgraph::storage::TypeConstraintKind;
using memgraph::storage::TypeConstraints;
using memgraph::storage::Vertex;
using memgraph::storage::durability::ParallelizedSchemaCreationInfo;

namespace {

constexpr uint32_t kVertexCount = 6;

// Two batches of three so the parallel paths actually spawn workers.
ParallelizedSchemaCreationInfo ParallelInfo() {
  return ParallelizedSchemaCreationInfo{
      .vertex_recovery_info = std::vector<std::pair<Gid, uint64_t>>{{Gid::FromUint(1), 3}, {Gid::FromUint(4), 3}},
      .thread_count = 2};
}

auto AlwaysCancel() -> memgraph::storage::CheckCancelFunction {
  return [] { return true; };
}

}  // namespace

class SchemaCancellationTest : public ::testing::Test {
 public:
  void SetUp() override {
    InitActiveIndicesStore();
    auto acc = vertices_.access();
    for (uint32_t i = 1; i <= kVertexCount; i++) {
      auto vertex = Vertex{Gid::FromUint(i), nullptr};
      vertex.labels.emplace_back(label_);
      // Distinct values so an uncancelled unique-constraint validation succeeds; that is what makes the retry in
      // UniqueConstraintSingleThreadedCancels a clean signal that the cancelled attempt deregistered itself.
      const std::vector<std::pair<PropertyId, PropertyValue>> prop_data{
          {prop_, PropertyValue{static_cast<int64_t>(i)}}};
      vertex.properties.InitProperties(prop_data);
      auto [_, inserted] = acc.insert(std::move(vertex));
      ASSERT_TRUE(inserted);
    }
  }

  void InitActiveIndicesStore() {
    active_indices_store_.WithLock([](ActiveIndicesPtr &ai) {
      ai = std::make_shared<ActiveIndices>(
          std::make_shared<InMemoryLabelIndex::ActiveIndices>(),
          std::make_shared<InMemoryLabelPropertyIndex::ActiveIndices>(),
          std::make_shared<InMemoryEdgeTypeIndex::ActiveIndices>(),
          std::make_shared<InMemoryEdgeTypePropertyIndex::ActiveIndices>(),
          std::make_shared<InMemoryEdgePropertyIndex::ActiveIndices>(),
          std::make_shared<memgraph::storage::InMemoryVertexPropertyIndex::ActiveIndices>(),
          std::make_shared<memgraph::storage::TextIndex::ActiveIndices>(),
          std::make_shared<memgraph::storage::TextEdgeIndex::ActiveIndices>(),
          std::make_shared<memgraph::storage::PointIndexStorage::ActiveIndices>(),
          std::make_shared<memgraph::storage::VectorIndex::ActiveIndices>(),
          std::make_shared<memgraph::storage::VectorEdgeIndex::ActiveIndices>());
    });
  }

  LabelId label_{LabelId::FromUint(1)};
  PropertyId prop_{PropertyId::FromUint(1)};
  memgraph::utils::SkipListDb<Vertex> vertices_;
  ActiveIndicesStore active_indices_store_;
};

TEST_F(SchemaCancellationTest, ExistenceConstraintSingleThreadedCancels) {
  EXPECT_THROW(
      {
        auto res = ExistenceConstraints::ValidateVerticesOnConstraint(
            vertices_.access(), label_, prop_, std::nullopt, {}, AlwaysCancel());
        (void)res;
      },
      PopulateCancel);
}

// Would have terminated the process before the per-worker catch was added.
TEST_F(SchemaCancellationTest, ExistenceConstraintParallelCancels) {
  auto par_info = ParallelInfo();
  EXPECT_THROW(
      {
        auto res = ExistenceConstraints::ValidateVerticesOnConstraint(
            vertices_.access(), label_, prop_, par_info, {}, AlwaysCancel());
        (void)res;
      },
      PopulateCancel);
}

TEST_F(SchemaCancellationTest, ExistenceConstraintCompletesWithoutCancellation) {
  auto par_info = ParallelInfo();
  auto res = ExistenceConstraints::ValidateVerticesOnConstraint(
      vertices_.access(), label_, prop_, par_info, {}, memgraph::storage::neverCancel);
  EXPECT_TRUE(res.has_value());
}

TEST_F(SchemaCancellationTest, UniqueConstraintSingleThreadedCancels) {
  InMemoryUniqueConstraints unique_constraints;
  auto vertices_acc = vertices_.access();
  EXPECT_THROW(
      {
        auto res = unique_constraints.CreateConstraint(
            label_, std::set<PropertyId>{prop_}, vertices_acc, std::nullopt, {}, AlwaysCancel());
        (void)res;
      },
      PopulateCancel);

  // The constraint is installed before validation runs, so cancelling has to take it back out -- otherwise a
  // half-validated constraint stays visible and the retry below would report ALREADY_EXISTS.
  auto retry = unique_constraints.CreateConstraint(
      label_, std::set<PropertyId>{prop_}, vertices_acc, std::nullopt, {}, memgraph::storage::neverCancel);
  ASSERT_TRUE(retry.has_value());
  EXPECT_EQ(retry.value(), InMemoryUniqueConstraints::CreationStatus::SUCCESS);
}

TEST_F(SchemaCancellationTest, UniqueConstraintParallelCancels) {
  InMemoryUniqueConstraints unique_constraints;
  auto vertices_acc = vertices_.access();
  auto par_info = ParallelInfo();
  EXPECT_THROW(
      {
        auto res = unique_constraints.CreateConstraint(
            label_, std::set<PropertyId>{prop_}, vertices_acc, par_info, {}, AlwaysCancel());
        (void)res;
      },
      PopulateCancel);
}

TEST_F(SchemaCancellationTest, TypeConstraintCancels) {
  EXPECT_THROW(
      {
        auto res = TypeConstraints::ValidateVerticesOnConstraint(
            vertices_.access(), label_, prop_, TypeConstraintKind::INTEGER, {}, AlwaysCancel());
        (void)res;
      },
      PopulateCancel);
}

TEST_F(SchemaCancellationTest, LabelIndexParallelPopulationCancels) {
  InMemoryLabelIndex label_idx;
  auto updater = ActiveIndicesUpdater{active_indices_store_};
  ASSERT_TRUE(label_idx.RegisterIndex(label_, updater));

  auto par_info = ParallelInfo();
  auto res = label_idx.PopulateIndex(label_, vertices_.access(), par_info, updater, {}, nullptr, AlwaysCancel());
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error(), memgraph::storage::IndexPopulateError::Cancellation);
}

TEST_F(SchemaCancellationTest, LabelIndexParallelPopulationCompletesWithoutCancellation) {
  InMemoryLabelIndex label_idx;
  auto updater = ActiveIndicesUpdater{active_indices_store_};
  ASSERT_TRUE(label_idx.RegisterIndex(label_, updater));

  auto par_info = ParallelInfo();
  auto res = label_idx.PopulateIndex(
      label_, vertices_.access(), par_info, updater, {}, nullptr, memgraph::storage::neverCancel);
  EXPECT_TRUE(res.has_value());
}

// Cancellation must not mask a real answer about the data: a violation the scan already found still wins.
TEST_F(SchemaCancellationTest, ViolationOutranksCancellation) {
  auto missing_prop = PropertyId::FromUint(99);
  std::atomic<bool> cancel_now{false};
  auto cancel_after_first = [&cancel_now]() { return cancel_now.exchange(true); };

  auto res = ExistenceConstraints::ValidateVerticesOnConstraint(
      vertices_.access(), label_, missing_prop, std::nullopt, {}, cancel_after_first);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(res.error().type, memgraph::storage::ConstraintViolation::Type::EXISTENCE);
}
