// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <limits>
#include <variant>

#include <gmock/gmock.h>
#include <gtest/gtest-death-test.h>
#include <gtest/gtest.h>

#include "coordinator/hybrid_logical_clock.hpp"
#include "io/time.hpp"
#include "storage/v3/delta.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage_v3_test_utils.hpp"
#include "utils/exceptions.hpp"

using testing::UnorderedElementsAre;

namespace {

class AlreadyInsertedException : public std::exception {};

}  // namespace

namespace memgraph::storage::v3::tests {

class StorageV3 : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override { store.StoreMapping({{1, "label"}, {2, "property"}}); }

  void TearDown() override { CleanupHlc(last_hlc); }

  VertexAccessor CreateVertexAndValidate(Shard::Accessor &acc, const std::vector<LabelId> &labels,
                                         const PropertyValue &primary_key,
                                         const std::vector<std::pair<PropertyId, PropertyValue>> &properties) {
    auto vtx = acc.CreateVertexAndValidate(labels, {primary_key}, properties);
    EXPECT_TRUE(vtx.HasValue());
    return *vtx;
  }

  LabelId NameToLabelId(std::string_view label_name) { return store.NameToLabel(label_name); }

  PropertyId NameToPropertyId(std::string_view property_name) { return store.NameToProperty(property_name); }

  EdgeTypeId NameToEdgeTypeId(std::string_view edge_type_name) { return store.NameToEdgeType(edge_type_name); }

  coordinator::Hlc GetNextHlc() {
    ++last_hlc.logical_id;
    last_hlc.coordinator_wall_clock += wall_clock_increment;
    return last_hlc;
  }

  void CleanupHlc(const coordinator::Hlc hlc) {
    if (with_gc) {
      store.CollectGarbage(hlc.coordinator_wall_clock + reclamation_interval + one_time_unit);
    }
  }

  const bool with_gc = GetParam();
  static constexpr std::chrono::seconds wall_clock_increment{10};
  static constexpr std::chrono::seconds reclamation_interval{wall_clock_increment / 2};
  static constexpr io::Duration one_time_unit{1};
  const std::vector<PropertyValue> min_pk{PropertyValue{0}};
  const std::vector<PropertyValue> pk{PropertyValue{0}};
  const LabelId primary_label{LabelId::FromUint(1)};
  const PropertyId primary_property{PropertyId::FromUint(2)};
  std::vector<storage::v3::SchemaProperty> schema_property_vector = {
      storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}};
  Shard store{primary_label, min_pk, std::nullopt /*max_primary_key*/, schema_property_vector,
              Config{.gc = {.reclamation_interval = reclamation_interval}}};
  coordinator::Hlc last_hlc{0, io::Time{}};
};
INSTANTIATE_TEST_CASE_P(WithGc, StorageV3, ::testing::Values(true));
INSTANTIATE_TEST_CASE_P(WithoutGc, StorageV3, ::testing::Values(false));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, Commit) {
  const auto test_vertex_exists = [this](const coordinator::Hlc hlc) {
    auto acc = store.Access(hlc);
    ASSERT_TRUE(acc.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
  };
  const auto test_vertex_not_exists = [this](const coordinator::Hlc hlc) {
    auto acc = store.Access(GetNextHlc());
    ASSERT_FALSE(acc.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_FALSE(acc.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
  };

  const auto create_start_hlc = GetNextHlc();
  {
    auto acc = store.Access(create_start_hlc);
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    acc.Commit(GetNextHlc());
  }
  const auto after_create_hlc = GetNextHlc();

  ASSERT_NO_FATAL_FAILURE(test_vertex_exists(GetNextHlc()));
  CleanupHlc(create_start_hlc);
  ASSERT_NO_FATAL_FAILURE(test_vertex_exists(GetNextHlc()));

  const auto delete_start_hlc = GetNextHlc();
  {
    auto acc = store.Access(delete_start_hlc);
    auto vertex = acc.FindVertex(pk, View::NEW);
    ASSERT_TRUE(vertex);

    auto res = acc.DeleteVertex(&*vertex);
    ASSERT_FALSE(res.HasError());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);

    acc.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);

    acc.Commit(GetNextHlc());
  }

  ASSERT_NO_FATAL_FAILURE(test_vertex_not_exists(GetNextHlc()));
  // The delta about deleting the vertex is still there, it
  ASSERT_NO_FATAL_FAILURE(test_vertex_exists(after_create_hlc));
  CleanupHlc(delete_start_hlc);

  ASSERT_NO_FATAL_FAILURE(test_vertex_not_exists(GetNextHlc()));

  if (with_gc) {
    ASSERT_NO_FATAL_FAILURE(test_vertex_not_exists(create_start_hlc));
  } else {
    ASSERT_NO_FATAL_FAILURE(test_vertex_exists(after_create_hlc));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, Abort) {
  {
    auto acc = store.Access(GetNextHlc());
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    acc.Abort();
  }
  {
    auto acc = store.Access(GetNextHlc());
    ASSERT_FALSE(acc.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_FALSE(acc.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, AbortByGc) {
  if (!with_gc) {
    return;
  }
  {
    const auto hlc = GetNextHlc();
    auto acc = store.Access(hlc);
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    CleanupHlc(hlc);
  }
  {
    auto acc = store.Access(GetNextHlc());
    ASSERT_FALSE(acc.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_FALSE(acc.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, AdvanceCommandCommit) {
  std::vector<PropertyValue> pk1{PropertyValue{0}};
  std::vector<PropertyValue> pk2{PropertyValue(2)};

  {
    auto acc = store.Access(GetNextHlc());

    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc.FindVertex(pk1, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(pk1, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);

    acc.AdvanceCommand();

    CreateVertexAndValidate(acc, {}, PropertyValue{2}, {});
    ASSERT_FALSE(acc.FindVertex(pk2, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(pk2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 2U);

    ASSERT_TRUE(acc.FindVertex(pk1, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(pk1, View::NEW).has_value());

    acc.Commit(GetNextHlc());
  }
  CleanupHlc(last_hlc);
  {
    auto acc = store.Access(GetNextHlc());
    ASSERT_TRUE(acc.FindVertex(pk1, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(pk1, View::NEW).has_value());
    ASSERT_TRUE(acc.FindVertex(pk2, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(pk2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 2U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 2U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, AdvanceCommandAbort) {
  std::vector<PropertyValue> pk1{PropertyValue{0}};
  std::vector<PropertyValue> pk2{PropertyValue(2)};
  {
    auto acc = store.Access(GetNextHlc());

    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc.FindVertex(pk1, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(pk1, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);

    acc.AdvanceCommand();

    CreateVertexAndValidate(acc, {}, PropertyValue{2}, {});
    ASSERT_FALSE(acc.FindVertex(pk2, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(pk2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 2U);

    ASSERT_TRUE(acc.FindVertex(pk1, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(pk1, View::NEW).has_value());

    acc.Abort();
  }
  const auto check_vertex_not_exists = [this, &pk1, &pk2]() {
    auto acc = store.Access(GetNextHlc());
    ASSERT_FALSE(acc.FindVertex(pk1, View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(pk1, View::NEW).has_value());
    ASSERT_FALSE(acc.FindVertex(pk2, View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(pk2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  };
  ASSERT_NO_FATAL_FAILURE(check_vertex_not_exists());
  CleanupHlc(last_hlc);
  ASSERT_NO_FATAL_FAILURE(check_vertex_not_exists());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, SnapshotIsolation) {
  const auto start_hlc1 = GetNextHlc();
  auto acc1 = store.Access(start_hlc1);
  auto acc2 = store.Access(GetNextHlc());

  CreateVertexAndValidate(acc1, {}, PropertyValue{0}, {});

  ASSERT_FALSE(acc2.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  EXPECT_EQ(CountVertices(acc2, View::OLD), 0U);
  ASSERT_FALSE(acc2.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 1U);
  EXPECT_EQ(CountVertices(acc2, View::NEW), 0U);

  acc1.Commit(GetNextHlc());

  ASSERT_FALSE(acc2.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc2, View::OLD), 0U);
  ASSERT_FALSE(acc2.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc2, View::NEW), 0U);

  acc2.Abort();

  auto acc3 = store.Access(GetNextHlc());
  ASSERT_TRUE(acc3.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  CleanupHlc(start_hlc1);
  ASSERT_TRUE(acc3.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);
  acc3.Abort();
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, AccessorMove) {
  {
    auto acc = store.Access(GetNextHlc());
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});

    ASSERT_FALSE(acc.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);

    Shard::Accessor moved(std::move(acc));

    ASSERT_FALSE(moved.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(moved, View::OLD), 0U);
    ASSERT_TRUE(moved.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(moved, View::NEW), 1U);

    moved.Commit(GetNextHlc());
  }
  {
    auto acc = store.Access(GetNextHlc());
    ASSERT_TRUE(acc.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 1U);
    ASSERT_TRUE(acc.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexDeleteCommit) {
  auto acc1 = store.Access(GetNextHlc());  // read transaction
  auto acc2 = store.Access(GetNextHlc());  // write transaction

  // Create the vertex in transaction 2
  {
    CreateVertexAndValidate(acc2, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc2.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc2, View::OLD), 0U);
    ASSERT_TRUE(acc2.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
    acc2.Commit(GetNextHlc());
  }

  auto acc3 = store.Access(GetNextHlc());  // read transaction
  auto acc4 = store.Access(GetNextHlc());  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Delete the vertex in transaction 4
  {
    auto vertex = acc4.FindVertex(pk, View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc4, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 1U);

    auto res = acc4.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(acc4, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 0U);

    acc4.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc4, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 0U);

    acc4.Commit(GetNextHlc());
  }

  auto acc5 = store.Access(GetNextHlc());  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_FALSE(acc5.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc5, View::OLD), 0U);
  ASSERT_FALSE(acc5.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc5, View::NEW), 0U);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexDeleteAbort) {
  auto acc1 = store.Access(GetNextHlc());  // read transaction
  auto acc2 = store.Access(GetNextHlc());  // write transaction

  // Create the vertex in transaction 2
  {
    CreateVertexAndValidate(acc2, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc2.FindVertex(pk, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc2, View::OLD), 0U);
    ASSERT_TRUE(acc2.FindVertex(pk, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
    acc2.Commit(GetNextHlc());
  }

  auto acc3 = store.Access(GetNextHlc());  // read transaction
  auto acc4 = store.Access(GetNextHlc());  // write transaction (aborted)

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Delete the vertex in transaction 4, but abort the transaction
  {
    auto vertex = acc4.FindVertex(pk, View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc4, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 1U);

    auto res = acc4.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(acc4, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 0U);

    acc4.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc4, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc4, View::NEW), 0U);

    acc4.Abort();
  }

  auto acc5 = store.Access(GetNextHlc());  // read transaction
  auto acc6 = store.Access(GetNextHlc());  // write transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc5, View::OLD), 1U);
  ASSERT_TRUE(acc5.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc5, View::NEW), 1U);

  // Delete the vertex in transaction 6
  {
    auto vertex = acc6.FindVertex(pk, View::NEW);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc6, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc6, View::NEW), 1U);

    auto res = acc6.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasValue());
    EXPECT_EQ(CountVertices(acc6, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc6, View::NEW), 0U);

    acc6.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc6, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc6, View::NEW), 0U);

    acc6.Commit(GetNextHlc());
  }

  auto acc7 = store.Access(GetNextHlc());  // read transaction

  // Check whether the vertex exists in transaction 1
  ASSERT_FALSE(acc1.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
  ASSERT_FALSE(acc1.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);

  // Check whether the vertex exists in transaction 3
  ASSERT_TRUE(acc3.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc3, View::OLD), 1U);
  ASSERT_TRUE(acc3.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc3, View::NEW), 1U);

  // Check whether the vertex exists in transaction 5
  ASSERT_TRUE(acc5.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc5, View::OLD), 1U);
  ASSERT_TRUE(acc5.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc5, View::NEW), 1U);

  // Check whether the vertex exists in transaction 7
  ASSERT_FALSE(acc7.FindVertex(pk, View::OLD).has_value());
  EXPECT_EQ(CountVertices(acc7, View::OLD), 0U);
  ASSERT_FALSE(acc7.FindVertex(pk, View::NEW).has_value());
  EXPECT_EQ(CountVertices(acc7, View::NEW), 0U);

  // Commit all accessors
  acc1.Commit(GetNextHlc());
  acc3.Commit(GetNextHlc());
  acc5.Commit(GetNextHlc());
  acc7.Commit(GetNextHlc());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexDeleteSerializationError) {
  // Create vertex
  {
    auto acc = store.Access(GetNextHlc());
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    acc.Commit(GetNextHlc());
  }

  auto acc1 = store.Access(GetNextHlc());
  auto acc2 = store.Access(GetNextHlc());

  // Delete vertex in accessor 1
  {
    auto vertex = acc1.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc1, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc1, View::NEW), 1U);

    {
      auto res = acc1.DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
      EXPECT_EQ(CountVertices(acc1, View::OLD), 1U);
      EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);
    }

    {
      auto res = acc1.DeleteVertex(&*vertex);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
      EXPECT_EQ(CountVertices(acc1, View::OLD), 1U);
      EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);
    }

    acc1.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc1, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc1, View::NEW), 0U);
  }

  // Delete vertex in accessor 2
  {
    auto vertex = acc2.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);
    EXPECT_EQ(CountVertices(acc2, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
    auto res = acc2.DeleteVertex(&*vertex);
    ASSERT_TRUE(res.HasError());
    ASSERT_EQ(res.GetError(), Error::SERIALIZATION_ERROR);
    EXPECT_EQ(CountVertices(acc2, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
    acc2.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc2, View::OLD), 1U);
    EXPECT_EQ(CountVertices(acc2, View::NEW), 1U);
  }

  // Finalize both accessors
  acc1.Commit(GetNextHlc());
  acc2.Abort();
  CleanupHlc(last_hlc);

  // Check whether the vertex exists
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_FALSE(vertex);
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Commit(GetNextHlc());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexDeleteSpecialCases) {
  std::vector<PropertyValue> pk1{PropertyValue{0}};
  std::vector<PropertyValue> pk2{PropertyValue(2)};

  // Create vertex and delete it in the same transaction, but abort the
  // transaction
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc.FindVertex(pk1, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(pk1, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    auto res = acc.DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  }

  // Create vertex and delete it in the same transaction
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = CreateVertexAndValidate(acc, {}, PropertyValue{2}, {});
    ASSERT_FALSE(acc.FindVertex(pk2, View::OLD).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    ASSERT_TRUE(acc.FindVertex(pk2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::NEW), 1U);
    auto res = acc.DeleteVertex(&vertex);
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.AdvanceCommand();
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Commit(GetNextHlc());
  }

  // Check whether the vertices exist
  {
    auto acc = store.Access(GetNextHlc());
    ASSERT_FALSE(acc.FindVertex(pk1, View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(pk1, View::NEW).has_value());
    ASSERT_FALSE(acc.FindVertex(pk2, View::OLD).has_value());
    ASSERT_FALSE(acc.FindVertex(pk2, View::NEW).has_value());
    EXPECT_EQ(CountVertices(acc, View::OLD), 0U);
    EXPECT_EQ(CountVertices(acc, View::NEW), 0U);
    acc.Abort();
  }
}

template <typename TError, typename TResultHolder>
void AssertErrorInVariant(TResultHolder &holder, TError error_type) {
  ASSERT_TRUE(holder.HasError());
  const auto error = holder.GetError();
  ASSERT_TRUE(std::holds_alternative<TError>(error));
  ASSERT_EQ(std::get<TError>(error), error_type);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexDeleteLabel) {
  // Create the vertex
  {
    auto acc = store.Access(GetNextHlc());
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc.FindVertex(pk, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(pk, View::NEW).has_value());
    acc.Commit(GetNextHlc());
  }

  // Add label, delete the vertex and check the label API (same command)
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::NEW);
    ASSERT_TRUE(vertex);

    auto label5 = NameToLabelId("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label5, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label5, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabelAndValidate(label5).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label5, View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label5, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label5);
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label5, View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(label5, View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabelAndValidate(label5);
      AssertErrorInVariant(ret, Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabelAndValidate(label5);
      AssertErrorInVariant(ret, Error::DELETED_OBJECT);
    }

    acc.Abort();
  }

  // Add label, delete the vertex and check the label API (different command)
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::NEW);
    ASSERT_TRUE(vertex);

    auto label5 = NameToLabelId("label5");

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label5, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label5, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    // Add label 5
    ASSERT_TRUE(vertex->AddLabelAndValidate(label5).GetValue());

    // Check whether label 5 exists
    ASSERT_FALSE(vertex->HasLabel(label5, View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label5, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label5);
    }

    // Advance command
    acc.AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label5, View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label5, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label5);
    }
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label5);
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->HasLabel(label5, View::OLD).GetValue());
    ASSERT_EQ(vertex->HasLabel(label5, View::NEW).GetError(), Error::DELETED_OBJECT);
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label5);
    }
    ASSERT_EQ(vertex->Labels(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Advance command
    acc.AdvanceCommand();

    // Check whether label 5 exists
    ASSERT_EQ(vertex->HasLabel(label5, View::OLD).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->HasLabel(label5, View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(View::OLD).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Labels(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Try to add the label
    {
      auto ret = vertex->AddLabelAndValidate(label5);
      AssertErrorInVariant(ret, Error::DELETED_OBJECT);
    }

    // Try to remove the label
    {
      auto ret = vertex->RemoveLabelAndValidate(label5);
      AssertErrorInVariant(ret, Error::DELETED_OBJECT);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp - special - member - functions)
TEST_P(StorageV3, VertexDeleteProperty) {
  // Create the vertex
  {
    auto acc = store.Access(GetNextHlc());
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    ASSERT_FALSE(acc.FindVertex(pk, View::OLD).has_value());
    ASSERT_TRUE(acc.FindVertex(pk, View::NEW).has_value());
    acc.Commit(GetNextHlc());
  }

  // Set property, delete the vertex and check the property API (same command)
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::NEW);
    ASSERT_TRUE(vertex);

    auto property5 = NameToPropertyId("property5");

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property5, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property5, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_TRUE(vertex->SetPropertyAndValidate(property5, PropertyValue("nandare"))->IsNull());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property5, View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property5, View::NEW)->ValueString(), "nandare");
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property5].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether label 5 exists
    ASSERT_TRUE(vertex->GetProperty(property5, View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property5, View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Try to set the property5
    {
      auto ret = vertex->SetPropertyAndValidate(property5, PropertyValue("haihai"));
      AssertErrorInVariant(ret, Error::DELETED_OBJECT);
    }

    acc.Abort();
  }

  // Set property, delete the vertex and check the property API (different
  // command)
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::NEW);
    ASSERT_TRUE(vertex);

    auto property5 = NameToPropertyId("property5");

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property5, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property5, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    // Set property 5 to "nandare"
    ASSERT_TRUE(vertex->SetPropertyAndValidate(property5, PropertyValue("nandare"))->IsNull());

    // Check whether property 5 exists
    ASSERT_TRUE(vertex->GetProperty(property5, View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property5, View::NEW)->ValueString(), "nandare");
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property5].ValueString(), "nandare");
    }

    // Advance command
    acc.AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property5, View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property5, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property5].ValueString(), "nandare");
    }
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property5].ValueString(), "nandare");
    }

    // Delete the vertex
    ASSERT_TRUE(acc.DeleteVertex(&*vertex).GetValue());

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property5, View::OLD)->ValueString(), "nandare");
    ASSERT_EQ(vertex->GetProperty(property5, View::NEW).GetError(), Error::DELETED_OBJECT);
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property5].ValueString(), "nandare");
    }
    ASSERT_EQ(vertex->Properties(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Advance command
    acc.AdvanceCommand();

    // Check whether property 5 exists
    ASSERT_EQ(vertex->GetProperty(property5, View::OLD).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->GetProperty(property5, View::NEW).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(View::OLD).GetError(), Error::DELETED_OBJECT);
    ASSERT_EQ(vertex->Properties(View::NEW).GetError(), Error::DELETED_OBJECT);

    // Try to set the property
    {
      auto ret = vertex->SetPropertyAndValidate(property5, PropertyValue("haihai"));
      AssertErrorInVariant(ret, Error::DELETED_OBJECT);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexLabelCommit) {
  store.StoreMapping({{1, "label"}, {2, "property"}, {3, "label5"}, {4, "other"}});
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});

    auto label = NameToLabelId("label5");

    ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);

    {
      auto res = vertex.AddLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex.Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex.AddLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Commit(GetNextHlc());
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = NameToLabelId("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    {
      auto res = vertex->RemoveLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Commit(GetNextHlc());
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    auto other_label = NameToLabelId("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexLabelAbort) {
  store.StoreMapping({{1, "label"}, {2, "property"}, {3, "label5"}, {4, "other"}});
  // Create the vertex.
  {
    auto acc = store.Access(GetNextHlc());
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    acc.Commit(GetNextHlc());
  }

  // Add label 5, but abort the transaction.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex->AddLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Abort();
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    auto other_label = NameToLabelId("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }

  // Add label 5.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    {
      auto res = vertex->AddLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Commit(GetNextHlc());
  }

  // Check that label 5 exists.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = NameToLabelId("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }

  // Remove label 5, but abort the transaction.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    {
      auto res = vertex->RemoveLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Abort();
  }

  // Check that label 5 exists.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    auto other_label = NameToLabelId("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }

  // Remove label 5.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    {
      auto res = vertex->RemoveLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_TRUE(vertex->HasLabel(label, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label);
    }

    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->RemoveLabelAndValidate(label);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }

    acc.Commit(GetNextHlc());
  }

  // Check that label 5 doesn't exist.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label = NameToLabelId("label5");

    ASSERT_FALSE(vertex->HasLabel(label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    auto other_label = NameToLabelId("other");

    ASSERT_FALSE(vertex->HasLabel(other_label, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(other_label, View::NEW).GetValue());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexLabelSerializationError) {
  store.StoreMapping({{1, "label"}, {2, "property"}, {3, "label1"}, {4, "label2"}});
  {
    auto acc = store.Access(GetNextHlc());
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    acc.Commit(GetNextHlc());
  }

  auto acc1 = store.Access(GetNextHlc());
  auto acc2 = store.Access(GetNextHlc());

  // Add label 1 in accessor 1.
  {
    auto vertex = acc1.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = NameToLabelId("label1");
    auto label2 = NameToLabelId("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label1, View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabelAndValidate(label1);
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    ASSERT_FALSE(vertex->HasLabel(label1, View::OLD).GetValue());
    ASSERT_TRUE(vertex->HasLabel(label1, View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    {
      auto res = vertex->AddLabelAndValidate(label1);
      ASSERT_TRUE(res.HasValue());
      ASSERT_FALSE(res.GetValue());
    }
  }

  // Add label 2 in accessor 2.
  {
    auto vertex = acc2.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = NameToLabelId("label1");
    auto label2 = NameToLabelId("label2");

    ASSERT_FALSE(vertex->HasLabel(label1, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label1, View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::NEW).GetValue());
    ASSERT_EQ(vertex->Labels(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Labels(View::NEW)->size(), 0);

    {
      auto res = vertex->AddLabelAndValidate(label1);
      AssertErrorInVariant(res, Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  acc1.Commit(GetNextHlc());
  acc2.Abort();

  // Check which labels exist.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto label1 = NameToLabelId("label1");
    auto label2 = NameToLabelId("label2");

    ASSERT_TRUE(vertex->HasLabel(label1, View::OLD).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::OLD).GetValue());
    {
      auto labels = vertex->Labels(View::OLD).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    ASSERT_TRUE(vertex->HasLabel(label1, View::NEW).GetValue());
    ASSERT_FALSE(vertex->HasLabel(label2, View::NEW).GetValue());
    {
      auto labels = vertex->Labels(View::NEW).GetValue();
      ASSERT_EQ(labels.size(), 1);
      ASSERT_EQ(labels[0], label1);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexPropertyCommit) {
  store.StoreMapping({{1, "label"}, {2, "property"}, {3, "property5"}, {4, "other"}});
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});

    auto property = NameToPropertyId("property5");

    ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex.SetPropertyAndValidate(property, PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex.Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex.SetPropertyAndValidate(property, PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex.Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    acc.Commit(GetNextHlc());
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = NameToPropertyId("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    {
      auto old_value = vertex->SetPropertyAndValidate(property, PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetPropertyAndValidate(property, PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    acc.Commit(GetNextHlc());
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    auto other_property = NameToPropertyId("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexPropertyAbort) {
  store.StoreMapping({{1, "label"}, {2, "property"}, {3, "property5"}, {4, "other"}});
  // Create the vertex.
  {
    auto acc = store.Access(GetNextHlc());
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    acc.Commit(GetNextHlc());
  }

  // Set property 5 to "nandare", but abort the transaction.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetPropertyAndValidate(property, PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetPropertyAndValidate(property, PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    acc.Abort();
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    auto other_property = NameToPropertyId("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to "nandare".
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetPropertyAndValidate(property, PropertyValue("temporary"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "temporary");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "temporary");
    }

    {
      auto old_value = vertex->SetPropertyAndValidate(property, PropertyValue("nandare"));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    acc.Commit(GetNextHlc());
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = NameToPropertyId("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null, but abort the transaction.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetPropertyAndValidate(property, PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    acc.Abort();
  }

  // Check that property 5 is "nandare".
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    auto other_property = NameToPropertyId("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }

  // Set property 5 to null.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_EQ(vertex->GetProperty(property, View::NEW)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    {
      auto old_value = vertex->SetPropertyAndValidate(property, PropertyValue());
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_FALSE(old_value->IsNull());
    }

    ASSERT_EQ(vertex->GetProperty(property, View::OLD)->ValueString(), "nandare");
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property].ValueString(), "nandare");
    }

    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    acc.Commit(GetNextHlc());
  }

  // Check that property 5 is null.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property = NameToPropertyId("property5");

    ASSERT_TRUE(vertex->GetProperty(property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    auto other_property = NameToPropertyId("other");

    ASSERT_TRUE(vertex->GetProperty(other_property, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(other_property, View::NEW)->IsNull());

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexPropertySerializationError) {
  store.StoreMapping({{1, "label"}, {2, "property"}, {3, "property1"}, {4, "property2"}});
  {
    auto acc = store.Access(GetNextHlc());
    CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    acc.Commit(GetNextHlc());
  }

  auto acc1 = store.Access(GetNextHlc());
  auto acc2 = store.Access(GetNextHlc());

  // Set property 1 to 123 in accessor 1.
  {
    auto vertex = acc1.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = NameToPropertyId("property1");
    auto property2 = NameToPropertyId("property2");

    ASSERT_TRUE(vertex->GetProperty(property1, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto old_value = vertex->SetPropertyAndValidate(property1, PropertyValue(123));
      ASSERT_TRUE(old_value.HasValue());
      ASSERT_TRUE(old_value->IsNull());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::OLD)->IsNull());
    ASSERT_EQ(vertex->GetProperty(property1, View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }
  }

  // Set property 2 to "nandare" in accessor 2.
  {
    auto vertex = acc2.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = NameToPropertyId("property1");
    auto property2 = NameToPropertyId("property2");

    ASSERT_TRUE(vertex->GetProperty(property1, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::OLD)->size(), 0);
    ASSERT_EQ(vertex->Properties(View::NEW)->size(), 0);

    {
      auto res = vertex->SetPropertyAndValidate(property2, PropertyValue("nandare"));
      AssertErrorInVariant(res, Error::SERIALIZATION_ERROR);
    }
  }

  // Finalize both accessors.
  acc1.Commit(GetNextHlc());
  acc2.Abort();

  // Check which properties exist.
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto property1 = NameToPropertyId("property1");
    auto property2 = NameToPropertyId("property2");

    ASSERT_EQ(vertex->GetProperty(property1, View::OLD)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    {
      auto properties = vertex->Properties(View::OLD).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    ASSERT_EQ(vertex->GetProperty(property1, View::NEW)->ValueInt(), 123);
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    {
      auto properties = vertex->Properties(View::NEW).GetValue();
      ASSERT_EQ(properties.size(), 1);
      ASSERT_EQ(properties[property1].ValueInt(), 123);
    }

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, VertexLabelPropertyMixed) {
  store.StoreMapping({{1, "label"}, {2, "property"}, {3, "label5"}, {4, "property5"}});
  auto acc = store.Access(GetNextHlc());
  auto vertex = CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});

  auto label = NameToLabelId("label5");
  auto property = NameToPropertyId("property5");

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  // Add label 5
  ASSERT_TRUE(vertex.AddLabelAndValidate(label).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, View::OLD)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  // Set property 5 to "nandare"
  ASSERT_TRUE(vertex.SetPropertyAndValidate(property, PropertyValue("nandare"))->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_TRUE(vertex.GetProperty(property, View::OLD)->IsNull());
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "nandare");
  ASSERT_EQ(vertex.Properties(View::OLD)->size(), 0);
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "nandare");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }

  // Set property 5 to "haihai"
  ASSERT_FALSE(vertex.SetPropertyAndValidate(property, PropertyValue("haihai"))->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "nandare");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "nandare");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_TRUE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  {
    auto labels = vertex.Labels(View::NEW).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Remove label 5
  ASSERT_TRUE(vertex.RemoveLabelAndValidate(label).GetValue());

  // Check whether label 5 and property 5 exist
  ASSERT_TRUE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  {
    auto labels = vertex.Labels(View::OLD).GetValue();
    ASSERT_EQ(labels.size(), 1);
    ASSERT_EQ(labels[0], label);
  }
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "haihai");
  ASSERT_EQ(vertex.GetProperty(property, View::NEW)->ValueString(), "haihai");
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  {
    auto properties = vertex.Properties(View::NEW).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }

  // Set property 5 to null
  ASSERT_FALSE(vertex.SetPropertyAndValidate(property, PropertyValue())->IsNull());

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_EQ(vertex.GetProperty(property, View::OLD)->ValueString(), "haihai");
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  {
    auto properties = vertex.Properties(View::OLD).GetValue();
    ASSERT_EQ(properties.size(), 1);
    ASSERT_EQ(properties[property].ValueString(), "haihai");
  }
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  // Advance command
  acc.AdvanceCommand();

  // Check whether label 5 and property 5 exist
  ASSERT_FALSE(vertex.HasLabel(label, View::OLD).GetValue());
  ASSERT_FALSE(vertex.HasLabel(label, View::NEW).GetValue());
  ASSERT_EQ(vertex.Labels(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_TRUE(vertex.GetProperty(property, View::NEW)->IsNull());
  ASSERT_EQ(vertex.Properties(View::OLD)->size(), 0);
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);

  acc.Commit(GetNextHlc());
}

TEST_P(StorageV3, VertexPropertyClear) {
  store.StoreMapping({{1, "label"}, {2, "property"}, {3, "property1"}, {4, "property2"}});
  auto property1 = NameToPropertyId("property1");
  auto property2 = NameToPropertyId("property2");
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});

    auto old_value = vertex.SetPropertyAndValidate(property1, PropertyValue("value"));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    acc.Commit(GetNextHlc());
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, View::OLD)->ValueString(), "value");
    ASSERT_TRUE(vertex->GetProperty(property2, View::OLD)->IsNull());
    ASSERT_THAT(vertex->Properties(View::OLD).GetValue(),
                UnorderedElementsAre(std::pair(property1, PropertyValue("value"))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    acc.Abort();
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    auto old_value = vertex->SetPropertyAndValidate(property2, PropertyValue(42));
    ASSERT_TRUE(old_value.HasValue());
    ASSERT_TRUE(old_value->IsNull());

    acc.Commit(GetNextHlc());
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_EQ(vertex->GetProperty(property1, View::OLD)->ValueString(), "value");
    ASSERT_EQ(vertex->GetProperty(property2, View::OLD)->ValueInt(), 42);
    ASSERT_THAT(
        vertex->Properties(View::OLD).GetValue(),
        UnorderedElementsAre(std::pair(property1, PropertyValue("value")), std::pair(property2, PropertyValue(42))));

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_FALSE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    {
      auto old_values = vertex->ClearProperties();
      ASSERT_TRUE(old_values.HasValue());
      ASSERT_TRUE(old_values->empty());
    }

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    acc.Commit(GetNextHlc());
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(vertex->GetProperty(property1, View::NEW)->IsNull());
    ASSERT_TRUE(vertex->GetProperty(property2, View::NEW)->IsNull());
    ASSERT_EQ(vertex->Properties(View::NEW).GetValue().size(), 0);

    acc.Abort();
  }
}

TEST_P(StorageV3, VertexNonexistentLabelPropertyEdgeAPI) {
  store.StoreMapping({{1, "label"}, {2, "property"}, {3, "label1"}, {4, "property1"}, {5, "edge"}});

  auto label1 = NameToLabelId("label1");
  auto property1 = NameToPropertyId("property1");

  auto acc = store.Access(GetNextHlc());
  auto vertex = CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});

  // Check state before (OLD view).
  ASSERT_EQ(vertex.Labels(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label1, View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property1, View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);

  // Check state before (NEW view).
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.HasLabel(label1, View::NEW), false);
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.GetProperty(property1, View::NEW), PropertyValue());
  ASSERT_EQ(vertex.InEdges(View::NEW)->size(), 0);
  ASSERT_EQ(vertex.OutEdges(View::NEW)->size(), 0);
  ASSERT_EQ(*vertex.InDegree(View::NEW), 0);
  ASSERT_EQ(*vertex.OutDegree(View::NEW), 0);

  // Modify vertex.
  ASSERT_TRUE(vertex.AddLabelAndValidate(label1).HasValue());
  ASSERT_TRUE(vertex.SetPropertyAndValidate(property1, PropertyValue("value")).HasValue());
  ASSERT_TRUE(acc.CreateEdge(vertex.Id(View::NEW).GetValue(), vertex.Id(View::NEW).GetValue(), NameToEdgeTypeId("edge"),
                             Gid::FromUint(1))
                  .HasValue());

  // Check state after (OLD view).
  ASSERT_EQ(vertex.Labels(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.HasLabel(label1, View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.Properties(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.GetProperty(property1, View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InEdges(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutEdges(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.InDegree(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);
  ASSERT_EQ(vertex.OutDegree(View::OLD).GetError(), Error::NONEXISTENT_OBJECT);

  // Check state after (NEW view).
  ASSERT_EQ(vertex.Labels(View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.HasLabel(label1, View::NEW), true);
  ASSERT_EQ(vertex.Properties(View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.GetProperty(property1, View::NEW), PropertyValue("value"));
  ASSERT_EQ(vertex.InEdges(View::NEW)->size(), 1);
  ASSERT_EQ(vertex.OutEdges(View::NEW)->size(), 1);
  ASSERT_EQ(*vertex.InDegree(View::NEW), 1);
  ASSERT_EQ(*vertex.OutDegree(View::NEW), 1);

  acc.Commit(GetNextHlc());
}

TEST_P(StorageV3, VertexVisibilitySingleTransaction) {
  auto acc1 = store.Access(GetNextHlc());
  auto acc2 = store.Access(GetNextHlc());

  auto vertex = CreateVertexAndValidate(acc1, {}, PropertyValue{0}, {});

  EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
  EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(pk, View::NEW));

  ASSERT_TRUE(vertex.AddLabelAndValidate(NameToLabelId("label1")).HasValue());

  EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
  EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(pk, View::NEW));

  ASSERT_TRUE(vertex.SetPropertyAndValidate(NameToPropertyId("meaning"), PropertyValue(42)).HasValue());

  auto acc3 = store.Access(GetNextHlc());

  EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
  EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(pk, View::NEW));
  EXPECT_FALSE(acc3.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc3.FindVertex(pk, View::NEW));

  ASSERT_TRUE(acc1.DeleteVertex(&vertex).HasValue());

  EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(pk, View::NEW));
  EXPECT_FALSE(acc3.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc3.FindVertex(pk, View::NEW));

  acc1.AdvanceCommand();
  acc3.AdvanceCommand();

  EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
  EXPECT_FALSE(acc2.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc2.FindVertex(pk, View::NEW));
  EXPECT_FALSE(acc3.FindVertex(pk, View::OLD));
  EXPECT_FALSE(acc3.FindVertex(pk, View::NEW));

  acc1.Abort();
  acc2.Abort();
  acc3.Abort();
}

TEST_P(StorageV3, VertexVisibilityMultipleTransactions) {
  {
    auto acc1 = store.Access(GetNextHlc());
    auto acc2 = store.Access(GetNextHlc());

    CreateVertexAndValidate(acc1, {}, PropertyValue{0}, {});

    EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_FALSE(acc2.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc2.FindVertex(pk, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_FALSE(acc2.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc2.FindVertex(pk, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_FALSE(acc2.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc2.FindVertex(pk, View::NEW));

    acc1.Commit(GetNextHlc());
    acc2.Commit(GetNextHlc());
  }

  {
    auto acc1 = store.Access(GetNextHlc());
    auto acc2 = store.Access(GetNextHlc());

    auto vertex = acc1.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));

    ASSERT_TRUE(vertex->AddLabelAndValidate(NameToLabelId("label1")).HasValue());

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));

    ASSERT_TRUE(vertex->SetPropertyAndValidate(NameToPropertyId("meaning"), PropertyValue(42)).HasValue());

    auto acc3 = store.Access(GetNextHlc());

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc3.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc1.Commit(GetNextHlc());
    acc2.Commit(GetNextHlc());
    acc3.Commit(GetNextHlc());
    CleanupHlc(last_hlc);
  }

  {
    auto acc1 = store.Access(GetNextHlc());
    auto acc2 = store.Access(GetNextHlc());

    auto vertex = acc1.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1.DeleteVertex(&*vertex).HasValue());

    auto acc3 = store.Access(GetNextHlc());

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc3.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc1.Abort();
    acc2.Abort();
    acc3.Abort();
    CleanupHlc(last_hlc);
  }

  {
    auto acc = store.Access(GetNextHlc());

    EXPECT_TRUE(acc.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc.FindVertex(pk, View::NEW));

    acc.AdvanceCommand();

    EXPECT_TRUE(acc.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc.FindVertex(pk, View::NEW));

    acc.Abort();
  }

  {
    auto acc1 = store.Access(GetNextHlc());
    auto acc2 = store.Access(GetNextHlc());

    auto vertex = acc1.FindVertex(pk, View::OLD);
    ASSERT_TRUE(vertex);

    ASSERT_TRUE(acc1.DeleteVertex(&*vertex).HasValue());

    auto acc3 = store.Access(GetNextHlc());

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc1.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc3.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc1.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc2.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc2.FindVertex(pk, View::NEW));
    EXPECT_TRUE(acc3.FindVertex(pk, View::OLD));
    EXPECT_TRUE(acc3.FindVertex(pk, View::NEW));

    acc1.Commit(GetNextHlc());
    acc2.Commit(GetNextHlc());
    acc3.Commit(GetNextHlc());
  }

  {
    auto acc = store.Access(GetNextHlc());

    EXPECT_FALSE(acc.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc.FindVertex(pk, View::NEW));

    acc.AdvanceCommand();

    EXPECT_FALSE(acc.FindVertex(pk, View::OLD));
    EXPECT_FALSE(acc.FindVertex(pk, View::NEW));

    acc.Abort();
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageV3, DeletedVertexAccessor) {
  const auto property1 = NameToPropertyId("property1");
  const PropertyValue property_value{"property_value"};

  // Create the vertex
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = CreateVertexAndValidate(acc, {}, PropertyValue{0}, {});
    ASSERT_FALSE(vertex.SetPropertyAndValidate(property1, property_value).HasError());
    acc.Commit(GetNextHlc());
  }

  auto acc = store.Access(GetNextHlc());
  auto vertex = acc.FindVertex(pk, View::OLD);
  ASSERT_TRUE(vertex);
  auto maybe_deleted_vertex = acc.DeleteVertex(&*vertex);
  ASSERT_FALSE(maybe_deleted_vertex.HasError());

  auto deleted_vertex = maybe_deleted_vertex.GetValue();
  ASSERT_TRUE(deleted_vertex);
  // you cannot modify deleted vertex
  ASSERT_TRUE(deleted_vertex->ClearProperties().HasError());

  // you can call read only methods
  const auto maybe_property = deleted_vertex->GetProperty(property1, View::OLD);
  ASSERT_FALSE(maybe_property.HasError());
  ASSERT_EQ(property_value, *maybe_property);
  acc.Commit(GetNextHlc());

  {
    // you can call read only methods and get valid results even after the
    // transaction which deleted the vertex committed, but only if the transaction
    // accessor is still alive
    const auto maybe_property = deleted_vertex->GetProperty(property1, View::OLD);
    ASSERT_FALSE(maybe_property.HasError());
    ASSERT_EQ(property_value, *maybe_property);
  }
}

TEST_P(StorageV3, TestCreateVertexAndValidate) {
  {
    auto acc = store.Access(GetNextHlc());
    const auto label1 = NameToLabelId("label1");
    const auto prop1 = NameToPropertyId("prop1");
    auto vertex = acc.CreateVertexAndValidate({label1}, {PropertyValue(0)}, {{prop1, PropertyValue(111)}});
    ASSERT_TRUE(vertex.HasValue());
    ASSERT_TRUE(vertex->PrimaryLabel(View::NEW).HasValue());
    EXPECT_EQ(vertex->PrimaryLabel(View::NEW).GetValue(), primary_label);
    ASSERT_TRUE(vertex->PrimaryKey(View::NEW).HasValue());
    EXPECT_EQ(vertex->PrimaryKey(View::NEW).GetValue(), PrimaryKey{{PropertyValue(0)}});
    ASSERT_TRUE(vertex->Properties(View::NEW).HasValue());
    EXPECT_EQ(vertex->Properties(View::NEW).GetValue(),
              (std::map<PropertyId, PropertyValue>{{prop1, PropertyValue(111)}}));
  }
  {
    EXPECT_THROW(
        {
          Shard store(primary_label, min_pk, std::nullopt /*max_primary_key*/, schema_property_vector);
          auto acc = store.Access(GetNextHlc());
          auto vertex1 = acc.CreateVertexAndValidate({}, {PropertyValue{0}}, {});
          auto vertex2 = acc.CreateVertexAndValidate({}, {PropertyValue{0}}, {});

          if (vertex2.HasError()) {
            auto error = vertex2.GetError();
            if (auto error_ptr = std::get_if<memgraph::storage::v3::Error>(&error)) {
              if (*error_ptr == storage::v3::Error::VERTEX_ALREADY_INSERTED) throw AlreadyInsertedException();
            }
          }
        },
        AlreadyInsertedException);
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.CreateVertexAndValidate({primary_label}, {PropertyValue{0}}, {});
    ASSERT_TRUE(vertex.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(vertex.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(vertex.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY, primary_label));
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.CreateVertexAndValidate({primary_label}, {PropertyValue{0}}, {});
    ASSERT_TRUE(vertex.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(vertex.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(vertex.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY, primary_label));
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.CreateVertexAndValidate({}, {}, {});
    ASSERT_TRUE(vertex.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(vertex.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(vertex.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_PRIMARY_PROPERTIES_UNDEFINED, primary_label));
  }
  {
    auto acc = store.Access(GetNextHlc());
    auto vertex = acc.CreateVertexAndValidate({}, {PropertyValue{"test"}}, {});
    ASSERT_TRUE(vertex.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(vertex.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(vertex.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE, primary_label,
                              {primary_property, common::SchemaType::INT}, PropertyValue("test")));
  }
}
}  // namespace memgraph::storage::v3::tests
