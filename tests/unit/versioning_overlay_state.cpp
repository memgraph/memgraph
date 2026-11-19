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

// The overlay encoding on stock fields (overlay_state.hpp): copied labels, explicit-null property removal and its abort
// round trip, the delete mark read, and the edge-type marker ids.

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/bound.hpp"
#include "versioning/overlay_mark_deleted.hpp"
#include "versioning/overlay_state.hpp"

namespace ms = memgraph::storage;
namespace overlay = memgraph::versioning::overlay;

namespace {

constexpr auto kSnapshot = ms::IsolationLevel::SNAPSHOT_ISOLATION;
const auto kLabelA = ms::LabelId::FromUint(1);
const auto kLabelB = ms::LabelId::FromUint(2);
const auto kLabelC = ms::LabelId::FromUint(3);
const auto kKey = ms::PropertyId::FromUint(1);
const auto kEdgeType = ms::EdgeTypeId::FromUint(1);

class OverlayStateTest : public testing::Test {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    storage = std::make_unique<ms::InMemoryStorage>(config);
    vertex = std::make_unique<ms::Vertex>(ms::Gid::FromUint(7), nullptr);
    edge = std::make_unique<ms::Edge>(ms::Gid::FromUint(9), nullptr);
    from = std::make_unique<ms::Vertex>(ms::Gid::FromUint(5), nullptr);
    to = std::make_unique<ms::Vertex>(ms::Gid::FromUint(6), nullptr);
  }

  std::unique_ptr<ms::Storage::Accessor> Writer() { return storage->Access(ms::WRITE, kSnapshot, std::nullopt); }

  static void Commit(ms::Storage::Accessor &acc) {
    ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::vector<ms::LabelId> Labels() {
    auto sorted = std::vector<ms::LabelId>{vertex->labels.begin(), vertex->labels.end()};
    std::ranges::sort(sorted);
    return sorted;
  }

  std::unique_ptr<ms::InMemoryStorage> storage;
  std::unique_ptr<ms::Vertex> vertex;
  std::unique_ptr<ms::Edge> edge;
  std::unique_ptr<ms::Vertex> from;
  std::unique_ptr<ms::Vertex> to;
};

// The overlay's storage carries every index an overlay can be in, as a branch's diff engine mirrors them.
class OverlayStateIndexedTest : public OverlayStateTest {
 protected:
  void SetUp() override {
    OverlayStateTest::SetUp();
    Ddl([](ms::Storage::Accessor &acc) {
      return acc.CreateIndex(kLabelA, ms::PropertiesPaths{ms::PropertyPath{kKey}});
    });
    Ddl([](ms::Storage::Accessor &acc) { return acc.CreateGlobalVertexIndex(kKey); });
    Ddl([](ms::Storage::Accessor &acc) { return acc.CreateIndex(kEdgeType, kKey); });
    Ddl([](ms::Storage::Accessor &acc) { return acc.CreateGlobalEdgeIndex(kKey); });
    overlay::CopyBaseLabels(*storage, vertex.get(), std::vector{kLabelA});
  }

  void Ddl(auto &&create) {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(create(*acc).has_value());
    Commit(*acc);
  }

  // Index hits for `value` at the latest view, per index.
  struct Hits {
    size_t label_property;
    size_t vertex_property;
    size_t edge_type_property;
    size_t edge_property;
  };

  Hits Scan(int64_t value) {
    auto acc = storage->Access(ms::READ, kSnapshot, std::nullopt);
    auto const point = ms::PropertyValue{value};
    auto const paths = std::array{ms::PropertyPath{kKey}};
    auto const ranges = std::array{ms::PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(point),
                                                                   memgraph::utils::MakeBoundInclusive(point))};
    auto const count = [](auto &&iterable) {
      size_t n = 0;
      for ([[maybe_unused]] auto const &_ : iterable) ++n;
      return n;
    };
    return {count(acc->Vertices(kLabelA, paths, ranges, ms::View::NEW)),
            count(acc->Vertices(kKey, point, ms::View::NEW)),
            count(acc->Edges(kEdgeType, kKey, point, ms::View::NEW)),
            count(acc->Edges(kKey, point, ms::View::NEW))};
  }

  Hits Entries() {
    auto acc = storage->Access(ms::READ, kSnapshot, std::nullopt);
    auto const paths = std::array{ms::PropertyPath{kKey}};
    return {acc->ApproximateVertexCount(kLabelA, paths),
            acc->ApproximateVertexCount(kKey),
            acc->ApproximateEdgeCount(kEdgeType, kKey),
            acc->ApproximateEdgeCount(kKey)};
  }

  ms::Result<void> WriteVertex(ms::Storage::Accessor &acc, const ms::PropertyValue &value) {
    return overlay::WriteProperty(acc.GetTransaction(), *storage, vertex.get(), kKey, value);
  }

  ms::Result<void> WriteEdge(ms::Storage::Accessor &acc, const ms::PropertyValue &value) {
    return overlay::WriteProperty(
        acc.GetTransaction(), *storage, edge.get(), from.get(), to.get(), kEdgeType, kKey, value);
  }
};

struct CountedMapper : ms::NameIdMapper {
  void SetCounter(uint64_t value) { counter_.store(value); }
};

}  // namespace

TEST(OverlayMarkers, OnlyEdgeTypesCarryTheReservedBit) {
  for (uint32_t id : {0U, 1U, 65536U, ms::kLastRealId - 1, ms::kLastRealId}) {
    auto const t = ms::EdgeTypeId::FromUint(id);
    EXPECT_FALSE(ms::IsReservedId(t));
    EXPECT_TRUE(ms::IsReservedId(overlay::Mark(t)));
    EXPECT_EQ(ms::StripReservedId(overlay::Mark(t)), t);
    EXPECT_NE(overlay::Mark(t), ms::kInvalidEdgeTypeId);
  }
}

TEST_F(OverlayStateTest, LabelsAreCopiedOnceAndThenStockAddAndRemoveApply) {
  overlay::CopyBaseLabels(*storage, vertex.get(), std::vector{kLabelA, kLabelB});
  EXPECT_EQ(Labels(), (std::vector{kLabelA, kLabelB}));

  auto writer = Writer();
  auto accessor = ms::VertexAccessor::Create(vertex.get(), storage.get(), writer->GetTransaction(), ms::View::NEW);
  ASSERT_TRUE(accessor.has_value());
  EXPECT_TRUE(*accessor->AddLabel(kLabelC));
  EXPECT_TRUE(*accessor->RemoveLabel(kLabelA));
  EXPECT_EQ(Labels(), (std::vector{kLabelB, kLabelC}));
  writer->Abort();
  EXPECT_EQ(Labels(), (std::vector{kLabelA, kLabelB})) << "an abort restores the copied set";
}

TEST_F(OverlayStateTest, ARemovedPropertyIsAnExplicitNullAndAbortRestoresEachPriorState) {
  auto const state = [&] { return overlay::FindProperty(*vertex, kKey); };
  EXPECT_FALSE(state().has_value());

  {  // absent -> explicit null, aborted: absent again
    auto writer = Writer();
    ASSERT_TRUE(overlay::WriteProperty(writer->GetTransaction(), *storage, vertex.get(), kKey, ms::PropertyValue{}));
    ASSERT_TRUE(state().has_value());
    EXPECT_TRUE(state()->IsNull());
    writer->Abort();
    EXPECT_FALSE(state().has_value());
  }
  {  // absent -> explicit null, committed
    auto writer = Writer();
    ASSERT_TRUE(overlay::WriteProperty(writer->GetTransaction(), *storage, vertex.get(), kKey, ms::PropertyValue{}));
    Commit(*writer);
  }
  {  // explicit null -> value, aborted: the explicit null, not absent
    auto writer = Writer();
    ASSERT_TRUE(overlay::WriteProperty(writer->GetTransaction(), *storage, vertex.get(), kKey, ms::PropertyValue{5}));
    EXPECT_EQ(state()->ValueInt(), 5);
    writer->Abort();
    ASSERT_TRUE(state().has_value()) << "abort erased the explicit null";
    EXPECT_TRUE(state()->IsNull());
  }
  {  // explicit null -> value -> committed, then value -> explicit null aborted: the value
    auto writer = Writer();
    ASSERT_TRUE(overlay::WriteProperty(writer->GetTransaction(), *storage, vertex.get(), kKey, ms::PropertyValue{5}));
    Commit(*writer);
    auto remover = Writer();
    ASSERT_TRUE(overlay::WriteProperty(remover->GetTransaction(), *storage, vertex.get(), kKey, ms::PropertyValue{}));
    EXPECT_TRUE(state()->IsNull());
    remover->Abort();
    EXPECT_EQ(state()->ValueInt(), 5);
  }
}

TEST_F(OverlayStateTest, AnEdgeOverlayPropertyRoundTripsThroughAbortToo) {
  {
    auto writer = Writer();
    ASSERT_TRUE(overlay::WriteProperty(
        writer->GetTransaction(), *storage, edge.get(), from.get(), to.get(), kEdgeType, kKey, ms::PropertyValue{}));
    Commit(*writer);
  }
  auto writer = Writer();
  ASSERT_TRUE(overlay::WriteProperty(
      writer->GetTransaction(), *storage, edge.get(), from.get(), to.get(), kEdgeType, kKey, ms::PropertyValue{1}));
  writer->Abort();
  ASSERT_TRUE(overlay::FindProperty(*edge, kKey).has_value());
  EXPECT_TRUE(overlay::FindProperty(*edge, kKey)->IsNull());
}

TEST_F(OverlayStateTest, AMarkDeletedIsReadPerViewAndRefusesFurtherWrites) {
  auto old_reader = storage->Access(ms::READ, kSnapshot, std::nullopt);
  auto writer = Writer();
  auto *const tx = writer->GetTransaction();
  EXPECT_FALSE(overlay::IsDeleted(tx, vertex.get(), ms::View::NEW));
  EXPECT_FALSE(overlay::IsDeleted(tx, edge.get(), ms::View::NEW));
  ASSERT_TRUE(*overlay::MarkVertexDeleted(tx, *storage, vertex.get()));
  ASSERT_TRUE(*overlay::MarkEdgeDeleted(tx, edge.get()));
  EXPECT_TRUE(overlay::IsDeleted(tx, vertex.get(), ms::View::NEW));
  EXPECT_TRUE(overlay::IsDeleted(tx, edge.get(), ms::View::NEW));
  EXPECT_FALSE(overlay::IsDeleted(tx, vertex.get(), ms::View::OLD)) << "not before the command advances";
  EXPECT_FALSE(overlay::IsDeleted(tx, edge.get(), ms::View::OLD));
  EXPECT_EQ(overlay::WriteProperty(tx, *storage, vertex.get(), kKey, ms::PropertyValue{1}).error(),
            ms::Error::DELETED_OBJECT);
  EXPECT_EQ(
      overlay::WriteProperty(tx, *storage, edge.get(), from.get(), to.get(), kEdgeType, kKey, ms::PropertyValue{1})
          .error(),
      ms::Error::DELETED_OBJECT);
  Commit(*writer);

  auto *const old_tx = old_reader->GetTransaction();
  EXPECT_FALSE(overlay::IsDeleted(old_tx, vertex.get(), ms::View::NEW)) << "a snapshot older than the commit";
  EXPECT_FALSE(overlay::IsDeleted(old_tx, edge.get(), ms::View::NEW));
  auto reader = storage->Access(ms::READ, kSnapshot, std::nullopt);
  EXPECT_TRUE(overlay::IsDeleted(reader->GetTransaction(), vertex.get(), ms::View::NEW));
  EXPECT_TRUE(overlay::IsDeleted(reader->GetTransaction(), edge.get(), ms::View::NEW));
}

TEST(OverlayMarkers, NameMapperRefusesNewNamesPastTheLastRealId) {
  CountedMapper mapper;
  EXPECT_EQ(mapper.NameToId("known"), 0U);
  mapper.SetCounter(ms::kLastRealId);
  EXPECT_EQ(mapper.NameToId("last"), ms::kLastRealId);
  EXPECT_THROW(mapper.NameToId("past"), memgraph::utils::BasicException);
  EXPECT_FALSE(mapper.NameToIdIfExists("past").has_value());
  EXPECT_EQ(mapper.NameToId("known"), 0U);
}

TEST_F(OverlayStateIndexedTest, AVertexOverlayWriteIsInTheLabelPropertyAndGlobalPropertyIndexes) {
  {
    auto writer = Writer();
    ASSERT_TRUE(WriteVertex(*writer, ms::PropertyValue{5}));
    Commit(*writer);
  }
  auto const hits = Scan(5);
  EXPECT_EQ(hits.label_property, 1U);
  EXPECT_EQ(hits.vertex_property, 1U);

  auto writer = Writer();
  ASSERT_TRUE(WriteVertex(*writer, ms::PropertyValue{6}));
  writer->Abort();
  EXPECT_EQ(Scan(6).label_property, 0U) << "an aborted write is not visible through the index";
  EXPECT_EQ(Scan(6).vertex_property, 0U);
  EXPECT_EQ(Scan(5).label_property, 1U);
}

TEST_F(OverlayStateIndexedTest, AnEdgeOverlayWriteIsInTheEdgeTypePropertyAndGlobalEdgePropertyIndexes) {
  {
    auto writer = Writer();
    ASSERT_TRUE(WriteEdge(*writer, ms::PropertyValue{5}));
    Commit(*writer);
  }
  EXPECT_EQ(Scan(5).edge_type_property, 1U);
  EXPECT_EQ(Scan(5).edge_property, 1U);
}

TEST_F(OverlayStateIndexedTest, AbortingAnEdgeOverlayWriteWithEdgePropertyIndexesRestoresIt) {
  {
    auto writer = Writer();
    ASSERT_TRUE(WriteEdge(*writer, ms::PropertyValue{1}));
    Commit(*writer);
  }
  auto writer = Writer();
  ASSERT_TRUE(WriteEdge(*writer, ms::PropertyValue{2}));
  writer->Abort();  // the delta's out vertex is dereferenced by the index abort processing
  EXPECT_EQ(overlay::FindProperty(*edge, kKey)->ValueInt(), 1);
  EXPECT_EQ(Scan(2).edge_type_property, 0U);
  EXPECT_EQ(Scan(1).edge_type_property, 1U);
}

TEST_F(OverlayStateIndexedTest, AbortingAWriteOverAnExplicitNullRestoresTheNullWithIndexesPresent) {
  {
    auto writer = Writer();
    ASSERT_TRUE(WriteEdge(*writer, ms::PropertyValue{}));
    ASSERT_TRUE(WriteVertex(*writer, ms::PropertyValue{}));
    Commit(*writer);
  }
  auto writer = Writer();
  ASSERT_TRUE(WriteEdge(*writer, ms::PropertyValue{3}));
  ASSERT_TRUE(WriteVertex(*writer, ms::PropertyValue{3}));
  writer->Abort();
  ASSERT_TRUE(overlay::FindProperty(*edge, kKey).has_value());
  EXPECT_TRUE(overlay::FindProperty(*edge, kKey)->IsNull());
  ASSERT_TRUE(overlay::FindProperty(*vertex, kKey).has_value());
  EXPECT_TRUE(overlay::FindProperty(*vertex, kKey)->IsNull());
}

TEST_F(OverlayStateIndexedTest, AnExplicitNullIsNeverIndexed) {
  auto writer = Writer();
  ASSERT_TRUE(WriteVertex(*writer, ms::PropertyValue{}));
  ASSERT_TRUE(WriteEdge(*writer, ms::PropertyValue{}));
  Commit(*writer);
  auto const entries = Entries();
  EXPECT_EQ(entries.label_property, 0U);
  EXPECT_EQ(entries.vertex_property, 0U);
  EXPECT_EQ(entries.edge_type_property, 0U);
  EXPECT_EQ(entries.edge_property, 0U);
}

TEST_F(OverlayStateTest, ARemovalOfAnAbsentKeyStillRecordsWhenIdenticalWritesAreSkipped) {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::NONE};
  config.salient.items.delta_on_identical_property_update = false;
  auto skipping = std::make_unique<ms::InMemoryStorage>(config);
  auto writer = skipping->Access(ms::WRITE, kSnapshot, std::nullopt);
  ASSERT_TRUE(overlay::WriteProperty(writer->GetTransaction(), *skipping, vertex.get(), kKey, ms::PropertyValue{}));
  ASSERT_TRUE(overlay::FindProperty(*vertex, kKey).has_value()) << "null over absent is not an identical write here";
  writer->Abort();
  EXPECT_FALSE(overlay::FindProperty(*vertex, kKey).has_value()) << "the delta was recorded, so abort undoes it";
}
