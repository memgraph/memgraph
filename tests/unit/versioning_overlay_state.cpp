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
#include <optional>
#include <utility>
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
#include "utils/atomic_utils.hpp"
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

  ms::Result<void> Write(ms::Storage::Accessor &acc, bool on_edge, const ms::PropertyValue &value) {
    auto *const tx = acc.GetTransaction();
    return on_edge ? overlay::WriteProperty(tx, *storage, edge.get(), from.get(), to.get(), kEdgeType, kKey, value)
                   : overlay::WriteProperty(tx, *storage, vertex.get(), kKey, value);
  }

  // The overlay's latest stored state of kKey: nullopt when absent, a Null value for an explicit null.
  std::optional<ms::PropertyValue> Stored(bool on_edge) const {
    return on_edge ? edge->properties.FindProperty(kKey) : vertex->properties.FindProperty(kKey);
  }

  // Every prior state of a property that an abort has to restore, on a vertex overlay and an edge overlay.
  void ExplicitNullRoundTrip() {
    for (bool const on_edge : {false, true}) {
      EXPECT_FALSE(Stored(on_edge).has_value());
      {  // absent -> explicit null, aborted: absent again
        auto writer = Writer();
        ASSERT_TRUE(Write(*writer, on_edge, ms::PropertyValue{}));
        ASSERT_TRUE(Stored(on_edge).has_value());
        EXPECT_TRUE(Stored(on_edge)->IsNull());
        writer->Abort();
        EXPECT_FALSE(Stored(on_edge).has_value());
      }
      {  // absent -> explicit null, committed
        auto writer = Writer();
        ASSERT_TRUE(Write(*writer, on_edge, ms::PropertyValue{}));
        Commit(*writer);
      }
      {  // explicit null -> value, aborted: the explicit null, not absent
        auto writer = Writer();
        ASSERT_TRUE(Write(*writer, on_edge, ms::PropertyValue{5}));
        EXPECT_EQ(Stored(on_edge)->ValueInt(), 5);
        writer->Abort();
        ASSERT_TRUE(Stored(on_edge).has_value()) << "abort erased the explicit null";
        EXPECT_TRUE(Stored(on_edge)->IsNull());
      }
      {  // explicit null -> value committed, then value -> explicit null aborted: the value
        auto writer = Writer();
        ASSERT_TRUE(Write(*writer, on_edge, ms::PropertyValue{5}));
        Commit(*writer);
        auto remover = Writer();
        ASSERT_TRUE(Write(*remover, on_edge, ms::PropertyValue{}));
        EXPECT_TRUE(Stored(on_edge)->IsNull());
        remover->Abort();
        EXPECT_EQ(Stored(on_edge)->ValueInt(), 5);
      }
    }
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
    // As the diff engine's ctor does: index scans skip gids at or above the id counters, and overlays sit below them.
    constexpr uint64_t kGidFloor = 100;
    atomic_fetch_max_explicit(&storage->vertex_id_, kGidFloor, std::memory_order_acq_rel);
    atomic_fetch_max_explicit(&storage->edge_id_, kGidFloor, std::memory_order_acq_rel);
    Ddl([](ms::Storage::Accessor &acc) {
      return acc.CreateIndex(kLabelA, ms::PropertiesPaths{ms::PropertyPath{kKey}});
    });
    Ddl([](ms::Storage::Accessor &acc) { return acc.CreateGlobalVertexIndex(kKey); });
    Ddl([](ms::Storage::Accessor &acc) { return acc.CreateIndex(kEdgeType, kKey); });
    Ddl([](ms::Storage::Accessor &acc) { return acc.CreateGlobalEdgeIndex(kKey); });
    overlay::CopyBaseLabels(vertex.get(), std::vector{kLabelA});
  }

  void Ddl(auto &&create) {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(create(*acc).has_value());
    Commit(*acc);
  }

  // Index hits, per index: label+property, global vertex property, edge-type+property, global edge property.
  struct Hits {
    size_t label_property;
    size_t vertex_property;
    size_t edge_type_property;
    size_t edge_property;

    bool operator==(const Hits &) const = default;
  };

  // Index hits for `value` at the latest view.
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

TEST(OverlayMarkers, NameMapperRefusesNewNamesPastTheLastRealId) {
  CountedMapper mapper;
  EXPECT_EQ(mapper.NameToId("known"), 0U);
  mapper.SetCounter(ms::kLastRealId);
  EXPECT_EQ(mapper.NameToId("last"), ms::kLastRealId);
  EXPECT_THROW(mapper.NameToId("past"), memgraph::utils::BasicException);
  EXPECT_FALSE(mapper.NameToIdIfExists("past").has_value());
  EXPECT_EQ(mapper.NameToId("known"), 0U);
}

TEST_F(OverlayStateTest, LabelsAreCopiedOnceAndThenStockAddAndRemoveApply) {
  overlay::CopyBaseLabels(vertex.get(), std::vector{kLabelA, kLabelB});
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

TEST_F(OverlayStateTest, ARemovedPropertyIsAnExplicitNullAndAbortRestoresEachPriorState) { ExplicitNullRoundTrip(); }

TEST_F(OverlayStateIndexedTest, ARemovedPropertyIsAnExplicitNullAndAbortRestoresEachPriorStateWithIndexesPresent) {
  ExplicitNullRoundTrip();
}

TEST_F(OverlayStateTest, ARemovalOfAnAbsentKeyStillRecordsWhenIdenticalWritesAreSkipped) {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::NONE};
  config.salient.items.delta_on_identical_property_update = false;
  auto skipping = std::make_unique<ms::InMemoryStorage>(config);
  auto writer = skipping->Access(ms::WRITE, kSnapshot, std::nullopt);
  ASSERT_TRUE(overlay::WriteProperty(writer->GetTransaction(), *skipping, vertex.get(), kKey, ms::PropertyValue{}));
  ASSERT_TRUE(Stored(false).has_value()) << "null over absent is not an identical write here";
  writer->Abort();
  EXPECT_FALSE(Stored(false).has_value()) << "the delta was recorded, so abort undoes it";
}

TEST_F(OverlayStateTest, AMarkDeletedIsReadPerViewAndRefusesFurtherWrites) {
  auto old_reader = storage->Access(ms::READ, kSnapshot, std::nullopt);
  auto writer = Writer();
  auto *const tx = writer->GetTransaction();
  auto const deleted = [&](ms::Transaction *t, ms::View view) {
    return std::pair{overlay::IsDeleted(t, vertex.get(), view), overlay::IsDeleted(t, edge.get(), view)};
  };
  EXPECT_EQ(deleted(tx, ms::View::NEW), (std::pair{false, false}));
  auto vertex_accessor = ms::VertexAccessor{vertex.get(), storage.get(), tx};
  ASSERT_TRUE(writer->DeleteVertex(&vertex_accessor)->has_value());
  ASSERT_TRUE(*overlay::MarkEdgeDeleted(tx, edge.get()));
  EXPECT_EQ(deleted(tx, ms::View::NEW), (std::pair{true, true}));
  EXPECT_EQ(deleted(tx, ms::View::OLD), (std::pair{false, false})) << "not before the command advances";
  for (bool const on_edge : {false, true}) {
    EXPECT_EQ(Write(*writer, on_edge, ms::PropertyValue{1}).error(), ms::Error::DELETED_OBJECT);
  }
  Commit(*writer);

  EXPECT_EQ(deleted(old_reader->GetTransaction(), ms::View::NEW), (std::pair{false, false}))
      << "a snapshot older than the commit";
  auto reader = storage->Access(ms::READ, kSnapshot, std::nullopt);
  EXPECT_EQ(deleted(reader->GetTransaction(), ms::View::NEW), (std::pair{true, true}));
}

TEST_F(OverlayStateIndexedTest, OverlayWritesAreIndexedAndAnAbortRestoresTheIndexes) {
  {
    auto writer = Writer();
    ASSERT_TRUE(Write(*writer, false, ms::PropertyValue{1}));
    ASSERT_TRUE(Write(*writer, true, ms::PropertyValue{1}));
    Commit(*writer);
  }
  EXPECT_EQ(Scan(1), (Hits{1, 1, 1, 1}));

  auto writer = Writer();
  ASSERT_TRUE(Write(*writer, false, ms::PropertyValue{2}));
  ASSERT_TRUE(Write(*writer, true, ms::PropertyValue{2}));
  writer->Abort();  // the edge delta's out vertex is dereferenced by the index abort processing
  EXPECT_EQ(Scan(2), (Hits{0, 0, 0, 0})) << "an aborted write is not visible through the index";
  EXPECT_EQ(Scan(1), (Hits{1, 1, 1, 1}));
  EXPECT_EQ(Stored(false)->ValueInt(), 1);
  EXPECT_EQ(Stored(true)->ValueInt(), 1);
}

TEST_F(OverlayStateIndexedTest, AnExplicitNullIsNeverIndexed) {
  auto writer = Writer();
  ASSERT_TRUE(Write(*writer, false, ms::PropertyValue{}));
  ASSERT_TRUE(Write(*writer, true, ms::PropertyValue{}));
  Commit(*writer);
  EXPECT_EQ(Entries(), (Hits{0, 0, 0, 0}));
}
