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

// Marker ids (overlay_state.hpp), the OverlayState canonical form, and its round trip through a stock vertex or edge
// via the unit-test-only codec (versioning_overlay_codec.hpp).

#include <cstdint>
#include <memory>

#include <gtest/gtest.h>

#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/unit/versioning_overlay_codec.hpp"
#include "versioning/overlay_state.hpp"

namespace ms = memgraph::storage;
namespace overlay = memgraph::versioning::overlay;
using memgraph::versioning::OverlayState;

namespace {

ms::LabelId L(uint32_t id) { return ms::LabelId::FromUint(id); }

ms::PropertyId P(uint32_t id) { return ms::PropertyId::FromUint(id); }

ms::PropertyValue V(int64_t v) { return ms::PropertyValue{v}; }

void ExpectSameState(const OverlayState &actual, const OverlayState &expected) {
  EXPECT_EQ(actual.deleted, expected.deleted);
  EXPECT_EQ(actual.added_labels, expected.added_labels);
  EXPECT_EQ(actual.removed_labels, expected.removed_labels);
  EXPECT_EQ(actual.removed_pids, expected.removed_pids);
  EXPECT_EQ(actual.properties.Properties(), expected.properties.Properties());
}

std::unique_ptr<ms::InMemoryStorage> MakeStorage() {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::NONE};
  return std::make_unique<ms::InMemoryStorage>(config);
}

}  // namespace

TEST(OverlayMarkers, MarkAndStripAreInverseAndReservedOnEveryRealId) {
  for (uint32_t id : {0U, 1U, 65536U, ms::kLastRealId - 1, ms::kLastRealId}) {
    auto const t = ms::EdgeTypeId::FromUint(id);
    EXPECT_FALSE(ms::IsReservedId(L(id)));
    EXPECT_TRUE(ms::IsReservedId(overlay::Mark(L(id))));
    EXPECT_TRUE(ms::IsReservedId(overlay::Mark(P(id))));
    EXPECT_TRUE(ms::IsReservedId(overlay::Mark(t)));
    EXPECT_EQ(ms::StripReservedId(overlay::Mark(L(id))), L(id));
    EXPECT_EQ(ms::StripReservedId(overlay::Mark(P(id))), P(id));
    EXPECT_EQ(ms::StripReservedId(overlay::Mark(t)), t);
    EXPECT_NE(overlay::Mark(L(id)), overlay::kDeleteLabel);
  }
}

TEST(OverlayCodec, AMarkIsStoredOnTheReservedIdAndNotOnTheRealOne) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();

  OverlayState after;
  after.AddLabel(L(3));
  after.RemoveLabel(L(4));
  after.SetProperty(P(1), V(10));
  after.RemoveProperty(P(5));
  ASSERT_FALSE(overlay::Apply(v, OverlayState{}, after).has_value());

  EXPECT_FALSE(*v.HasLabel(L(4), ms::View::NEW));
  EXPECT_TRUE(*v.HasLabel(overlay::Mark(L(4)), ms::View::NEW));
  EXPECT_TRUE(v.GetProperty(P(5), ms::View::NEW)->IsNull());
  EXPECT_EQ(*v.GetProperty(overlay::Mark(P(5)), ms::View::NEW), ms::PropertyValue{true});
  ExpectSameState(*overlay::Decode(v, ms::View::NEW), after);
  acc->Abort();
}

TEST(OverlayCodec, ADeleteDecodesToTheDeleteAlone) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  OverlayState changed;
  changed.AddLabel(L(1));
  changed.SetProperty(P(1), V(1));
  ASSERT_FALSE(overlay::Apply(v, OverlayState{}, changed).has_value());

  OverlayState deleted;
  deleted.MarkDeleted();
  ASSERT_FALSE(overlay::Apply(v, changed, deleted).has_value());

  EXPECT_TRUE(*overlay::IsDeleted(v, ms::View::NEW));
  EXPECT_TRUE(overlay::HasDeleteLabel(*v.Labels(ms::View::NEW)));
  EXPECT_TRUE(*v.HasLabel(L(1), ms::View::NEW)) << "the delete writes only its marker";
  ExpectSameState(*overlay::Decode(v, ms::View::NEW), deleted);
  acc->Abort();
}

// WriteKey's per-key order: the removal marker is on before the base value is dropped and off after the new value is
// set, so a reader never sees a key undecided.
TEST(OverlayWriteKey, MarksARemovalAndUnmarksOnTheNextSet) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  auto const marker = [&] { return *v.GetProperty(overlay::Mark(P(1)), ms::View::NEW); };

  ASSERT_TRUE(overlay::WriteKey(v, P(1), ms::PropertyValue{}, /*in_base=*/true, /*marked=*/false));
  EXPECT_EQ(marker(), ms::PropertyValue{true});
  EXPECT_TRUE(v.GetProperty(P(1), ms::View::NEW)->IsNull());

  ASSERT_TRUE(overlay::WriteKey(v, P(1), V(7), /*in_base=*/true, /*marked=*/true));
  EXPECT_TRUE(marker().IsNull());
  EXPECT_EQ(*v.GetProperty(P(1), ms::View::NEW), V(7));

  // Removing a value only the branch held leaves no marker.
  ASSERT_TRUE(overlay::WriteKey(v, P(1), ms::PropertyValue{}, /*in_base=*/false, /*marked=*/false));
  EXPECT_TRUE(marker().IsNull());
  EXPECT_TRUE(v.GetProperty(P(1), ms::View::NEW)->IsNull());
  acc->Abort();
}

TEST(OverlayCodec, AnEdgeOverlayRoundTripsItsPropertiesRemovalsAndDelete) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto from = acc->CreateVertex();
  auto to = acc->CreateVertex();
  auto e = *acc->CreateEdge(&from, &to, acc->NameToEdgeType("E"));

  OverlayState after;
  after.SetProperty(P(1), V(1));
  after.RemoveProperty(P(2));
  ASSERT_FALSE(overlay::ApplyEdge(e, OverlayState{}, after).has_value());
  ExpectSameState(*overlay::DecodeEdge(e, ms::View::NEW), after);
  EXPECT_FALSE(*overlay::IsEdgeDeleted(e, ms::View::NEW));

  OverlayState deleted;
  deleted.MarkDeleted();
  ASSERT_FALSE(overlay::ApplyEdge(e, after, deleted).has_value());
  EXPECT_TRUE(*overlay::IsEdgeDeleted(e, ms::View::NEW));
  ExpectSameState(*overlay::DecodeEdge(e, ms::View::NEW), deleted);
  acc->Abort();
}

namespace {
struct CountedMapper : ms::NameIdMapper {
  void SetCounter(uint64_t value) { counter_.store(value); }
};
}  // namespace

TEST(OverlayMarkers, NameMapperRefusesNewNamesPastTheLastRealId) {
  CountedMapper mapper;
  EXPECT_EQ(mapper.NameToId("known"), 0U);
  mapper.SetCounter(ms::kLastRealId);
  EXPECT_EQ(mapper.NameToId("last"), ms::kLastRealId);
  EXPECT_THROW(mapper.NameToId("past"), memgraph::utils::BasicException);
  EXPECT_FALSE(mapper.NameToIdIfExists("past").has_value());
  EXPECT_EQ(mapper.NameToId("known"), 0U);
}
