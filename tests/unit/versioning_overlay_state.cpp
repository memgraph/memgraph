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
#include <map>
#include <memory>
#include <random>
#include <set>
#include <vector>

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

TEST(OverlayMarkers, MarkAndStripAreInverseOnEveryRealId) {
  for (uint32_t id : {0U, 1U, 255U, 65536U, overlay::kLastRealId - 1, overlay::kLastRealId}) {
    EXPECT_FALSE(ms::IsReservedId(L(id)));
    EXPECT_TRUE(ms::IsReservedId(overlay::Mark(L(id))));
    EXPECT_EQ(overlay::Strip(overlay::Mark(L(id))), L(id));
    EXPECT_EQ(overlay::Strip(overlay::Mark(P(id))), P(id));
    EXPECT_NE(overlay::Mark(L(id)), L(id));

    auto const t = ms::EdgeTypeId::FromUint(id);
    EXPECT_FALSE(ms::IsReservedId(t));
    EXPECT_TRUE(ms::IsReservedId(overlay::Mark(t)));
    EXPECT_EQ(overlay::Strip(overlay::Mark(t)), t);
  }
}

TEST(OverlayMarkers, ADeleteMarkerIsNotTheMarkOfAnyRealId) {
  EXPECT_TRUE(ms::IsReservedId(overlay::kDeleteLabel));
  EXPECT_TRUE(ms::IsReservedId(overlay::kEdgeDeletePid));
  EXPECT_NE(overlay::Mark(L(overlay::kLastRealId - 1)), overlay::kDeleteLabel);
  EXPECT_NE(overlay::Mark(P(overlay::kLastRealId - 1)), overlay::kEdgeDeletePid);
  EXPECT_NE(overlay::kDeleteLabel, ms::kInvalidLabelId);

  auto const last = overlay::Mark(ms::EdgeTypeId::FromUint(overlay::kLastRealId));
  EXPECT_LT(last.AsUint(), overlay::kDeleteId);
  EXPECT_NE(last, ms::kInvalidEdgeTypeId);
  EXPECT_NE(overlay::Mark(ms::EdgeTypeId::FromUint(7)).AsUint(), overlay::kDeleteId);
}

TEST(OverlayMarkers, IdsFromANameMapperNeverCarryTheBit) {
  ms::NameIdMapper mapper;
  for (int i = 0; i < 5000; ++i) {
    EXPECT_FALSE(ms::IsReservedId(static_cast<uint32_t>(mapper.NameToId("name" + std::to_string(i)))));
  }
}

TEST(OverlayState, LabelMutatorsKeepTheFormCanonical) {
  {  // a removed label is never also added, and the other way round
    OverlayState s;
    s.AddLabel(L(1));
    s.RemoveLabel(L(1));
    EXPECT_TRUE(s.added_labels.empty());
    EXPECT_EQ(s.removed_labels, std::vector{L(1)});
    s.AddLabel(L(1));
    EXPECT_EQ(s.added_labels, std::vector{L(1)});
    EXPECT_TRUE(s.removed_labels.empty());
  }
  {  // unhiding only unmarks
    OverlayState s;
    s.RemoveLabel(L(4));
    s.UnhideLabel(L(4));
    EXPECT_TRUE(s.removed_labels.empty());
    EXPECT_TRUE(s.added_labels.empty());
  }
  {  // sorted and unique
    OverlayState s;
    for (uint32_t l : {5U, 1U, 9U, 1U, 5U, 3U}) s.AddLabel(L(l));
    EXPECT_EQ(s.added_labels, (std::vector{L(1), L(3), L(5), L(9)}));
    s.DropAddedLabel(L(3));
    EXPECT_EQ(s.added_labels, (std::vector{L(1), L(5), L(9)}));
  }
}

TEST(OverlayState, PropertyMutatorsDeleteAndCopy) {
  {  // a null write is a removal
    OverlayState s;
    s.SetProperty(P(1), V(7));
    s.SetProperty(P(1), ms::PropertyValue{});
    EXPECT_TRUE(s.properties.Properties().empty());
    EXPECT_EQ(s.removed_pids, std::vector{P(1)});
  }
  {  // setting a removed property clears its marker
    OverlayState s;
    s.RemoveProperty(P(2));
    s.SetProperty(P(2), V(3));
    EXPECT_TRUE(s.removed_pids.empty());
    EXPECT_EQ(s.properties.Properties().at(P(2)), V(3));
  }
  {  // a delete wins over everything else
    OverlayState s;
    s.AddLabel(L(1));
    s.RemoveLabel(L(2));
    s.SetProperty(P(1), V(1));
    s.RemoveProperty(P(2));
    s.MarkDeleted();
    EXPECT_TRUE(s.deleted);
    EXPECT_TRUE(s.added_labels.empty());
    EXPECT_TRUE(s.removed_labels.empty());
    EXPECT_TRUE(s.removed_pids.empty());
    EXPECT_TRUE(s.properties.Properties().empty());
  }
  {  // a copy is independent
    OverlayState s;
    s.SetProperty(P(1), V(1));
    s.AddLabel(L(1));
    auto copy = s.Copy();
    s.SetProperty(P(1), V(2));
    s.AddLabel(L(2));
    EXPECT_EQ(copy.properties.Properties().at(P(1)), V(1));
    EXPECT_EQ(copy.added_labels, std::vector{L(1)});
  }
}

TEST(OverlayCodec, AMarkIsStoredOnTheReservedIdAndNotOnTheRealOne) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  auto fresh = overlay::Decode(v, ms::View::NEW);
  ASSERT_TRUE(fresh.has_value());
  ExpectSameState(*fresh, OverlayState{});

  OverlayState after;
  after.AddLabel(L(3));
  after.RemoveLabel(L(4));
  after.SetProperty(P(1), V(10));
  after.SetProperty(P(2), ms::PropertyValue{"text"});
  after.RemoveProperty(P(5));
  ASSERT_FALSE(overlay::Apply(v, OverlayState{}, after).has_value());

  EXPECT_FALSE(*v.HasLabel(L(4), ms::View::NEW));
  EXPECT_TRUE(*v.HasLabel(overlay::Mark(L(4)), ms::View::NEW));
  EXPECT_TRUE(v.GetProperty(P(5), ms::View::NEW)->IsNull());
  EXPECT_EQ(*v.GetProperty(overlay::Mark(P(5)), ms::View::NEW), ms::PropertyValue{true});

  auto decoded = overlay::Decode(v, ms::View::NEW);
  ASSERT_TRUE(decoded.has_value());
  ExpectSameState(*decoded, after);
  acc->Abort();
}

TEST(OverlayCodec, AReAddedBaseLabelLeavesNoTraceOnTheOverlay) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  OverlayState removed;
  removed.RemoveLabel(L(4));
  ASSERT_FALSE(overlay::Apply(v, OverlayState{}, removed).has_value());

  OverlayState restored;
  restored.UnhideLabel(L(4));
  ASSERT_FALSE(overlay::Apply(v, removed, restored).has_value());
  EXPECT_TRUE(v.Labels(ms::View::NEW)->empty());
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
  auto decoded = overlay::Decode(v, ms::View::NEW);
  ASSERT_TRUE(decoded.has_value());
  ExpectSameState(*decoded, deleted);

  // The delete writes only its marker; what the overlay held stays on it, hidden by Decode. Re-applying is a no-op.
  ASSERT_FALSE(overlay::Apply(v, deleted, deleted).has_value());
  EXPECT_EQ(v.Labels(ms::View::NEW)->size(), 2U);
  EXPECT_TRUE(*v.HasLabel(L(1), ms::View::NEW));
  EXPECT_TRUE(*v.HasLabel(overlay::kDeleteLabel, ms::View::NEW));
  EXPECT_EQ(*v.GetProperty(P(1), ms::View::NEW), V(1));
  ExpectSameState(*overlay::Decode(v, ms::View::NEW), deleted);
  acc->Abort();
}

TEST(OverlayCodec, DecodeFollowsTheCommandId) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  OverlayState after;
  after.AddLabel(L(1));
  after.SetProperty(P(1), V(1));
  ASSERT_FALSE(overlay::Apply(v, OverlayState{}, after).has_value());

  ExpectSameState(*overlay::Decode(v, ms::View::NEW), after);
  // A vertex created in this command is invisible to OLD, so look at it through the next one.
  acc->AdvanceCommand();
  ExpectSameState(*overlay::Decode(v, ms::View::OLD), after);

  OverlayState later = after.Copy();
  later.RemoveProperty(P(1));
  later.RemoveLabel(L(1));
  ASSERT_FALSE(overlay::Apply(v, after, later).has_value());
  ExpectSameState(*overlay::Decode(v, ms::View::OLD), after);
  ExpectSameState(*overlay::Decode(v, ms::View::NEW), later);
  acc->Abort();
}

// Random edits of a base object against a model; base-owned labels/properties are the ones a mutator hides or unhides.
TEST(OverlayCodec, RandomEditsRoundTripAndMatchTheModel) {
  constexpr uint32_t kIds = 8;
  for (uint32_t seed = 0; seed < 40; ++seed) {
    SCOPED_TRACE(testing::Message() << "seed " << seed);
    std::mt19937 rng{seed};
    auto const roll = [&](uint32_t n) { return std::uniform_int_distribution<uint32_t>{0, n - 1}(rng); };

    std::set<uint32_t> base_labels;
    std::set<uint32_t> base_props;
    for (uint32_t i = 0; i < kIds; ++i) {
      if (roll(2) != 0) base_labels.insert(i);
      if (roll(2) != 0) base_props.insert(i);
    }

    // The model: what the branch sees, built independently of OverlayState.
    std::set<uint32_t> labels = base_labels;
    std::map<uint32_t, int64_t> values;  // the properties the branch set
    std::set<uint32_t> removed_props;    // the base properties the branch removed
    auto const expected = [&] {
      OverlayState e;
      for (uint32_t l : labels) {
        if (!base_labels.contains(l)) e.added_labels.push_back(L(l));
      }
      for (uint32_t l : base_labels) {
        if (!labels.contains(l)) e.removed_labels.push_back(L(l));
      }
      for (auto const &[p, v] : values) e.properties.SetProperty(P(p), V(v));
      for (uint32_t p : removed_props) e.removed_pids.push_back(P(p));
      return e;
    };

    auto storage = MakeStorage();
    auto acc = storage->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    OverlayState state;
    for (int step = 0; step < 60; ++step) {
      auto before = state.Copy();
      for (int edit = 0, edits = 1 + static_cast<int>(roll(4)); edit < edits; ++edit) {
        uint32_t const id = roll(kIds);
        switch (roll(4)) {
          case 0:
            if (base_labels.contains(id)) {
              state.UnhideLabel(L(id));
            } else {
              state.AddLabel(L(id));
            }
            labels.insert(id);
            break;
          case 1:
            if (base_labels.contains(id)) {
              state.RemoveLabel(L(id));
            } else {
              state.DropAddedLabel(L(id));
            }
            labels.erase(id);
            break;
          case 2: {
            auto const value = static_cast<int64_t>(roll(100));
            state.SetProperty(P(id), V(value));
            values[id] = value;
            removed_props.erase(id);
            break;
          }
          default:
            if (base_props.contains(id)) {
              state.RemoveProperty(P(id));
              removed_props.insert(id);
            } else {
              state.properties.SetProperty(P(id), ms::PropertyValue{});
            }
            values.erase(id);
            break;
        }
      }
      ASSERT_FALSE(overlay::Apply(v, before, state).has_value());
      ExpectSameState(state, expected());
      auto decoded = overlay::Decode(v, ms::View::NEW);
      ASSERT_TRUE(decoded.has_value());
      ExpectSameState(*decoded, state);
      if (roll(3) == 0) acc->AdvanceCommand();
    }

    auto before = state.Copy();
    state.MarkDeleted();
    ASSERT_FALSE(overlay::Apply(v, before, state).has_value());
    ExpectSameState(*overlay::Decode(v, ms::View::NEW), state);
    acc->Abort();
  }
}

namespace {

ms::EdgeAccessor MakeEdge(ms::Storage::Accessor &acc) {
  auto from = acc.CreateVertex();
  auto to = acc.CreateVertex();
  auto edge = acc.CreateEdge(&from, &to, acc.NameToEdgeType("E"));
  return *edge;
}

}  // namespace

TEST(OverlayCodec, AnEdgeOverlayRoundTripsItsPropertiesRemovalsAndDelete) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto e = MakeEdge(*acc);
  ExpectSameState(*overlay::DecodeEdge(e, ms::View::NEW), OverlayState{});

  OverlayState after;
  after.SetProperty(P(1), V(1));
  after.RemoveProperty(P(2));
  ASSERT_FALSE(overlay::ApplyEdge(e, OverlayState{}, after).has_value());
  ExpectSameState(*overlay::DecodeEdge(e, ms::View::NEW), after);
  EXPECT_FALSE(*overlay::IsEdgeDeleted(e, ms::View::NEW));

  OverlayState later = after.Copy();
  later.SetProperty(P(2), V(2));
  later.SetProperty(P(1), ms::PropertyValue{});
  ASSERT_FALSE(overlay::ApplyEdge(e, after, later).has_value());
  ExpectSameState(*overlay::DecodeEdge(e, ms::View::NEW), later);

  OverlayState deleted;
  deleted.MarkDeleted();
  ASSERT_FALSE(overlay::ApplyEdge(e, later, deleted).has_value());
  EXPECT_TRUE(*overlay::IsEdgeDeleted(e, ms::View::NEW));
  ExpectSameState(*overlay::DecodeEdge(e, ms::View::NEW), deleted);
  acc->Abort();
}
