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

// Marker ids (stub_state.hpp), the StubState canonical form, and its round trip through a stock vertex or edge via the
// unit-test-only codec (versioning_stub_codec.hpp).

#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <span>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/unit/versioning_stub_codec.hpp"
#include "versioning/stub_merge.hpp"
#include "versioning/stub_state.hpp"

namespace ms = memgraph::storage;
namespace stub = memgraph::versioning::stub;
using memgraph::versioning::StubState;

namespace {

ms::LabelId L(uint32_t id) { return ms::LabelId::FromUint(id); }

ms::PropertyId P(uint32_t id) { return ms::PropertyId::FromUint(id); }

ms::PropertyValue V(int64_t v) { return ms::PropertyValue{v}; }

void ExpectSameState(const StubState &actual, const StubState &expected) {
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

TEST(StubMarkers, MarkAndStripAreInverseOnEveryRealId) {
  for (uint32_t id : {0U, 1U, 255U, 65536U, stub::kLastRealId - 1, stub::kLastRealId}) {
    EXPECT_FALSE(ms::IsReservedId(L(id)));
    EXPECT_TRUE(ms::IsReservedId(stub::Mark(L(id))));
    EXPECT_EQ(stub::Strip(stub::Mark(L(id))), L(id));
    EXPECT_EQ(stub::Strip(stub::Mark(P(id))), P(id));
    EXPECT_NE(stub::Mark(L(id)), L(id));

    auto const t = ms::EdgeTypeId::FromUint(id);
    EXPECT_FALSE(ms::IsReservedId(t));
    EXPECT_TRUE(ms::IsReservedId(stub::Mark(t)));
    EXPECT_EQ(stub::Strip(stub::Mark(t)), t);
  }
}

TEST(StubMarkers, ADeleteMarkerIsNotTheMarkOfAnyRealId) {
  EXPECT_TRUE(ms::IsReservedId(stub::kDeleteLabel));
  EXPECT_TRUE(ms::IsReservedId(stub::kEdgeDeletePid));
  EXPECT_NE(stub::Mark(L(stub::kLastRealId - 1)), stub::kDeleteLabel);
  EXPECT_NE(stub::Mark(P(stub::kLastRealId - 1)), stub::kEdgeDeletePid);
  EXPECT_NE(stub::kDeleteLabel, ms::kInvalidLabelId);

  auto const last = stub::Mark(ms::EdgeTypeId::FromUint(stub::kLastRealId));
  EXPECT_LT(last.AsUint(), stub::kDeleteId);
  EXPECT_NE(last, ms::kInvalidEdgeTypeId);
  EXPECT_NE(stub::Mark(ms::EdgeTypeId::FromUint(7)).AsUint(), stub::kDeleteId);
}

TEST(StubMarkers, IdsFromANameMapperNeverCarryTheBit) {
  ms::NameIdMapper mapper;
  for (int i = 0; i < 5000; ++i) {
    EXPECT_FALSE(ms::IsReservedId(static_cast<uint32_t>(mapper.NameToId("name" + std::to_string(i)))));
  }
}

TEST(StubState, LabelMutatorsKeepTheFormCanonical) {
  {  // a removed label is never also added, and the other way round
    StubState s;
    s.AddLabel(L(1));
    s.RemoveLabel(L(1));
    EXPECT_TRUE(s.added_labels.empty());
    EXPECT_EQ(s.removed_labels, std::vector{L(1)});
    s.AddLabel(L(1));
    EXPECT_EQ(s.added_labels, std::vector{L(1)});
    EXPECT_TRUE(s.removed_labels.empty());
  }
  {  // unhiding only unmarks
    StubState s;
    s.RemoveLabel(L(4));
    s.UnhideLabel(L(4));
    EXPECT_TRUE(s.removed_labels.empty());
    EXPECT_TRUE(s.added_labels.empty());
  }
  {  // sorted and unique
    StubState s;
    for (uint32_t l : {5U, 1U, 9U, 1U, 5U, 3U}) s.AddLabel(L(l));
    EXPECT_EQ(s.added_labels, (std::vector{L(1), L(3), L(5), L(9)}));
    s.DropAddedLabel(L(3));
    EXPECT_EQ(s.added_labels, (std::vector{L(1), L(5), L(9)}));
  }
}

TEST(StubState, PropertyMutatorsDeleteAndCopy) {
  {  // a null write is a removal
    StubState s;
    s.SetProperty(P(1), V(7));
    s.SetProperty(P(1), ms::PropertyValue{});
    EXPECT_TRUE(s.properties.Properties().empty());
    EXPECT_EQ(s.removed_pids, std::vector{P(1)});
  }
  {  // setting a removed property clears its marker
    StubState s;
    s.RemoveProperty(P(2));
    s.SetProperty(P(2), V(3));
    EXPECT_TRUE(s.removed_pids.empty());
    EXPECT_EQ(s.properties.Properties().at(P(2)), V(3));
  }
  {  // a delete wins over everything else
    StubState s;
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
    StubState s;
    s.SetProperty(P(1), V(1));
    s.AddLabel(L(1));
    auto copy = s.Copy();
    s.SetProperty(P(1), V(2));
    s.AddLabel(L(2));
    EXPECT_EQ(copy.properties.Properties().at(P(1)), V(1));
    EXPECT_EQ(copy.added_labels, std::vector{L(1)});
  }
}

TEST(StubCodec, AMarkIsStoredOnTheReservedIdAndNotOnTheRealOne) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  auto fresh = stub::Decode(v, ms::View::NEW);
  ASSERT_TRUE(fresh.has_value());
  ExpectSameState(*fresh, StubState{});

  StubState after;
  after.AddLabel(L(3));
  after.RemoveLabel(L(4));
  after.SetProperty(P(1), V(10));
  after.SetProperty(P(2), ms::PropertyValue{"text"});
  after.RemoveProperty(P(5));
  ASSERT_FALSE(stub::Apply(v, StubState{}, after).has_value());

  EXPECT_FALSE(*v.HasLabel(L(4), ms::View::NEW));
  EXPECT_TRUE(*v.HasLabel(stub::Mark(L(4)), ms::View::NEW));
  EXPECT_TRUE(v.GetProperty(P(5), ms::View::NEW)->IsNull());
  EXPECT_EQ(*v.GetProperty(stub::Mark(P(5)), ms::View::NEW), ms::PropertyValue{true});

  auto decoded = stub::Decode(v, ms::View::NEW);
  ASSERT_TRUE(decoded.has_value());
  ExpectSameState(*decoded, after);
  acc->Abort();
}

TEST(StubCodec, AReAddedForkLabelLeavesNoTraceOnTheStub) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  StubState removed;
  removed.RemoveLabel(L(4));
  ASSERT_FALSE(stub::Apply(v, StubState{}, removed).has_value());

  StubState restored;
  restored.UnhideLabel(L(4));
  ASSERT_FALSE(stub::Apply(v, removed, restored).has_value());
  EXPECT_TRUE(v.Labels(ms::View::NEW)->empty());
  acc->Abort();
}

TEST(StubCodec, ADeleteDecodesToTheDeleteAlone) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  StubState changed;
  changed.AddLabel(L(1));
  changed.SetProperty(P(1), V(1));
  ASSERT_FALSE(stub::Apply(v, StubState{}, changed).has_value());

  StubState deleted;
  deleted.MarkDeleted();
  ASSERT_FALSE(stub::Apply(v, changed, deleted).has_value());

  EXPECT_TRUE(*stub::IsDeleted(v, ms::View::NEW));
  auto decoded = stub::Decode(v, ms::View::NEW);
  ASSERT_TRUE(decoded.has_value());
  ExpectSameState(*decoded, deleted);

  // The delete writes only its marker; what the stub held stays on it, hidden by Decode. Re-applying is a no-op.
  ASSERT_FALSE(stub::Apply(v, deleted, deleted).has_value());
  EXPECT_EQ(v.Labels(ms::View::NEW)->size(), 2U);
  EXPECT_TRUE(*v.HasLabel(L(1), ms::View::NEW));
  EXPECT_TRUE(*v.HasLabel(stub::kDeleteLabel, ms::View::NEW));
  EXPECT_EQ(*v.GetProperty(P(1), ms::View::NEW), V(1));
  ExpectSameState(*stub::Decode(v, ms::View::NEW), deleted);
  acc->Abort();
}

TEST(StubCodec, DecodeFollowsTheCommandId) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  StubState after;
  after.AddLabel(L(1));
  after.SetProperty(P(1), V(1));
  ASSERT_FALSE(stub::Apply(v, StubState{}, after).has_value());

  ExpectSameState(*stub::Decode(v, ms::View::NEW), after);
  // A vertex created in this command is invisible to OLD, so look at it through the next one.
  acc->AdvanceCommand();
  ExpectSameState(*stub::Decode(v, ms::View::OLD), after);

  StubState later = after.Copy();
  later.RemoveProperty(P(1));
  later.RemoveLabel(L(1));
  ASSERT_FALSE(stub::Apply(v, after, later).has_value());
  ExpectSameState(*stub::Decode(v, ms::View::OLD), after);
  ExpectSameState(*stub::Decode(v, ms::View::NEW), later);
  acc->Abort();
}

// Random edits of a fork object against a model; fork-owned labels/properties are the ones a mutator hides or unhides.
TEST(StubCodec, RandomEditsRoundTripAndMatchTheModel) {
  constexpr uint32_t kIds = 8;
  for (uint32_t seed = 0; seed < 40; ++seed) {
    SCOPED_TRACE(testing::Message() << "seed " << seed);
    std::mt19937 rng{seed};
    auto const roll = [&](uint32_t n) { return std::uniform_int_distribution<uint32_t>{0, n - 1}(rng); };

    std::set<uint32_t> fork_labels;
    std::set<uint32_t> fork_props;
    for (uint32_t i = 0; i < kIds; ++i) {
      if (roll(2) != 0) fork_labels.insert(i);
      if (roll(2) != 0) fork_props.insert(i);
    }

    // The model: what the branch sees, built independently of StubState.
    std::set<uint32_t> labels = fork_labels;
    std::map<uint32_t, int64_t> values;  // the properties the branch set
    std::set<uint32_t> removed_props;    // the fork properties the branch removed
    auto const expected = [&] {
      StubState e;
      for (uint32_t l : labels) {
        if (!fork_labels.contains(l)) e.added_labels.push_back(L(l));
      }
      for (uint32_t l : fork_labels) {
        if (!labels.contains(l)) e.removed_labels.push_back(L(l));
      }
      for (auto const &[p, v] : values) e.properties.SetProperty(P(p), V(v));
      for (uint32_t p : removed_props) e.removed_pids.push_back(P(p));
      return e;
    };

    auto storage = MakeStorage();
    auto acc = storage->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    StubState state;
    for (int step = 0; step < 60; ++step) {
      auto before = state.Copy();
      for (int edit = 0, edits = 1 + static_cast<int>(roll(4)); edit < edits; ++edit) {
        uint32_t const id = roll(kIds);
        switch (roll(4)) {
          case 0:
            if (fork_labels.contains(id)) {
              state.UnhideLabel(L(id));
            } else {
              state.AddLabel(L(id));
            }
            labels.insert(id);
            break;
          case 1:
            if (fork_labels.contains(id)) {
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
            if (fork_props.contains(id)) {
              state.RemoveProperty(P(id));
              removed_props.insert(id);
            } else {
              state.properties.SetProperty(P(id), ms::PropertyValue{});
            }
            values.erase(id);
            break;
        }
      }
      ASSERT_FALSE(stub::Apply(v, before, state).has_value());
      ExpectSameState(state, expected());
      auto decoded = stub::Decode(v, ms::View::NEW);
      ASSERT_TRUE(decoded.has_value());
      ExpectSameState(*decoded, state);
      if (roll(3) == 0) acc->AdvanceCommand();
    }

    auto before = state.Copy();
    state.MarkDeleted();
    ASSERT_FALSE(stub::Apply(v, before, state).has_value());
    ExpectSameState(*stub::Decode(v, ms::View::NEW), state);
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

TEST(StubCodec, AnEdgeStubRoundTripsItsPropertiesRemovalsAndDelete) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto e = MakeEdge(*acc);
  ExpectSameState(*stub::DecodeEdge(e, ms::View::NEW), StubState{});

  StubState after;
  after.SetProperty(P(1), V(1));
  after.RemoveProperty(P(2));
  ASSERT_FALSE(stub::ApplyEdge(e, StubState{}, after).has_value());
  ExpectSameState(*stub::DecodeEdge(e, ms::View::NEW), after);
  EXPECT_FALSE(*stub::IsEdgeDeleted(e, ms::View::NEW));

  StubState later = after.Copy();
  later.SetProperty(P(2), V(2));
  later.SetProperty(P(1), ms::PropertyValue{});
  ASSERT_FALSE(stub::ApplyEdge(e, after, later).has_value());
  ExpectSameState(*stub::DecodeEdge(e, ms::View::NEW), later);

  StubState deleted;
  deleted.MarkDeleted();
  ASSERT_FALSE(stub::ApplyEdge(e, later, deleted).has_value());
  EXPECT_TRUE(*stub::IsEdgeDeleted(e, ms::View::NEW));
  ExpectSameState(*stub::DecodeEdge(e, ms::View::NEW), deleted);
  acc->Abort();
}

// Reads through the stub and the fork (stub_merge.hpp).

TEST(StubReads, ALabelIsDecidedByTheStubOrLeftToTheFork) {
  auto storage = MakeStorage();
  auto acc = storage->Access(ms::WRITE);
  auto v = acc->CreateVertex();
  StubState s;
  s.AddLabel(L(1));
  s.RemoveLabel(L(2));
  ASSERT_FALSE(stub::Apply(v, StubState{}, s).has_value());

  EXPECT_EQ(**stub::DecidesLabel(v, L(1), ms::View::NEW), true);
  EXPECT_EQ(**stub::DecidesLabel(v, L(2), ms::View::NEW), false);
  EXPECT_FALSE(stub::DecidesLabel(v, L(3), ms::View::NEW)->has_value());
  acc->Abort();
}

TEST(StubReads, APropertyIsTheStubsValueNullWhenRemovedElseTheForks) {
  auto storage = MakeStorage();
  ms::Gid fork_gid;
  {
    auto setup = storage->Access(ms::WRITE);
    auto created = setup->CreateVertex();
    fork_gid = created.Gid();
    ASSERT_TRUE(created.SetProperty(P(1), V(100)).has_value());
    ASSERT_TRUE(created.SetProperty(P(2), V(200)).has_value());
    ASSERT_TRUE(created.SetProperty(P(3), V(300)).has_value());
    ASSERT_TRUE(setup->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  auto acc = storage->Access(ms::WRITE);
  auto fork = acc->FindVertex(fork_gid, ms::View::OLD);
  ASSERT_TRUE(fork.has_value());
  auto v = acc->CreateVertex();
  StubState s;
  s.SetProperty(P(1), V(1));
  s.RemoveProperty(P(2));
  ASSERT_FALSE(stub::Apply(v, StubState{}, s).has_value());

  EXPECT_EQ(*stub::MergeGetProperty(v, *fork, P(1), ms::View::NEW), V(1));
  EXPECT_TRUE(stub::MergeGetProperty(v, *fork, P(2), ms::View::NEW)->IsNull());
  EXPECT_EQ(*stub::MergeGetProperty(v, *fork, P(3), ms::View::NEW), V(300));
  EXPECT_TRUE(stub::MergeGetProperty(v, *fork, P(9), ms::View::NEW)->IsNull());

  EXPECT_GT(*stub::MergeGetPropertySize(v, *fork, P(1), ms::View::NEW), 0U);
  EXPECT_EQ(*stub::MergeGetPropertySize(v, *fork, P(2), ms::View::NEW), 0U);
  EXPECT_EQ(*stub::MergeGetPropertySize(v, *fork, P(3), ms::View::NEW), *fork->GetPropertySize(P(3), ms::View::NEW));
  EXPECT_EQ(*stub::MergeGetPropertySize(v, *fork, P(9), ms::View::NEW), 0U);
  acc->Abort();
}

namespace {

// A stub whose single-key read misses a property that its multi-key read then finds (a torn READ_COMMITTED read).
struct TornStub {
  ms::PropertyId key;
  ms::PropertyValue value;
  ms::PropertyValue marker;

  ms::Result<ms::PropertyValue> GetProperty(ms::PropertyId, ms::View) const { return ms::PropertyValue{}; }

  ms::Result<uint64_t> GetPropertySize(ms::PropertyId, ms::View) const { return uint64_t{0}; }

  ms::Result<void> ReadPropertyValues(ms::OrderedPropertyPaths const &ordered, std::span<ms::PropertyValue> out,
                                      ms::View) const {
    for (size_t i = 0; i < out.size(); ++i) {
      out[i] = ordered.paths()[i] == ms::PropertyPath{key} ? value : marker;
    }
    return {};
  }
};

struct SentinelFork {
  ms::Result<ms::PropertyValue> GetProperty(ms::PropertyId, ms::View) const { return V(-1); }

  ms::Result<uint64_t> GetPropertySize(ms::PropertyId, ms::View) const { return uint64_t{999}; }
};

}  // namespace

TEST(StubReads, ATornStubReadFindingTheValueWinsOverTheMarkerAndTheFork) {
  auto const marker_only = ms::PropertyValue{true};
  ms::PropertyStore store;
  store.SetProperty(P(1), V(42));
  auto const real_size = uint64_t{store.PropertySize(P(1))};
  ASSERT_GT(real_size, 0U);

  for (auto const &marker : {ms::PropertyValue{}, marker_only}) {
    TornStub const torn{.key = P(1), .value = V(42), .marker = marker};
    EXPECT_EQ(*stub::MergeGetProperty(torn, SentinelFork{}, P(1), ms::View::NEW), V(42));
    EXPECT_EQ(*stub::MergeGetPropertySize(torn, SentinelFork{}, P(1), ms::View::NEW), real_size);
  }
}
