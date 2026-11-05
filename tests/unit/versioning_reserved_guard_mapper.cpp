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

// ReservedGuardMapper: the name form of the stub markers, over a plain NameIdMapper.

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "versioning/reserved_guard_mapper.hpp"
#include "versioning/stub_state.hpp"

namespace ms = memgraph::storage;
namespace stub = memgraph::versioning::stub;
using memgraph::versioning::ReservedGuardMapper;

namespace {

uint64_t MarkOf(uint64_t id) { return id | ms::kReservedIdBit; }

constexpr uint64_t kDeleteId = 0xFFFFFFFEU;

class ReservedGuardMapperTest : public testing::Test {
 protected:
  std::shared_ptr<ms::NameIdMapper> inner = std::make_shared<ms::NameIdMapper>();
  ReservedGuardMapper guard{inner};
};

// Names that are, or look like, the encoded form.
const std::vector<std::string> kAwkwardNames = {"__mg_removed_x",
                                                "__mg_deleted",
                                                "__mg_lit_x",
                                                "__mg_",
                                                "",
                                                "__mg_lit_",
                                                "__mg_lit___mg_removed_x",
                                                "__mg_removed_",
                                                "__mg_removed___mg_deleted",
                                                "__mg",
                                                "_mg_x",
                                                "x__mg_",
                                                "Person"};

}  // namespace

TEST_F(ReservedGuardMapperTest, OrdinaryNamesPassThroughToTheWrappedMapper) {
  auto const id = guard.NameToId("Person");
  EXPECT_EQ(id, inner->NameToId("Person"));
  EXPECT_EQ(guard.IdToName(id), "Person");
  EXPECT_EQ(guard.NameToIdIfExists("Person"), id);
  EXPECT_FALSE(guard.NameToIdIfExists("Unknown").has_value());
  EXPECT_FALSE(inner->NameToIdIfExists("Unknown").has_value());
}

TEST_F(ReservedGuardMapperTest, AMarkerIsNamedAfterWhatItMarks) {
  auto const id = inner->NameToId("Person");
  EXPECT_EQ(guard.IdToName(MarkOf(id)), "__mg_removed_Person");
  EXPECT_EQ(guard.NameToId("__mg_removed_Person"), MarkOf(id));
  EXPECT_EQ(guard.NameToIdIfExists("__mg_removed_Person"), MarkOf(id));
}

TEST_F(ReservedGuardMapperTest, AMarkerNameDoesNotDependOnTheIdsAssignmentOrder) {
  // A restart assigns main's ids in another order; the encoded name must still find the same label.
  auto other = std::make_shared<ms::NameIdMapper>();
  ReservedGuardMapper other_guard{other};
  other->NameToId("filler");
  auto const a = guard.NameToId("__mg_removed_Person");
  auto const b = other_guard.NameToId("__mg_removed_Person");
  EXPECT_EQ(guard.IdToName(a), other_guard.IdToName(b));
  EXPECT_NE(a, b);
}

TEST_F(ReservedGuardMapperTest, AMarkerForANameMainDoesNotKnowIsNotInvented) {
  EXPECT_FALSE(guard.NameToIdIfExists("__mg_removed_Unknown").has_value());
  EXPECT_FALSE(inner->NameToIdIfExists("Unknown").has_value());
}

TEST_F(ReservedGuardMapperTest, TheDeleteMarkerHasOneNameForBothLabelsAndProperties) {
  EXPECT_EQ(guard.IdToName(kDeleteId), "__mg_deleted");
  EXPECT_EQ(guard.NameToId("__mg_deleted"), kDeleteId);
  EXPECT_EQ(guard.NameToIdIfExists("__mg_deleted"), kDeleteId);
  EXPECT_EQ(stub::kDeleteLabel.AsUint(), kDeleteId);
  EXPECT_EQ(stub::kEdgeDeletePid.AsUint(), kDeleteId);
}

// A user label, property or map key that spells a marker must not read back as one, on a restart or anywhere else
// the name is the only thing that survives.
TEST_F(ReservedGuardMapperTest, ARealNameThatLooksLikeAMarkerIsNotOne) {
  auto const x = inner->NameToId("x");
  auto const real = inner->NameToId("__mg_removed_x");  // a user's name, on the wrapped (user-facing) mapper
  ASSERT_NE(real, x);

  auto const &name = guard.IdToName(real);
  EXPECT_NE(name, "__mg_removed_x");
  EXPECT_EQ(guard.NameToId(name), real);
  EXPECT_NE(guard.NameToId(name), MarkOf(x));
  EXPECT_FALSE(ms::IsReservedId(static_cast<uint32_t>(guard.NameToId(name))));

  auto const real_deleted = inner->NameToId("__mg_deleted");
  EXPECT_NE(guard.IdToName(real_deleted), "__mg_deleted");
  EXPECT_EQ(guard.NameToId(guard.IdToName(real_deleted)), real_deleted);
  EXPECT_NE(guard.NameToId(guard.IdToName(real_deleted)), kDeleteId);
}

TEST_F(ReservedGuardMapperTest, NamesAndIdsAreInOneToOneCorrespondence) {
  std::set<std::string> names;
  std::vector<uint64_t> ids;
  for (auto const &real_name : kAwkwardNames) {
    auto const real = inner->NameToId(real_name);
    for (auto const id : {real, MarkOf(real)}) {
      auto const &name = guard.IdToName(id);
      EXPECT_EQ(guard.NameToId(name), id) << "name: " << name;
      EXPECT_EQ(guard.NameToIdIfExists(name), id) << "name: " << name;
      EXPECT_TRUE(names.insert(name).second) << "two ids are named " << name;
      ids.push_back(id);
    }
  }
  EXPECT_TRUE(names.insert(guard.IdToName(kDeleteId)).second);
  EXPECT_EQ(names.size(), (kAwkwardNames.size() * 2) + 1);
}

TEST_F(ReservedGuardMapperTest, EscapingNestsAndNeverFiresOnAnOrdinaryName) {
  auto const real = inner->NameToId("__mg_lit_x");
  auto const &escaped = guard.IdToName(real);
  EXPECT_EQ(escaped, "__mg_lit___mg_lit_x");
  EXPECT_EQ(guard.NameToId(escaped), real);

  EXPECT_EQ(guard.IdToName(inner->NameToId("mg_lit_x")), "mg_lit_x");
  EXPECT_EQ(guard.IdToName(inner->NameToId("x__mg_")), "x__mg_");
}

TEST_F(ReservedGuardMapperTest, TheMarkerOfAnEscapedNameCarriesTheEscape) {
  auto const real = inner->NameToId("__mg_removed_x");
  EXPECT_EQ(guard.IdToName(MarkOf(real)), "__mg_removed___mg_lit___mg_removed_x");
  EXPECT_EQ(guard.NameToId("__mg_removed___mg_lit___mg_removed_x"), MarkOf(real));
}

// IdToName returns a reference, so two lookups alive at once must not share storage.
TEST_F(ReservedGuardMapperTest, TwoTranslatedNamesAreAliveAtTheSameTime) {
  auto const a = inner->NameToId("Alpha");
  auto const b = inner->NameToId("Beta");
  auto const c = inner->NameToId("__mg_gamma");
  const std::string &first = guard.IdToName(MarkOf(a));
  const std::string &second = guard.IdToName(MarkOf(b));
  const std::string &third = guard.IdToName(c);
  const std::string &fourth = guard.IdToName(kDeleteId);
  EXPECT_EQ(first, "__mg_removed_Alpha");
  EXPECT_EQ(second, "__mg_removed_Beta");
  EXPECT_EQ(third, "__mg_lit___mg_gamma");
  EXPECT_EQ(fourth, "__mg_deleted");
  EXPECT_NE(&first, &second);
  EXPECT_NE(&second, &third);
}

TEST_F(ReservedGuardMapperTest, ARepeatedLookupReturnsTheSameInternedName) {
  auto const a = inner->NameToId("Alpha");
  EXPECT_EQ(&guard.IdToName(MarkOf(a)), &guard.IdToName(MarkOf(a)));
  auto const c = inner->NameToId("__mg_gamma");
  EXPECT_EQ(&guard.IdToName(c), &guard.IdToName(c));
}

TEST_F(ReservedGuardMapperTest, ClearDoesNotResetTheSharedMapper) {
  auto const id = guard.NameToId("Person");
  const std::string &marker = guard.IdToName(MarkOf(id));
  const std::string &plain = guard.IdToName(id);
  guard.Clear();
  EXPECT_EQ(inner->NameToIdIfExists("Person"), id);
  EXPECT_EQ(guard.NameToId("Person"), id);
  EXPECT_EQ(marker, "__mg_removed_Person");
  EXPECT_EQ(plain, "Person");
  EXPECT_EQ(guard.IdToName(MarkOf(id)), "__mg_removed_Person");
}

TEST_F(ReservedGuardMapperTest, TheGuardKeepsTheWrappedMapperAlive) {
  auto const id = inner->NameToId("Person");
  inner.reset();  // the guard keeps main's mapper alive
  EXPECT_EQ(guard.IdToName(MarkOf(id)), "__mg_removed_Person");
}

TEST_F(ReservedGuardMapperTest, ConcurrentLookupsAgree) {
  constexpr int kThreads = 6;
  constexpr int kNames = 200;
  std::vector<uint64_t> ids(kNames);
  for (int i = 0; i < kNames; ++i)
    ids[i] = inner->NameToId(i % 3 == 0 ? "__mg_n" + std::to_string(i) : "n" + std::to_string(i));

  std::atomic<bool> go{false};
  std::atomic<int> mismatches{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      while (!go.load(std::memory_order_acquire)) {
      }
      for (int round = 0; round < 20; ++round) {
        for (int k = 0; k < kNames; ++k) {
          auto const i = (k + (t * 31)) % kNames;
          auto const id = (round % 2 == 0) ? ids[i] : MarkOf(ids[i]);
          const std::string &name = guard.IdToName(id);
          if (guard.NameToId(name) != id || guard.NameToIdIfExists(name) != id) mismatches.fetch_add(1);
          if (&name != &guard.IdToName(id) && (id & ms::kReservedIdBit) != 0) mismatches.fetch_add(1);
        }
      }
    });
  }
  go.store(true, std::memory_order_release);
  for (auto &th : threads) th.join();
  EXPECT_EQ(mismatches.load(), 0);
}
