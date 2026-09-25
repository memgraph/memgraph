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

// The order two property names sort in, carried as one integer per identifier.
//
// An identifier says nothing about where its name sorts: it records when the
// name was first seen. This turns that around, so a comparison holding two
// identifiers can answer in the order their names give without reading either
// name. What has to hold is that the numbers order the way the names do, and
// that it keeps holding as names arrive in the middle of ones already there.

#include <algorithm>
#include <atomic>
#include <barrier>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/property_name_order.hpp"
#include "storage/v2/property_value.hpp"

using memgraph::storage::PropertyNameOrder;

namespace {

/// Interns each name in the order given and hands back the identifier each got,
/// which is the order they arrived in rather than the order they sort in.
std::vector<uint32_t> InternAll(PropertyNameOrder &name_order, std::vector<std::string> const &names) {
  auto ids = std::vector<uint32_t>{};
  ids.reserve(names.size());
  for (auto const &name : names) {
    auto const id = static_cast<uint32_t>(ids.size());
    name_order.Add(id, name);
    ids.push_back(id);
  }
  return ids;
}

/// Whether the numbers place every pair the way the names do.
::testing::AssertionResult OrdersLikeTheNames(PropertyNameOrder const &name_order,
                                              std::vector<std::string> const &names, std::vector<uint32_t> const &ids) {
  auto const table = name_order.Read();
  for (auto lhs = 0U; lhs != names.size(); ++lhs) {
    for (auto rhs = 0U; rhs != names.size(); ++rhs) {
      auto const by_name = names[lhs] <=> names[rhs];
      auto const by_order = table->At(ids[lhs]) <=> table->At(ids[rhs]);
      if (std::is_lt(by_name) != std::is_lt(by_order) || std::is_gt(by_name) != std::is_gt(by_order)) {
        return ::testing::AssertionFailure()
               << '"' << names[lhs] << "\" against \"" << names[rhs] << "\": the names say "
               << (std::is_lt(by_name)   ? "before"
                   : std::is_gt(by_name) ? "after"
                                         : "together")
               << " and the numbers say "
               << (std::is_lt(by_order)   ? "before"
                   : std::is_gt(by_order) ? "after"
                                          : "together");
      }
    }
  }
  return ::testing::AssertionSuccess();
}

}  // namespace

TEST(PropertyNameOrder, PlacesNamesTheWayTheySort) {
  PropertyNameOrder name_order;
  auto const names = std::vector<std::string>{"delta", "alpha", "echo", "bravo", "charlie"};
  auto const ids = InternAll(name_order, names);

  EXPECT_TRUE(OrdersLikeTheNames(name_order, names, ids));
}

TEST(PropertyNameOrder, PlacesANameArrivingBetweenTwoAlreadyThere) {
  PropertyNameOrder name_order;
  // The middle name arrives last, so its number cannot come from a counter.
  auto const names = std::vector<std::string>{"a", "c", "b"};
  auto const ids = InternAll(name_order, names);

  EXPECT_TRUE(OrdersLikeTheNames(name_order, names, ids));
}

TEST(PropertyNameOrder, KeepsPlacingThemAfterAGapIsUsedUp) {
  // Every name lands in the same gap, between the two before it, so the room
  // between siblings halves each time. Enough of them and there is none left,
  // which is the point the numbers have to be laid out afresh. The names are
  // built to bisect: each is the one before it with a character appended, so it
  // sorts after its parent and before the upper bound throughout.
  PropertyNameOrder name_order;
  auto names = std::vector<std::string>{"a", "z"};
  auto probe = std::string{"a"};
  for (auto step = 0; step != 200; ++step) {
    probe += 'm';
    names.push_back(probe);
  }

  auto const before = name_order.LayoutsPublished();
  auto const ids = InternAll(name_order, names);

  EXPECT_TRUE(OrdersLikeTheNames(name_order, names, ids));
  EXPECT_GT(name_order.LayoutsPublished(), before + 1)
      << "the gap never ran out, so this says nothing about what happens when it does";
}

TEST(PropertyNameOrder, PlacesAThousandNamesArrivingInAnyOrder) {
  PropertyNameOrder name_order;
  auto names = std::vector<std::string>{};
  for (auto i = 0; i != 1000; ++i) names.push_back("property_" + std::to_string(i));
  std::ranges::shuffle(names, std::mt19937{20260925});

  auto const ids = InternAll(name_order, names);
  EXPECT_TRUE(OrdersLikeTheNames(name_order, names, ids));
}

TEST(PropertyNameOrder, AgreesWithAnOrderThatSawTheNamesInAnotherOrder) {
  // The law the whole scheme exists for. Two instances of a database intern the
  // same names in whatever order each happened to see them, so the numbers they
  // hand out differ. What may not differ is the order those numbers place a
  // pair in, because that is what a query returns.
  auto names = std::vector<std::string>{"zeta", "alpha", "mu", "beta", "omega", "gamma"};

  PropertyNameOrder one;
  auto const ids_of_one = InternAll(one, names);

  auto reversed = names;
  std::ranges::reverse(reversed);
  PropertyNameOrder other;
  auto const ids_of_other = InternAll(other, reversed);

  auto const order_of_one = one.Read();
  auto const order_of_other = other.Read();

  for (auto lhs = 0U; lhs != names.size(); ++lhs) {
    for (auto rhs = 0U; rhs != names.size(); ++rhs) {
      auto const on_one = order_of_one->At(ids_of_one[lhs]) <=> order_of_one->At(ids_of_one[rhs]);

      auto const at_other = [&](std::string const &name) {
        auto const where = std::ranges::find(reversed, name) - reversed.begin();
        return order_of_other->At(ids_of_other[where]);
      };
      auto const on_other = at_other(names[lhs]) <=> at_other(names[rhs]);

      EXPECT_EQ(std::is_lt(on_one), std::is_lt(on_other))
          << '"' << names[lhs] << "\" against \"" << names[rhs] << "\" is placed differently by two orders";
    }
  }
}

TEST(PropertyNameOrder, AnsweringReadersDoNotSeeAHalfWrittenTable) {
  // A reader holds no lock, so what it must never see is one name's number from
  // before a fresh layout and another's from after: the two would place a pair
  // by numbers that were never in one table together. Both are read through one
  // load of the published table, and this runs a writer alongside readers to
  // say so.
  PropertyNameOrder name_order;
  auto const settled = std::vector<std::string>{"aaa", "mmm", "zzz"};
  auto const settled_ids = InternAll(name_order, settled);

  std::atomic<bool> stop{false};
  std::atomic<int> disagreements{0};

  // The writer below takes a few milliseconds, which a thread can take to
  // start. Without this the readers would look on after it had finished.
  constexpr auto kReaders = 4;
  std::barrier everyone_running{kReaders + 1};

  auto readers = std::vector<std::jthread>{};
  for (auto reader = 0; reader != kReaders; ++reader) {
    readers.emplace_back([&] {
      everyone_running.arrive_and_wait();
      while (!stop.load(std::memory_order_relaxed)) {
        // Held while it is read, so that a layout this thread is looking at is
        // not one the writer has decided nobody holds.
        auto const table = name_order.Read();
        auto const first = table->At(settled_ids[0]);
        auto const middle = table->At(settled_ids[1]);
        auto const last = table->At(settled_ids[2]);
        if (!(first < middle && middle < last)) disagreements.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  everyone_running.arrive_and_wait();

  // Names that keep landing between the settled ones, which is what forces a
  // fresh layout while the readers are running.
  auto probe = std::string{"b"};
  for (auto step = 0; step != 2000; ++step) {
    probe += 'q';
    name_order.Add(static_cast<uint32_t>(settled.size() + step), probe);
  }

  auto const layouts = name_order.LayoutsPublished();

  stop.store(true, std::memory_order_relaxed);
  readers.clear();

  EXPECT_EQ(disagreements.load(), 0) << "a reader placed three settled names out of order";
  EXPECT_GT(layouts, 1U) << "no fresh layout was published while the readers ran, so they read one table throughout";
}

// What an index rests on: a pair of stored maps keeps the order it was put in.
//
// A skip list places an entry once, by asking this comparison, and never asks
// again. Were the answer for a pair already in it to change, the structure
// would be left unsorted and a scan would miss rows. Names arriving afterwards
// take places between the ones already there, and a gap running out lays every
// number out afresh, so the numbers a pair is placed by do change. What may not
// change is which of the two comes first.
TEST(PropertyNameOrder, KeepsAPairOfStoredMapsInTheOrderItPlacedThem) {
  using memgraph::storage::PropertyId;
  using memgraph::storage::PropertyValue;

  PropertyNameOrder name_order;
  auto const settled = std::vector<std::string>{"aaa", "zzz"};
  auto const settled_ids = InternAll(name_order, settled);
  memgraph::storage::PointThisThreadAt(name_order);

  auto map_of = [](uint32_t key) {
    return PropertyValue{PropertyValue::map_t{{PropertyId::FromUint(key), PropertyValue{int64_t{1}}}}};
  };
  auto const first = map_of(settled_ids[0]);
  auto const second = map_of(settled_ids[1]);

  ASSERT_TRUE(std::is_lt(first <=> second)) << "the pair was not placed before the names started arriving";

  std::atomic<bool> stop{false};
  std::atomic<int> flipped{0};
  std::atomic<int> compared{0};

  constexpr auto kReaders = 4;
  std::barrier everyone_running{kReaders + 1};

  auto readers = std::vector<std::jthread>{};
  for (auto reader = 0; reader != kReaders; ++reader) {
    readers.emplace_back([&] {
      memgraph::storage::PointThisThreadAt(name_order);
      everyone_running.arrive_and_wait();
      while (!stop.load(std::memory_order_relaxed)) {
        if (!std::is_lt(first <=> second) || !std::is_gt(second <=> first)) {
          flipped.fetch_add(1, std::memory_order_relaxed);
        }
        compared.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  everyone_running.arrive_and_wait();

  // Names that keep landing between the two, which is what uses the gap up and
  // forces the numbers to be laid out afresh while the readers are comparing.
  auto probe = std::string{"b"};
  for (auto step = 0; step != 3000; ++step) {
    probe += 'q';
    name_order.Add(static_cast<uint32_t>(settled.size() + step), probe);
  }
  auto const layouts = name_order.LayoutsPublished();

  stop.store(true, std::memory_order_relaxed);
  readers.clear();

  EXPECT_EQ(flipped.load(), 0) << "a pair of stored maps changed places while names were being interned";
  EXPECT_GT(layouts, 1U) << "no fresh layout was published, so the readers only ever saw one";
  EXPECT_GT(compared.load(), 0) << "no comparison ran alongside the writer";
}

// What the layouts hold on to, measured rather than left unwatched.
//
// Every layout ever published is kept, because a reader holds one without
// saying so and nothing tells it to let go. A layout is made when the numbers
// are laid out afresh or outgrow their room, so what is retained follows how
// often names arrive between two already there, not how many rows are read.
TEST(PropertyNameOrder, HoldsOnToLittleForNamesThatDoNotCrowdOneGap) {
  PropertyNameOrder name_order;
  auto names = std::vector<std::string>{};
  for (auto i = 0; i != 1000; ++i) names.push_back("property_" + std::to_string(i));
  std::ranges::shuffle(names, std::mt19937{20260925});
  InternAll(name_order, names);

  // Only the layouts that came of outgrowing the room, which doubles.
  EXPECT_LE(name_order.RetainedBytes(), 12U * 1024U) << "a thousand names retained " << name_order.RetainedBytes()
                                                     << " bytes across " << name_order.LayoutsPublished() << " layouts";
}

TEST(PropertyNameOrder, HoldsOnToMoreWhenEveryNameCrowdsOneGap) {
  // The shape that costs: each name lands between the two before it, so a gap
  // is used up every so often and every number is laid out afresh. This is the
  // worst case a schema can make, and it is recorded so that a change making it
  // worse is visible.
  PropertyNameOrder name_order;
  auto names = std::vector<std::string>{"a", "z"};
  auto probe = std::string{"a"};
  for (auto step = 0; step != 2000; ++step) {
    probe += 'm';
    names.push_back(probe);
  }
  InternAll(name_order, names);

  EXPECT_LE(name_order.RetainedBytes(), 64U * 1024U)
      << "two thousand names crowding one gap retained " << name_order.RetainedBytes() << " bytes across "
      << name_order.LayoutsPublished() << " layouts";
}

TEST(PropertyNameOrder, LetsGoOfEveryLayoutItWasHoldingWhenScrubbed) {
  PropertyNameOrder name_order;
  auto names = std::vector<std::string>{"a", "z"};
  auto probe = std::string{"a"};
  for (auto step = 0; step != 500; ++step) {
    probe += 'm';
    names.push_back(probe);
  }
  InternAll(name_order, names);
  ASSERT_GT(name_order.LayoutsPublished(), 1U);

  name_order.Clear();

  EXPECT_EQ(name_order.LayoutsPublished(), 1U) << "a scrub left layouts behind";
  EXPECT_LE(name_order.RetainedBytes(), 1U * 1024U) << "a scrub left " << name_order.RetainedBytes() << " bytes behind";
}

// A thread reads the name_order it was pointed at, and no other.
//
// Which name_order a comparison reads is carried by the thread, because the
// comparison is the ordering of an index's own structure and has no argument to
// take one through. That puts a requirement on every thread that takes up
// storage work: it must be pointed at the storage it is working in. This says
// what happens to one that is not, so the answer is a refusal rather than
// another database's numbers.
TEST(PropertyNameOrder, PointsOneThreadWithoutPointingAnother) {
  PropertyNameOrder name_order;
  memgraph::storage::PointThisThreadAt(name_order);
  ASSERT_EQ(memgraph::storage::t_name_order, &name_order);

  auto pointed_elsewhere = true;
  {
    std::jthread fresh{[&] { pointed_elsewhere = memgraph::storage::t_name_order != nullptr; }};
  }

  EXPECT_FALSE(pointed_elsewhere)
      << "a thread that was never pointed at a name_order came away holding one, so a thread taking up storage work "
         "without being pointed would read another database's numbers rather than being refused";
}

// A thread taking up another's storage work reads that thread's order.
//
// Populating an index spreads the work over several threads, and recovery does
// it with no accessor anywhere in sight. A worker that read nothing would refuse
// the pair, and one that read whatever it last served would place a column of
// maps by another database's names. Either way the index would be built in an
// order no sort agrees with, and nothing later would notice.
TEST(PropertyNameOrder, IsInheritedByAThreadDoingAnotherThreadsWork) {
  PropertyNameOrder name_order;
  memgraph::storage::PointThisThreadAt(name_order);

  // What a pooled worker is: a thread that served something else first.
  PropertyNameOrder served_before;

  auto seen = std::vector<PropertyNameOrder const *>(4, nullptr);
  {
    auto const *inherited = memgraph::storage::t_name_order;
    auto workers = std::vector<std::jthread>{};
    for (auto worker = 0U; worker != seen.size(); ++worker) {
      workers.emplace_back([&, worker, inherited] {
        memgraph::storage::PointThisThreadAt(served_before);
        if (inherited != nullptr) memgraph::storage::PointThisThreadAt(*inherited);
        seen[worker] = memgraph::storage::t_name_order;
      });
    }
  }

  for (auto const *read : seen) {
    EXPECT_EQ(read, &name_order) << "a worker read an order other than the one the thread spawning it was pointed at";
  }
}
